package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.lockers.server.tools.traceWork
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.GlobalOpenTelemetry
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.LockerEnvelope
import com.latenighthack.lockers.server.LockerWireValidation
import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktbuf.net.ServerDescriptor
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.LockersConfig
import com.latenighthack.lockers.server.CpuAdmission
import com.latenighthack.lockers.server.ProtocolValidation
import com.latenighthack.lockers.server.ReadAdmission
import com.latenighthack.lockers.server.invalidArgument
import com.latenighthack.lockers.server.ServerCore
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.server.storage.v1.ServerLock
import com.latenighthack.lockers.server.storage.v1.ServerLocker
import com.latenighthack.lockers.server.storage.v1.ServerLockerId
import com.latenighthack.lockers.server.storage.v1.ServerRoomId
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import com.latenighthack.lockers.server.tools.*
import com.latenighthack.lockers.session.v1.PostEventRequest
import io.github.reactivecircus.cache4k.Cache
import io.github.reactivecircus.cache4k.CacheEvent
import io.micrometer.core.instrument.MeterRegistry
import com.latenighthack.lockers.observability.*
import me.tatarka.inject.annotations.Component
import me.tatarka.inject.annotations.Inject
import me.tatarka.inject.annotations.Provides
import org.slf4j.LoggerFactory
import kotlin.random.Random
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import com.latenighthack.lockers.server.agents.IdempotentLockerAgentRegistry
import com.latenighthack.lockers.server.storage.v2.*

/** One-hop stamp honored only for authenticated peers, or explicit legacy direct routing. */
private const val FORWARDED_PARAM = "fwd"
private const val FORWARD_TIMEOUT_MS = 5_000L

@ServiceScope
@Component
abstract class RoomServiceModule(
    @Component val serverCore: ServerCore,
    @get:Provides val sessionGatewayDiscovery: SessionGatewayDiscovery,
    @get:Provides val roomOwnership: RoomOwnership,
): GrpcRouteProvider<RoomServer> {
    abstract val serverImpl: RoomServiceImpl

    override val server: RoomServer get() = com.latenighthack.lockers.server.services.session.v1.AuthorizedRoomServer(serverImpl, serverCore.sessionProofVerifier)
    override val descriptor: ServerDescriptor = RoomServer.Descriptor
}


@ServiceScope
@Inject
// Composition keeps optional instrumentation alongside existing injected dependencies.
@Suppress("LongParameterList")
class RoomServiceImpl(
    private val subscriptionStore: SubscriptionStore,
    private val lockerStore: LockerStore,
    private val lockStore: LockStore,
    private val sessionGatewayDiscovery: SessionGatewayDiscovery,
    private val roomOwnership: RoomOwnership,
    private val agentRegistry: LockerAgentRegistry,
    private val meterRegistry: MeterRegistry,
    private val config: LockersConfig,
    deliveryOutbox: DeliveryOutboxStore? = null,
    private val telemetry: LockersTelemetry = LockersTelemetry.NONE,
    private val coroutineContext: kotlin.coroutines.CoroutineContext = kotlin.coroutines.EmptyCoroutineContext,
    private val agentTimeoutMs: Long = 30_000,
    private val agentMaxAttempts: Int = 8,
    private val cpuAdmission: CpuAdmission = CpuAdmission(config.resourceLimits),

    private val workTelemetry: OpenTelemetry = GlobalOpenTelemetry.get(),
) : BaseServiceImpl(), RoomServer {
    init { require(agentTimeoutMs in 1..300_000 && agentMaxAttempts in 1..32) }
    private class AgentOutputRejected(message: String) : IllegalArgumentException(message)
    private fun requireAgentOutput(valid: Boolean, message: String) { if (!valid) throw AgentOutputRejected(message) }
    private val deliveryOutbox = deliveryOutbox ?: lockStore.deliveryOutbox()
    private val lifecycleStarted = java.util.concurrent.atomic.AtomicBoolean(false)
    private val lifecycleClosed = java.util.concurrent.atomic.AtomicBoolean(false)
    private val logger = LoggerFactory.getLogger(RoomServiceImpl::class.java)
    private suspend fun processAgent(effectKey: ByteArray, room: RoomId, id: LockerId, locker: Locker): List<LockerAgentRegistry.LockerWrite> {
        val start = System.nanoTime(); var outcome = "error"
        try { return telemetry.observe(TelemetryOperation.AGENT_EXECUTE) {
            val outputs = if (agentRegistry is IdempotentLockerAgentRegistry) agentRegistry.processPayload(effectKey.copyOf(), room, id, locker) else agentRegistry.processPayload(room, id, locker)
            requireAgentOutput(outputs.size <= 64, "Agent output limit exceeded")
            var bytes = 0L
            // Freeze inside the observed call, before telemetry teardown or storage can suspend.
            outputs.map { output ->
                requireAgentOutput(ProtocolValidation.locker(output.lockerId), "Invalid derived locker")
                val encoded = output.locker.toByteArray()
                bytes += encoded.size
                requireAgentOutput(bytes <= minOf(8L * 1024 * 1024, config.maxLockerPayloadBytes.toLong()), "Agent output limit exceeded")
                LockerAgentRegistry.LockerWrite(LockerId.fromByteArray(output.lockerId.toByteArray()), Locker.fromByteArray(encoded))
            }
        }.also {
            requireAgentOutput(it.all { output -> LockerEnvelope.isSupported(output.locker) }, "Unsupported derived locker envelope")
            outcome = "ok"; meterRegistry.safeMeters { counter("lockers.agent.derived.writes").increment(it.size.toDouble()) }
        } } catch (cancelled: kotlinx.coroutines.CancellationException) { outcome = "cancelled"; throw cancelled }
        finally {
            meterRegistry.safeMeters {
                counter("lockers.agent.invocations", "outcome", outcome).increment()
                timer("lockers.agent.duration", "outcome", outcome).record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS)
            }
        }
    }

    private val deliveryWorker = if (config.deliveryWorkerEnabled)
        DeliveryWorker(requireNotNull(this.deliveryOutbox), sessionGatewayDiscovery, meterRegistry, telemetry,
            coroutineContext = coroutineContext, workTelemetry = workTelemetry) else null
    private val agentVersion = (agentRegistry as? IdempotentLockerAgentRegistry)?.agentVersion.orEmpty().also {
        require(agentRegistry !is IdempotentLockerAgentRegistry || (it.isNotBlank() && it.encodeToByteArray().size <= 128)) { "Durable agent version must be nonempty and bounded" }
    }
    private val agentWorkflow = AgentWorkflow(requireNotNull(this.deliveryOutbox), roomOwnership, agentVersion, ::executeAgentWork, coroutineContext)
    private val lockVerifier = LockVerifier(lockStore)
    private val dispatchers = ShardedDispatcher<RoomId>(config.shardCount, "room-shard", meterRegistry) {
        it.rawValue.contentHashCode()
    }
    private val snapshots by lazy { lockerStore.snapshotStore(config.resourceLimits) }
    private val readAdmission = ReadAdmission(config.resourceLimits)
    private suspend fun validateRead(room: RoomId) = readAdmission.require(room)
    private val rateLimiter = RoomRateLimiter(config.roomWritesPerSecond, config.roomWriteBurst)

    // Authority is always read from storage; a negative replica cache is not a fence.
    private suspend fun effectiveLockOrNull(roomId: RoomId, lockerId: LockerId): ServerLock? =
        lockVerifier.resolveEffective(roomId, lockerId.keyspace?.value ?: 0L, lockerId.rawValue)

    private suspend fun lockStateFor(roomId: RoomId, lockerId: LockerId): LockState? =
        effectiveLockOrNull(roomId, lockerId)?.let { lockVerifier.stateOf(it) }

    private fun <T> boundedResponse(response: T, write: (T, com.latenighthack.ktbuf.ProtobufWriter) -> Unit): T {
        try {
            // Validate while encoding, before allocating an oversized packet or its final copy.
            com.latenighthack.ktbuf.ProtobufOutputStream(limits = com.latenighthack.ktbuf.ProtobufOutputLimits(
                maxMessageBytes = ProtocolValidation.MAX_ENVELOPE_BYTES)).write { write(response, it) }
        } catch (_: com.latenighthack.ktbuf.ProtobufOutputLimitException) {
            protocolCapacityExceeded("Complete response exceeds transport envelope")
        } catch (_: com.latenighthack.ktbuf.bytes.ByteArrayLimitException) {
            protocolCapacityExceeded("Complete response exceeds transport envelope")
        }
        return response
    }

    private data class StoredRead(val value: IdentifiedLocker, val valid: Boolean)
    private suspend fun storedRead(room: RoomId, stored: ServerLocker): StoredRead {
        val id = LockerId(requireNotNull(stored.lockerId).rawValue, LockerKeyspace(stored.keyspace))
        val body = if (stored.deleted || !LockerWireValidation.valid(stored.locker)) null else try { Locker.fromByteArray(stored.locker) }
            catch (cancelled: kotlinx.coroutines.CancellationException) { throw cancelled }
            catch (_: Exception) { null }
        val valid = stored.deleted || (body != null && LockerEnvelope.isSupported(body))
        return StoredRead(IdentifiedLocker(id, if (valid) body else null, stored.version,
            lockStateFor(room, id)), valid)
    }

    /**
     * Locker writes are gated to the node that owns the room's `(keyspace, roomId)` shard. On a
     * non-owner node this returns a [ShardRedirect] to the owner so the caller answers NOT_OWNER;
     * on the owner (and in a monolith) it returns null and the write proceeds. Reads and the
     * per-room subscription registry stay ungated — they run against the shared store.
     */
    private suspend fun redirectIfNotOwner(keyspace: Long, roomId: RoomId): ShardRedirect? = try {
        when (val owner = roomOwnership.resolve(keyspace, roomId)) {
            is RoomOwner.Local -> null
            is RoomOwner.Remote -> ShardRedirect {
                ownerAddress = owner.address
                epoch = owner.epoch
            }
        }
    } catch (_: com.latenighthack.lockers.server.claim.RoomClaimCapacityExceeded) {
        namespaceExhausted("Permanent room claim namespace exhausted")
    }

    // Public clients retry through their public seed. Private owner addresses are only usable
    // by credentialed peers, so a failed forward or handoff must not seed a public route cache.
    // Authenticated forwarded calls retain NOT_OWNER and stop after one hop.
    private fun trustedPeer(context: GrpcRequestContext): Boolean = config.peerToken?.let { token ->
        com.latenighthack.lockers.server.validPeerToken(token,
            context.headers.entries.firstOrNull { it.key.equals(com.latenighthack.lockers.server.PEER_TOKEN_HEADER, true) }?.value.orEmpty())
    } ?: false

    private fun requireRedirectAccess(context: GrpcRequestContext) {
        if (config.peerToken != null && !trustedPeer(context)) throw com.latenighthack.ktbuf.net.RpcResponseException(
            context.originalUrl, "POST", com.latenighthack.ktbuf.proto.Codes.UNAVAILABLE, "Room owner temporarily unavailable")
    }

    private fun isNotOwner(response: Any?): Boolean = when (response) {
        is PostLockerChangesResponse -> response.result == PostLockerChangesResponse.Result.NOT_OWNER
        is DeleteLockerResponse -> response.result == DeleteLockerResponse.Result.NOT_OWNER
        is LockLockerResponse -> response.result == LockLockerResponse.Result.NOT_OWNER
        is UnlockLockerResponse -> response.result == UnlockLockerResponse.Result.NOT_OWNER
        else -> false
    }

    private val forwardConnections = com.latenighthack.lockers.server.cluster.PeerConnectionPool(peerToken = config.peerToken)

    private val forwardedWritesCounter = meterRegistry.counter("lockers.room.forward.writes")
    private val forwardFailureCounter = meterRegistry.counter("lockers.room.forward.failures")

    private suspend fun <R> forwardToOwnerOrNull(
        context: GrpcRequestContext,
        room: RoomId,
        redirect: ShardRedirect,
        call: suspend (RoomService) -> R,
    ): R? {
        if (context.query.containsKey(FORWARDED_PARAM) && (config.peerToken == null || trustedPeer(context))) return null
        // Advertise addresses are schemeless host:port — exactly what JVM HttpRpcClient wants.
        val address = redirect.ownerAddress.takeIf { it.isNotBlank() } ?: run {
            roomOwnership.invalidate(room)
            requireRedirectAccess(context)
            return null
        }
        val response = try {
            val stub = RoomServiceRpc(forwardConnections.clientFor(address)) { _, _ -> mapOf(FORWARDED_PARAM to "1") }
            kotlinx.coroutines.withTimeout(FORWARD_TIMEOUT_MS) { call(stub) }
                .also { forwardedWritesCounter.increment() }
        } catch (expectedForwardTimeout: kotlinx.coroutines.TimeoutCancellationException) {
            // A caller deadline is cancellation, not a private-owner routing failure.
            kotlinx.coroutines.currentCoroutineContext().ensureActive()
            forwardFailureCounter.increment()
            logger.warn("write forward to {} timed out", address)
            forwardConnections.evict(address)

            null
        } catch (e: kotlinx.coroutines.CancellationException) {
            throw e
        } catch (expectedOperationFailure: com.latenighthack.ktbuf.net.RpcResponseException) {
            forwardFailureCounter.increment()
            if (config.peerToken != null && (expectedOperationFailure.code == com.latenighthack.ktbuf.proto.Codes.UNAUTHENTICATED ||
                    expectedOperationFailure.code == com.latenighthack.ktbuf.proto.Codes.PERMISSION_DENIED)) {
                // These credentials belong to the private transport, not the public operation.
                logger.warn("write forward to {} rejected the peer credential", address)
                forwardConnections.evict(address)
                null
            } else {
                // Preserve Codes.retriable(): permanent operation errors must remain permanent.
                if (expectedOperationFailure.retriable()) {
                    roomOwnership.invalidate(room)
                    forwardConnections.evict(address)
                }
                // Rebuild the exception so neither its path nor diagnostic contains a private URL.
                throw com.latenighthack.ktbuf.net.RpcResponseException(context.originalUrl, "POST", expectedOperationFailure.code,
                    "Room owner could not complete operation")
            }
        } catch (e: Exception) {
            forwardFailureCounter.increment()
            logger.warn("write forward to {} failed ({})", address, e.message)
            forwardConnections.evict(address)

            null
        }
        if (response == null || isNotOwner(response)) {
            roomOwnership.invalidate(room)
            forwardConnections.evict(address)
            requireRedirectAccess(context)
        }
        return response
    }

    private suspend fun <T> runRoomMutation(context: GrpcRequestContext, room: RoomId, onLost: suspend () -> T, block: suspend () -> T): T {
        return try {
            val fence = roomOwnership.mutationFence(0, room)
            kotlinx.coroutines.withContext(fence) { dispatchers.runOnDispatcher(room, block) }
        } catch (expectedOwnershipLoss: RoomOwnershipLost) { roomOwnership.invalidate(room); requireRedirectAccess(context); onLost() }
        catch (_: com.latenighthack.lockers.server.claim.RoomClaimCapacityExceeded) { namespaceExhausted("Permanent room claim namespace exhausted") }

    }

    private val gatewayLookupFailureCounter = meterRegistry.counter("lockers.room.gateway.lookup.failures")
    init { meterRegistry.counter("lockers.room.locker.rejected.oversize") }
    private val rateLimitedCounter = meterRegistry.counter("lockers.room.locker.rejected.ratelimited")
    init { meterRegistry.counter("lockers.room.events.post.success") }
    init { meterRegistry.counter("lockers.room.events.post.failure") }
    private val agentFailureCounter = meterRegistry.counter("lockers.room.agent.failures")
    init { meterRegistry.counter("lockers.room.cache.hits") }
    private val cacheMissCounter = meterRegistry.counter("lockers.room.cache.misses")

    // Reshard observability (§7). A cache miss on the room→session set is the lazy-rebuild path a
    // new shard owner takes after a fenced handoff, so it also increments reshard.rooms.rebuilt.
    // A version-CAS reject (UPDATE_LOCAL_VERSION) is the dual-coordination backstop firing, counted
    // as reshard.cas.conflicts — if two nodes ever briefly both coordinate a room, exactly one wins.
    private val reshardRoomsRebuiltCounter = meterRegistry.counter("lockers.reshard.rooms.rebuilt")
    private val reshardCasConflictsCounter = meterRegistry.counter("lockers.reshard.cas.conflicts")
    private val dispatcherWaitTimer = meterRegistry.timer("lockers.room.dispatcher.time")
    private val getLockerTimer = meterRegistry.timer("lockers.room.locker.get.time")
    init { meterRegistry.timer("lockers.room.locker.getall.time") }
    init { meterRegistry.summary("lockers.room.locker.count") }

    private val roomToSessionCache = Cache.Builder<RoomId, Set<SessionId>>()
        .eventListener { event ->
            when (event) {
                is CacheEvent.Created<*, *> -> {
                }
                is CacheEvent.Evicted<*, *> -> {
                }
                is CacheEvent.Expired<*, *> -> {
                }
                is CacheEvent.Removed<*, *> -> {
                }
                is CacheEvent.Updated<*, *> -> {
                }
            }
        }
        .maximumCacheSize(config.sessionCacheSize)
        .build()
    
    private val cacheSizeGauge = meterRegistry.gauge("lockers.room.cache.size", roomToSessionCache) { it.asMap().size.toDouble() }

    private suspend fun lookupSessions(roomId: RoomId): Set<SessionId> {
        // Another replica can change subscriptions. Recipient authority is always durable;
        // the local cache is an observational snapshot, never an authorization/fanout source.
        cacheMissCounter.increment()
        reshardRoomsRebuiltCounter.increment()
        val sessions = subscriptionStore.getAllSessions(ServerRoomId(roomId.rawValue))
            .map { SessionId(it.rawValue) }
            .toSet()
        roomToSessionCache.put(roomId, sessions)
        return sessions
    }

    override suspend fun getWriteOutcome(context: GrpcRequestContext, request: GetWriteOutcomeRequest): GetWriteOutcomeResponse {
        val room = request.roomId
        if (room == null || !ProtocolValidation.room(room) || request.writeRequestId.size !in 16..64)
            return GetWriteOutcomeResponse(result = GetWriteOutcomeResponse.Result.INVALID)
        validateRead(room)
        val receipt = deliveryOutbox?.receipt(room, request.writeRequestId)
            ?: return GetWriteOutcomeResponse(result = GetWriteOutcomeResponse.Result.NOT_FOUND)
        val response = PostLockerChangesResponse.fromByteArray(receipt.encodedOutcome)
        val work = deliveryOutbox.agentWork.find(room, request.writeRequestId)
        // Historical pending receipts have no recoverable agent input. Never promise replay.
        val state = work?.let { agentState(it.state) } ?: when {
            response.agentPending || response.agentIndeterminate -> WriteOutcome.AgentState.INDETERMINATE
            response.agentFailed -> WriteOutcome.AgentState.FAILED
            else -> WriteOutcome.AgentState.APPLIED
        }
        return GetWriteOutcomeResponse(outcome = WriteOutcome(roomId = room, writeRequestId = request.writeRequestId,
            sourceVersions = response.sourceVersions.ifEmpty { response.changes.map { WriteSourceVersion(version = it.version) } }, agentState = state))
    }

    override suspend fun capabilities(context: GrpcRequestContext, request: CapabilitiesRequest) = meterRegistry.trackRpc(TelemetryOperation.ROOM_CAPABILITIES, telemetry) { CapabilitiesResponse(
        subscriptionRevisions = subscriptionStore.supportsIntentRevisions(), authorityV2 = true, deleteReceipts = true, snapshotPaging = lockerStore.supportsSnapshotPaging(), writeOutcomes = true,
        subscribeAndSnapshot = config.deliveryOutboxEnabled, getLockers = true,
        postLockerChanges = config.deliveryOutboxEnabled, writeReceipts = config.deliveryOutboxEnabled,
        maxBatchItems = 64, maxBatchBytes = minOf(8 * 1024 * 1024, config.maxLockerPayloadBytes)
    ) }

    override suspend fun getLockScope(context: GrpcRequestContext, request: GetLockScopeRequest): GetLockScopeResponse {
        val room = request.roomId ?: return GetLockScopeResponse(GetLockScopeResponse.Result.INVALID)
        val scope = request.scope ?: return GetLockScopeResponse(GetLockScopeResponse.Result.INVALID)
        if (!ProtocolValidation.scope(scope)) return GetLockScopeResponse(GetLockScopeResponse.Result.INVALID)
        validateRead(room)
        return boundedResponse(GetLockScopeResponse(scopeState = lockVerifier.scopeState(room, scope), parentState = lockVerifier.parentState(room, scope)), GetLockScopeResponse::writeTo)
    }

    override suspend fun getLockers(context: GrpcRequestContext, request: GetLockersRequest): GetLockersResponse = meterRegistry.trackRpc(TelemetryOperation.ROOM_GET_MANY, telemetry) { observedGetLockers(context, request) }

    private suspend fun observedGetLockers(context: GrpcRequestContext, request: GetLockersRequest): GetLockersResponse {
        if (request.lockerIds.size > 64 || request.toByteArray().size > ProtocolValidation.MAX_ENVELOPE_BYTES ||
            request.lockerIds.any { !ProtocolValidation.locker(it) }) invalidArgument("Invalid locker lookup")
        val room = request.roomId ?: invalidArgument("Missing room identity")
        validateRead(room)
        val responses = mutableListOf<GetLockerResponse>()
        val encoding = com.latenighthack.ktbuf.ProtobufOutputStream(limits = com.latenighthack.ktbuf.ProtobufOutputLimits(
            maxMessageBytes = ProtocolValidation.MAX_ENVELOPE_BYTES))
        // Each store call may retain large payloads; never fetch the rest after the reply is full.
        for (chunk in request.lockerIds.chunked(4)) {
            val all = lockerStore.getLockers(ServerRoomId(room.rawValue), chunk.map { (it.keyspace?.value ?: 0L) to ServerLockerId(it.rawValue) })
                .associateBy { (it.keyspace to requireNotNull(it.lockerId).rawValue.toList()) }
            for (id in chunk) {
                val stored = all[(id.keyspace?.value ?: 0L) to id.rawValue.toList()]
                val response = stored?.let {
                    val read = storedRead(room, it)
                    GetLockerResponse(result = if (read.valid) GetLockerResponse.Result.OK else GetLockerResponse.Result.INVALID_DATA, locker = read.value)
                } ?: GetLockerResponse(result = GetLockerResponse.Result.OK,
                    locker = IdentifiedLocker(id, version = 0, lockState = lockStateFor(room, id)))
                try { encoding.write { writer -> writer.encode(1) { response.writeTo(this) } } }
                catch (_: com.latenighthack.ktbuf.ProtobufOutputLimitException) { protocolCapacityExceeded("Complete response exceeds transport envelope") }
                catch (_: com.latenighthack.ktbuf.bytes.ByteArrayLimitException) { protocolCapacityExceeded("Complete response exceeds transport envelope") }
                responses.add(response)
            }
        }
        return GetLockersResponse(responses)
    }

    override suspend fun subscribeAndSnapshot(context: GrpcRequestContext, request: SubscribeAndSnapshotRequest): SubscribeAndSnapshotResponse = meterRegistry.trackRpc(TelemetryOperation.ROOM_SNAPSHOT, telemetry, { rpcOutcome(it.result.toString()) }) { observedSubscribeAndSnapshot(context, request) }

    private suspend fun observedSubscribeAndSnapshot(context: GrpcRequestContext, request: SubscribeAndSnapshotRequest): SubscribeAndSnapshotResponse {
        if (request.keyspaces.size > 64 || request.toByteArray().size > ProtocolValidation.MAX_ENVELOPE_BYTES ||
            !ProtocolValidation.identity(request.sessionId?.rawValue)) invalidArgument("Invalid snapshot request")
        val room = request.roomId?.let { RoomId(it.rawValue.copyOf()) } ?: invalidArgument("Missing room identity")
        val session = SessionId(requireNotNull(request.sessionId).rawValue.copyOf())
        validateRead(room)
        snapshots.validate(request.pageSize, request.pageToken)
        val spaces = request.keyspaces.map { it.value }.toSet()
        if (request.intentRevision < 0) invalidArgument("Negative subscription intent revision")
        if (request.pageToken.isNotEmpty()) {
            val observed = subscriptionStore.observeIntent(ServerSessionId(session.rawValue), ServerRoomId(room.rawValue), request.intentRevision) {
                val page = snapshots.next(1, room, session, spaces, request.pageSize, request.pageToken, request.intentRevision)
                SubscribeAndSnapshotResponse(SubscriptionResponse.Result.OK, page.lockers, page.roomSequence, page.nextPageToken)
            }
            return if (observed.stale) SubscribeAndSnapshotResponse(result = SubscriptionResponse.Result.STALE_INTENT, currentRevision = observed.currentRevision)
                else requireNotNull(observed.value).copy(currentRevision = observed.currentRevision)
        }
        check(config.deliveryOutboxEnabled)
        val changed = subscriptionStore.withIntent(ServerSessionId(session.rawValue), ServerRoomId(room.rawValue), request.intentRevision, true) {
            requireNotNull(deliveryOutbox).atomic(room) {
                val reads = captureSnapshot(room, spaces)
                if (reads.any { !it.valid }) return@atomic SubscribeAndSnapshotResponse(result = SubscriptionResponse.Result.INVALID_DATA,
                    lockers = listOf(reads.first { !it.valid }.value))
                val page = snapshots.create(1, room, session, spaces, request.pageSize,
                    requireNotNull(deliveryOutbox).watermark(room), reads.map { it.value }, request.intentRevision)
                subscriptionStore.addSubscription(ServerSessionId(session.rawValue), ServerRoomId(room.rawValue))
                roomToSessionCache.invalidate(room)
                SubscribeAndSnapshotResponse(SubscriptionResponse.Result.OK, page.lockers, page.roomSequence, page.nextPageToken)
            }
        }
        return if (changed.stale) SubscribeAndSnapshotResponse(result = SubscriptionResponse.Result.STALE_INTENT, currentRevision = changed.currentRevision)
            else requireNotNull(changed.value).copy(currentRevision = changed.currentRevision)
    }

    private fun canonicalLockerId(id: LockerId) = LockerId(id.rawValue, LockerKeyspace(id.keyspace?.value ?: 0L))

    private class BatchRejected(val response: PostLockerChangesResponse) : RuntimeException()

    override suspend fun postLockerChanges(context: GrpcRequestContext, request: PostLockerChangesRequest): PostLockerChangesResponse = meterRegistry.trackRpc(TelemetryOperation.ROOM_BATCH_WRITE, telemetry, { rpcOutcome(it.result.toString()) }) { observedPostLockerChanges(context, request) }

    private suspend fun observedPostLockerChanges(context: GrpcRequestContext, request: PostLockerChangesRequest): PostLockerChangesResponse {
        val trace = WriteTrace(request.writeRequestId.take(8).joinToString("") { "%02x".format(it) }, meterRegistry, telemetry)
        trace.requestBytes = request.toByteArray().size
        return trace.run { postLockerChangesTraced(context, request, trace).also { trace.result = it.result.toString() } }
    }

    private suspend fun postLockerChangesTraced(context: GrpcRequestContext, request: PostLockerChangesRequest, trace: WriteTrace): PostLockerChangesResponse {
        val room = request.roomId ?: return PostLockerChangesResponse(result = PostLockerChangesResponse.Result.INVALID)
        val changes = request.changes
        if (changes.isEmpty() || changes.size > 64 ||
            request.writeRequestId.size !in 16..64 || changes.any { it.lockerId == null || it.locker == null || (it.roomId != null && it.roomId != room) } ||
            changes.map { it.lockerId?.let { id -> canonicalLockerId(id) } }.distinct().size != changes.size ||
            request.toByteArray().size > minOf(8 * 1024 * 1024, config.maxLockerPayloadBytes)) {
            return PostLockerChangesResponse(result = PostLockerChangesResponse.Result.INVALID)
        }
        if (!ProtocolValidation.identity(room.rawValue) || changes.any { change ->
                !ProtocolValidation.locker(change.lockerId) || !CpuAdmission.signatureShape(change.writeSignature) ||
                    !CpuAdmission.signatureShape(change.locker?.sealed?.payload?.enclosure?.signature) ||
                    change.ratchet?.let { !CpuAdmission.keyShape(it.newPublicKey?.rawValue) ||
                        !ProtocolValidation.sharedKeys(it.newSharedKeys) || !CpuAdmission.signatureShape(it.signature, required = true) } == true
            } || request.initialLock?.let { !CpuAdmission.keyShape(it.publicKey?.rawValue) ||
                !CpuAdmission.signatureShape(it.parentSignature) } == true)
            return PostLockerChangesResponse(result = PostLockerChangesResponse.Result.INVALID)
        cpuAdmission.require(trace.requestBytes, 1 + changes.size * 12 + if (request.initialLock == null) 0 else 8)
        if (!ProtocolValidation.room(room) || request.parentLockVersion < 0 ||
            request.initialLock?.let { !ProtocolValidation.grant(it) } == true ||
            changes.any { !ProtocolValidation.change(it, room) })
            return PostLockerChangesResponse(result = PostLockerChangesResponse.Result.INVALID)

        for (space in changes.map { it.lockerId?.keyspace?.value ?: 0L }.distinct()) {
            trace.phase("ownership") { redirectIfNotOwner(space, room) }?.let { redirect ->
                trace.phase("forward") { forwardToOwnerOrNull(context, room, redirect) { it.postLockerChanges(request) } }?.let { return it }
                return PostLockerChangesResponse(result = PostLockerChangesResponse.Result.NOT_OWNER, redirect = redirect)
            }
        }
        if (!rateLimiter.tryAcquire(room)) return PostLockerChangesResponse(result = PostLockerChangesResponse.Result.UNKNOWN_ERROR)
        val outbox = requireNotNull(deliveryOutbox)
        val digest = com.latenighthack.ktcrypto.SHA256.digest(request.toByteArray())
        val queuedAt = System.nanoTime()
        val committed = runRoomMutation(context, room, onLost = { PostLockerChangesResponse(result = PostLockerChangesResponse.Result.NOT_OWNER, redirect = redirectIfNotOwner(0, room)) }) {
            meterRegistry.timer("lockers.write.phase", "phase", "room_queue").record(System.nanoTime() - queuedAt, java.util.concurrent.TimeUnit.NANOSECONDS)
            val events = mutableListOf<Event>()
            var replay: PostLockerChangesResponse? = null
            var pending: PostLockerChangesResponse? = null
            val normalized = changes.map { it.copy(roomId = room) }
            val recipients = mutableListOf<SessionId>()
            try {
                trace.phase("source_validation_commit") { outbox.commit(room, recipients, events) {
                    recipients.addAll(subscriptionStore.getAllSessions(ServerRoomId(room.rawValue)).map { SessionId(it.rawValue) })
                    val old = outbox.receipt(room, request.writeRequestId)
                    if (old != null) {
                        replay = if (old.requestDigest.contentEquals(digest)) PostLockerChangesResponse.fromByteArray(old.encodedOutcome)
                            else PostLockerChangesResponse(result = PostLockerChangesResponse.Result.REQUEST_ID_REUSED)
                        return@commit
                    }
                    var lockState: LockState? = null
                    request.initialLock?.let { grant ->
                        when (val outcome = lockVerifier.applyLock(room, grant, request.parentLockVersion)) {
                            is LockVerifier.LockOutcome.Ok -> { lockState = outcome.state }
                            is LockVerifier.LockOutcome.Stale -> throw BatchRejected(PostLockerChangesResponse(result = PostLockerChangesResponse.Result.CONFLICT, lockState = outcome.state))
                            else -> throw BatchRejected(PostLockerChangesResponse(result = PostLockerChangesResponse.Result.NOT_AUTHORIZED))
                        }
                    }
                    val existing = lockerStore.getLockers(ServerRoomId(room.rawValue), normalized.map { (it.lockerId!!.keyspace?.value ?: 0L) to ServerLockerId(it.lockerId!!.rawValue) })
                        .associateBy { LockerId(it.lockerId!!.rawValue, LockerKeyspace(it.keyspace)) }
                    val roomLocks = lockStore.getAllLocksInRoom(ServerRoomId(room.rawValue)).filter { lockVerifier.stateOf(it).locked }
                    val sourceWrites = mutableListOf<ServerLocker>()
                    val results = normalized.map { change ->
                        val id = change.lockerId!!
                        val effective = roomLocks.filter { lock ->
                            lock.scopeKind == LockVerifier.SCOPE_ROOM ||
                                (lock.keyspace == (id.keyspace?.value ?: 0L) && (lock.scopeKind == LockVerifier.SCOPE_KEYSPACE || lock.lockerId?.rawValue.contentEquals(id.rawValue)))
                        }.minByOrNull { it.scopeKind }
                        performLockerChange(change, events, sourceWrites, existing[canonicalLockerId(id)], effective,
                            prefetchLocks = normalized.none { it.ratchet != null })
                    }
                    if (results.any { !it.result.isOk() }) throw BatchRejected(boundedResponse(PostLockerChangesResponse(
                        result = PostLockerChangesResponse.Result.CONFLICT, changes = results, lockState = lockState), PostLockerChangesResponse::writeTo))
                    lockerStore.updateLockers(sourceWrites)
                    pending = boundedResponse(PostLockerChangesResponse(result = PostLockerChangesResponse.Result.OK, changes = results,
                        lockState = lockState, agentPending = agentRegistry !== LockerAgentRegistry.None, writeRequestId = request.writeRequestId,
                        sourceVersions = normalized.zip(results) { change, result -> WriteSourceVersion(change.lockerId, result.version) }), PostLockerChangesResponse::writeTo)
                    val order = outbox.watermark(room)
                    check(order < Long.MAX_VALUE) { "Agent source order exhausted" }
                    outbox.agentWork.create(room, request.copy(changes = normalized), pending, agentVersion, order + 1,
                        applied = agentRegistry === LockerAgentRegistry.None)

                    outbox.saveReceipt(com.latenighthack.lockers.server.storage.v1.ServerWriteReceipt(
                        request.writeRequestId, ServerRoomId(room.rawValue), digest, pending.toByteArray()))
                }
                }
                trace.recipients = recipients.size
            } catch (e: BatchRejected) {
                return@runRoomMutation e.response
            }
            replay?.let { response ->
                val work = outbox.agentWork.find(room, request.writeRequestId)
                return@runRoomMutation if (response.result.isOk() && work != null) responseForWork(work) else response
            }
            pending!!
        }
        if (!committed.result.isOk() || !committed.agentPending || !lifecycleStarted.get()) return committed
        // Waiting observes owned work. A disconnected caller never owns its execution job.
        return withTimeoutOrNull(5_000) {
            writeOutcomeFlow(room, request.writeRequestId).first { it.agentState in setOf(WriteOutcome.AgentState.APPLIED, WriteOutcome.AgentState.FAILED, WriteOutcome.AgentState.INDETERMINATE) }
            outbox.agentWork.find(room, request.writeRequestId)?.let(::responseForWork) ?: committed
        } ?: committed
    }

    private fun agentState(state: Int): WriteOutcome.AgentState = when (state) {
        AgentWorkState.PENDING -> WriteOutcome.AgentState.PENDING
        AgentWorkState.RUNNING, AgentWorkState.READY -> WriteOutcome.AgentState.RUNNING
        AgentWorkState.APPLIED -> WriteOutcome.AgentState.APPLIED
        AgentWorkState.FAILED -> WriteOutcome.AgentState.FAILED
        else -> WriteOutcome.AgentState.INDETERMINATE
    }
    private fun responseForWork(work: ServerAgentWork): PostLockerChangesResponse = PostLockerChangesResponse.fromByteArray(work.encodedOutcome).copy(
        writeRequestId = work.writeRequestId.copyOf(), agentPending = work.state in setOf(AgentWorkState.PENDING, AgentWorkState.RUNNING, AgentWorkState.READY),
        agentFailed = work.state == AgentWorkState.FAILED, agentIndeterminate = work.state == AgentWorkState.INDETERMINATE)

    /** Cold metadata-only observation for trusted server extensions; collection owns polling. */
    fun writeOutcomeFlow(room: RoomId, writeRequestId: ByteArray): Flow<WriteOutcome> {
        val identity = writeRequestId.copyOf(); val roomCopy = room.copy(rawValue = room.rawValue.copyOf())
        return flow {
            require(identity.size in 16..64 && ProtocolValidation.room(roomCopy))
            var previous: ByteArray? = null
            while (currentCoroutineContext().isActive) {
                val work = deliveryOutbox!!.agentWork.find(roomCopy, identity)
                if (work != null) {
                    val response = responseForWork(work)
                    val outcome = WriteOutcome(roomCopy, identity.copyOf(), response.sourceVersions, agentState(work.state))
                    val bytes = outcome.toByteArray()
                    if (previous?.contentEquals(bytes) != true) { emit(outcome); previous = bytes }
                    if (AgentWorkState.terminal(work.state) || work.state == AgentWorkState.INDETERMINATE) return@flow
                }
                delay(50)
            }
        }
    }

    private suspend fun executeAgentWork(claim: ServerAgentWork, fence: RoomMutationFence) =
        traceWork("agent.execute", listOf(claim.traceparent), workTelemetry) { coroutineScope {
        val outbox = requireNotNull(deliveryOutbox)
        val processing = currentCoroutineContext()[Job]!!
        val heartbeat = launch {
            while (isActive) {
                delay(10_000)
                if (!outbox.agentWork.renew(claim, System.currentTimeMillis())) processing.cancel(CancellationException("Agent execution lease revoked"))
            }
        }
        try {
            var ready = claim
            if (claim.state != AgentWorkState.READY) {
                val request = PostLockerChangesRequest.fromByteArray(claim.encodedRequest)
                val writes = mutableListOf<ServerAgentDerivedWrite>()
                val completed = withTimeoutOrNull(agentTimeoutMs) {
                request.changes.forEachIndexed { index, change ->
                    val effect = SHA256.digest(claim.effectKey + java.nio.ByteBuffer.allocate(4).putInt(index).array())
                    for (write in processAgent(effect, RoomId(claim.roomId), requireNotNull(change.lockerId), requireNotNull(change.locker))) {
                        requireAgentOutput(ProtocolValidation.locker(write.lockerId) && LockerWireValidation.valid(write.locker.toByteArray()), "Invalid derived locker")
                        writes.add(ServerAgentDerivedWrite(write.lockerId.rawValue, write.lockerId.keyspace?.value ?: 0, write.locker.toByteArray()))
                        requireAgentOutput(writes.size <= 64 && writes.sumOf { it.encodedLocker.size.toLong() } <= minOf(8L * 1024 * 1024, config.maxLockerPayloadBytes.toLong()), "Agent output limit exceeded")
                    }
                }
                true
                }
                if (completed != true) throw java.util.concurrent.TimeoutException("Agent execution deadline exceeded")
                ready = outbox.agentWork.result(claim, writes) ?: return@coroutineScope
            }
            applyAgentResult(ready, fence)
        } catch (cancelled: CancellationException) {
            withContext(NonCancellable) { outbox.agentWork.interrupt(claim, "Execution cancelled") }
            throw cancelled
        } catch (expectedOwnershipLoss: RoomOwnershipLost) {
            roomOwnership.invalidate(RoomId(claim.roomId))
            outbox.agentWork.interrupt(claim, "Room authority changed")
        } catch (error: Exception) {
            val current = outbox.agentWork.find(RoomId(claim.roomId), claim.writeRequestId)
            if (current?.state == AgentWorkState.READY && error !is AgentOutputRejected) {
                outbox.agentWork.interrupt(claim, "Derived commit interrupted")
                return@coroutineScope
            }
            if (claim.agentVersion.isNotEmpty() && error !is AgentOutputRejected && claim.attempts < agentMaxAttempts) {
                outbox.agentWork.retry(claim, error.message ?: "Agent execution failed")
                return@coroutineScope
            }
            agentFailureCounter.increment(); logger.error("Agent execution failed after source commit", error)
            // An arbitrary extension can throw after applying a non-idempotent external effect.
            val state = if (claim.agentVersion.isEmpty() && current?.state != AgentWorkState.READY) AgentWorkState.INDETERMINATE else AgentWorkState.FAILED
            val response = responseForWork(claim).copy(agentPending = false, agentFailed = state == AgentWorkState.FAILED, agentIndeterminate = state == AgentWorkState.INDETERMINATE)
            outbox.agentWork.complete(claim, state, response) {
                val receipt = outbox.receipt(RoomId(claim.roomId), claim.writeRequestId)!!
                outbox.saveReceipt(receipt.copy(encodedOutcome = response.toByteArray()))
            }
        } finally { withContext(NonCancellable) { heartbeat.cancelAndJoin() } }
    }

    }

    private suspend fun applyAgentResult(ready: ServerAgentWork, fence: RoomMutationFence) {
        val room = RoomId(ready.roomId); val outbox = requireNotNull(deliveryOutbox)
        withContext(fence) {
            dispatchers.runOnDispatcher(room) {
                val events = mutableListOf<Event>(); val recipients = mutableListOf<SessionId>()
                outbox.commit(room, recipients, events) {
                    val complete = responseForWork(ready).copy(agentPending = false, agentFailed = false, agentIndeterminate = false)
                    outbox.agentWork.complete(ready, AgentWorkState.APPLIED, complete) {
                        recipients.addAll(subscriptionStore.getAllSessions(ServerRoomId(room.rawValue)).map { SessionId(it.rawValue) })
                        val writes = linkedMapOf<LockerId, ServerLocker>()
                        for (write in ready.writes) {
                            val id = LockerId(write.lockerId, LockerKeyspace(write.keyspace))
                            val old = writes[id] ?: lockerStore.getLocker(ServerRoomId(room.rawValue), write.keyspace, ServerLockerId(write.lockerId))
                            val version = old?.version ?: 0
                            requireAgentOutput(version in 0 until Long.MAX_VALUE, "Derived locker version exhausted")
                            writes[id] = ServerLocker(ServerRoomId(room.rawValue), write.keyspace, ServerLockerId(write.lockerId), write.encodedLocker, version + 1)
                            val event = Event(roomId = room, eventId = EventId(Random.nextBytes(32)), locker = IdentifiedLocker(id, Locker.fromByteArray(write.encodedLocker), version + 1))
                            requireAgentOutput(ProtocolValidation.event(event.copy(roomSequence = Long.MAX_VALUE)), "Derived delivery event exceeds protocol bounds")
                            events.add(event)
                        }
                        lockerStore.updateLockers(writes.values.toList())
                        val receipt = requireNotNull(outbox.receipt(room, ready.writeRequestId))
                        outbox.saveReceipt(receipt.copy(encodedOutcome = complete.toByteArray()))
                    }
                }
            }
        }
    }

    /** Trusted reconciliation hook. It never invokes the external effect again. */
    suspend fun reconcileWriteOutcome(roomInput: RoomId, writeRequestIdInput: ByteArray, expectedEffectKeyInput: ByteArray,
        writesInput: List<LockerAgentRegistry.LockerWrite> = emptyList(), failed: Boolean = false): Boolean {
        val room = RoomId.fromByteArray(roomInput.toByteArray())
        val writeRequestId = writeRequestIdInput.copyOf(); val expectedEffectKey = expectedEffectKeyInput.copyOf()
        val writes = writesInput.map { LockerAgentRegistry.LockerWrite(LockerId.fromByteArray(it.lockerId.toByteArray()), Locker.fromByteArray(it.locker.toByteArray())) }
        require(ProtocolValidation.room(room) && writeRequestId.size in 16..64 && expectedEffectKey.size == 32)
        require(writes.size <= 64 && writes.all { ProtocolValidation.locker(it.lockerId) && LockerEnvelope.isSupported(it.locker) && LockerWireValidation.valid(it.locker.toByteArray()) } &&
            writes.sumOf { it.locker.toByteArray().size.toLong() } <= minOf(8L * 1024 * 1024, config.maxLockerPayloadBytes.toLong()))
        val fence = roomOwnership.mutationFence(0, room)
        val outbox = requireNotNull(deliveryOutbox)
        val result = withContext(fence) { outbox.atomic(room) {
            outbox.agentWork.reconcile(room, writeRequestId, expectedEffectKey,
                writes.map { ServerAgentDerivedWrite(it.lockerId.rawValue, it.lockerId.keyspace?.value ?: 0, it.locker.toByteArray()) }, failed)?.also { row ->
                if (failed) {
                    val receipt = requireNotNull(outbox.receipt(room, writeRequestId))
                    outbox.saveReceipt(receipt.copy(encodedOutcome = responseForWork(row).toByteArray()))
                }
            }
        } } ?: return false
        if (!failed) {
            val claim = outbox.agentWork.claim(room, writeRequestId, result.agentVersion, System.currentTimeMillis()) ?: return true
            applyAgentResult(claim, fence)
        }
        return true
    }

    override suspend fun subscription(
        context: GrpcRequestContext,
        request: SubscriptionRequest
    ) = meterRegistry.trackResponse("lockers.room.locker.subscribe", SubscriptionResponse::result, telemetry) {
        val roomId = request.roomId?.let { RoomId(it.rawValue.copyOf()) } ?: return@trackResponse SubscriptionResponse(result = SubscriptionResponse.Result.UNKNOWN_ERROR)
        val sessionId = request.sessionId?.let { SessionId(it.rawValue.copyOf()) } ?: return@trackResponse SubscriptionResponse(result = SubscriptionResponse.Result.UNKNOWN_ERROR)

        if (!ProtocolValidation.room(roomId)) invalidArgument("Invalid subscription room identity")
        if (request.intentRevision < 0) invalidArgument("Negative subscription intent revision")
        val subscribed = when (request.kind) {
            is SubscriptionRequest.OneOfKind.subscribe -> true
            is SubscriptionRequest.OneOfKind.unsubscribe -> false
            null -> {
                meterRegistry.counter("lockers.room.subscriptions", "operation", "unknown", "result", "ERROR").increment()
                return@trackResponse SubscriptionResponse(result = SubscriptionResponse.Result.UNKNOWN_ERROR)
            }
        }
        val startTime = System.nanoTime()
        // Public proof verification already owns a DB transaction. The intent transaction
        // serializes membership with writes; a dispatcher mutex here would reverse write lock order.
        val changed = dispatchers.runWithoutKeyLock(roomId) {
            dispatcherWaitTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)
            subscriptionStore.withIntent(ServerSessionId(sessionId.rawValue), ServerRoomId(roomId.rawValue), request.intentRevision, subscribed) {
                val cachedSet = lookupSessions(roomId)
                val updatedSet = if (subscribed) {
                    subscriptionStore.addSubscription(ServerSessionId(sessionId.rawValue), ServerRoomId(roomId.rawValue)); cachedSet + sessionId
                } else {
                    subscriptionStore.removeSubscription(ServerSessionId(sessionId.rawValue), ServerRoomId(roomId.rawValue)); cachedSet - sessionId
                }
                roomToSessionCache.put(roomId, updatedSet)
                meterRegistry.counter("lockers.room.subscriptions", "operation", if (subscribed) "subscribe" else "unsubscribe", "result", "OK").increment()
                SubscriptionResponse(result = SubscriptionResponse.Result.OK)
            }
        }
        if (changed.stale) SubscriptionResponse(result = SubscriptionResponse.Result.STALE_INTENT, currentRevision = changed.currentRevision)
        else requireNotNull(changed.value).copy(currentRevision = changed.currentRevision)
    }

    override suspend fun getLocker(
        context: GrpcRequestContext,
        request: GetLockerRequest
    ) = meterRegistry.trackResponse("lockers.room.locker.get", GetLockerResponse::result, telemetry) {
        val startTime = System.nanoTime()
        val lockerId = request.lockerId
        val roomId = request.roomId

        if (lockerId == null || roomId == null) {
            return@trackResponse GetLockerResponse(result = GetLockerResponse.Result.UNKNOWN_ERROR)
        }

        if (!ProtocolValidation.locker(lockerId)) invalidArgument("Invalid locker identity")
        validateRead(roomId)
        val storedLocker = lockerStore.getLocker(ServerRoomId(roomId.rawValue), lockerId.keyspace?.value ?: 0L, ServerLockerId(lockerId.rawValue))
        
        if (storedLocker == null) {
            getLockerTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)
            return@trackResponse boundedResponse(GetLockerResponse(result = GetLockerResponse.Result.OK, locker = IdentifiedLocker(lockerId, version = 0, lockState = lockStateFor(roomId, lockerId))), GetLockerResponse::writeTo)
        }

        getLockerTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)
        val read = storedRead(roomId, storedLocker)
        boundedResponse(GetLockerResponse(result = if (read.valid) GetLockerResponse.Result.OK else GetLockerResponse.Result.INVALID_DATA,
            locker = read.value), GetLockerResponse::writeTo)
    }

    private suspend fun captureSnapshot(room: RoomId, spaces: Set<Long>): List<StoredRead> {
        val reads = mutableListOf<StoredRead>(); var bytes = 0L
        lockerStore.lockerPages(ServerRoomId(room.rawValue)).collect { page ->
            for (stored in page) if (spaces.isEmpty() || stored.keyspace in spaces) {
                val read = storedRead(room, stored)
                bytes += read.value.toByteArray().size + 16
                if (reads.size == config.resourceLimits.maxSnapshotLockers || bytes > config.resourceLimits.maxSnapshotBytes)
                    throw com.latenighthack.ktbuf.net.RpcResponseException("", "RPC", com.latenighthack.ktbuf.proto.Codes.OUT_OF_RANGE, "Snapshot capture capacity exhausted")
                reads.add(read)
            }
        }
        return reads
    }

    override suspend fun getAllLockers(
        context: GrpcRequestContext,
        request: GetAllLockersRequest
    ) = meterRegistry.trackResponse("lockers.room.locker.getall", GetAllLockersResponse::result, telemetry) {
        val roomId = request.roomId ?: invalidArgument("Missing room identity")
        validateRead(roomId)
        snapshots.validate(request.pageSize, request.pageToken)
        val spaces = request.keyspace?.let { setOf(it.value) } ?: emptySet()
        if (request.pageToken.isNotEmpty()) return@trackResponse snapshots.next(0, roomId, null, spaces, request.pageSize, request.pageToken)
        lockStore.atomic(ServerRoomId(roomId.rawValue)) {
            val reads = captureSnapshot(roomId, spaces)
            if (reads.any { !it.valid }) return@atomic GetAllLockersResponse(result = GetAllLockersResponse.Result.INVALID_DATA,
                lockers = listOf(reads.first { !it.valid }.value))
            snapshots.create(0, roomId, null, spaces, request.pageSize, deliveryOutbox?.watermark(roomId) ?: 0, reads.map { it.value })
        }
    }

    override suspend fun postLockerChange(
        context: GrpcRequestContext,
        request: PostLockerChangeRequest
    ) = meterRegistry.trackResponse("lockers.room.locker.postlockerchange", PostLockerChangeResponse::result, telemetry) {
            val room = request.roomId
            if (room == null || !ProtocolValidation.identity(room.rawValue) ||
                request.writeRequestId.isNotEmpty() && request.writeRequestId.size !in 16..64)
                return@trackResponse PostLockerChangeResponse(result = PostLockerChangeResponse.Result.NOT_AUTHORIZED)
            val batch = postLockerChanges(context, PostLockerChangesRequest(roomId = request.roomId,
                changes = listOf(request.copy(writeRequestId = byteArrayOf())), writeRequestId = request.writeRequestId.takeIf { it.isNotEmpty() } ?: Random.nextBytes(32)))
            return@trackResponse batch.changes.firstOrNull()?.copy(agentFailed = batch.agentFailed, agentPending = batch.agentPending, agentIndeterminate = batch.agentIndeterminate,
                writeRequestId = batch.writeRequestId, sourceVersions = batch.sourceVersions)
                ?: PostLockerChangeResponse(result = if (batch.result is PostLockerChangesResponse.Result.NOT_OWNER)
                    PostLockerChangeResponse.Result.NOT_OWNER else PostLockerChangeResponse.Result.UNKNOWN_ERROR, redirect = batch.redirect)
    }

    private suspend fun performLockerChange(request: PostLockerChangeRequest, pendingEvents: MutableList<Event>,
        pendingWrites: MutableList<ServerLocker>, prefetchedLocker: ServerLocker?,
        prefetchedLock: ServerLock? = null, prefetchLocks: Boolean = false): PostLockerChangeResponse {
        val requestRoomId = requireNotNull(request.roomId)
        val requestLockerId = requireNotNull(request.lockerId)
        val updatedLocker = requireNotNull(request.locker)
        if (!LockerEnvelope.isSupported(updatedLocker)) return PostLockerChangeResponse(result = PostLockerChangeResponse.Result.NOT_AUTHORIZED)
        val encodedLocker = updatedLocker.toByteArray()
        val requestEventId = EventId(Random.nextBytes(32))
        val requestVersion = request.parentVersion
            val storedLocker = prefetchedLocker

            val effectiveLock = if (prefetchLocks) prefetchedLock else effectiveLockOrNull(requestRoomId, requestLockerId)
            if (request.ratchet != null && effectiveLock == null) return PostLockerChangeResponse(result = PostLockerChangeResponse.Result.NOT_AUTHORIZED)
            var effectiveState = effectiveLock?.let { lockVerifier.stateOf(it) }

            val updatedLockerVersion = if (storedLocker == null && requestVersion == 0L) {
                1L
            } else if (storedLocker != null && storedLocker.version == requestVersion && requestVersion in 0 until Long.MAX_VALUE) {
                requestVersion + 1
            } else {
                reshardCasConflictsCounter.increment()
                return PostLockerChangeResponse {
                    result = PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION
                    version = storedLocker?.version ?: 0L
                    existingLocker = storedLocker?.takeUnless { it.deleted }?.let { stored ->
                        if (!LockerWireValidation.valid(stored.locker)) throw com.latenighthack.ktbuf.net.RpcResponseException("", "RPC", com.latenighthack.ktbuf.proto.Codes.DATA_LOSS, "Invalid locker data at version ${stored.version}")
                        Locker.fromByteArray(stored.locker).also { if (!LockerEnvelope.isSupported(it)) throw com.latenighthack.ktbuf.net.RpcResponseException("", "RPC", com.latenighthack.ktbuf.proto.Codes.DATA_LOSS, "Unsupported locker data at version ${stored.version}") }
                    }
                    lockState = effectiveState
                }
            }

            // Locked lockers: writes must carry a signature over the canonical write
            // context that verifies against the effective lock's current key. The
            // signature binds the parent version, so the client re-signs on retry.
            if (effectiveLock != null) {
                val enclosure = updatedLocker.sealed?.payload?.enclosure
                if (enclosure == null) {
                    return PostLockerChangeResponse {
                        result = PostLockerChangeResponse.Result.SIGNATURE_REQUIRED
                        lockState = effectiveState
                    }
                }
                val hash = lockVerifier.contentHash(enclosure.innerPayload)
                val checksum = updatedLocker.sealed?.payload?.checksum
                if (checksum != null && checksum.isNotEmpty() && !checksum.contentEquals(hash)) {
                    return PostLockerChangeResponse {
                        result = PostLockerChangeResponse.Result.SIGNATURE_INVALID
                        lockState = effectiveState
                    }
                }
                when (lockVerifier.verifyWrite(effectiveLock, requestRoomId, requestLockerId, requestVersion, hash, request.writeSignature, request.notification)) {
                    LockVerifier.WriteVerdict.REQUIRED -> return PostLockerChangeResponse {
                        result = PostLockerChangeResponse.Result.SIGNATURE_REQUIRED
                        lockState = effectiveState
                    }
                    LockVerifier.WriteVerdict.INVALID -> return PostLockerChangeResponse {
                        result = PostLockerChangeResponse.Result.SIGNATURE_INVALID
                        lockState = effectiveState
                    }
                    LockVerifier.WriteVerdict.OK -> {}
                }
                val ratchet = request.ratchet
                if (ratchet != null) {
                    when (val outcome = lockVerifier.applyRatchet(effectiveLock, requestRoomId, requestLockerId, requestVersion, ratchet)) {
                        is LockVerifier.RatchetOutcome.Invalid -> return PostLockerChangeResponse {
                            result = PostLockerChangeResponse.Result.SIGNATURE_INVALID
                            lockState = effectiveState
                        }
                        is LockVerifier.RatchetOutcome.Ok -> {
                            effectiveState = outcome.state
                        }
                    }
                }
            }

            val serverLocker = ServerLocker {
                lockerId = ServerLockerId(requestLockerId.rawValue)
                roomId = ServerRoomId(requestRoomId.rawValue)
                locker = encodedLocker
                keyspace = (requestLockerId.keyspace?.value ?: 0L)
                version = updatedLockerVersion
            }
            val sourceEvent = Event {
                roomId = requestRoomId
                eventId = requestEventId
                locker { locker = updatedLocker; lockerId = requestLockerId; version = updatedLockerVersion; lockState = effectiveState }
                notification = request.notification
            }
            pendingWrites.add(serverLocker)
            pendingEvents.add(sourceEvent)
            return PostLockerChangeResponse(result = PostLockerChangeResponse.Result.OK, version = updatedLockerVersion, lockState = effectiveState)
    }

    override suspend fun deleteLocker(
        context: GrpcRequestContext,
        request: DeleteLockerRequest
    ) = boundedResponse(meterRegistry.trackResponse("lockers.room.locker.deletelocker", DeleteLockerResponse::result, telemetry) {
        val room = request.roomId
        val id = request.lockerId
        val encodedBytes = request.toByteArray().size
        if (room == null || !ProtocolValidation.identity(room.rawValue) || !ProtocolValidation.locker(id) ||
            encodedBytes > ProtocolValidation.MAX_ENVELOPE_BYTES || !CpuAdmission.signatureShape(request.writeSignature))
            return@trackResponse DeleteLockerResponse(result = DeleteLockerResponse.Result.UNKNOWN_ERROR)
        cpuAdmission.require(encodedBytes, 4)
        if (!ProtocolValidation.room(room) || request.parentVersion < 0 || !ProtocolValidation.notification(request.notification) ||
            !ProtocolValidation.signature(request.writeSignature) ||
            (request.writeRequestId.isNotEmpty() && request.writeRequestId.size !in 16..64))
            return@trackResponse DeleteLockerResponse(result = DeleteLockerResponse.Result.UNKNOWN_ERROR)
        val lockerId = requireNotNull(id)
        redirectIfNotOwner(lockerId.keyspace?.value ?: 0L, room)?.let { redirect ->
            forwardToOwnerOrNull(context, room, redirect) { it.deleteLocker(request) }?.let { return@trackResponse it }
            return@trackResponse DeleteLockerResponse(result = DeleteLockerResponse.Result.NOT_OWNER, redirect = redirect)

        }
        if (!rateLimiter.tryAcquire(room)) {
            rateLimitedCounter.increment()
            return@trackResponse DeleteLockerResponse(result = DeleteLockerResponse.Result.UNKNOWN_ERROR)
        }
        val outbox = requireNotNull(deliveryOutbox)
        val digest = com.latenighthack.ktcrypto.SHA256.digest("delete:v1".encodeToByteArray() + request.toByteArray())
        return@trackResponse runRoomMutation(context, room, onLost = {
            DeleteLockerResponse(result = DeleteLockerResponse.Result.NOT_OWNER, redirect = redirectIfNotOwner(0, room))
        }) {
            val events = mutableListOf<Event>()

            val recipients = mutableListOf<SessionId>()
            outbox.commit(room, recipients, events) {
                if (request.writeRequestId.isNotEmpty()) {
                    outbox.receipt(room, request.writeRequestId)?.let { old ->
                        if (!old.requestDigest.contentEquals(digest))
                            return@commit DeleteLockerResponse(result = DeleteLockerResponse.Result.REQUEST_ID_REUSED, writeRequestId = request.writeRequestId)
                        val saved = PostLockerChangesResponse.fromByteArray(old.encodedOutcome).changes.single()
                        return@commit DeleteLockerResponse(result = DeleteLockerResponse.Result.OK, version = saved.version,
                            lockState = saved.lockState, writeRequestId = request.writeRequestId)
                    }
                }
                val stored = lockerStore.getLocker(ServerRoomId(room.rawValue), lockerId.keyspace?.value ?: 0L, ServerLockerId(lockerId.rawValue))
                val effective = effectiveLockOrNull(room, lockerId)
                val state = effective?.let { lockVerifier.stateOf(it) }
                if (effective != null) {
                    when (lockVerifier.verifyWrite(effective, room, lockerId, request.parentVersion, ByteArray(0), request.writeSignature, request.notification)) {
                        LockVerifier.WriteVerdict.REQUIRED -> return@commit DeleteLockerResponse(result = DeleteLockerResponse.Result.SIGNATURE_REQUIRED, lockState = state)
                        LockVerifier.WriteVerdict.INVALID -> return@commit DeleteLockerResponse(result = DeleteLockerResponse.Result.SIGNATURE_INVALID, lockState = state)
                        LockVerifier.WriteVerdict.OK -> {}
                    }
                }
                val alreadyDeleted = stored?.deleted == true && stored.version == request.parentVersion
                val version = when {
                    alreadyDeleted -> requireNotNull(stored).version
                    stored == null && request.parentVersion == 0L -> 1L
                    stored != null && stored.version == request.parentVersion && request.parentVersion < Long.MAX_VALUE -> request.parentVersion + 1
                    else -> {
                        reshardCasConflictsCounter.increment()
                        return@commit DeleteLockerResponse(result = DeleteLockerResponse.Result.UPDATE_LOCAL_VERSION, version = stored?.version ?: 0L,
                            existingLocker = stored?.takeUnless { it.deleted }?.let { existing ->
                                if (!LockerWireValidation.valid(existing.locker)) throw com.latenighthack.ktbuf.net.RpcResponseException("", "RPC", com.latenighthack.ktbuf.proto.Codes.DATA_LOSS, "Invalid locker data at version ${existing.version}")
                                Locker.fromByteArray(existing.locker).also { if (!LockerEnvelope.isSupported(it)) throw com.latenighthack.ktbuf.net.RpcResponseException("", "RPC", com.latenighthack.ktbuf.proto.Codes.DATA_LOSS, "Unsupported locker data at version ${existing.version}") }
                            }, lockState = state)
                    }
                }
                if (!alreadyDeleted) {
                    recipients.addAll(subscriptionStore.getAllSessions(ServerRoomId(room.rawValue)).map { SessionId(it.rawValue) })
                    lockerStore.updateLocker(ServerLocker(ServerRoomId(room.rawValue), lockerId.keyspace?.value ?: 0L,
                        ServerLockerId(lockerId.rawValue), byteArrayOf(), version, deleted = true))
                    events.add(Event(roomId = room, eventId = EventId(Random.nextBytes(32)),
                        locker = IdentifiedLocker(lockerId, version = version, lockState = state), notification = request.notification))
                }
                if (request.writeRequestId.isNotEmpty()) {
                    val saved = PostLockerChangesResponse(changes = listOf(PostLockerChangeResponse(version = version, lockState = state)),
                        sourceVersions = listOf(WriteSourceVersion(lockerId = lockerId, version = version)))
                    outbox.saveReceipt(com.latenighthack.lockers.server.storage.v1.ServerWriteReceipt(
                        request.writeRequestId, ServerRoomId(room.rawValue), digest, saved.toByteArray()))
                }
                DeleteLockerResponse(result = DeleteLockerResponse.Result.OK, version = version, lockState = state, writeRequestId = request.writeRequestId)
            }
        }
    }, DeleteLockerResponse::writeTo)


    override suspend fun lockLocker(
        context: GrpcRequestContext,
        request: LockLockerRequest
    ) = boundedResponse(meterRegistry.trackResponse("lockers.room.locker.lock", LockLockerResponse::result, telemetry) {
        val requestRoomId = request.roomId ?: return@trackResponse LockLockerResponse(result = LockLockerResponse.Result.UNKNOWN_ERROR)
        val grant = request.grant ?: return@trackResponse LockLockerResponse(result = LockLockerResponse.Result.UNKNOWN_ERROR)

        // A lock and the lockers it governs must be coordinated on the same shard, so gate by the
        // scope's keyspace; a room-wide scope carries no keyspace and pins to keyspace 0.
        val encodedBytes = request.toByteArray().size
        if (encodedBytes > ProtocolValidation.MAX_ENVELOPE_BYTES || !ProtocolValidation.identity(requestRoomId.rawValue) ||
            !CpuAdmission.signatureShape(request.grant?.parentSignature) || !CpuAdmission.keyShape(grant.publicKey?.rawValue) ||
            grant.toByteArray().size > 1024 || !ProtocolValidation.scope(grant.scope)) com.latenighthack.lockers.server.invalidArgument("Invalid write shape")
        cpuAdmission.require(encodedBytes, 5)
        if (!ProtocolValidation.room(requestRoomId) || !ProtocolValidation.grant(grant) || request.parentLockVersion < 0)
            return@trackResponse LockLockerResponse(result = LockLockerResponse.Result.NOT_AUTHORIZED)
        redirectIfNotOwner(grant.scope?.keyspace?.value ?: 0L, requestRoomId)?.let { redirect ->
            forwardToOwnerOrNull(context, requestRoomId, redirect) { it.lockLocker(request) }?.let {
                return@trackResponse it
            }
            return@trackResponse LockLockerResponse {
                result = LockLockerResponse.Result.NOT_OWNER
                this.redirect = redirect
            }
        }

        return@trackResponse runRoomMutation(context, requestRoomId, onLost = { LockLockerResponse(result = LockLockerResponse.Result.NOT_OWNER, redirect = redirectIfNotOwner(0, requestRoomId)) }) { lockStore.atomic(ServerRoomId(requestRoomId.rawValue)) {
            when (val outcome = lockVerifier.applyLock(requestRoomId, grant, request.parentLockVersion)) {
                is LockVerifier.LockOutcome.Ok -> {
                    LockLockerResponse {
                        result = LockLockerResponse.Result.OK
                        lockState = outcome.state
                    }
                }
                is LockVerifier.LockOutcome.Stale -> LockLockerResponse {
                    result = LockLockerResponse.Result.UPDATE_LOCAL_VERSION
                    lockState = outcome.state
                }
                is LockVerifier.LockOutcome.NotAuthorized -> LockLockerResponse(result = LockLockerResponse.Result.NOT_AUTHORIZED)
            }
        } }
    }, LockLockerResponse::writeTo)

    override suspend fun unlockLocker(
        context: GrpcRequestContext,
        request: UnlockLockerRequest
    ) = meterRegistry.trackResponse("lockers.room.locker.unlock", UnlockLockerResponse::result, telemetry) {
        val requestRoomId = request.roomId ?: return@trackResponse UnlockLockerResponse(result = UnlockLockerResponse.Result.UNKNOWN_ERROR)
        val scope = request.scope ?: return@trackResponse UnlockLockerResponse(result = UnlockLockerResponse.Result.UNKNOWN_ERROR)

        val encodedBytes = request.toByteArray().size
        if (encodedBytes > ProtocolValidation.MAX_ENVELOPE_BYTES || !ProtocolValidation.identity(requestRoomId.rawValue) ||
            !CpuAdmission.signatureShape(request.signature, required = true) || !ProtocolValidation.scope(scope)) com.latenighthack.lockers.server.invalidArgument("Invalid write shape")
        cpuAdmission.require(encodedBytes, 4)
        if (!ProtocolValidation.room(requestRoomId) || !ProtocolValidation.scope(scope) || request.parentLockVersion < 0 ||
            !ProtocolValidation.signature(request.signature, required = true))
            return@trackResponse UnlockLockerResponse(result = UnlockLockerResponse.Result.SIGNATURE_INVALID)
        redirectIfNotOwner(scope.keyspace?.value ?: 0L, requestRoomId)?.let { redirect ->
            forwardToOwnerOrNull(context, requestRoomId, redirect) { it.unlockLocker(request) }?.let {
                return@trackResponse it
            }
            return@trackResponse UnlockLockerResponse {
                result = UnlockLockerResponse.Result.NOT_OWNER
                this.redirect = redirect
            }
        }

        return@trackResponse runRoomMutation(context, requestRoomId, onLost = { UnlockLockerResponse(result = UnlockLockerResponse.Result.NOT_OWNER, redirect = redirectIfNotOwner(0, requestRoomId)) }) { lockStore.atomic(ServerRoomId(requestRoomId.rawValue)) {
            val outcome = lockVerifier.applyUnlock(requestRoomId, scope, request.signature, request.parentLockVersion)
            when (outcome) {
                is LockVerifier.UnlockOutcome.Ok -> {
                    UnlockLockerResponse(result = UnlockLockerResponse.Result.OK)
                }
                is LockVerifier.UnlockOutcome.Stale -> UnlockLockerResponse(result = UnlockLockerResponse.Result.UPDATE_LOCAL_VERSION)
                is LockVerifier.UnlockOutcome.SignatureInvalid -> UnlockLockerResponse(result = UnlockLockerResponse.Result.SIGNATURE_INVALID)
            }
        } }
    }

    /**
     * Quiesce hook for the owner lifecycle: on a fenced handoff of shards away from this node, drop
     * the per-room routing/lock caches so the new owner rebuilds them lazily from the shared store
     * on first access (the `lookupSessions` cache-miss path). Shard→room is one-way (partition is a
     * hash), so we clear the whole cache — safe and cheap: it only forces a rebuild-on-miss and
     * moves no durable data. Called only in a clustered deployment.
     */
    fun evictRoomCaches() {
        roomToSessionCache.invalidateAll()
    }

    fun start() {
        check(!lifecycleClosed.get() && lifecycleStarted.compareAndSet(false, true)) { "Service already started or closed" }
        deliveryWorker?.start()
        agentWorkflow.start()
    }
    suspend fun closeAndJoin() {
        ServiceLifecycle.requireExternalClose()
        lifecycleClosed.set(true)
        kotlinx.coroutines.withContext(kotlinx.coroutines.NonCancellable) {
            var failed: Throwable? = null
            suspend fun cleanup(block: suspend () -> Unit) {
                try { block() } catch (failure: Throwable) {
                    if (failed == null) failed = failure else failed.addSuppressed(failure)
                }
            }
            cleanup { agentWorkflow.closeAndJoin() }
            cleanup { forwardConnections.closeAndJoin() }
            cleanup { deliveryWorker?.closeAndJoin() }
            cleanup { dispatchers.closeAndJoin() }
            failed?.let { throw it }
        }
    }
    fun close() = ServiceLifecycle.blockingClose(coroutineContext) { closeAndJoin() }
}
