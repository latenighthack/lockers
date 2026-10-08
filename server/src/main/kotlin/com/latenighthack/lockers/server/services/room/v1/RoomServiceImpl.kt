package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.LockerEnvelope
import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktbuf.net.ServerDescriptor
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.LockersConfig
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
import kotlinx.coroutines.async

/** Query-param stamp on east-west forwarded writes; its presence means "do not forward again". */
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
class RoomServiceImpl(
    private val subscriptionStore: SubscriptionStore,
    private val lockerStore: LockerStore,
    private val lockStore: LockStore,
    private val sessionGatewayDiscovery: SessionGatewayDiscovery,
    private val roomOwnership: RoomOwnership,
    private val agentRegistry: LockerAgentRegistry,
    private val meterRegistry: MeterRegistry,
    private val config: LockersConfig,
    private val deliveryOutbox: DeliveryOutboxStore? = null,
    private val telemetry: LockersTelemetry = LockersTelemetry.NONE,
) : BaseServiceImpl(), RoomServer {
    private val logger = LoggerFactory.getLogger(RoomServiceImpl::class.java)
    private suspend fun processAgent(room: RoomId, id: LockerId, locker: Locker): List<LockerAgentRegistry.LockerWrite> {
        val start = System.nanoTime(); var outcome = "error"
        try { return telemetry.observe(TelemetryOperation.AGENT_EXECUTE) { agentRegistry.processPayload(room, id, locker) }.also {
            outcome = "ok"; meterRegistry.safeMeters { counter("lockers.agent.derived.writes").increment(it.size.toDouble()) }
        } } catch (cancelled: kotlinx.coroutines.CancellationException) { outcome = "cancelled"; throw cancelled }
        finally {
            meterRegistry.safeMeters {
                counter("lockers.agent.invocations", "outcome", outcome).increment()
                timer("lockers.agent.duration", "outcome", outcome).record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS)
            }
        }
    }

    private val deliveryWorker = if (config.deliveryOutboxEnabled && config.deliveryWorkerEnabled)
        DeliveryWorker(requireNotNull(deliveryOutbox), sessionGatewayDiscovery, meterRegistry, telemetry).also { it.start() } else null
    private val lockVerifier = LockVerifier(lockStore)
    private val dispatchers = ShardedDispatcher<RoomId>(config.shardCount, "room-shard") {
        it.rawValue.contentHashCode()
    }
    private val rateLimiter = RoomRateLimiter(config.roomWritesPerSecond, config.roomWriteBurst)

    // Authority is always read from storage; a negative replica cache is not a fence.
    private suspend fun effectiveLockOrNull(roomId: RoomId, lockerId: LockerId): ServerLock? =
        lockVerifier.resolveEffective(roomId, lockerId.keyspace?.value ?: 0L, lockerId.rawValue)

    private suspend fun lockStateFor(roomId: RoomId, lockerId: LockerId): LockState? =
        effectiveLockOrNull(roomId, lockerId)?.let { lockVerifier.stateOf(it) }

    /**
     * Locker writes are gated to the node that owns the room's `(keyspace, roomId)` shard. On a
     * non-owner node this returns a [ShardRedirect] to the owner so the caller answers NOT_OWNER;
     * on the owner (and in a monolith) it returns null and the write proceeds. Reads and the
     * per-room subscription registry stay ungated — they run against the shared store.
     */
    private suspend fun redirectIfNotOwner(keyspace: Long, roomId: RoomId): ShardRedirect? =
        when (val owner = roomOwnership.resolve(keyspace, roomId)) {
            is RoomOwner.Local -> null
            is RoomOwner.Remote -> ShardRedirect {
                ownerAddress = owner.address
                epoch = owner.epoch
            }
        }

    // East-west write forwarding: public clients sit behind one domain (plain HttpRpcClient) and
    // cannot dial a redirect's cluster-internal owner address, so a NOT_OWNER answer strands any
    // client whose proxy routing disagrees with claim placement (e.g. a room claimed by a
    // server-side first write, or claimed before a routing-policy change). Instead the non-owner
    // proxies the write to the owner over the same east-west HTTP path the gateways use and
    // relays the owner's response verbatim. Forwarded calls are stamped `?fwd=1`; a node that is
    // still not the owner for a forwarded call answers NOT_OWNER as before — one hop max, no
    // ping-pong, and the redirect stays intact for smart routing clients (RoutingRpcClient).
    private val forwardStubs = java.util.concurrent.ConcurrentHashMap<String, RoomServiceRpc>()
    private val forwardedWritesCounter = meterRegistry.counter("lockers.room.forward.writes")
    private val forwardFailureCounter = meterRegistry.counter("lockers.room.forward.failures")

    private suspend fun <R> forwardToOwnerOrNull(
        context: GrpcRequestContext,
        redirect: ShardRedirect,
        call: suspend (RoomService) -> R,
    ): R? {
        if (context.query.containsKey(FORWARDED_PARAM)) return null
        // Advertise addresses are schemeless host:port — exactly what JVM HttpRpcClient wants.
        val address = redirect.ownerAddress?.takeIf { it.isNotBlank() } ?: return null
        val stub = forwardStubs.computeIfAbsent(address) {
            RoomServiceRpc(com.latenighthack.ktbuf.rpc.HttpRpcClient(it)) { _, _ -> mapOf(FORWARDED_PARAM to "1") }
        }
        return try {
            kotlinx.coroutines.withTimeout(FORWARD_TIMEOUT_MS) { call(stub) }
                .also { forwardedWritesCounter.increment() }
        } catch (e: kotlinx.coroutines.TimeoutCancellationException) {
            forwardFailureCounter.increment()
            logger.warn("write forward to {} timed out; answering NOT_OWNER", address)
            forwardStubs.remove(address)
            null
        } catch (e: kotlinx.coroutines.CancellationException) {
            throw e
        } catch (e: Exception) {
            forwardFailureCounter.increment()
            logger.warn("write forward to {} failed ({}); answering NOT_OWNER", address, e.message)
            forwardStubs.remove(address)
            null
        }
    }

    private val gatewayLookupFailureCounter = meterRegistry.counter("lockers.room.gateway.lookup.failures")
    private val oversizeRejectedCounter = meterRegistry.counter("lockers.room.locker.rejected.oversize")
    private val rateLimitedCounter = meterRegistry.counter("lockers.room.locker.rejected.ratelimited")
    private val postEventSuccessCounter = meterRegistry.counter("lockers.room.events.post.success")
    private val postEventFailureCounter = meterRegistry.counter("lockers.room.events.post.failure")
    private val agentFailureCounter = meterRegistry.counter("lockers.room.agent.failures")
    private val cacheHitCounter = meterRegistry.counter("lockers.room.cache.hits")
    private val cacheMissCounter = meterRegistry.counter("lockers.room.cache.misses")

    // Reshard observability (§7). A cache miss on the room→session set is the lazy-rebuild path a
    // new shard owner takes after a fenced handoff, so it also increments reshard.rooms.rebuilt.
    // A version-CAS reject (UPDATE_LOCAL_VERSION) is the dual-coordination backstop firing, counted
    // as reshard.cas.conflicts — if two nodes ever briefly both coordinate a room, exactly one wins.
    private val reshardRoomsRebuiltCounter = meterRegistry.counter("lockers.reshard.rooms.rebuilt")
    private val reshardCasConflictsCounter = meterRegistry.counter("lockers.reshard.cas.conflicts")
    private val dispatcherWaitTimer = meterRegistry.timer("lockers.room.dispatcher.time")
    private val getLockerTimer = meterRegistry.timer("lockers.room.locker.get.time")
    private val getAllLockersTimer = meterRegistry.timer("lockers.room.locker.getall.time")
    private val lockersReturnedSummary = meterRegistry.summary("lockers.room.locker.count")

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
        val cached = roomToSessionCache.get(roomId)
        if (cached != null) {
            cacheHitCounter.increment()
            return cached
        }
        
        cacheMissCounter.increment()
        // Rebuilding room→session routing from the durable store: this is exactly the path a new
        // shard owner takes on first access after a fenced handoff (Model A — no state transfer).
        reshardRoomsRebuiltCounter.increment()
        val sessions = subscriptionStore.getAllSessions(ServerRoomId(roomId.rawValue))
            .map { SessionId(it.rawValue) }
            .toSet()
        roomToSessionCache.put(roomId, sessions)
        return sessions
    }

    override suspend fun capabilities(context: GrpcRequestContext, request: CapabilitiesRequest) = meterRegistry.trackRpc(TelemetryOperation.ROOM_CAPABILITIES, telemetry) { CapabilitiesResponse(
        subscribeAndSnapshot = config.deliveryOutboxEnabled, getLockers = true,
        postLockerChanges = config.deliveryOutboxEnabled, writeReceipts = config.deliveryOutboxEnabled,
        maxBatchItems = 64, maxBatchBytes = minOf(8 * 1024 * 1024, config.maxLockerPayloadBytes)
    ) }

    override suspend fun getLockers(context: GrpcRequestContext, request: GetLockersRequest): GetLockersResponse = meterRegistry.trackRpc(TelemetryOperation.ROOM_GET_MANY, telemetry) { observedGetLockers(context, request) }

    private suspend fun observedGetLockers(context: GrpcRequestContext, request: GetLockersRequest): GetLockersResponse {
        require(request.lockerIds.size <= 64 && request.toByteArray().size <= 8 * 1024 * 1024)
        val room = requireNotNull(request.roomId)
        val all = lockerStore.getLockers(ServerRoomId(room.rawValue), request.lockerIds.map { (it.keyspace?.value ?: 0L) to ServerLockerId(it.rawValue) }).associateBy {
            LockerId(requireNotNull(it.lockerId).rawValue, LockerKeyspace(it.keyspace))
        }
        return GetLockersResponse(request.lockerIds.map { id ->
            all[id.copy(keyspace = id.keyspace ?: LockerKeyspace(0))]?.let { stored -> GetLockerResponse(result = GetLockerResponse.Result.OK,
                locker = IdentifiedLocker(id, Locker.fromByteArray(stored.locker), stored.version, lockStateFor(room, id))) }
                ?: GetLockerResponse(result = GetLockerResponse.Result.UNKNOWN_ERROR)
        })
    }

    override suspend fun subscribeAndSnapshot(context: GrpcRequestContext, request: SubscribeAndSnapshotRequest): SubscribeAndSnapshotResponse = meterRegistry.trackRpc(TelemetryOperation.ROOM_SNAPSHOT, telemetry, { rpcOutcome(it.result.toString()) }) { observedSubscribeAndSnapshot(context, request) }

    private suspend fun observedSubscribeAndSnapshot(context: GrpcRequestContext, request: SubscribeAndSnapshotRequest): SubscribeAndSnapshotResponse {
        require(request.keyspaces.size <= 64)
        val room = requireNotNull(request.roomId)
        val session = requireNotNull(request.sessionId)
        check(config.deliveryOutboxEnabled)
        return requireNotNull(deliveryOutbox).atomic {
            subscriptionStore.addSubscription(ServerSessionId(session.rawValue), ServerRoomId(room.rawValue))
            roomToSessionCache.invalidate(room)
            val spaces = request.keyspaces.map { it.value }.toSet()
            val lockers = lockerStore.getAllLockers(ServerRoomId(room.rawValue)).filter { spaces.isEmpty() || it.keyspace in spaces }.map {
                val id = LockerId(requireNotNull(it.lockerId).rawValue, LockerKeyspace(it.keyspace))
                IdentifiedLocker(id, if (it.deleted) null else Locker.fromByteArray(it.locker), it.version, lockStateFor(room, id))
            }
            SubscribeAndSnapshotResponse(result = SubscriptionResponse.Result.OK, lockers = lockers,
                roomSequence = requireNotNull(deliveryOutbox).watermark(room))
        }
    }

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
        if (!config.deliveryOutboxEnabled || changes.isEmpty() || changes.size > 64 ||
            request.writeRequestId.size !in 16..64 || changes.any { it.lockerId == null || it.locker == null || (it.roomId != null && it.roomId != room) } ||
            changes.map { it.lockerId }.distinct().size != changes.size ||
            request.toByteArray().size > minOf(8 * 1024 * 1024, config.maxLockerPayloadBytes)) {
            return PostLockerChangesResponse(result = PostLockerChangesResponse.Result.INVALID)
        }
        for (space in changes.map { it.lockerId?.keyspace?.value ?: 0L }.distinct()) {
            trace.phase("ownership") { redirectIfNotOwner(space, room) }?.let { redirect ->
                trace.phase("forward") { forwardToOwnerOrNull(context, redirect) { it.postLockerChanges(request) } }?.let { return it }
                return PostLockerChangesResponse(result = PostLockerChangesResponse.Result.NOT_OWNER, redirect = redirect)
            }
        }
        if (!rateLimiter.tryAcquire(room)) return PostLockerChangesResponse(result = PostLockerChangesResponse.Result.UNKNOWN_ERROR)
        val outbox = requireNotNull(deliveryOutbox)
        val digest = com.latenighthack.ktcrypto.SHA256.digest(request.toByteArray())
        val queuedAt = System.nanoTime()
        return dispatchers.runOnDispatcher(room) {
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
                    val roomLocks = lockStore.getAllLocksInRoom(ServerRoomId(room.rawValue))
                    val sourceWrites = mutableListOf<ServerLocker>()
                    val results = normalized.map { change ->
                        val id = change.lockerId!!
                        val effective = roomLocks.filter { lock ->
                            lock.scopeKind == LockVerifier.SCOPE_ROOM ||
                                (lock.keyspace == (id.keyspace?.value ?: 0L) && (lock.scopeKind == LockVerifier.SCOPE_KEYSPACE || lock.lockerId?.rawValue.contentEquals(id.rawValue)))
                        }.minByOrNull { it.scopeKind }
                        performLockerChange(change, events, sourceWrites, existing[id.copy(keyspace = id.keyspace ?: LockerKeyspace(0))], effective,
                            prefetchLocks = normalized.none { it.ratchet != null })
                    }
                    if (results.any { !it.result.isOk() }) throw BatchRejected(PostLockerChangesResponse(
                        result = PostLockerChangesResponse.Result.CONFLICT, changes = results, lockState = lockState))
                    lockerStore.updateLockers(sourceWrites)
                    pending = PostLockerChangesResponse(result = PostLockerChangesResponse.Result.OK, changes = results,
                        lockState = lockState, agentPending = true)
                    outbox.saveReceipt(com.latenighthack.lockers.server.storage.v1.ServerWriteReceipt(
                        request.writeRequestId, ServerRoomId(room.rawValue), digest, pending!!.toByteArray()))
                }
                }
                trace.recipients = recipients.size
            } catch (e: BatchRejected) {
                return@runOnDispatcher e.response
            }
            replay?.let { return@runOnDispatcher it }
            // The durable pending receipt prevents an ambiguous retry from executing an agent twice.
            // After a crash here, the client sees committed/pending, not an invitation to replay a move.
            var failed = false
            try {
                val writes = linkedMapOf<LockerId, ServerLocker>()
                val derivedEvents = mutableListOf<Event>()
                for (change in normalized) {
                    for (derived in trace.phase("agent") { processAgent(room, change.lockerId!!, change.locker!!) }) {
                        val id = derived.lockerId
                        val existing = writes[id] ?: lockerStore.getLocker(ServerRoomId(room.rawValue), id.keyspace?.value ?: 0L, ServerLockerId(id.rawValue))
                        val version = (existing?.version ?: 0L) + 1
                        writes[id] = ServerLocker(ServerRoomId(room.rawValue), id.keyspace?.value ?: 0L,
                            ServerLockerId(id.rawValue), derived.locker.toByteArray(), version)
                        derivedEvents.add(Event(roomId = room, eventId = EventId(Random.nextBytes(32)),
                            locker = IdentifiedLocker(id, derived.locker, version)))
                    }
                }
                val complete = pending!!.copy(agentPending = false)
                trace.phase("derived_commit") { outbox.commit(room, recipients, derivedEvents) {
                    recipients.clear()
                    recipients.addAll(subscriptionStore.getAllSessions(ServerRoomId(room.rawValue)).map { SessionId(it.rawValue) })
                    lockerStore.updateLockers(writes.values.toList())
                    outbox.saveReceipt(com.latenighthack.lockers.server.storage.v1.ServerWriteReceipt(
                        request.writeRequestId, ServerRoomId(room.rawValue), digest, complete.toByteArray()))
                }
                }
            } catch (e: kotlinx.coroutines.CancellationException) { throw e }
            catch (e: Exception) {
                failed = true
                agentFailureCounter.increment()
                logger.error("agent processing failed after batch commit", e)
                outbox.atomic { outbox.saveReceipt(com.latenighthack.lockers.server.storage.v1.ServerWriteReceipt(
                    request.writeRequestId, ServerRoomId(room.rawValue), digest, pending!!.copy(agentPending = false, agentFailed = true).toByteArray())) }
            }
            pending!!.copy(agentPending = false, agentFailed = failed)
        }
    }

    override suspend fun subscription(
        context: GrpcRequestContext,
        request: SubscriptionRequest
    ) = meterRegistry.trackResponse("lockers.room.locker.subscribe", SubscriptionResponse::result, telemetry) {
        val roomId = request.roomId ?: return@trackResponse SubscriptionResponse(result = SubscriptionResponse.Result.UNKNOWN_ERROR)
        val sessionId = request.sessionId ?: return@trackResponse SubscriptionResponse(result = SubscriptionResponse.Result.UNKNOWN_ERROR)

        val startTime = System.nanoTime()
        return@trackResponse dispatchers.runOnDispatcher(roomId) {
            dispatcherWaitTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)
            val cachedSet = lookupSessions(roomId)
            val updatedSet = when (request.kind) {
                is SubscriptionRequest.OneOfKind.subscribe -> {
                    meterRegistry.counter("lockers.room.subscriptions", "operation", "subscribe", "result", "OK").increment()
                    subscriptionStore.addSubscription(ServerSessionId(sessionId.rawValue), ServerRoomId(roomId.rawValue))
                    cachedSet + sessionId
                }
                is SubscriptionRequest.OneOfKind.unsubscribe -> {
                    meterRegistry.counter("lockers.room.subscriptions", "operation", "unsubscribe", "result", "OK").increment()
                    subscriptionStore.removeSubscription(ServerSessionId(sessionId.rawValue), ServerRoomId(roomId.rawValue))
                    cachedSet - sessionId
                }
                null -> {
                    meterRegistry.counter("lockers.room.subscriptions", "operation", "unknown", "result", "ERROR").increment()
                    return@runOnDispatcher SubscriptionResponse(result = SubscriptionResponse.Result.UNKNOWN_ERROR)
                }
            }

            roomToSessionCache.put(roomId, updatedSet)

            SubscriptionResponse {
                result = SubscriptionResponse.Result.OK
            }
        }
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

        val storedLocker = lockerStore.getLocker(ServerRoomId(roomId.rawValue), lockerId.keyspace?.value ?: 0L, ServerLockerId(lockerId.rawValue))
        val lockerPayload = storedLocker?.takeUnless { it.deleted }?.locker?.let { Locker.fromByteArray(it) }
        
        if (lockerPayload == null) {
            getLockerTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)
            return@trackResponse GetLockerResponse(result = GetLockerResponse.Result.UNKNOWN_ERROR)
        }

        getLockerTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)
        val effectiveState = lockStateFor(roomId, lockerId)
        GetLockerResponse {
            result = GetLockerResponse.Result.OK
            locker {
                this.lockerId = lockerId
                locker = lockerPayload
                version = storedLocker.version
                lockState = effectiveState
            }
        }
    }

    override suspend fun getAllLockers(
        context: GrpcRequestContext,
        request: GetAllLockersRequest
    ) = meterRegistry.trackResponse("lockers.room.locker.getall", GetAllLockersResponse::result, telemetry) {
        val startTime = System.nanoTime()
        val roomId = request.roomId
            ?: return@trackResponse GetAllLockersResponse(result = GetAllLockersResponse.Result.UNKNOWN_ERROR)

        val keyspace = request.keyspace

        val storedLockers = if (keyspace == null) {
            lockerStore.getAllLockers(ServerRoomId(roomId.rawValue))
        } else {
            lockerStore.getAllLockersInKeyspace(ServerRoomId(roomId.rawValue), keyspace.value)
        }

        lockersReturnedSummary.record(storedLockers.size.toDouble())
        getAllLockersTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)

        // Resolve lock state up front: the builder lambdas below are not suspend contexts.
        val identified = storedLockers.filterNot { it.deleted }.map { storedLocker ->
            val storedLockerId = LockerId(
                rawValue = storedLocker.lockerId?.rawValue!!,
                keyspace = LockerKeyspace { value = storedLocker.keyspace }
            )
            IdentifiedLocker(
                lockerId = storedLockerId,
                locker = if (storedLocker.deleted) null else Locker.fromByteArray(storedLocker.locker),
                version = storedLocker.version,
                lockState = lockStateFor(roomId, storedLockerId),
            )
        }

        return@trackResponse GetAllLockersResponse {
            result = GetAllLockersResponse.Result.OK
            lockers = identified
        }
    }

    override suspend fun postLockerChange(
        context: GrpcRequestContext,
        request: PostLockerChangeRequest
    ) = meterRegistry.trackResponse("lockers.room.locker.postlockerchange", PostLockerChangeResponse::result, telemetry) {
        if (config.deliveryOutboxEnabled) {
            val batch = postLockerChanges(context, PostLockerChangesRequest(roomId = request.roomId,
                changes = listOf(request.copy(writeRequestId = byteArrayOf())), writeRequestId = request.writeRequestId.takeIf { it.isNotEmpty() } ?: Random.nextBytes(32)))
            return@trackResponse batch.changes.firstOrNull()?.copy(agentFailed = batch.agentFailed, agentPending = batch.agentPending)
                ?: PostLockerChangeResponse(result = if (batch.result is PostLockerChangesResponse.Result.NOT_OWNER)
                    PostLockerChangeResponse.Result.NOT_OWNER else PostLockerChangeResponse.Result.UNKNOWN_ERROR, redirect = batch.redirect)
        }
        val requestRoomId = request.roomId ?: return@trackResponse PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UNKNOWN_ERROR)
        val requestEventId = EventId(Random.nextBytes(32))
        val updatedLocker = request.locker ?: return@trackResponse PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UNKNOWN_ERROR)
        val requestLockerId = request.lockerId ?: return@trackResponse PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UNKNOWN_ERROR)
        val requestVersion = request.parentVersion

        redirectIfNotOwner(requestLockerId.keyspace?.value ?: 0L, requestRoomId)?.let { redirect ->
            forwardToOwnerOrNull(context, redirect) { it.postLockerChange(request) }?.let {
                return@trackResponse it
            }
            return@trackResponse PostLockerChangeResponse {
                result = PostLockerChangeResponse.Result.NOT_OWNER
                this.redirect = redirect
            }
        }

        if (!rateLimiter.tryAcquire(requestRoomId)) {
            rateLimitedCounter.increment()
            logger.warn("rate limit exceeded for room; rejecting locker change")
            return@trackResponse PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UNKNOWN_ERROR)
        }

        val encodedLocker = updatedLocker.toByteArray()
        if (encodedLocker.size > config.maxLockerPayloadBytes) {
            oversizeRejectedCounter.increment()
            logger.warn("locker payload ${encodedLocker.size}B exceeds limit ${config.maxLockerPayloadBytes}B; rejecting")
            return@trackResponse PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UNKNOWN_ERROR)
        }

        val startTime = System.nanoTime()
        return@trackResponse dispatchers.runOnDispatcher(requestRoomId) {
            dispatcherWaitTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)

            performLockerChange(request)
        }
    }

    private suspend fun performLockerChange(request: PostLockerChangeRequest, pendingEvents: MutableList<Event>? = null,
        pendingWrites: MutableList<ServerLocker>? = null, prefetchedLocker: ServerLocker? = null,
        prefetchedLock: ServerLock? = null, prefetchLocks: Boolean = false): PostLockerChangeResponse {
        val requestRoomId = requireNotNull(request.roomId)
        val requestLockerId = requireNotNull(request.lockerId)
        val updatedLocker = requireNotNull(request.locker)
        if (!LockerEnvelope.isSupported(updatedLocker)) return PostLockerChangeResponse(result = PostLockerChangeResponse.Result.NOT_AUTHORIZED)
        val encodedLocker = updatedLocker.toByteArray()
        val requestEventId = EventId(Random.nextBytes(32))
        val requestVersion = request.parentVersion
            val storedLocker = if (pendingWrites != null) prefetchedLocker else lockerStore.getLocker(
                ServerRoomId(requestRoomId.rawValue),
                (requestLockerId.keyspace?.value ?: 0L),
                ServerLockerId(requestLockerId.rawValue)
            )

            val effectiveLock = if (prefetchLocks) prefetchedLock else effectiveLockOrNull(requestRoomId, requestLockerId)
            var effectiveState = effectiveLock?.let { lockVerifier.stateOf(it) }

            val updatedLockerVersion = if (storedLocker == null) {
                requestVersion
            } else if (storedLocker.version == requestVersion) {
                storedLocker.version + 1
            } else {
                reshardCasConflictsCounter.increment()
                return PostLockerChangeResponse {
                    result = PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION
                    version = storedLocker.version
                    existingLocker = if (storedLocker.deleted) null else Locker.fromByteArray(storedLocker.locker)
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
                when (lockVerifier.verifyWrite(effectiveLock, requestRoomId, requestLockerId, requestVersion, hash, request.writeSignature)) {
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
            val sessionIds = if (pendingEvents != null) mutableListOf() else lookupSessions(requestRoomId).toMutableList()
            val sourceEvent = Event {
                roomId = requestRoomId
                eventId = requestEventId
                locker { locker = updatedLocker; lockerId = requestLockerId; version = updatedLockerVersion; lockState = effectiveState }
                notification = request.notification
            }
            if (pendingEvents != null) {
                requireNotNull(pendingWrites).add(serverLocker)
                pendingEvents.add(sourceEvent)
                return PostLockerChangeResponse(result = PostLockerChangeResponse.Result.OK, version = updatedLockerVersion, lockState = effectiveState)
            } else if (config.deliveryOutboxEnabled) {
                requireNotNull(deliveryOutbox).commit(requestRoomId, sessionIds, listOf(sourceEvent)) {
                    sessionIds.clear()
                    sessionIds.addAll(subscriptionStore.getAllSessions(ServerRoomId(requestRoomId.rawValue)).map { SessionId(it.rawValue) })
                    lockerStore.updateLocker(serverLocker)
                }
            } else {
                lockerStore.updateLocker(serverLocker)
                deliver(listOf(sourceEvent), sessionIds)
            }
            var agentFailed = false

            // let the pluggable agent derive additional lockers; write + broadcast them.
            // Keyspaces stay opaque to the core — the agent decides what to act on.
            // The client's write is already persisted + fanned out, so an agent failure
            // must not fail the RPC — a 500 here punishes a successful write and the
            // client has no way to retry into a consistent state.
            try {
                val frameLockers = processAgent(requestRoomId, requestLockerId, updatedLocker)

                val derived = frameLockers.map { frameLocker ->
                    val existing = lockerStore.getLocker(ServerRoomId(requestRoomId.rawValue), frameLocker.lockerId.keyspace?.value ?: 0L, ServerLockerId(frameLocker.lockerId.rawValue))
                    val version = (existing?.version ?: 0L) + 1
                    val stored = ServerLocker(ServerRoomId(requestRoomId.rawValue), frameLocker.lockerId.keyspace?.value ?: 0L,
                        ServerLockerId(frameLocker.lockerId.rawValue), frameLocker.locker.toByteArray(), version)
                    val event = Event {
                        roomId = requestRoomId; eventId = EventId(Random.nextBytes(32))
                        locker { locker = frameLocker.locker; lockerId = frameLocker.lockerId; this.version = version }
                    }
                    stored to event
                }
                if (derived.isNotEmpty()) {
                    if (config.deliveryOutboxEnabled) requireNotNull(deliveryOutbox).commit(requestRoomId, sessionIds, derived.map { it.second }) {
                        lockerStore.updateLockers(derived.map { it.first })
                    } else {
                        lockerStore.updateLockers(derived.map { it.first })
                        deliver(derived.map { it.second }, sessionIds)
                    }
                }
            } catch (e: kotlinx.coroutines.CancellationException) { throw e
            } catch (e: Exception) {
                agentFailed = true
                agentFailureCounter.increment()
                logger.error("agent processing failed for locker change (keyspace=${requestLockerId.keyspace?.value})", e)
            }

            return PostLockerChangeResponse {
                result = PostLockerChangeResponse.Result.OK
                version = updatedLockerVersion
                lockState = effectiveState
                this.agentFailed = agentFailed
            }
    }

    private suspend fun deliver(events: List<Event>, sessionIds: List<SessionId>) {
        val groups = sessionGatewayDiscovery.resolveGroups(sessionIds)
        for (event in events) kotlinx.coroutines.coroutineScope {
            val permits = kotlinx.coroutines.sync.Semaphore(4)
            groups.map { group -> async {
                permits.acquire()
                try {
                    val response = group.service.postEvent(PostEventRequest(group.sessionIds, event))
                    if (response.result.isOk()) postEventSuccessCounter.increment() else postEventFailureCounter.increment()
                } catch (e: kotlinx.coroutines.CancellationException) { throw e }
                catch (e: Exception) { postEventFailureCounter.increment(); logger.warn("gateway delivery failed", e) }
                finally { permits.release() }
            } }.forEach { it.await() }
        }
    }

    override suspend fun deleteLocker(
        context: GrpcRequestContext,
        request: DeleteLockerRequest
    ) = meterRegistry.trackResponse("lockers.room.locker.deletelocker", DeleteLockerResponse::result, telemetry) {
        val requestRoomId = request.roomId ?: return@trackResponse DeleteLockerResponse(result = DeleteLockerResponse.Result.UNKNOWN_ERROR)
        val requestEventId = EventId(Random.nextBytes(32))
        val requestLockerId = request.lockerId ?: return@trackResponse DeleteLockerResponse(result = DeleteLockerResponse.Result.UNKNOWN_ERROR)
        val requestVersion = request.parentVersion

        redirectIfNotOwner(requestLockerId.keyspace?.value ?: 0L, requestRoomId)?.let { redirect ->
            forwardToOwnerOrNull(context, redirect) { it.deleteLocker(request) }?.let {
                return@trackResponse it
            }
            return@trackResponse DeleteLockerResponse {
                result = DeleteLockerResponse.Result.NOT_OWNER
                this.redirect = redirect
            }
        }

        if (!rateLimiter.tryAcquire(requestRoomId)) {
            rateLimitedCounter.increment()
            logger.warn("rate limit exceeded for room; rejecting locker delete")
            return@trackResponse DeleteLockerResponse(result = DeleteLockerResponse.Result.UNKNOWN_ERROR)
        }

        val startTime = System.nanoTime()
        return@trackResponse dispatchers.runOnDispatcher(requestRoomId) {
            dispatcherWaitTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)

            val storedLocker = lockerStore.getLocker(
                ServerRoomId(requestRoomId.rawValue),
                (requestLockerId.keyspace?.value ?: 0L),
                ServerLockerId(requestLockerId.rawValue)
            )

            val effectiveLock = effectiveLockOrNull(requestRoomId, requestLockerId)
            val effectiveState = effectiveLock?.let { lockVerifier.stateOf(it) }

            val updatedLockerVersion = if (storedLocker == null) {
                requestVersion
            } else if (storedLocker.version == requestVersion) {
                storedLocker.version + 1
            } else {
                reshardCasConflictsCounter.increment()
                return@runOnDispatcher DeleteLockerResponse {
                    result = DeleteLockerResponse.Result.UPDATE_LOCAL_VERSION
                    version = storedLocker.version
                    existingLocker = if (storedLocker.deleted) null else Locker.fromByteArray(storedLocker.locker)
                    lockState = effectiveState
                }
            }

            // Locked lockers: deletes must be signed by the effective lock key over the
            // write context with an empty content hash.
            if (effectiveLock != null) {
                when (lockVerifier.verifyWrite(effectiveLock, requestRoomId, requestLockerId, requestVersion, ByteArray(0), request.writeSignature)) {
                    LockVerifier.WriteVerdict.REQUIRED -> return@runOnDispatcher DeleteLockerResponse {
                        result = DeleteLockerResponse.Result.SIGNATURE_REQUIRED
                        lockState = effectiveState
                    }
                    LockVerifier.WriteVerdict.INVALID -> return@runOnDispatcher DeleteLockerResponse {
                        result = DeleteLockerResponse.Result.SIGNATURE_INVALID
                        lockState = effectiveState
                    }
                    LockVerifier.WriteVerdict.OK -> {}
                }
            }

            val tombstone = ServerLocker(ServerRoomId(requestRoomId.rawValue), requestLockerId.keyspace?.value ?: 0L,
                ServerLockerId(requestLockerId.rawValue), byteArrayOf(), updatedLockerVersion, deleted = true)
            val event = Event(roomId = requestRoomId, eventId = requestEventId,
                locker = IdentifiedLocker(requestLockerId, version = updatedLockerVersion, lockState = effectiveState), notification = request.notification)
            val recipients = mutableListOf<SessionId>()
            if (config.deliveryOutboxEnabled) requireNotNull(deliveryOutbox).commit(requestRoomId, recipients, listOf(event)) {
                recipients.addAll(subscriptionStore.getAllSessions(ServerRoomId(requestRoomId.rawValue)).map { SessionId(it.rawValue) })
                lockerStore.updateLocker(tombstone)
            } else {
                lockerStore.updateLocker(tombstone)
                deliver(listOf(event), lookupSessions(requestRoomId).toList())
            }

            DeleteLockerResponse {
                result = DeleteLockerResponse.Result.OK
                version = updatedLockerVersion
                lockState = effectiveState
            }
        }
    }

    override suspend fun lockLocker(
        context: GrpcRequestContext,
        request: LockLockerRequest
    ) = meterRegistry.trackResponse("lockers.room.locker.lock", LockLockerResponse::result, telemetry) {
        val requestRoomId = request.roomId ?: return@trackResponse LockLockerResponse(result = LockLockerResponse.Result.UNKNOWN_ERROR)
        val grant = request.grant ?: return@trackResponse LockLockerResponse(result = LockLockerResponse.Result.UNKNOWN_ERROR)

        // A lock and the lockers it governs must be coordinated on the same shard, so gate by the
        // scope's keyspace; a room-wide scope carries no keyspace and pins to keyspace 0.
        redirectIfNotOwner(grant.scope?.keyspace?.value ?: 0L, requestRoomId)?.let { redirect ->
            forwardToOwnerOrNull(context, redirect) { it.lockLocker(request) }?.let {
                return@trackResponse it
            }
            return@trackResponse LockLockerResponse {
                result = LockLockerResponse.Result.NOT_OWNER
                this.redirect = redirect
            }
        }

        return@trackResponse dispatchers.runOnDispatcher(requestRoomId) {
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
        }
    }

    override suspend fun unlockLocker(
        context: GrpcRequestContext,
        request: UnlockLockerRequest
    ) = meterRegistry.trackResponse("lockers.room.locker.unlock", UnlockLockerResponse::result, telemetry) {
        val requestRoomId = request.roomId ?: return@trackResponse UnlockLockerResponse(result = UnlockLockerResponse.Result.UNKNOWN_ERROR)
        val scope = request.scope ?: return@trackResponse UnlockLockerResponse(result = UnlockLockerResponse.Result.UNKNOWN_ERROR)

        redirectIfNotOwner(scope.keyspace?.value ?: 0L, requestRoomId)?.let { redirect ->
            forwardToOwnerOrNull(context, redirect) { it.unlockLocker(request) }?.let {
                return@trackResponse it
            }
            return@trackResponse UnlockLockerResponse {
                result = UnlockLockerResponse.Result.NOT_OWNER
                this.redirect = redirect
            }
        }

        return@trackResponse dispatchers.runOnDispatcher(requestRoomId) {
            val outcome = lockVerifier.applyUnlock(requestRoomId, scope, request.signature, request.parentLockVersion)
            when (outcome) {
                is LockVerifier.UnlockOutcome.Ok -> {
                    UnlockLockerResponse(result = UnlockLockerResponse.Result.OK)
                }
                is LockVerifier.UnlockOutcome.Stale -> UnlockLockerResponse(result = UnlockLockerResponse.Result.UPDATE_LOCAL_VERSION)
                is LockVerifier.UnlockOutcome.SignatureInvalid -> UnlockLockerResponse(result = UnlockLockerResponse.Result.SIGNATURE_INVALID)
            }
        }
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

    fun close() {
        deliveryWorker?.close()
        dispatchers.close()
    }
}
