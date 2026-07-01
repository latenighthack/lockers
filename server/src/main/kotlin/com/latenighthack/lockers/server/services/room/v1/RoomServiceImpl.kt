package com.latenighthack.lockers.server.services.room.v1

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
import me.tatarka.inject.annotations.Component
import me.tatarka.inject.annotations.Inject
import me.tatarka.inject.annotations.Provides
import org.slf4j.LoggerFactory
import kotlin.random.Random

@ServiceScope
@Component
abstract class RoomServiceModule(
    @Component val serverCore: ServerCore,
    @get:Provides val sessionGatewayDiscovery: SessionGatewayDiscovery
): GrpcRouteProvider<RoomServer> {
    abstract val serverImpl: RoomServiceImpl

    override val server: RoomServer get() = serverImpl
    override val descriptor: ServerDescriptor = RoomServer.Descriptor
}


@ServiceScope
@Inject
class RoomServiceImpl(
    private val subscriptionStore: SubscriptionStore,
    private val lockerStore: LockerStore,
    private val lockStore: LockStore,
    private val sessionGatewayDiscovery: SessionGatewayDiscovery,
    private val agentRegistry: LockerAgentRegistry,
    private val meterRegistry: MeterRegistry,
    private val config: LockersConfig,
) : BaseServiceImpl(), RoomServer {
    private val logger = LoggerFactory.getLogger(RoomServiceImpl::class.java)
    private val lockVerifier = LockVerifier(lockStore)
    private val dispatchers = ShardedDispatcher<RoomId>(config.shardCount, "room-shard") {
        it.rawValue.contentHashCode()
    }
    private val rateLimiter = RoomRateLimiter(config.roomWritesPerSecond, config.roomWriteBurst)

    // Whether a room has any locks. Lets the common open-locker write/read path skip
    // effective-lock resolution entirely. Kept fresh by lock/unlock; recomputed on miss.
    private val roomHasLocksCache = Cache.Builder<RoomId, Boolean>()
        .maximumCacheSize(config.sessionCacheSize)
        .build()

    private suspend fun roomHasLocks(roomId: RoomId): Boolean {
        roomHasLocksCache.get(roomId)?.let { return it }
        val has = lockVerifier.roomHasLocks(roomId)
        roomHasLocksCache.put(roomId, has)
        return has
    }

    private suspend fun effectiveLockOrNull(roomId: RoomId, lockerId: LockerId): ServerLock? =
        if (roomHasLocks(roomId)) {
            lockVerifier.resolveEffective(roomId, lockerId.keyspace?.value ?: 0L, lockerId.rawValue)
        } else {
            null
        }

    private suspend fun lockStateFor(roomId: RoomId, lockerId: LockerId): LockState? =
        effectiveLockOrNull(roomId, lockerId)?.let { lockVerifier.stateOf(it) }

    private val gatewayLookupFailureCounter = meterRegistry.counter("lockers.room.gateway.lookup.failures")
    private val oversizeRejectedCounter = meterRegistry.counter("lockers.room.locker.rejected.oversize")
    private val rateLimitedCounter = meterRegistry.counter("lockers.room.locker.rejected.ratelimited")
    private val postEventSuccessCounter = meterRegistry.counter("lockers.room.events.post.success")
    private val postEventFailureCounter = meterRegistry.counter("lockers.room.events.post.failure")
    private val cacheHitCounter = meterRegistry.counter("lockers.room.cache.hits")
    private val cacheMissCounter = meterRegistry.counter("lockers.room.cache.misses")
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
        val sessions = subscriptionStore.getAllSessions(ServerRoomId(roomId.rawValue))
            .map { SessionId(it.rawValue) }
            .toSet()
        roomToSessionCache.put(roomId, sessions)
        return sessions
    }

    override suspend fun subscription(
        context: GrpcRequestContext,
        request: SubscriptionRequest
    ) = meterRegistry.trackResponse("lockers.room.locker.subscribe", SubscriptionResponse::result) {
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
    ) = meterRegistry.trackResponse("lockers.room.locker.get", GetLockerResponse::result) {
        val startTime = System.nanoTime()
        val lockerId = request.lockerId
        val roomId = request.roomId

        if (lockerId == null || roomId == null) {
            return@trackResponse GetLockerResponse(result = GetLockerResponse.Result.UNKNOWN_ERROR)
        }

        val storedLocker = lockerStore.getLocker(ServerRoomId(roomId.rawValue), lockerId.keyspace?.value ?: 0L, ServerLockerId(lockerId.rawValue))
        val lockerPayload = storedLocker?.locker?.let { Locker.fromByteArray(it) }
        
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
    ) = meterRegistry.trackResponse("lockers.room.locker.getall", GetAllLockersResponse::result) {
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
        val identified = storedLockers.map { storedLocker ->
            val storedLockerId = LockerId(
                rawValue = storedLocker.lockerId?.rawValue!!,
                keyspace = LockerKeyspace { value = storedLocker.keyspace }
            )
            IdentifiedLocker(
                lockerId = storedLockerId,
                locker = Locker.fromByteArray(storedLocker.locker),
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
    ) = meterRegistry.trackResponse("lockers.room.locker.postlockerchange", PostLockerChangeResponse::result) {
        val requestRoomId = request.roomId ?: return@trackResponse PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UNKNOWN_ERROR)
        val requestEventId = EventId(Random.nextBytes(32))
        val updatedLocker = request.locker ?: return@trackResponse PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UNKNOWN_ERROR)
        val requestLockerId = request.lockerId ?: return@trackResponse PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UNKNOWN_ERROR)
        val requestVersion = request.parentVersion

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

            val storedLocker = lockerStore.getLocker(
                ServerRoomId(requestRoomId.rawValue),
                (requestLockerId.keyspace?.value ?: 0L),
                ServerLockerId(requestLockerId.rawValue)
            )

            val effectiveLock = effectiveLockOrNull(requestRoomId, requestLockerId)
            var effectiveState = effectiveLock?.let { lockVerifier.stateOf(it) }

            val updatedLockerVersion = if (storedLocker == null) {
                requestVersion
            } else if (storedLocker.version == requestVersion) {
                storedLocker.version + 1
            } else {
                return@runOnDispatcher PostLockerChangeResponse {
                    result = PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION
                    version = storedLocker.version
                    existingLocker = Locker.fromByteArray(storedLocker.locker)
                    lockState = effectiveState
                }
            }

            // Locked lockers: writes must carry a signature over the canonical write
            // context that verifies against the effective lock's current key. The
            // signature binds the parent version, so the client re-signs on retry.
            if (effectiveLock != null) {
                val enclosure = updatedLocker.sealed?.payload?.enclosure
                if (enclosure == null) {
                    return@runOnDispatcher PostLockerChangeResponse {
                        result = PostLockerChangeResponse.Result.SIGNATURE_REQUIRED
                        lockState = effectiveState
                    }
                }
                val hash = lockVerifier.contentHash(enclosure.innerPayload)
                val checksum = updatedLocker.sealed?.payload?.checksum
                if (checksum != null && checksum.isNotEmpty() && !checksum.contentEquals(hash)) {
                    return@runOnDispatcher PostLockerChangeResponse {
                        result = PostLockerChangeResponse.Result.SIGNATURE_INVALID
                        lockState = effectiveState
                    }
                }
                when (lockVerifier.verifyWrite(effectiveLock, requestRoomId, requestLockerId, requestVersion, hash, request.writeSignature)) {
                    LockVerifier.WriteVerdict.REQUIRED -> return@runOnDispatcher PostLockerChangeResponse {
                        result = PostLockerChangeResponse.Result.SIGNATURE_REQUIRED
                        lockState = effectiveState
                    }
                    LockVerifier.WriteVerdict.INVALID -> return@runOnDispatcher PostLockerChangeResponse {
                        result = PostLockerChangeResponse.Result.SIGNATURE_INVALID
                        lockState = effectiveState
                    }
                    LockVerifier.WriteVerdict.OK -> {}
                }
                val ratchet = request.ratchet
                if (ratchet != null) {
                    when (val outcome = lockVerifier.applyRatchet(effectiveLock, requestRoomId, requestLockerId, requestVersion, ratchet)) {
                        is LockVerifier.RatchetOutcome.Invalid -> return@runOnDispatcher PostLockerChangeResponse {
                            result = PostLockerChangeResponse.Result.SIGNATURE_INVALID
                            lockState = effectiveState
                        }
                        is LockVerifier.RatchetOutcome.Ok -> {
                            effectiveState = outcome.state
                            roomHasLocksCache.put(requestRoomId, true)
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
            lockerStore.updateLocker(serverLocker)

            val sessionIds = lookupSessions(requestRoomId)
            for (sessionId in sessionIds) {
                val gatewayServiceRpc = sessionGatewayDiscovery.findServer(sessionId)
                if (gatewayServiceRpc == null) {
                    gatewayLookupFailureCounter.increment()
                    return@runOnDispatcher PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UNKNOWN_ERROR)
                }

                val postEventResponse = gatewayServiceRpc.postEvent(PostEventRequest {
                    event {
                        roomId = requestRoomId
                        eventId = requestEventId
                        locker {
                            locker = updatedLocker
                            lockerId = requestLockerId
                            version = updatedLockerVersion
                            lockState = effectiveState
                        }
                        notification {
                            push = request.notification?.push
                            payload = request.notification?.payload
                        }
                    }
                    sessionIds {
                        addSessionId {
                            rawValue = sessionId.rawValue
                        }
                    }
                })

                if (!postEventResponse.result.isOk()) {
                    postEventFailureCounter.increment()
                    return@runOnDispatcher PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UNKNOWN_ERROR)
                } else {
                    postEventSuccessCounter.increment()
                }
            }

            // let the pluggable agent derive additional lockers; write + broadcast them.
            // Keyspaces stay opaque to the core — the agent decides what to act on.
            val frameLockers = agentRegistry.processPayload(requestRoomId, requestLockerId, updatedLocker)

            for (frameLocker in frameLockers) {
                val existingDerivedLocker = lockerStore.getLocker(
                    ServerRoomId(requestRoomId.rawValue),
                    frameLocker.lockerId.keyspace?.value ?: 0L,
                    ServerLockerId(frameLocker.lockerId.rawValue)
                )
                val frameLockerVersion = (existingDerivedLocker?.version ?: 0L) + 1

                lockerStore.updateLocker(ServerLocker {
                    lockerId = ServerLockerId(frameLocker.lockerId.rawValue)
                    roomId = ServerRoomId(requestRoomId.rawValue)
                    locker = frameLocker.locker.toByteArray()
                    keyspace = frameLocker.lockerId.keyspace?.value ?: 0L
                    version = frameLockerVersion
                })

                val frameEventId = EventId(Random.nextBytes(32))
                for (sessionId in sessionIds) {
                    val gatewayServiceRpc = sessionGatewayDiscovery.findServer(sessionId) ?: continue
                    gatewayServiceRpc.postEvent(PostEventRequest {
                        event {
                            roomId = requestRoomId
                            eventId = frameEventId
                            locker {
                                locker = frameLocker.locker
                                lockerId = frameLocker.lockerId
                                version = frameLockerVersion
                            }
                        }
                        sessionIds {
                            addSessionId { rawValue = sessionId.rawValue }
                        }
                    })
                }
            }

            PostLockerChangeResponse {
                result = PostLockerChangeResponse.Result.OK
                version = updatedLockerVersion
                lockState = effectiveState
            }
        }
    }

    override suspend fun deleteLocker(
        context: GrpcRequestContext,
        request: DeleteLockerRequest
    ) = meterRegistry.trackResponse("lockers.room.locker.deletelocker", DeleteLockerResponse::result) {
        val requestRoomId = request.roomId ?: return@trackResponse DeleteLockerResponse(result = DeleteLockerResponse.Result.UNKNOWN_ERROR)
        val requestEventId = EventId(Random.nextBytes(32))
        val requestLockerId = request.lockerId ?: return@trackResponse DeleteLockerResponse(result = DeleteLockerResponse.Result.UNKNOWN_ERROR)
        val requestVersion = request.parentVersion

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
                return@runOnDispatcher DeleteLockerResponse {
                    result = DeleteLockerResponse.Result.UPDATE_LOCAL_VERSION
                    version = storedLocker.version
                    existingLocker = Locker.fromByteArray(storedLocker.locker)
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

            lockerStore.deleteLocker(
                ServerRoomId(requestRoomId.rawValue),
                (requestLockerId.keyspace?.value ?: 0L),
                ServerLockerId(requestLockerId.rawValue)
            )

            val sessionIds = lookupSessions(requestRoomId)

            // notify change event
            for (sessionId in sessionIds) {
                val gatewayServiceRpc = sessionGatewayDiscovery.findServer(sessionId)
                if (gatewayServiceRpc == null) {
                    gatewayLookupFailureCounter.increment()
                    return@runOnDispatcher DeleteLockerResponse(result = DeleteLockerResponse.Result.UNKNOWN_ERROR)
                }

                val postEventResponse = gatewayServiceRpc.postEvent(PostEventRequest {
                    event {
                        roomId = requestRoomId
                        eventId = requestEventId
                        locker {
                            lockerId = requestLockerId
                            version = updatedLockerVersion
                        }
                        notification {
                            push = request.notification?.push
                            payload = request.notification?.payload
                        }
                    }
                    sessionIds {
                        addSessionId {
                            rawValue = sessionId.rawValue
                        }
                    }
                })

                if (!postEventResponse.result.isOk()) {
                    postEventFailureCounter.increment()
                    return@runOnDispatcher DeleteLockerResponse(result = DeleteLockerResponse.Result.UNKNOWN_ERROR)
                } else {
                    postEventSuccessCounter.increment()
                }
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
    ) = meterRegistry.trackResponse("lockers.room.locker.lock", LockLockerResponse::result) {
        val requestRoomId = request.roomId ?: return@trackResponse LockLockerResponse(result = LockLockerResponse.Result.UNKNOWN_ERROR)
        val grant = request.grant ?: return@trackResponse LockLockerResponse(result = LockLockerResponse.Result.UNKNOWN_ERROR)

        return@trackResponse dispatchers.runOnDispatcher(requestRoomId) {
            when (val outcome = lockVerifier.applyLock(requestRoomId, grant, request.parentLockVersion)) {
                is LockVerifier.LockOutcome.Ok -> {
                    roomHasLocksCache.put(requestRoomId, true)
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
    ) = meterRegistry.trackResponse("lockers.room.locker.unlock", UnlockLockerResponse::result) {
        val requestRoomId = request.roomId ?: return@trackResponse UnlockLockerResponse(result = UnlockLockerResponse.Result.UNKNOWN_ERROR)
        val scope = request.scope ?: return@trackResponse UnlockLockerResponse(result = UnlockLockerResponse.Result.UNKNOWN_ERROR)

        return@trackResponse dispatchers.runOnDispatcher(requestRoomId) {
            val outcome = lockVerifier.applyUnlock(requestRoomId, scope, request.signature, request.parentLockVersion)
            when (outcome) {
                is LockVerifier.UnlockOutcome.Ok -> {
                    roomHasLocksCache.put(requestRoomId, lockVerifier.roomHasLocks(requestRoomId))
                    UnlockLockerResponse(result = UnlockLockerResponse.Result.OK)
                }
                is LockVerifier.UnlockOutcome.Stale -> UnlockLockerResponse(result = UnlockLockerResponse.Result.UPDATE_LOCAL_VERSION)
                is LockVerifier.UnlockOutcome.SignatureInvalid -> UnlockLockerResponse(result = UnlockLockerResponse.Result.SIGNATURE_INVALID)
            }
        }
    }

    fun close() {
        dispatchers.close()
    }
}
