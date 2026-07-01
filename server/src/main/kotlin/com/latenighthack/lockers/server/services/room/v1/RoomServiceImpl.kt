package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktbuf.net.ServerDescriptor
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.ServerCore
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
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
    private val sessionGatewayDiscovery: SessionGatewayDiscovery,
    private val agentRegistry: LockerAgentRegistry,
    private val meterRegistry: MeterRegistry,
    private val roomShardCount: Int = Runtime.getRuntime().availableProcessors() * 4,
    private val sessionCacheSize: Long = 1000
) : BaseServiceImpl(), RoomServer {
    private val dispatchers = ShardedDispatcher<RoomId>(roomShardCount, "room-shard") {
        it.rawValue.contentHashCode()
    }
    
    private val gatewayLookupFailureCounter = meterRegistry.counter("fullhouse.room.gateway.lookup.failures")
    private val postEventSuccessCounter = meterRegistry.counter("fullhouse.room.events.post.success")
    private val postEventFailureCounter = meterRegistry.counter("fullhouse.room.events.post.failure")
    private val cacheHitCounter = meterRegistry.counter("fullhouse.room.cache.hits")
    private val cacheMissCounter = meterRegistry.counter("fullhouse.room.cache.misses")
    private val dispatcherWaitTimer = meterRegistry.timer("fullhouse.room.dispatcher.time")
    private val getLockerTimer = meterRegistry.timer("fullhouse.room.locker.get.time")
    private val getAllLockersTimer = meterRegistry.timer("fullhouse.room.locker.getall.time")
    private val lockersReturnedSummary = meterRegistry.summary("fullhouse.room.locker.count")

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
        .maximumCacheSize(sessionCacheSize)
        .build()
    
    private val cacheSizeGauge = meterRegistry.gauge("fullhouse.room.cache.size", roomToSessionCache) { it.asMap().size.toDouble() }

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
    ) = meterRegistry.trackResponse("fullhouse.room.locker.subscribe", SubscriptionResponse::result) {
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
                    meterRegistry.counter("fullhouse.room.subscriptions", "operation", "unsubscribe", "result", "OK").increment()
                    subscriptionStore.removeSubscription(ServerSessionId(sessionId.rawValue), ServerRoomId(roomId.rawValue))
                    cachedSet - sessionId
                }
                null -> {
                    meterRegistry.counter("fullhouse.room.subscriptions", "operation", "unknown", "result", "ERROR").increment()
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
    ) = meterRegistry.trackResponse("fullhouse.room.locker.get", GetLockerResponse::result) {
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
        GetLockerResponse {
            result = GetLockerResponse.Result.OK
            locker {
                this.lockerId = lockerId
                locker = lockerPayload
                version = storedLocker.version
            }
        }
    }

    override suspend fun getAllLockers(
        context: GrpcRequestContext,
        request: GetAllLockersRequest
    ) = meterRegistry.trackResponse("fullhouse.room.locker.getall", GetAllLockersResponse::result) {
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

        return@trackResponse GetAllLockersResponse {
            result = GetAllLockersResponse.Result.OK
            lockers {
                for (storedLocker in storedLockers) {
                    addIdentifiedLocker {
                        locker = Locker.fromByteArray(storedLocker.locker)
                        lockerId = LockerId(
                            rawValue = storedLocker.lockerId?.rawValue!!,
                            keyspace = LockerKeyspace { value = storedLocker.keyspace }
                        )
                    }
                }
            }
        }
    }

    override suspend fun postLockerChange(
        context: GrpcRequestContext,
        request: PostLockerChangeRequest
    ) = meterRegistry.trackResponse("fullhouse.room.locker.postlockerchange", PostLockerChangeResponse::result) {
        val requestRoomId = request.roomId ?: return@trackResponse PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UNKNOWN_ERROR)
        val requestEventId = EventId(Random.nextBytes(32))
        val updatedLocker = request.locker ?: return@trackResponse PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UNKNOWN_ERROR)
        val requestLockerId = request.lockerId ?: return@trackResponse PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UNKNOWN_ERROR)
        val requestVersion = request.parentVersion

        val startTime = System.nanoTime()
        return@trackResponse dispatchers.runOnDispatcher(requestRoomId) {
            dispatcherWaitTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)

            val storedLocker = lockerStore.getLocker(
                ServerRoomId(requestRoomId.rawValue),
                (requestLockerId.keyspace?.value ?: 0L),
                ServerLockerId(requestLockerId.rawValue)
            )

            val updatedLockerVersion = if (storedLocker == null) {
                requestVersion
            } else if (storedLocker.version == requestVersion) {
                storedLocker.version + 1
            } else {
                return@runOnDispatcher PostLockerChangeResponse {
                    result = PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION
                    version = storedLocker.version
                    existingLocker = Locker.fromByteArray(storedLocker.locker)
                }
            }

            val serverLocker = ServerLocker {
                lockerId = ServerLockerId(requestLockerId.rawValue)
                roomId = ServerRoomId(requestRoomId.rawValue)
                locker = updatedLocker.toByteArray()
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
            }
        }
    }

    override suspend fun deleteLocker(
        context: GrpcRequestContext,
        request: DeleteLockerRequest
    ) = meterRegistry.trackResponse("fullhouse.room.locker.deletelocker", DeleteLockerResponse::result) {
        val requestRoomId = request.roomId ?: return@trackResponse DeleteLockerResponse(result = DeleteLockerResponse.Result.UNKNOWN_ERROR)
        val requestEventId = EventId(Random.nextBytes(32))
        val requestLockerId = request.lockerId ?: return@trackResponse DeleteLockerResponse(result = DeleteLockerResponse.Result.UNKNOWN_ERROR)
        val requestVersion = request.parentVersion

        val startTime = System.nanoTime()
        return@trackResponse dispatchers.runOnDispatcher(requestRoomId) {
            dispatcherWaitTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)

            val storedLocker = lockerStore.getLocker(
                ServerRoomId(requestRoomId.rawValue),
                (requestLockerId.keyspace?.value ?: 0L),
                ServerLockerId(requestLockerId.rawValue)
            )

            val updatedLockerVersion = if (storedLocker == null) {
                requestVersion
            } else if (storedLocker.version == requestVersion) {
                storedLocker.version + 1
            } else {
                return@runOnDispatcher DeleteLockerResponse {
                    result = DeleteLockerResponse.Result.UPDATE_LOCAL_VERSION
                    version = storedLocker.version
                    existingLocker = Locker.fromByteArray(storedLocker.locker)
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
            }
        }
    }
}
