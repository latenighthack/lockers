package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktbuf.net.ServerDescriptor
import com.latenighthack.ktbuf.net.StreamControlEvent
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.broadcast.v1.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.push.v1.SendPushRequest
import com.latenighthack.lockers.server.ServerCore
import com.latenighthack.lockers.server.services.push.v1.PushGatewayDiscovery
import com.latenighthack.lockers.server.services.push.v1.PushServiceImpl.Companion.ADMIN_TOKEN_HEADER
import java.util.concurrent.TimeUnit
import com.latenighthack.lockers.server.tools.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.MeterRegistry
import com.latenighthack.lockers.observability.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import me.tatarka.inject.annotations.Component
import me.tatarka.inject.annotations.Inject
import me.tatarka.inject.annotations.Provides
import org.slf4j.LoggerFactory
import com.latenighthack.lockers.server.LockersConfig
import com.latenighthack.lockers.server.ProtocolValidation
import com.latenighthack.lockers.server.ResourceLimitException
import java.security.SignatureException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicInteger as AtomicInt
import kotlin.random.Random

@ServiceScope
@Component
abstract class SessionServiceModule(
    @Component val serverCore: ServerCore,
    @get:Provides val pushGatewayDiscovery: PushGatewayDiscovery,
    @get:Provides val sessionOwnership: SessionOwnership,
    @get:Provides val sessionRegistry: SessionRegistry,
): GrpcRouteProvider<SessionServer> {
    abstract val serverImpl: SessionServiceImpl

    override val server: SessionServer get() = AuthorizedSessionServer(serverImpl, serverCore.sessionProofVerifier)
    override val descriptor: ServerDescriptor = SessionServer.Descriptor
}

@Component
abstract class SessionGatewayServiceModule(
    @Component val serverCore: ServerCore,
    @Component val sessionServiceModule: SessionServiceModule
): GrpcRouteProvider<SessionGatewayServer> {
    override val server: SessionGatewayServer get() = sessionServiceModule.serverImpl
    override val descriptor: ServerDescriptor = SessionGatewayServer.Descriptor
}

@Component
abstract class BroadcastAdminServiceModule(
    @Component val serverCore: ServerCore,
    @Component val sessionServiceModule: SessionServiceModule
): GrpcRouteProvider<BroadcastAdminServer> {
    override val server: BroadcastAdminServer get() = sessionServiceModule.serverImpl
    override val descriptor: ServerDescriptor = BroadcastAdminServer.Descriptor
}

data class SessionGatewayGroup(val sessionIds: List<SessionId>, val service: SessionGatewayService)

interface SessionGatewayDiscovery {
    suspend fun findServer(sessionId: SessionId): SessionGatewayService?
    suspend fun resolveGroups(sessionIds: List<SessionId>): List<SessionGatewayGroup> =
        sessionIds.distinct().mapNotNull { id -> findServer(id)?.let { SessionGatewayGroup(listOf(id), it) } }
}

class LocalSessionGatewayDiscovery(private val sessionGatewayServer: SessionGatewayServer) : SessionGatewayDiscovery {
    override suspend fun resolveGroups(sessionIds: List<SessionId>) =
        listOf(SessionGatewayGroup(sessionIds.distinct(), LocalSessionGatewayServiceRpc(sessionGatewayServer)))
    override suspend fun findServer(sessionId: SessionId): SessionGatewayService? {
        return LocalSessionGatewayServiceRpc(sessionGatewayServer)
    }
}



private data class OnlineServerSessionEvent(val event: ServerSessionEvent? = null, val receiveTime: Long = 0L, val sequence: Long = 0L, val clientEvent: Event? = null)

private const val BROADCAST_EVENT_ID_BYTES = 16

@ServiceScope
@Inject
class SessionServiceImpl(
    private val sessionStore: SessionStore,
    private val sessionInboxStore: SessionInboxStore,
    private val meterRegistry: MeterRegistry,
    private val pushGatewayDiscovery: PushGatewayDiscovery,
    private val sessionOwnership: SessionOwnership,
    private val config: LockersConfig,
    private val sessionRegistry: SessionRegistry = SessionRegistry.Noop,
    private val pushDelivery: PushDeliveryStore? = null,
    private val telemetry: LockersTelemetry = LockersTelemetry.NONE,
    coroutineContext: kotlin.coroutines.CoroutineContext = kotlin.coroutines.EmptyCoroutineContext,
) : BaseServiceImpl(), SessionServer, SessionGatewayServer, BroadcastAdminServer {
    private val lifecycleStarted = java.util.concurrent.atomic.AtomicBoolean(false)
    private val lifecycleClosed = java.util.concurrent.atomic.AtomicBoolean(false)
    private val logger = LoggerFactory.getLogger(SessionServiceImpl::class.java)
    private val deliveryScope = CoroutineScope(coroutineContext + SupervisorJob(coroutineContext[Job]) + Dispatchers.IO + ServiceLifecycle.context)
    private val pushPermits = kotlinx.coroutines.sync.Semaphore(4)
    private val pushWorker = pushDelivery?.let { store ->
        com.latenighthack.lockers.server.services.room.v1.DeliveryWorker(store.outbox, object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService = object : SessionGatewayService {
                override suspend fun postEvent(request: PostEventRequest): PostEventResponse {
                    val gateway = pushGatewayDiscovery.findServer(sessionId) ?: error("push gateway unavailable")
                    val response = gateway.sendPush(SendPushRequest(sessionId, request.event?.notification?.push, request.event?.eventId?.rawValue ?: byteArrayOf()))
                    return PostEventResponse(result = if (response.result.isOk()) PostEventResponse.Result.OK else PostEventResponse.Result.UNKNOWN_ERROR)
                }
                override suspend fun postEvents(request: PostEventsRequest) = PostEventsResponse(request.groups.map { postEvent(it) })
            }
        }, meterRegistry, telemetry, "push_delivery", coroutineContext)
    }
    private val dispatchers = ShardedDispatcher<SessionId>(config.shardCount, "session-shard") {
        it.rawValue.contentHashCode()
    }
    private val openStreamCancellationChannels = ConcurrentHashMap<ServerSessionId, Channel<Unit>>()
    // Live delivery must never suspend the emitter: enqueueEvent runs on the session shard inside
    // the room shard's postLockerChange, so a stalled stream collector would wedge both shards for
    // every room hashed to them. Sequence gaps trigger immediate inbox catch-up; reconnect also
    // replays the durable inbox.
    private val eventSerial = java.util.concurrent.atomic.AtomicLong()
    private val incomingEvents = MutableSharedFlow<OnlineServerSessionEvent>(
        extraBufferCapacity = 1024,
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
    )
    
    private val activeStreamsCount = AtomicInt(0)
    private val activeStreamsGauge = meterRegistry.gauge("lockers.session.streams.active", activeStreamsCount) { it.get().toDouble() }
    private val eventsPostedCounter = meterRegistry.counter("lockers.session.events.posted")
    private val eventsDeliveredCounter = meterRegistry.counter("lockers.session.events.delivered", "delivery_type", "live")
    private val eventsDeliveredQueuedCounter = meterRegistry.counter("lockers.session.events.delivered", "delivery_type", "queued")
    private val eventsQueuedCounter = meterRegistry.counter("lockers.session.events.queued")
    private val streamCancellationCounter = meterRegistry.counter("lockers.session.streams.cancelled")
    private val inboxSavesCounter = meterRegistry.counter("lockers.session.inbox.saves")
    private val inboxDeletesCounter = meterRegistry.counter("lockers.session.inbox.deletes")
    private val eventsPreOpen = meterRegistry.counter("lockers.session.events.preopen")
    private val dispatcherWaitTimer = meterRegistry.timer("lockers.session.dispatcher.time")

    override suspend fun destroySession(
        context: GrpcRequestContext,
        request: DestroySessionRequest
    ): DestroySessionResponse = meterRegistry.trackRpc(TelemetryOperation.SESSION_DESTROY, telemetry, { rpcOutcome(it.result.toString()) }) { observedDestroySession(context, request) }

    private suspend fun observedDestroySession(
        context: GrpcRequestContext,
        request: DestroySessionRequest
    ): DestroySessionResponse {
        val id = request.sessionId ?: return DestroySessionResponse(result = DestroySessionResponse.Result.INVALID_SEQUENCE)
        if (id.rawValue.size !in 1..128) return DestroySessionResponse(result = DestroySessionResponse.Result.INVALID_SEQUENCE)
        val storedId = ServerSessionId(id.rawValue)
        sessionStore.destroySession(storedId)
        closeSessionStream(storedId)
        return DestroySessionResponse(result = DestroySessionResponse.Result.OK)
    }

    private data class OpenState(val sessionId: ServerSessionId, val open: WatchSessionResponse.Open)

    // Ends the response stream after a rejected open/create. Must NOT be a CancellationException:
    // cancelling the collector can abort the transport before the error response and close frame
    // are flushed, leaving the client hanging on a half-dead socket instead of seeing the result.
    private class StreamRejected : Exception()

    override fun watchSession(
        context: GrpcRequestContext,
        request: Flow<WatchSessionRequest>
    ): Flow<StreamControlEvent<WatchSessionResponse>> {
        return channelFlow {
        val cancellationChannel = Channel<Unit>(Channel.CONFLATED)
        val registeredSession = AtomicReference<ServerSessionId?>()
        val openState = CompletableDeferred<OpenState?>()
        val openSent = CompletableDeferred<Unit>()

        val producer = deliveryScope.launch(currentCoroutineContext().minusKey(Job) + ServiceLifecycle.context) {
        flow { coroutineScope {
        val streamJob = currentCoroutineContext()[Job]!!
        merge(flow {
            try {
                request.collectFirst({ openOrCreate ->
                // attempt to handle the open OR create
                val openBuilder = WatchSessionResponse_OpenBuilder()
                val sessionId = when (val oneOf = openOrCreate.request) {
                    is WatchSessionRequest.OneOfRequest.open -> openBuilder.handleOpen(oneOf.getOpen()!!)
                    is WatchSessionRequest.OneOfRequest.create -> openBuilder.handleCreate(oneOf.getCreate()!!)
                    else -> {
                        // First frame wasn't open/create (e.g. a client bug sent an ack first).
                        // Result must be explicit: the proto default is OK, and answering OK here
                        // made such a client believe it was connected to a stream that then closed.
                        openBuilder.result = WatchSessionResponse.Open.Result.UNKNOWN_ERROR
                        eventsPreOpen.increment()
                        null
                    }
                }

                if (sessionId == null) {
                    // The stream could not be opened: report the result, then end every merged
                    // branch NORMALLY so the transport flushes the response and closes cleanly.
                    emit(StreamControlEvent.Message(WatchSessionResponse {
                        response.open = openBuilder.build()
                    }))
                    emit(StreamControlEvent.Close())

                    openState.complete(null)
                    cancellationChannel.close()

                    throw StreamRejected()
                }

                val open = openBuilder.build()

                // Attachment must still belong to the accepted challenge. A later open may
                // already have rotated it, or destruction may have revoked the session.
                val attached = sessionStore.atomic(sessionId) {
                    val current = sessionStore.getSessionById(sessionId)
                    if (current == null || !current.nextKeyMaterial.contentEquals(open.nextSequenceKey)) {
                        openBuilder.result = WatchSessionResponse.Open.Result.INVALID_SEQUENCE
                        openBuilder.nextSequenceKey = current?.nextKeyMaterial ?: byteArrayOf()
                        false
                    } else {
                        openStreamCancellationChannels.put(sessionId, cancellationChannel)?.let {
                            streamCancellationCounter.increment()
                            it.trySend(Unit)
                        }
                        activeStreamsCount.incrementAndGet()
                        registeredSession.set(sessionId)
                        sessionRegistry.attachBeforeSnapshot(sessionId)
                        true
                    }
                }
                if (!attached) {
                    emit(StreamControlEvent.Message(WatchSessionResponse { response.open = openBuilder.build() }))
                    emit(StreamControlEvent.Close())
                    openState.complete(null)
                    cancellationChannel.close()
                    throw StreamRejected()
                }
                openState.complete(OpenState(sessionId, open))

                sessionId
            }) { sessionId, nextRequest ->
                // The opening snapshot precedes every subsequent response, including heartbeat.
                openSent.await()
                when (val message = nextRequest.request) {
                    // ping, pong rally's on
                    is WatchSessionRequest.OneOfRequest.ping -> {
                        emit(StreamControlEvent.Message(WatchSessionResponse {
                            response.pong {}
                        }))
                    }

                    // handle ack
                    is WatchSessionRequest.OneOfRequest.ack -> {
                        val ack = message.getAck()!!
                        val startTime = System.nanoTime()
                        val response = dispatchers.runOnDispatcher(SessionId(sessionId.rawValue)) {
                            dispatcherWaitTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)
                            sessionInboxStore.deleteEvents(ack.acks.mapNotNull { it.eventId?.let { id -> ServerEventId(id.rawValue) } }, sessionId)
                            inboxDeletesCounter.increment(ack.acks.size.toDouble())

                            WatchSessionResponse {
                                response.ack {
                                    confirmedAcks {
                                        for (requestedAck in ack.acks) {
                                            addEventAck {
                                                roomId = requestedAck.roomId
                                                eventId = requestedAck.eventId
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        emit(StreamControlEvent.Message(response))
                    }

                    // not allowed after initialization
                    is WatchSessionRequest.OneOfRequest.create -> {
                        emit(StreamControlEvent.Close())
                        awaitCancellation()
                    }
                    is WatchSessionRequest.OneOfRequest.open -> {
                        emit(StreamControlEvent.Close())
                        awaitCancellation()
                    }

                    // never allowed
                    null -> {
                        emit(StreamControlEvent.Close())
                        awaitCancellation()
                    }
                }
            }
            } catch (cancelled: CancellationException) {
                // A cancelled source must end every merge branch, including unopened waiters.
                streamJob.cancel(cancelled)
                throw cancelled
            } catch (e: StreamRejected) {
                // Rejection response and Close already emitted; end this branch normally.
            }
        }, cancellationChannel.receiveAsFlow().map {
            StreamControlEvent.Close()
        }, flow {
            val os = openState.await() ?: return@flow
            while (currentCoroutineContext().isActive) {
                delay(1000)
                if (sessionStore.getSessionById(os.sessionId) == null) {
                    cancellationChannel.trySend(Unit)
                    return@flow
                }
            }
        }, flow {
            val os = openState.await() ?: return@flow
            val sessionId = os.sessionId
            val originalOpen = os.open
            var isFirst = true
            var lastSequence = 0L
            // Keyed by List<Byte> — a Set<ByteArray> compares by reference and would never match,
            // re-delivering every open-queued event on the live path too.
            var storedQueuedEvents: Set<List<Byte>>? = null

            emitAll(incomingEvents
                .onSubscription {
                    emit(OnlineServerSessionEvent(sequence = eventSerial.get()))
                }
                .onEach {
                    if (!isFirst) {
                        if (it.sequence > lastSequence + 1) {
                            // A slow stream overran the bounded live buffer. Recover from the
                            // durable inbox immediately rather than waiting for reconnect.
                            val missed = sessionInboxStore.getAllClientEvents(sessionId)
                            if (missed.isNotEmpty()) emit(StreamControlEvent.Message(WatchSessionResponse {
                                response.events { event = missed }
                            }))
                            storedQueuedEvents = missed.mapNotNull { event -> event.eventId?.rawValue?.toList() }.toSet()
                        }
                        lastSequence = it.sequence
                        return@onEach
                    }

                    isFirst = false
                    lastSequence = it.sequence

                    val startTime = System.nanoTime()
                    val response = dispatchers.runOnDispatcher(SessionId(sessionId.rawValue)) {
                        dispatcherWaitTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)

                        val queuedEvents = sessionInboxStore.getAllClientEvents(sessionId)

                        storedQueuedEvents = queuedEvents.mapNotNull { it.eventId?.rawValue?.toList() }.toSet()

                        eventsDeliveredQueuedCounter.increment(queuedEvents.size.toDouble())

                        WatchSessionResponse {
                            response.open = originalOpen.copy(queuedEvents = queuedEvents)
                        }
                    }

                    emit(StreamControlEvent.Message(response))
                    openSent.complete(Unit)
                }
                .filter {
                    it.event?.sessionId?.rawValue.contentEquals(sessionId.rawValue)
                }
                .mapNotNull {
                    if (storedQueuedEvents?.contains(it.event?.eventId?.rawValue?.toList()) == true) {
                        return@mapNotNull null
                    }

                    meterRegistry.timer("lockers.session.events.delivery.time", "delivery_type", "live")
                        .record(System.nanoTime() - it.receiveTime, java.util.concurrent.TimeUnit.NANOSECONDS)
                    eventsDeliveredCounter.increment()

                    val clientEvent = requireNotNull(it.clientEvent)
                    StreamControlEvent.Message(WatchSessionResponse {
                        response.events {
                            event = listOf(clientEvent)
                        }
                    })
                }
            )
        }).onCompletion {
            registeredSession.getAndSet(null)?.let { sessionId ->
                activeStreamsCount.decrementAndGet()
                if (openStreamCancellationChannels.remove(sessionId, cancellationChannel)) {
                    sessionRegistry.detach(sessionId)
                }
            }
            cancellationChannel.close()
        }.collect { emit(it) }
        } }.collect { send(it) }
        }
        producer.invokeOnCompletion { close(it) }
        awaitClose { producer.cancel() }
        }
    }

    /** Immediately closes this node's stream; shared-store revocation also closes remote streams. */
    fun closeSessionStream(sessionId: ServerSessionId) {
        openStreamCancellationChannels[sessionId]?.trySend(Unit)
    }

    fun start() {
        check(!lifecycleClosed.get() && lifecycleStarted.compareAndSet(false, true)) { "Service already started or closed" }
        if (config.deliveryWorkerEnabled) pushWorker?.start()
    }
    suspend fun closeAndJoin() {
        ServiceLifecycle.requireExternalClose()
        lifecycleClosed.set(true)
        deliveryScope.coroutineContext[Job]!!.cancelAndJoin()
        pushWorker?.closeAndJoin()
        dispatchers.closeAndJoin()
    }
    fun close() = ServiceLifecycle.blockingClose { closeAndJoin() }

    override suspend fun postEvents(context: GrpcRequestContext, request: PostEventsRequest): PostEventsResponse =
        meterRegistry.trackRpc(TelemetryOperation.SESSION_POST_MANY, telemetry, { if (it.results.all { result -> result.result.isOk() }) TelemetryOutcome.OK else TelemetryOutcome.REJECTED }) { observedPostEvents(context, request) }

    private suspend fun observedPostEvents(context: GrpcRequestContext, request: PostEventsRequest): PostEventsResponse {
        if (request.groups.size > 64 || request.groups.sumOf { it.sessionIds.size.toLong() } > ProtocolValidation.MAX_RECIPIENTS ||
            request.toByteArray().size > ProtocolValidation.MAX_ENVELOPE_BYTES) {
            throw com.latenighthack.ktbuf.net.RpcResponseException("", "RPC", com.latenighthack.ktbuf.proto.Codes.INVALID_ARGUMENT, "Invalid gateway batch")
        }
        return PostEventsResponse(request.groups.map { postEvent(context, it) })
    }

    override suspend fun postEvent(context: GrpcRequestContext, request: PostEventRequest) = meterRegistry.trackResponse("lockers.session.post", PostEventResponse::result, telemetry) {
        if (request.sessionIds.size > ProtocolValidation.MAX_RECIPIENTS || request.sessionIds.any { !ProtocolValidation.identity(it.rawValue) } ||
            request.toByteArray().size > ProtocolValidation.MAX_ENVELOPE_BYTES || !ProtocolValidation.event(request.event))
            return@trackResponse PostEventResponse(result = PostEventResponse.Result.INVALID)
        try {
            val accepted = enqueueEvent(request.sessionIds, requireNotNull(request.event))
            val moved = sessionRegistry.remoteSessions(accepted)
            PostEventResponse(result = if (moved.isEmpty()) PostEventResponse.Result.OK else PostEventResponse.Result.RETRY_ROUTING)
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (rejected: GatewayRejected) { PostEventResponse(result = rejected.result) }
        catch (_: ResourceLimitException) { PostEventResponse(result = PostEventResponse.Result.RESOURCE_EXHAUSTED) }
    }

    override suspend fun broadcast(context: GrpcRequestContext, request: BroadcastRequest): BroadcastResponse {
        authorizeAdmin(context)

        return meterRegistry.trackResponse("lockers.session.broadcast", BroadcastResponse::result, telemetry) {
            val targets = when (val kind = request.target?.kind) {
                is BroadcastTarget.OneOfKind.all ->
                    sessionStore.getAllSessions().mapNotNull { session ->
                        session.sessionId?.rawValue?.let { SessionId { rawValue = it } }
                    }

                is BroadcastTarget.OneOfKind.sessions ->
                    kind.getSessions()?.sessionIds.orEmpty()

                else -> emptyList()
            }

            // A broadcast is an Event with no room association (empty room_id), given
            // a fresh event id so it dedups/acks per session like any other event.
            val event = Event {
                eventId { rawValue = Random.nextBytes(BROADCAST_EVENT_ID_BYTES) }
                roomId { rawValue = byteArrayOf() }
                request.notification?.let { this.notification = it }
            }

            val delivered = enqueueEvent(targets, event)

            return@trackResponse BroadcastResponse {
                result = BroadcastResponse.Result.OK
                this.delivered = delivered.size.toLong()
            }
        }
    }

    // Fan one event into each target session: fire a per-session push (when the
    // event carries one), persist to the session inbox and emit live. Shared by
    // room-scoped posts and server-wide broadcasts; returns the count enqueued.
    private class GatewayRejected(val result: PostEventResponse.Result) : RuntimeException()
    private suspend fun enqueueEvent(sessionIds: List<SessionId>, event: Event): List<SessionId> {
        val receiveTime = System.nanoTime()
        val requestPush = event.notification?.push
        val roomIdRaw = event.roomId?.rawValue ?: byteArrayOf()
        val eventIdRaw = event.eventId?.rawValue ?: byteArrayOf()
        val encodedPayload = event.notification?.payload?.rawValue ?: byteArrayOf()
        val encodedLocker = event.locker?.toByteArray() ?: byteArrayOf()

        if (sessionIds.size > ProtocolValidation.MAX_RECIPIENTS || sessionIds.any { !ProtocolValidation.identity(it.rawValue) } ||
            !ProtocolValidation.event(event)) throw GatewayRejected(PostEventResponse.Result.INVALID)
        if (sessionIds.isEmpty()) return emptyList()
        val recipients = sessionStore.atomic(ServerSessionId(sessionIds.first().rawValue)) {
            val accepted = sessionIds.distinct().filter { id ->
                val sid = ServerSessionId(id.rawValue)
                if (sessionStore.isRevoked(sid)) false
                else if (sessionStore.getSessionById(sid) == null) throw GatewayRejected(PostEventResponse.Result.UNKNOWN_SESSION)
                else true
            }
            val events = accepted.map { sessionId ->
                ServerSessionEvent(ServerSessionId(sessionId.rawValue), ServerRoomId(roomIdRaw),
                    ServerEventId(eventIdRaw), encodedPayload, encodedLocker, event.roomSequence)
            }
            if (accepted.isNotEmpty()) {
                if (requestPush != null && pushDelivery != null) {
                    val room = event.roomId ?: RoomId(byteArrayOf())
                    pushDelivery.outbox.commit(room, accepted, listOf(event.copy(locker = null))) {
                        sessionInboxStore.saveClientEvents(events.map { it to event })
                    }
                } else sessionInboxStore.saveClientEvents(events.map { it to event })
            }
            accepted
        }
        val events = recipients.map { sessionId -> ServerSessionEvent(ServerSessionId(sessionId.rawValue), ServerRoomId(roomIdRaw),
            ServerEventId(eventIdRaw), encodedPayload, encodedLocker, event.roomSequence) }
        inboxSavesCounter.increment(events.size.toDouble())
        eventsPostedCounter.increment(events.size.toDouble())
        eventsQueuedCounter.increment(events.size.toDouble())
        events.forEach { incomingEvents.emit(OnlineServerSessionEvent(it, receiveTime, eventSerial.incrementAndGet(), event)) }
        if (requestPush != null && pushDelivery == null) recipients.forEach { sessionId ->
            deliveryScope.launch {
                pushPermits.acquire()
                try {
                    pushGatewayDiscovery.findServer(sessionId)?.sendPush(SendPushRequest {
                        this.sessionId = sessionId
                        push = requestPush
                        deliveryId = eventIdRaw
                    })
                } catch (e: CancellationException) { throw e }
                catch (e: Exception) { logger.warn("push enqueue failed after durable inbox acceptance", e) }
                finally { pushPermits.release() }
            }
        }

        return recipients
    }

    /**
     * Defense in depth on top of binding the broadcast admin service to the internal
     * port: when an admin token is configured, the call must present it as the
     * [ADMIN_TOKEN_HEADER]. In-process callers (tests) with no token configured pass.
     */
    private fun authorizeAdmin(context: GrpcRequestContext) {
        val required = config.adminToken ?: return
        val presented = context.headers.entries
            .firstOrNull { it.key.equals(ADMIN_TOKEN_HEADER, ignoreCase = true) }
            ?.value
        if (presented != required) {
            throw SecurityException("broadcast admin: missing or invalid $ADMIN_TOKEN_HEADER")
        }
    }

    private suspend fun WatchSessionResponse_OpenBuilder.handleOpen(open: WatchSessionRequest.Open): ServerSessionId? = meterRegistry.trackRpc(TelemetryOperation.SESSION_OPEN, telemetry, { if (it == null) TelemetryOutcome.REJECTED else TelemetryOutcome.OK }) { observedHandleOpen(open) }

    private suspend fun WatchSessionResponse_OpenBuilder.observedHandleOpen(open: WatchSessionRequest.Open): ServerSessionId? {
        val serverSessionId = open.sessionId?.rawValue?.let { ServerSessionId(it) }
        val requestSequenceSignature = open.sequenceKeySignature?.signature

        if (requestSequenceSignature == null) {
            result = WatchSessionResponse.Open.Result.INVALID_SEQUENCE
            meterRegistry.counter("lockers.session.opens", "result", "INVALID_SEQUENCE").increment()
            return null
        }

        if (serverSessionId == null || !ProtocolValidation.identity(serverSessionId.rawValue)) {
            result = WatchSessionResponse.Open.Result.INVALID_SESSION_ID
            meterRegistry.counter("lockers.session.opens", "result", "INVALID_SESSION_ID").increment()
            return null
        }

        val session = sessionStore.getSessionById(serverSessionId)

        if (session == null) {
            result = WatchSessionResponse.Open.Result.UNKNOWN_SESSION
            meterRegistry.counter("lockers.session.opens", "result", "UNKNOWN_SESSION").increment()
            return null
        }

        // Gate the open to the node that owns this session's shard on the session ring. On a
        // non-owner node, reject with EPOCH_STALE + a redirect to the owner (rather than rotating
        // the sequence key here) so the client's reconnect loop re-resolves and opens against the
        // owner. In a monolith LocalSessionOwnership always returns Local, so this never fires.
        when (val owner = sessionOwnership.resolve(SessionId(serverSessionId.rawValue))) {
            is SessionOwner.Local -> {}
            is SessionOwner.Remote -> {
                result = WatchSessionResponse.Open.Result.EPOCH_STALE
                redirect = ShardRedirect {
                    ownerAddress = owner.address
                    epoch = owner.epoch
                }
                meterRegistry.counter("lockers.session.opens", "result", "EPOCH_STALE").increment()
                return null
            }
        }

        if (!ProtocolValidation.publicKey(session.authorizedPublicKey)) {
            result = WatchSessionResponse.Open.Result.INVALID_PUBLIC_KEY
            return null
        }
        try {
            val authorizedPublicKey = Secp256r1PublicKey.decode(session.authorizedPublicKey)
            if (!authorizedPublicKey.verify(session.nextKeyMaterial, requestSequenceSignature)) {
                result = WatchSessionResponse.Open.Result.INVALID_SEQUENCE
                // Recovery contract: hand back the material the client must sign. It is a nonce
                // challenge — useless without the session's private key — and without it a client
                // whose stored sequence bytes desynced (e.g. killed between our rotation and its
                // persist) is locked out of the session permanently.
                nextSequenceKey = session.nextKeyMaterial
                meterRegistry.counter("lockers.session.opens", "result", "INVALID_SEQUENCE").increment()
                return null
            }
            val updatedSession = session.copy(nextKeyMaterial = Random.nextBytes(32))

            if (!sessionStore.rotateIfCurrent(session, updatedSession)) {
                result = WatchSessionResponse.Open.Result.INVALID_SEQUENCE
                nextSequenceKey = sessionStore.getSessionById(serverSessionId)?.nextKeyMaterial ?: byteArrayOf()
                meterRegistry.counter("lockers.session.opens", "result", "INVALID_SEQUENCE").increment()
                return null
            }

            result = WatchSessionResponse.Open.Result.OK
            nextSequenceKey = updatedSession.nextKeyMaterial
            meterRegistry.counter("lockers.session.opens", "result", "OK").increment()
        } catch (signatureException: SignatureException) {
            result = WatchSessionResponse.Open.Result.INVALID_SEQUENCE
            nextSequenceKey = session.nextKeyMaterial
            meterRegistry.counter("lockers.session.opens", "result", "INVALID_SEQUENCE").increment()
            return null
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (ex: Exception) {
            logger.warn("session open failed unexpectedly", ex)
            result = WatchSessionResponse.Open.Result.UNKNOWN_ERROR
            meterRegistry.counter("lockers.session.opens", "result", "UNKNOWN_ERROR").increment()
            return null
        }

        return serverSessionId
    }

    private suspend fun WatchSessionResponse_OpenBuilder.handleCreate(create: WatchSessionRequest.Create): ServerSessionId? = meterRegistry.trackRpc(TelemetryOperation.SESSION_CREATE, telemetry, { if (it == null) TelemetryOutcome.REJECTED else TelemetryOutcome.OK }) { observedHandleCreate(create) }

    private suspend fun WatchSessionResponse_OpenBuilder.observedHandleCreate(create: WatchSessionRequest.Create): ServerSessionId? {
        val serverSessionId = create.sessionId?.rawValue?.let { ServerSessionId(it) }
        val requestPublicKey = create.publicKey?.rawValue

        if (!ProtocolValidation.publicKey(requestPublicKey)) {
            result = WatchSessionResponse.Open.Result.INVALID_PUBLIC_KEY
            meterRegistry.counter("lockers.session.creates", "result", "INVALID_PUBLIC_KEY").increment()
            return null
        }

        if (serverSessionId == null || !ProtocolValidation.identity(serverSessionId.rawValue)) {
            result = WatchSessionResponse.Open.Result.INVALID_SESSION_ID
            meterRegistry.counter("lockers.session.creates", "result", "INVALID_SESSION_ID").increment()
            return null
        }

        val session = sessionStore.getSessionById(serverSessionId)

        if (session != null) {
            result = WatchSessionResponse.Open.Result.SESSION_EXISTS
            meterRegistry.counter("lockers.session.creates", "result", "SESSION_EXISTS").increment()
            return null
        }

        try {
            val updatedSession = ServerSession {
                sessionId = serverSessionId
                nextKeyMaterial = Random.nextBytes(32)
                authorizedPublicKey = requireNotNull(requestPublicKey)
            }
            when (sessionStore.admitIfAbsent(updatedSession, config.resourceLimits)) {
                SessionAdmission.EXHAUSTED -> { result = WatchSessionResponse.Open.Result.RESOURCE_EXHAUSTED; return null }
                SessionAdmission.EXISTS -> {
                result = WatchSessionResponse.Open.Result.SESSION_EXISTS
                meterRegistry.counter("lockers.session.creates", "result", "SESSION_EXISTS").increment()
                return null
                }
                SessionAdmission.CREATED -> Unit
            }
            result = WatchSessionResponse.Open.Result.OK
            nextSequenceKey = updatedSession.nextKeyMaterial
            meterRegistry.counter("lockers.session.creates", "result", "OK").increment()
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (ex: Exception) {
            logger.warn("session create failed unexpectedly", ex)
            result = if (ex is UnsupportedOperationException) WatchSessionResponse.Open.Result.UPGRADE_REQUIRED
                else WatchSessionResponse.Open.Result.UNKNOWN_ERROR
            meterRegistry.counter("lockers.session.creates", "result", result.toString()).increment()
            return null
        }

        return serverSessionId
    }
}
