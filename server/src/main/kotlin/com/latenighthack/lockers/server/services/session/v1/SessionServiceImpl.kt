package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktbuf.net.ServerDescriptor
import com.latenighthack.ktbuf.net.StreamControlEvent
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.push.v1.SendPushRequest
import com.latenighthack.lockers.server.ServerCore
import com.latenighthack.lockers.server.services.push.v1.PushGatewayDiscovery
import com.latenighthack.lockers.server.tools.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import me.tatarka.inject.annotations.Component
import me.tatarka.inject.annotations.Inject
import me.tatarka.inject.annotations.Provides
import org.slf4j.LoggerFactory
import com.latenighthack.lockers.server.LockersConfig
import java.security.SignatureException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger as AtomicInt
import kotlin.random.Random

@ServiceScope
@Component
abstract class SessionServiceModule(
    @Component val serverCore: ServerCore,
    @get:Provides val pushGatewayDiscovery: PushGatewayDiscovery
): GrpcRouteProvider<SessionServer> {
    abstract val serverImpl: SessionServiceImpl

    override val server: SessionServer get() = serverImpl
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

interface SessionGatewayDiscovery {
    suspend fun findServer(sessionId: SessionId): SessionGatewayService?
}

class LocalSessionGatewayDiscovery(private val sessionGatewayServer: SessionGatewayServer) : SessionGatewayDiscovery {
    override suspend fun findServer(sessionId: SessionId): SessionGatewayService? {
        return LocalSessionGatewayServiceRpc(sessionGatewayServer)
    }
}



private data class OnlineServerSessionEvent(val event: ServerSessionEvent? = null, val receiveTime: Long = 0L)

@ServiceScope
@Inject
class SessionServiceImpl(
    private val sessionStore: SessionStore,
    private val sessionInboxStore: SessionInboxStore,
    private val meterRegistry: MeterRegistry,
    private val pushGatewayDiscovery: PushGatewayDiscovery,
    private val config: LockersConfig,
) : BaseServiceImpl(), SessionServer, SessionGatewayServer {
    private val logger = LoggerFactory.getLogger(SessionServiceImpl::class.java)
    private val dispatchers = ShardedDispatcher<SessionId>(config.shardCount, "session-shard") {
        it.rawValue.contentHashCode()
    }
    private val openStreamCancellationChannels = ConcurrentHashMap<ServerSessionId, Channel<Unit>>()
    private val incomingEvents = MutableSharedFlow<OnlineServerSessionEvent>()
    
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
    ): DestroySessionResponse {
        return DestroySessionResponse {
            result = DestroySessionResponse.Result.OK
        }
    }

    private data class OpenState(val sessionId: ServerSessionId, val open: WatchSessionResponse.Open)

    override fun watchSession(
        context: GrpcRequestContext,
        request: Flow<WatchSessionRequest>
    ): Flow<StreamControlEvent<WatchSessionResponse>> {
        val cancellationChannel = Channel<Unit>()
        val openState = CompletableDeferred<OpenState>()

        return merge(flow {
            request.collectFirst({ openOrCreate ->
                // attempt to handle the open OR create
                val openBuilder = WatchSessionResponse_OpenBuilder()
                val sessionId = when (val oneOf = openOrCreate.request) {
                    is WatchSessionRequest.OneOfRequest.open -> openBuilder.handleOpen(oneOf.getOpen()!!)
                    is WatchSessionRequest.OneOfRequest.create -> openBuilder.handleCreate(oneOf.getCreate()!!)
                    else -> {
                        eventsPreOpen.increment()
                        null
                    }
                }

                if (sessionId == null) {
                    // the stream could not be opened, close the socket
                    emit(StreamControlEvent.Message(WatchSessionResponse {
                        response.open = openBuilder.build()
                    }))
                    emit(StreamControlEvent.Close())

                    currentCoroutineContext().cancel()

                    return@collectFirst null
                }

                val open = openBuilder.build()

                openStreamCancellationChannels.put(sessionId, cancellationChannel)?.let {
                    streamCancellationCounter.increment()
                    it.send(Unit)
                }

                activeStreamsCount.incrementAndGet()
                openState.complete(OpenState(sessionId, open))

                sessionId
            }) { sessionId, nextRequest ->
                when (val message = nextRequest.request) {
                    // ping, pong rally's on
                    is WatchSessionRequest.OneOfRequest.ping -> {
                        StreamControlEvent.Message(WatchSessionResponse {
                            response.pong {}
                        })
                    }

                    // handle ack
                    is WatchSessionRequest.OneOfRequest.ack -> {
                        val ack = message.getAck()!!
                        val startTime = System.nanoTime()
                        val response = dispatchers.runOnDispatcher(SessionId(sessionId.rawValue)) {
                            dispatcherWaitTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)
                            for (eventAck in ack.acks) {
                                sessionInboxStore.deleteEvent(ServerEventId(eventAck.eventId!!.rawValue), sessionId)
                                inboxDeletesCounter.increment()
                            }

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
        }, cancellationChannel.receiveAsFlow().map {
            StreamControlEvent.Close()
        }, flow {
            val os = openState.await()
            val sessionId = os.sessionId
            val originalOpen = os.open
            var isFirst = true
            var storedQueuedEvents: Set<ByteArray>? = null

            emitAll(incomingEvents
                .onStart {
                    emit(OnlineServerSessionEvent())
                }
                .onEach {
                    if (!isFirst) {
                        return@onEach
                    }

                    isFirst = false

                    val startTime = System.nanoTime()
                    val response = dispatchers.runOnDispatcher(SessionId(sessionId.rawValue)) {
                        dispatcherWaitTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)

                        val queuedEvents = sessionInboxStore.getAllEvents(sessionId).map {
                            Event {
                                eventId { rawValue = it.eventId?.rawValue!! }
                                roomId { rawValue = it.roomId?.rawValue!! }
                                if (it.encodedPayload.isNotEmpty()) {
                                    notification {
                                        payload {
                                            rawValue = it.encodedPayload
                                        }
                                    }
                                }
                            }
                        }

                        storedQueuedEvents = setOf(*queuedEvents.mapNotNull { it.eventId?.rawValue }.toTypedArray())

                        eventsDeliveredQueuedCounter.increment(queuedEvents.size.toDouble())

                        WatchSessionResponse {
                            response.open = originalOpen.copy(queuedEvents = queuedEvents)
                        }
                    }

                    emit(StreamControlEvent.Message(response))
                }
                .filter {
                    it.event?.sessionId?.rawValue.contentEquals(sessionId.rawValue)
                }
                .mapNotNull {
                    if (storedQueuedEvents?.contains(it.event?.eventId?.rawValue) == true) {
                        return@mapNotNull null
                    }

                    meterRegistry.timer("lockers.session.events.delivery.time", "delivery_type", "live")
                        .record(System.nanoTime() - it.receiveTime, java.util.concurrent.TimeUnit.NANOSECONDS)
                    eventsDeliveredCounter.increment()

                    StreamControlEvent.Message(WatchSessionResponse {
                        response.events {
                            event {
                                addEvent {
                                    roomId { rawValue = it.event?.roomId!!.rawValue }
                                    eventId { rawValue = it.event?.eventId!!.rawValue }
                                    it.event?.encodedPayload?.let { encodedPayload ->
                                        notification {
                                            payload {
                                                rawValue = encodedPayload
                                            }
                                        }
                                    }
                                    it.event?.encodedLocker?.let { encodedLocker ->
                                        locker = IdentifiedLocker.fromByteArray(encodedLocker)
                                    }
                                }
                            }
                        }
                    })
                }
            )
        }).onCompletion {
            if (openState.isCompleted) {
                activeStreamsCount.decrementAndGet()
                // Only drop the entry if it is still this stream's channel — a newer
                // stream for the same session may have replaced it.
                openStreamCancellationChannels.remove(openState.await().sessionId, cancellationChannel)
            }
        }
    }

    fun close() {
        dispatchers.close()
    }

    override suspend fun postEvent(context: GrpcRequestContext, request: PostEventRequest) = meterRegistry.trackResponse("lockers.session.post", PostEventResponse::result) {
        val receiveTime = System.nanoTime()
        val events = request.sessionIds.map {
            OnlineServerSessionEvent(
                ServerSessionEvent(
                    ServerSessionId(it.rawValue),
                    ServerRoomId(request.event?.roomId!!.rawValue),
                    ServerEventId(request.event?.eventId!!.rawValue),
                    request.event?.notification?.payload?.rawValue ?: byteArrayOf(),
                    request.event?.locker?.toByteArray() ?: byteArrayOf()
                ),
                receiveTime
            )
        }
        val requestPush = request.event?.notification?.push

        for (event in events) {
            val sessionId = SessionId(event.event?.sessionId?.rawValue!!)

            if (requestPush != null) {
                val pushService = pushGatewayDiscovery.findServer(sessionId)

                pushService?.sendPush(SendPushRequest {
                    this.sessionId = sessionId
                    push = requestPush
                })
            }

            eventsPostedCounter.increment()
            val startTime = System.nanoTime()
            dispatchers.runOnDispatcher(sessionId) {
                dispatcherWaitTimer.record(System.nanoTime() - startTime, java.util.concurrent.TimeUnit.NANOSECONDS)
                sessionInboxStore.saveEvent(event.event)
                inboxSavesCounter.increment()
                eventsQueuedCounter.increment()
                incomingEvents.emit(event)
            }
        }

        return@trackResponse PostEventResponse {
            result = PostEventResponse.Result.OK
        }
    }

    private suspend fun WatchSessionResponse_OpenBuilder.handleOpen(open: WatchSessionRequest.Open): ServerSessionId? {
        val serverSessionId = open.sessionId?.rawValue?.let { ServerSessionId(it) }
        val requestSequenceSignature = open.sequenceKeySignature?.signature

        if (requestSequenceSignature == null) {
            result = WatchSessionResponse.Open.Result.INVALID_SEQUENCE
            meterRegistry.counter("lockers.session.opens", "result", "INVALID_SEQUENCE").increment()
            return null
        }

        if (serverSessionId == null) {
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

        val authorizedPublicKey = Secp256r1PublicKey.decode(session.authorizedPublicKey)

        try {
            if (!authorizedPublicKey.verify(session.nextKeyMaterial, requestSequenceSignature)) {
                result = WatchSessionResponse.Open.Result.INVALID_SEQUENCE
                meterRegistry.counter("lockers.session.opens", "result", "INVALID_SEQUENCE").increment()
                return null
            }
            val updatedSession = session.copy(nextKeyMaterial = Random.nextBytes(32))

            sessionStore.updateSession(updatedSession)

            result = WatchSessionResponse.Open.Result.OK
            nextSequenceKey = updatedSession.nextKeyMaterial
            meterRegistry.counter("lockers.session.opens", "result", "OK").increment()
        } catch (signatureException: SignatureException) {
            result = WatchSessionResponse.Open.Result.INVALID_SEQUENCE
            meterRegistry.counter("lockers.session.opens", "result", "INVALID_SEQUENCE").increment()
            return null
        } catch (ex: Exception) {
            logger.warn("session open failed unexpectedly", ex)
            result = WatchSessionResponse.Open.Result.UNKNOWN_ERROR
            meterRegistry.counter("lockers.session.opens", "result", "UNKNOWN_ERROR").increment()
            return null
        }

        return serverSessionId
    }

    private suspend fun WatchSessionResponse_OpenBuilder.handleCreate(create: WatchSessionRequest.Create): ServerSessionId? {
        val serverSessionId = create.sessionId?.rawValue?.let { ServerSessionId(it) }
        val requestPublicKey = create.publicKey?.rawValue

        if (requestPublicKey == null) {
            result = WatchSessionResponse.Open.Result.INVALID_PUBLIC_KEY
            meterRegistry.counter("lockers.session.creates", "result", "INVALID_PUBLIC_KEY").increment()
            return null
        }

        if (serverSessionId == null) {
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
                authorizedPublicKey = requestPublicKey
            }
            sessionStore.updateSession(updatedSession)

            result = WatchSessionResponse.Open.Result.OK
            nextSequenceKey = updatedSession.nextKeyMaterial
            meterRegistry.counter("lockers.session.creates", "result", "OK").increment()
        } catch (ex: Exception) {
            logger.warn("session create failed unexpectedly", ex)
            result = WatchSessionResponse.Open.Result.SESSION_EXISTS
            meterRegistry.counter("lockers.session.creates", "result", "SESSION_EXISTS").increment()
            return null
        }

        return serverSessionId
    }
}
