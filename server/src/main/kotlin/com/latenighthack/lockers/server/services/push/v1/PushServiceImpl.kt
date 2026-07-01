package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktbuf.net.ServerDescriptor
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.push.v1.*
import com.latenighthack.lockers.server.ServerCore
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.server.tools.*
import io.micrometer.core.instrument.MeterRegistry
import me.tatarka.inject.annotations.Component
import me.tatarka.inject.annotations.Inject
import com.eatthepath.pushy.apns.*
import com.eatthepath.pushy.apns.auth.ApnsSigningKey
import com.eatthepath.pushy.apns.util.SimpleApnsPushNotification
import com.eatthepath.pushy.apns.util.TokenUtil
import com.eatthepath.pushy.apns.util.SimpleApnsPayloadBuilder
import com.latenighthack.lockers.common.v1.Push
import com.latenighthack.lockers.common.v1.fromByteArray
import com.latenighthack.lockers.session.v1.PostEventResponse
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random
import kotlin.time.Duration.Companion.seconds

@ServiceScope
@Component
abstract class PushServiceModule(@Component val serverCore: ServerCore): GrpcRouteProvider<PushServer> {
    abstract val serverImpl: PushServiceImpl

    override val server: PushServer get() = serverImpl
    override val descriptor: ServerDescriptor = PushServer.Descriptor
    
    suspend fun start() {
        serverImpl.start()
    }
}

@Component
abstract class PushGatewayServiceModule(@Component val serverCore: ServerCore, @Component val pushServiceModule: PushServiceModule): GrpcRouteProvider<PushGatewayServer> {
    override val server: PushGatewayServer get() = pushServiceModule.serverImpl
    override val descriptor: ServerDescriptor = PushGatewayServer.Descriptor
}

interface PushGatewayDiscovery {
    suspend fun findServer(sessionId: SessionId): PushGatewayService?
}

class LocalPushGatewayDiscovery(private val pushGatewayServer: PushGatewayServer) : PushGatewayDiscovery {
    override suspend fun findServer(sessionId: SessionId): PushGatewayService? {
        return LocalPushGatewayServiceRpc(pushGatewayServer)
    }
}



@ServiceScope
@Inject
class PushServiceImpl(
    private val pushSessionStore: PushSessionStore,
    private val pushQueueStore: PushQueueStore,
    private val meterRegistry: MeterRegistry
) : BaseServiceImpl(), PushServer, PushGatewayServer {
    companion object {
        private val DEFAULT_PUSH_TOPIC = "com.latenighthack.lockers"
    }
    private val logger = LoggerFactory.getLogger(PushServiceImpl::class.java)
    
    private val newPushesFlow = MutableSharedFlow<ServerPush>(extraBufferCapacity = 1000)
    private val processorScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    
    private val queueSizeGauge = AtomicInteger(0)
    private val registrationsCounter = meterRegistry.counter("fullhouse.push.registrations")
    private val pushesSentCounter = meterRegistry.counter("fullhouse.push.sent")
    private val pushesEnqueuedCounter = meterRegistry.counter("fullhouse.push.enqueued")
    private val pushesFailedCounter = meterRegistry.counter("fullhouse.push.failed")
    private val pushesRetriedCounter = meterRegistry.counter("fullhouse.push.retried")
    private val pushesRejectedCounter = meterRegistry.counter("fullhouse.push.rejected")
    
    init {
        meterRegistry.gauge("fullhouse.push.queue.size", queueSizeGauge) { it.get().toDouble() }
    }
    
    private val apnsClient: ApnsClient? by lazy {
        try {
            val teamId = System.getenv("APNS_TEAM_ID")
            val keyId = System.getenv("APNS_KEY_ID")
            val keyPath = System.getenv("APNS_KEY_PATH")
            val environment = System.getenv("APNS_ENVIRONMENT") ?: "development"

            if (teamId == null || keyId == null || keyPath == null) {
                logger.warn("APNS credentials not configured. Push notifications will not be sent. " +
                    "Set APNS_TEAM_ID, APNS_KEY_ID, and APNS_KEY_PATH environment variables.")
                return@lazy null
            }

            val keyFile = File(keyPath)
            if (!keyFile.exists()) {
                logger.error("APNS key file not found at: $keyPath")
                return@lazy null
            }

            val signingKey = ApnsSigningKey.loadFromPkcs8File(keyFile, teamId, keyId)
            val host = if (environment == "production") {
                ApnsClientBuilder.PRODUCTION_APNS_HOST
            } else {
                ApnsClientBuilder.DEVELOPMENT_APNS_HOST
            }

            ApnsClientBuilder()
                .setApnsServer(host)
                .setSigningKey(signingKey)
                .build()
                .also {
                    logger.info("APNS client initialized for $environment environment")
                }
        } catch (e: Exception) {
            logger.error("Failed to initialize APNS client", e)
            null
        }
    }

    fun start() {
        logger.info("Starting push service processor")

        processorScope.launch {
            newPushesFlow
                .onStart {
                    val pendingPushes = pushQueueStore.getPendingPushes()

                    queueSizeGauge.set(pendingPushes.size)

                    logger.info("Loaded ${pendingPushes.size} pending pushes from queue")

                    emitAll(pendingPushes.asFlow())
                }
                .collect { push ->
                    processPush(push)
                }
        }
    }
    
    private suspend fun processPush(push: ServerPush, retryCount: Int = 0) {
        val maxRetries = 5
        val baseDelay = 1.seconds
        val maxDelay = 60.seconds
        
        try {
            val payloadData = Push.fromByteArray(push.payload)

            val sessionId = push.sessionId
            val pushInfo = pushSessionStore.getPushInfo(ServerSessionId(sessionId?.rawValue!!))
            
            if (pushInfo?.apnsToken == null || pushInfo.apnsToken.isEmpty()) {
                logger.warn("No APNS token found for session, removing from queue")
                pushQueueStore.clearPush(push.pushId!!)
                queueSizeGauge.decrementAndGet()
                return
            }
            
            if (apnsClient == null) {
                logger.warn("APNS client not available, skipping push")
                return
            }
            
            val token = TokenUtil.sanitizeTokenString(pushInfo.apnsToken.decodeToString())
            val topic = DEFAULT_PUSH_TOPIC
            
            val payloadBuilder = SimpleApnsPayloadBuilder()
            if (payloadData.title.isNotEmpty()) {
                payloadBuilder.setAlertTitle(payloadData.title)
            }
            if (payloadData.body.isNotEmpty()) {
                payloadBuilder.setAlertBody(payloadData.body)
            }
            val apnsPayload = payloadBuilder.build()
            
            val notification = SimpleApnsPushNotification(token, topic, apnsPayload)
            
            val sendFuture = apnsClient?.sendNotification(notification)
            
            sendFuture?.whenComplete { response, cause ->
                processorScope.launch {
                    if (cause != null) {
                        logger.error("Failed to send push notification", cause)
                        pushesFailedCounter.increment()
                        
                        if (retryCount < maxRetries) {
                            val delay = minOf(baseDelay * (1 shl retryCount), maxDelay)
                            logger.info("Retrying push after ${delay.inWholeSeconds}s (attempt ${retryCount + 1}/$maxRetries)")
                            pushesRetriedCounter.increment()
                            delay(delay)
                            processPush(push, retryCount + 1)
                        } else {
                            logger.error("Max retries exceeded for push, removing from queue")
                            pushQueueStore.clearPush(push.pushId!!)
                            queueSizeGauge.decrementAndGet()
                        }
                    } else if (response != null) {
                        if (response.isAccepted) {
                            logger.debug("Push notification sent successfully")
                            pushesSentCounter.increment()
                            pushQueueStore.clearPush(push.pushId!!)
                            queueSizeGauge.decrementAndGet()
                        } else {
                            logger.warn("Push notification rejected: ${response.rejectionReason}")
                            pushesRejectedCounter.increment()
                            
                            response.tokenInvalidationTimestamp.ifPresent { timestamp ->
                                logger.warn("Token invalidated at $timestamp")
                            }
                            
                            pushQueueStore.clearPush(push.pushId!!)
                            queueSizeGauge.decrementAndGet()
                        }
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("Error processing push", e)
            pushesFailedCounter.increment()
            
            if (retryCount < maxRetries) {
                val delay = minOf(baseDelay * (1 shl retryCount), maxDelay)
                pushesRetriedCounter.increment()
                delay(delay)
                processPush(push, retryCount + 1)
            } else {
                logger.error("Max retries exceeded for push, removing from queue")
                pushQueueStore.clearPush(push.pushId!!)
                queueSizeGauge.decrementAndGet()
            }
        }
    }
    
    private data class PayloadData(
        val sessionId: ServerSessionId,
        val apnsPayload: String,
        val topic: String?
    )
    
    private fun parsePayload(payload: String): PayloadData {
        val parts = payload.split("|||", limit = 3)
        if (parts.size < 2) {
            throw IllegalArgumentException("Invalid payload format")
        }
        return PayloadData(
            sessionId = ServerSessionId(parts[0].decodeBase64()),
            apnsPayload = parts[1],
            topic = if (parts.size > 2) parts[2] else null
        )
    }
    
    private fun String.decodeBase64(): ByteArray {
        return java.util.Base64.getDecoder().decode(this)
    }
    
    private fun ByteArray.encodeBase64(): String {
        return java.util.Base64.getEncoder().encodeToString(this)
    }

    override suspend fun registerSession(
        context: GrpcRequestContext,
        request: RegisterSessionRequest
    ) = meterRegistry.trackResponse("fullhouse.push.register", RegisterSessionResponse::result) {
        try {
            val sessionId = request.sessionId?.rawValue
            if (sessionId == null || sessionId.isEmpty()) {
                return@trackResponse RegisterSessionResponse {
                    result = RegisterSessionResponse.Result.UNKNOWN_ERROR
                }
            }

            val apnsToken = request.apnsToken

            if (apnsToken.isEmpty()) {
                logger.warn("No APNS token provided in metadata")
                return@trackResponse RegisterSessionResponse {
                    result = RegisterSessionResponse.Result.UNKNOWN_ERROR
                }
            }

            val serverSessionId = ServerSessionId(sessionId)
            val pushInfo = ServerPushInfo {
                this.sessionId = serverSessionId
                this.apnsToken = apnsToken
            }
            
            pushSessionStore.savePushInfo(pushInfo)
            registrationsCounter.increment()
            
            logger.info("Registered APNS token for session")

            return@trackResponse RegisterSessionResponse {
                result = RegisterSessionResponse.Result.OK
            }
        } catch (e: Exception) {
            logger.error("Failed to register session", e)
            return@trackResponse RegisterSessionResponse {
                result = RegisterSessionResponse.Result.UNKNOWN_ERROR
            }
        }
    }

    override suspend fun sendPush(
        context: GrpcRequestContext,
        request: SendPushRequest
    ) = meterRegistry.trackResponse("fullhouse.push.sendpush", SendPushResponse::result) {
        try {
            val sessionId = request.sessionId?.rawValue
            val push = request.push
            
            if (sessionId == null || sessionId.isEmpty()) {
                return@trackResponse SendPushResponse {
                    result = SendPushResponse.Result.UNKNOWN_ERROR
                }
            }
            
            if (push == null) {
                return@trackResponse SendPushResponse {
                    result = SendPushResponse.Result.UNKNOWN_ERROR
                }
            }
            
            val pushId = ServerPushId(Random.nextBytes(32))
            
            val payloadBuilder = SimpleApnsPayloadBuilder()
            if (push.title.isNotEmpty()) {
                payloadBuilder.setAlertTitle(push.title)
            }
            if (push.body.isNotEmpty()) {
                payloadBuilder.setAlertBody(push.body)
            }
            val apnsPayload = payloadBuilder.build()
            
            val sessionIdBase64 = sessionId.encodeBase64()
            val topic = "com.latenighthack.lockers"
            val combinedPayload = "$sessionIdBase64|||$apnsPayload|||$topic"
            
            val serverPush = ServerPush {
                this.pushId = pushId
                this.payload = combinedPayload.encodeToByteArray()
            }
            
            pushQueueStore.savePush(serverPush)
            queueSizeGauge.incrementAndGet()
            pushesEnqueuedCounter.increment()
            
            newPushesFlow.emit(serverPush)
            
            logger.debug("Enqueued push notification for session")

            return@trackResponse SendPushResponse {
                result = SendPushResponse.Result.OK
            }
        } catch (e: Exception) {
            logger.error("Failed to send push", e)
            return@trackResponse SendPushResponse {
                result = SendPushResponse.Result.UNKNOWN_ERROR
            }
        }
    }
}
