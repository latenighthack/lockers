package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktbuf.net.ServerDescriptor
import com.latenighthack.lockers.common.v1.Push
import com.latenighthack.lockers.common.v1.fromByteArray
import com.latenighthack.lockers.common.v1.toByteArray
import com.latenighthack.lockers.push.v1.*
import com.latenighthack.lockers.server.ServerCore
import com.latenighthack.lockers.server.services.push.v1.providers.PushBackendKind
import com.latenighthack.lockers.server.services.push.v1.providers.PushProvider
import com.latenighthack.lockers.server.services.push.v1.providers.PushResult
import com.latenighthack.lockers.server.services.push.v1.providers.WebPushProvider
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.server.tools.*
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import me.tatarka.inject.annotations.Component
import me.tatarka.inject.annotations.Inject
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

@ServiceScope
@Component
abstract class PushServiceModule(@Component val serverCore: ServerCore) : GrpcRouteProvider<PushServer> {
    abstract val serverImpl: PushServiceImpl

    override val server: PushServer get() = serverImpl
    override val descriptor: ServerDescriptor = PushServer.Descriptor

    suspend fun start() {
        serverImpl.start()
    }

    fun stop() {
        serverImpl.stop()
    }
}

@Component
abstract class PushGatewayServiceModule(
    @Component val serverCore: ServerCore,
    @Component val pushServiceModule: PushServiceModule,
) : GrpcRouteProvider<PushGatewayServer> {
    override val server: PushGatewayServer get() = pushServiceModule.serverImpl
    override val descriptor: ServerDescriptor = PushGatewayServer.Descriptor
}

@Component
abstract class PushAdminServiceModule(
    @Component val serverCore: ServerCore,
    @Component val pushServiceModule: PushServiceModule,
) : GrpcRouteProvider<PushAdminServer> {
    override val server: PushAdminServer get() = pushServiceModule.serverImpl
    override val descriptor: ServerDescriptor = PushAdminServer.Descriptor
}

interface PushGatewayDiscovery {
    suspend fun findServer(sessionId: com.latenighthack.lockers.common.v1.SessionId): PushGatewayService?
}

class LocalPushGatewayDiscovery(private val pushGatewayServer: PushGatewayServer) : PushGatewayDiscovery {
    override suspend fun findServer(sessionId: com.latenighthack.lockers.common.v1.SessionId): PushGatewayService {
        return LocalPushGatewayServiceRpc(pushGatewayServer)
    }
}

/**
 * Backend-agnostic push service. It owns a durable per-(session, backend) queue
 * with retry/backoff and dispatches each row to the matching [PushProvider].
 * Registrations are stored per session as a list of backend credentials, so a
 * client can register (and rotate) an APNS token, an FCM token and a web-push
 * subscription independently.
 *
 * A push that exhausts its (persisted) attempt budget or is permanently rejected
 * is moved to a dead-letter store rather than retried forever, so one poison push
 * never starves the queue. The [PushAdminServer] surface exposes queue/dead-letter
 * depths and lets an operator drain, retry or purge.
 */
@ServiceScope
@Inject
class PushServiceImpl(
    private val pushSessionStore: PushSessionStore,
    private val pushQueueStore: PushQueueStore,
    private val pushDeadLetterStore: PushDeadLetterStore,
    private val meterRegistry: MeterRegistry,
    private val pushProviders: List<PushProvider>,
    private val dispatch: PushDispatchConfig = PushDispatchConfig.DEFAULT,
) : BaseServiceImpl(), PushServer, PushGatewayServer, PushAdminServer {
    private val logger = LoggerFactory.getLogger(PushServiceImpl::class.java)

    private val providersByBackend = pushProviders.associateBy { it.backend }
    private val webPushProvider = pushProviders.filterIsInstance<WebPushProvider>().firstOrNull()

    // Caps concurrent sends per backend so a backlog burst can't open thousands of
    // simultaneous vendor calls, and one backend can't monopolize send capacity.
    private val sendSemaphores = PushBackendKind.entries.associateWith {
        Semaphore(dispatch.sendConcurrencyPerBackend.coerceAtLeast(1))
    }

    private val newPushesFlow = MutableSharedFlow<ServerPush>(extraBufferCapacity = 1000)
    private val processorScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val totalQueueGauge = AtomicInteger(0)
    private val queueDepth = PushBackendKind.entries.associateWith { AtomicInteger(0) }
    private val deadLetterDepth = PushBackendKind.entries.associateWith { AtomicInteger(0) }

    init {
        meterRegistry.gauge("lockers.push.queue.size", totalQueueGauge) { it.get().toDouble() }
        for ((backend, gauge) in queueDepth) {
            meterRegistry.gauge("lockers.push.queue.depth", listOf(Tag.of("backend", backend.tag)), gauge) { it.get().toDouble() }
        }
        for ((backend, gauge) in deadLetterDepth) {
            meterRegistry.gauge("lockers.push.deadletter.depth", listOf(Tag.of("backend", backend.tag)), gauge) { it.get().toDouble() }
        }
    }

    private val PushBackendKind.tag: String get() = name.lowercase()

    private fun counter(name: String, backend: PushBackendKind) =
        meterRegistry.counter(name, "backend", backend.tag)

    private fun incQueue(backend: PushBackendKind?) {
        totalQueueGauge.incrementAndGet()
        backend?.let { queueDepth[it]?.incrementAndGet() }
    }

    private fun decQueue(backend: PushBackendKind?) {
        totalQueueGauge.decrementAndGet()
        backend?.let { queueDepth[it]?.decrementAndGet() }
    }

    fun start() {
        if (!dispatch.workerEnabled) {
            // API-only replica: still report depths from the shared stores, but do
            // not drain — exactly one replica must own the queue (ktstore has no
            // atomic row claim, so a second drainer would double-send).
            logger.info("Push worker disabled on this replica; queue drain not started")
            processorScope.launch { seedGauges() }
            return
        }
        logger.info("Starting push service processor")
        processorScope.launch {
            newPushesFlow
                .onStart {
                    val pending = pushQueueStore.getPendingPushes()
                    seedGaugesFrom(pending, pushDeadLetterStore.getAllDeadLetters())
                    logger.info("Loaded ${pending.size} pending pushes")
                    emitAll(pending.asFlow())
                }
                .collect { push ->
                    // Process each row independently so one slow/blocking send
                    // never serializes the whole queue.
                    processorScope.launch { processPush(push) }
                }
        }
    }

    private suspend fun seedGauges() =
        seedGaugesFrom(pushQueueStore.getPendingPushes(), pushDeadLetterStore.getAllDeadLetters())

    private fun seedGaugesFrom(pending: List<ServerPush>, dead: List<ServerDeadLetter>) {
        totalQueueGauge.set(pending.size)
        for (backend in PushBackendKind.entries) {
            queueDepth[backend]?.set(pending.count { it.backend == backend.protoValue })
            deadLetterDepth[backend]?.set(dead.count { it.backend == backend.protoValue })
        }
    }

    fun stop() {
        logger.info("Stopping push service processor")
        processorScope.cancel()
        pushProviders.forEach { runCatching { it.close() } }
    }

    private suspend fun processPush(push: ServerPush) {
        val pushId = push.pushId ?: return
        val backend = PushBackendKind.fromProtoValue(push.backend)
        if (backend == null) {
            dequeue(pushId, null)
            return
        }

        val provider = providersByBackend[backend]
        if (provider == null || !provider.isConfigured) {
            // Unconfigured backend: drop rather than spin the queue.
            dequeue(pushId, backend)
            return
        }

        val serverSessionId = push.sessionId ?: run { dequeue(pushId, backend); return }
        val registration = pushSessionStore.getPushInfo(serverSessionId)
            ?.registrations
            ?.firstOrNull { it.backend == push.backend }
            ?.let { PushRegistration.fromByteArray(it.encodedRegistration) }
        if (registration == null) {
            logger.debug("No ${backend.name} registration for session, dropping push")
            dequeue(pushId, backend)
            return
        }

        val result = sendSemaphores.getValue(backend).withPermit {
            val startNanos = System.nanoTime()
            val sendResult = try {
                provider.send(registration, Push.fromByteArray(push.encodedPush))
            } catch (e: Exception) {
                PushResult.Retryable(e.message ?: "send threw")
            }
            recordSendDuration(backend, sendResult, System.nanoTime() - startNanos)
            sendResult
        }

        when (result) {
            is PushResult.Accepted -> {
                counter("lockers.push.sent", backend).increment()
                dequeue(pushId, backend)
            }

            is PushResult.Rejected -> {
                counter("lockers.push.rejected", backend).increment()
                if (result.tokenInvalid) {
                    removeRegistration(serverSessionId, push.backend)
                    dequeue(pushId, backend)
                } else {
                    // A permanent, non-token failure (bad payload, etc.): park it.
                    deadLetter(push, backend, result.reason)
                }
            }

            is PushResult.Retryable -> {
                counter("lockers.push.failed", backend).increment()
                val nextAttempt = push.attempt + 1
                if (nextAttempt < dispatch.retryPolicy.maxAttempts) {
                    counter("lockers.push.retried", backend).increment()
                    // Persist the attempt so the budget survives a restart.
                    val advanced = push.copy { attempt = nextAttempt }
                    pushQueueStore.savePush(advanced)
                    val shift = push.attempt.coerceAtMost(MAX_BACKOFF_SHIFT)
                    val backoff = minOf(dispatch.retryPolicy.baseDelay * (1 shl shift), dispatch.retryPolicy.maxDelay)
                    delay(backoff)
                    processPush(advanced)
                } else {
                    deadLetter(push.copy { attempt = nextAttempt }, backend, result.reason)
                }
            }
        }
    }

    private fun recordSendDuration(backend: PushBackendKind, result: PushResult, durationNanos: Long) {
        val outcome = when (result) {
            is PushResult.Accepted -> "accepted"
            is PushResult.Rejected -> "rejected"
            is PushResult.Retryable -> "retryable"
        }
        meterRegistry.timer("lockers.push.send.duration", "backend", backend.tag, "result", outcome)
            .record(durationNanos, TimeUnit.NANOSECONDS)
    }

    private suspend fun dequeue(pushId: ServerPushId, backend: PushBackendKind?) {
        pushQueueStore.clearPush(pushId)
        decQueue(backend)
    }

    private suspend fun deadLetter(push: ServerPush, backend: PushBackendKind, reason: String) {
        val pushId = push.pushId ?: return
        pushDeadLetterStore.saveDeadLetter(ServerDeadLetter {
            this.pushId = pushId
            push.sessionId?.let { this.sessionId = it }
            this.backend = push.backend
            this.encodedPush = push.encodedPush
            this.attempts = push.attempt
            this.reason = reason
            this.deadLetteredAt = System.currentTimeMillis()
        })
        pushQueueStore.clearPush(pushId)
        decQueue(backend)
        deadLetterDepth[backend]?.incrementAndGet()
        counter("lockers.push.deadlettered", backend).increment()
        logger.warn("Dead-lettered ${backend.name} push after ${push.attempt} attempts: $reason")
    }

    private suspend fun removeRegistration(sessionId: ServerSessionId, backend: Int) {
        val info = pushSessionStore.getPushInfo(sessionId) ?: return
        val remaining = info.registrations.filter { it.backend != backend }
        if (remaining.isEmpty()) {
            pushSessionStore.deletePushInfo(sessionId)
        } else {
            pushSessionStore.savePushInfo(ServerPushInfo {
                this.sessionId = sessionId
                this.registrations = remaining
            })
        }
    }

    override suspend fun registerSession(
        context: GrpcRequestContext,
        request: RegisterSessionRequest,
    ) = meterRegistry.trackResponse("lockers.push.register", RegisterSessionResponse::result) {
        try {
            val rawSessionId = request.sessionId?.rawValue
            val registration = request.registration
            if (rawSessionId == null || rawSessionId.isEmpty() || registration == null) {
                return@trackResponse RegisterSessionResponse { result = RegisterSessionResponse.Result.UNKNOWN_ERROR }
            }

            val backend = PushBackendKind.of(registration)
            if (backend == null) {
                logger.warn("Registration with no backend set")
                return@trackResponse RegisterSessionResponse { result = RegisterSessionResponse.Result.UNKNOWN_ERROR }
            }

            val serverSessionId = ServerSessionId(rawSessionId)
            val existing = pushSessionStore.getPushInfo(serverSessionId)?.registrations.orEmpty()
            // Upsert by backend: replace any prior credential for the same backend.
            val updated = existing.filter { it.backend != backend.protoValue } + ServerPushRegistration {
                this.backend = backend.protoValue
                this.encodedRegistration = registration.toByteArray()
            }

            pushSessionStore.savePushInfo(ServerPushInfo {
                this.sessionId = serverSessionId
                this.registrations = updated
            })
            counter("lockers.push.registrations", backend).increment()

            RegisterSessionResponse { result = RegisterSessionResponse.Result.OK }
        } catch (e: Exception) {
            logger.error("Failed to register session", e)
            RegisterSessionResponse { result = RegisterSessionResponse.Result.UNKNOWN_ERROR }
        }
    }

    override suspend fun unregisterSession(
        context: GrpcRequestContext,
        request: UnregisterSessionRequest,
    ) = meterRegistry.trackResponse("lockers.push.unregister", UnregisterSessionResponse::result) {
        try {
            val rawSessionId = request.sessionId?.rawValue
            if (rawSessionId == null || rawSessionId.isEmpty()) {
                return@trackResponse UnregisterSessionResponse { result = UnregisterSessionResponse.Result.UNKNOWN_ERROR }
            }
            removeRegistration(ServerSessionId(rawSessionId), request.backend.value)
            UnregisterSessionResponse { result = UnregisterSessionResponse.Result.OK }
        } catch (e: Exception) {
            logger.error("Failed to unregister session", e)
            UnregisterSessionResponse { result = UnregisterSessionResponse.Result.UNKNOWN_ERROR }
        }
    }

    override suspend fun getPushConfig(
        context: GrpcRequestContext,
        request: GetPushConfigRequest,
    ) = meterRegistry.trackResponse("lockers.push.config", GetPushConfigResponse::result) {
        val supported = pushProviders
            .filter { it.isConfigured }
            .map { PushBackend.fromInt(it.backend.protoValue) }
        val vapid = webPushProvider?.applicationServerKey ?: ByteArray(0)

        GetPushConfigResponse {
            result = GetPushConfigResponse.Result.OK
            config = PushConfig {
                this.supported = supported
                this.vapidPublicKey = vapid
            }
        }
    }

    override suspend fun sendPush(
        context: GrpcRequestContext,
        request: SendPushRequest,
    ) = meterRegistry.trackResponse("lockers.push.sendpush", SendPushResponse::result) {
        try {
            val rawSessionId = request.sessionId?.rawValue
            val push = request.push
            if (rawSessionId == null || rawSessionId.isEmpty() || push == null) {
                return@trackResponse SendPushResponse { result = SendPushResponse.Result.UNKNOWN_ERROR }
            }

            val serverSessionId = ServerSessionId(rawSessionId)
            val registrations = pushSessionStore.getPushInfo(serverSessionId)?.registrations.orEmpty()
            if (registrations.isEmpty()) {
                // No devices registered for this session — nothing to deliver.
                return@trackResponse SendPushResponse { result = SendPushResponse.Result.OK }
            }

            val encodedPush = push.toByteArray()
            for (registration in registrations) {
                val backend = PushBackendKind.fromProtoValue(registration.backend)
                val serverPush = ServerPush {
                    this.pushId = ServerPushId(Random.nextBytes(PUSH_ID_BYTES))
                    this.sessionId = serverSessionId
                    this.backend = registration.backend
                    this.encodedPush = encodedPush
                }
                pushQueueStore.savePush(serverPush)
                incQueue(backend)
                backend?.let { counter("lockers.push.enqueued", it).increment() }
                newPushesFlow.emit(serverPush)
            }

            SendPushResponse { result = SendPushResponse.Result.OK }
        } catch (e: Exception) {
            logger.error("Failed to send push", e)
            SendPushResponse { result = SendPushResponse.Result.UNKNOWN_ERROR }
        }
    }

    // --- PushAdmin (internal management surface) ---

    /**
     * Defense in depth on top of binding admin to an internal port: when an admin
     * token is configured, every admin call must present it as [ADMIN_TOKEN_HEADER].
     * In-process callers (tests) run with no token configured and pass through.
     */
    private fun authorizeAdmin(context: GrpcRequestContext) {
        val required = dispatch.adminToken ?: return
        val presented = context.headers.entries
            .firstOrNull { it.key.equals(ADMIN_TOKEN_HEADER, ignoreCase = true) }
            ?.value
        if (presented != required) {
            throw SecurityException("push admin: missing or invalid $ADMIN_TOKEN_HEADER")
        }
    }

    override suspend fun getQueueStats(
        context: GrpcRequestContext,
        request: GetQueueStatsRequest,
    ): GetQueueStatsResponse {
        authorizeAdmin(context)
        val queued = pushQueueStore.getPendingPushes()
        val dead = pushDeadLetterStore.getAllDeadLetters()
        return GetQueueStatsResponse {
            this.queued = queued.size.toLong()
            this.deadLettered = dead.size.toLong()
            this.queuedByBackend = backendCounts(queued.groupingBy { it.backend }.eachCount())
            this.deadLetteredByBackend = backendCounts(dead.groupingBy { it.backend }.eachCount())
        }
    }

    override suspend fun drainQueue(
        context: GrpcRequestContext,
        request: DrainQueueRequest,
    ): DrainQueueResponse {
        authorizeAdmin(context)
        val pending = pushQueueStore.getPendingPushes()
        for (push in pending) {
            newPushesFlow.emit(push)
        }
        logger.info("Drain re-fed ${pending.size} queued pushes to the processor")
        return DrainQueueResponse { drained = pending.size.toLong() }
    }

    override suspend fun listDeadLetters(
        context: GrpcRequestContext,
        request: ListDeadLettersRequest,
    ): ListDeadLettersResponse {
        authorizeAdmin(context)
        val limit = if (request.limit <= 0) DEFAULT_DEAD_LETTER_LIMIT else request.limit
        val items = pushDeadLetterStore.getAllDeadLetters().take(limit).map { it.toProto() }
        return ListDeadLettersResponse { deadLetters = items }
    }

    override suspend fun retryDeadLetters(
        context: GrpcRequestContext,
        request: RetryDeadLettersRequest,
    ): RetryDeadLettersResponse {
        authorizeAdmin(context)
        val targets = selectDeadLetters(request.pushIds)
        var retried = 0L
        for (dead in targets) {
            val pushId = dead.pushId ?: continue
            val backend = PushBackendKind.fromProtoValue(dead.backend)
            val requeued = ServerPush {
                this.pushId = pushId
                dead.sessionId?.let { this.sessionId = it }
                this.backend = dead.backend
                this.encodedPush = dead.encodedPush
                this.attempt = 0
            }
            pushQueueStore.savePush(requeued)
            incQueue(backend)
            backend?.let { counter("lockers.push.enqueued", it).increment() }
            pushDeadLetterStore.deleteDeadLetter(pushId)
            backend?.let { deadLetterDepth[it]?.decrementAndGet() }
            newPushesFlow.emit(requeued)
            retried++
        }
        logger.info("Retried $retried dead letters")
        return RetryDeadLettersResponse { this.retried = retried }
    }

    override suspend fun purgeDeadLetters(
        context: GrpcRequestContext,
        request: PurgeDeadLettersRequest,
    ): PurgeDeadLettersResponse {
        authorizeAdmin(context)
        val targets = selectDeadLetters(request.pushIds)
        var purged = 0L
        for (dead in targets) {
            val pushId = dead.pushId ?: continue
            pushDeadLetterStore.deleteDeadLetter(pushId)
            PushBackendKind.fromProtoValue(dead.backend)?.let { deadLetterDepth[it]?.decrementAndGet() }
            purged++
        }
        logger.info("Purged $purged dead letters")
        return PurgeDeadLettersResponse { this.purged = purged }
    }

    private suspend fun selectDeadLetters(pushIds: List<ByteArray>): List<ServerDeadLetter> =
        if (pushIds.isEmpty()) {
            pushDeadLetterStore.getAllDeadLetters()
        } else {
            pushIds.mapNotNull { pushDeadLetterStore.getDeadLetter(ServerPushId(it)) }
        }

    private fun backendCounts(counts: Map<Int, Int>): List<BackendCount> =
        counts.map { (backend, count) ->
            BackendCount {
                this.backend = PushBackend.fromInt(backend)
                this.count = count.toLong()
            }
        }

    private fun ServerDeadLetter.toProto(): DeadLetter = DeadLetter {
        this.pushId = this@toProto.pushId?.rawValue ?: ByteArray(0)
        this@toProto.sessionId?.let { this.sessionId = com.latenighthack.lockers.common.v1.SessionId(it.rawValue) }
        this.backend = PushBackend.fromInt(this@toProto.backend)
        this.push = Push.fromByteArray(this@toProto.encodedPush)
        this.attempts = this@toProto.attempts
        this.reason = this@toProto.reason
        this.deadLetteredAt = this@toProto.deadLetteredAt
    }

    companion object {
        const val ADMIN_TOKEN_HEADER = "x-admin-token"
        private const val PUSH_ID_BYTES = 32
        private const val MAX_BACKOFF_SHIFT = 6
        private const val DEFAULT_DEAD_LETTER_LIMIT = 100
    }
}
