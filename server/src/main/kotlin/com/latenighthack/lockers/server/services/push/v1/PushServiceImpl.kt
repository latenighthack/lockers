package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktcrypto.digest
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
import com.latenighthack.lockers.observability.*
import io.micrometer.core.instrument.Tag
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.sync.withLock
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

    override val server: PushServer get() = com.latenighthack.lockers.server.services.session.v1.AuthorizedPushServer(serverImpl, serverCore.sessionProofVerifier)
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
    private val telemetry: LockersTelemetry = LockersTelemetry.NONE,
    coroutineContext: kotlin.coroutines.CoroutineContext = kotlin.coroutines.EmptyCoroutineContext,
) : BaseServiceImpl(), PushServer, PushGatewayServer, PushAdminServer {
    private val logger = LoggerFactory.getLogger(PushServiceImpl::class.java)

    private val providersByBackend = pushProviders.associateBy { it.backend }
    private val webPushProvider = pushProviders.filterIsInstance<WebPushProvider>().firstOrNull()

    // Caps concurrent sends per backend so a backlog burst can't open thousands of
    // simultaneous vendor calls, and one backend can't monopolize send capacity.
    private val sendSemaphores = PushBackendKind.entries.associateWith {
        Semaphore(dispatch.sendConcurrencyPerBackend.coerceAtLeast(1))
    }

    private val workAvailable = Channel<Unit>(Channel.CONFLATED)
    private val processorScope = CoroutineScope(coroutineContext + Dispatchers.IO + SupervisorJob(coroutineContext[Job]) + ServiceLifecycle.context)
    private val closing = kotlinx.coroutines.sync.Mutex()
    private var closed = false
    private var started = false
    private val workerId = java.util.UUID.randomUUID().toString()
    private val localClaims = java.util.concurrent.ConcurrentHashMap<ServerPushId, PushClaim>()
    private var processorJob: Job? = null

    private val totalQueueGauge = AtomicInteger(0)
    private val queueDepth = PushBackendKind.entries.associateWith { AtomicInteger(0) }
    private val deadLetterDepth = PushBackendKind.entries.associateWith { AtomicInteger(0) }

    init {
        for (backend in PushBackendKind.entries) {
            meterRegistry.gauge("lockers.push.provider.configured", listOf(Tag.of("backend", backend.tag)), providersByBackend[backend]?.isConfigured == true) { if (it) 1.0 else 0.0 }
        }
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
        synchronized(this) { check(!closed && !started) { "Push service already started or closed" }; started = true }
        if (!dispatch.workerEnabled) {
            // API-only replica: still report depths from the shared stores, but do
            // not drain. Enabled replicas coordinate through durable claims.
            logger.info("Push worker disabled on this replica; queue drain not started")
            processorScope.launch { seedGauges() }
            return
        }
        logger.info("Starting push service processor")
        check(pushQueueStore.supportsClaims) { "Push workers require a durable queue with atomic claims" }
        check(processorJob == null) { "Push processor already started" }
        processorJob = processorScope.launch {
            coroutineScope {
                for (backend in listOf(0) + PushBackendKind.entries.map { it.protoValue }) {
                    val count = if (backend == 0) 1 else dispatch.sendConcurrencyPerBackend.coerceIn(1, 256)
                    val slots = Semaphore(count)
                    val claimed = Channel<PushClaim>(count)
                    repeat(count) { launch {
                        for (claim in claimed) {
                            try {
                                supervisorScope {
                                    val task = async { processPush(claim) }
                                    try { task.await() }
                                    catch (cancelled: CancellationException) { if (!currentCoroutineContext().isActive) throw cancelled }
                                    catch (error: Exception) { logger.warn("push attempt failed; claim remains recoverable", error) }
                                }
                            } finally { slots.release(); workAvailable.trySend(Unit) }
                        }
                    } }
                    launch {
                        while (isActive) {
                            var available = 0
                            repeat(count) { if (slots.tryAcquire()) available++ }
                            if (available == 0) { withTimeoutOrNull(250) { workAvailable.receive() }; continue }
                            val batch = try { pushQueueStore.claim(backend, workerId, System.currentTimeMillis(), available) }
                                catch (cancelled: CancellationException) { repeat(available) { slots.release() }; throw cancelled }
                                catch (error: Exception) { repeat(available) { slots.release() }; logger.warn("push claim failed; retrying durable queue", error); delay(250); continue }
                            repeat(available - batch.size) { slots.release() }
                            batch.forEach { claim -> localClaims[requireNotNull(claim.push.pushId)] = claim; claimed.send(claim) }
                            if (batch.isEmpty()) withTimeoutOrNull(250) { workAvailable.receive() }
                        }
                    }
                }
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

    fun stop() = ServiceLifecycle.blockingClose { stopAndJoin() }

    suspend fun stopAndJoin() {
        ServiceLifecycle.requireExternalClose()
        closing.withLock {
            if (closed) return
            closed = true
            withContext(NonCancellable) {
                processorScope.coroutineContext[Job]!!.cancelAndJoin()
                var failed: Throwable? = null
                fun record(failure: Throwable) { if (failed == null) failed = failure else failed!!.addSuppressed(failure) }
                for (claim in localClaims.values) {
                    try { pushQueueStore.release(claim, System.currentTimeMillis()) } catch (failure: Throwable) { record(failure) }
                }
                localClaims.clear()
                pushProviders.forEach { try { it.close() } catch (failure: Throwable) { record(failure) } }
                workAvailable.close()
                failed?.let { throw it }
            }
        }
    }

    private suspend fun processPush(claim: PushClaim) {
        val push = claim.push
        val pushId = push.pushId ?: return
        val processing = currentCoroutineContext()[Job]!!
        val heartbeat = CoroutineScope(currentCoroutineContext()).launch {
            while (isActive) {
                delay(10_000)
                if (!pushQueueStore.renew(claim, System.currentTimeMillis())) processing.cancel(CancellationException("push claim revoked"))
            }
        }
        try {
            processClaimedPush(claim)
        } finally {
            withContext(NonCancellable) {
                heartbeat.cancelAndJoin()
                pushQueueStore.release(claim, System.currentTimeMillis() + 250)
                localClaims.remove(pushId, claim)
            }
        }
    }

    private suspend fun processClaimedPush(claim: PushClaim) {
        val push = claim.push
        val pushId = push.pushId ?: return
        val backend = PushBackendKind.fromProtoValue(push.backend)
        if (backend == null) {
            dequeue(claim, null)
            return
        }

        val provider = providersByBackend[backend]
        if (provider == null || !provider.isConfigured) {
            // Unconfigured backend: drop rather than spin the queue.
            dequeue(claim, backend)
            return
        }

        val serverSessionId = push.sessionId ?: run { dequeue(claim, backend); return }
        val storedRegistration = pushSessionStore.getPushInfo(serverSessionId)?.registrations?.firstOrNull { it.backend == push.backend }
        val registration = storedRegistration?.let { PushRegistration.fromByteArray(it.encodedRegistration) }
        if (registration == null) {
            logger.debug("No ${backend.name} registration for session, dropping push")
            dequeue(claim, backend)
            return
        }

        val result = sendSemaphores.getValue(backend).withPermit {
            val startNanos = System.nanoTime()
            val sendResult = try {
                telemetry.observe(TelemetryOperation.PUSH_SEND, { when (it) { is PushResult.Accepted -> TelemetryOutcome.OK; is PushResult.Rejected -> TelemetryOutcome.REJECTED; is PushResult.Retryable -> TelemetryOutcome.ERROR } }) { provider.send(registration, Push.fromByteArray(push.encodedPush)) }
            } catch (e: CancellationException) { throw e }
            catch (e: Exception) {
                PushResult.Retryable(e.message ?: "send threw")
            }
            recordSendDuration(backend, sendResult, System.nanoTime() - startNanos)
            sendResult
        }

        when (result) {
            is PushResult.Accepted -> {
                if (dequeue(claim, backend)) counter("lockers.push.sent", backend).increment()
            }

            is PushResult.Rejected -> {
                counter("lockers.push.rejected", backend).increment()
                if (result.tokenInvalid) {
                    pushSessionStore.removeCredentialIfCurrent(serverSessionId, push.backend, requireNotNull(storedRegistration).encodedRegistration)
                    dequeue(claim, backend)
                } else {
                    // A permanent, non-token failure (bad payload, etc.): park it.
                    deadLetter(claim, backend, result.reason)
                }
            }

            is PushResult.Retryable -> {
                counter("lockers.push.failed", backend).increment()
                val nextAttempt = push.attempt + 1
                if (nextAttempt < dispatch.retryPolicy.maxAttempts) {
                    counter("lockers.push.retried", backend).increment()
                    // Persist the attempt so the budget survives a restart.
                    val shift = push.attempt.coerceIn(0, MAX_BACKOFF_SHIFT)
                    val backoff = minOf(dispatch.retryPolicy.baseDelay * (1 shl shift), dispatch.retryPolicy.maxDelay)
                    pushQueueStore.retry(claim, nextAttempt, System.currentTimeMillis() + backoff.inWholeMilliseconds)
                } else {
                    deadLetter(claim.copy(push = push.copy(attempt = nextAttempt)), backend, result.reason)
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

    private suspend fun dequeue(claim: PushClaim, backend: PushBackendKind?): Boolean {
        val finished = pushQueueStore.finish(claim)
        if (finished) decQueue(backend)
        return finished
    }

    private suspend fun deadLetter(claim: PushClaim, backend: PushBackendKind, reason: String) {
        val push = claim.push
        val pushId = push.pushId ?: return
        val finished = pushQueueStore.finish(claim, parkedReason = reason) {
            pushDeadLetterStore.saveDeadLetter(ServerDeadLetter {
                this.pushId = pushId
                push.sessionId?.let { this.sessionId = it }
                this.backend = push.backend
                this.encodedPush = push.encodedPush
                this.attempts = push.attempt
                this.reason = reason
                this.deadLetteredAt = System.currentTimeMillis()
            })
        }
        if (finished) {
            decQueue(backend)
            deadLetterDepth[backend]?.incrementAndGet()
            counter("lockers.push.deadlettered", backend).increment()
            logger.warn("Dead-lettered {} push after {} attempts", backend.name, push.attempt)
        }
    }

    override suspend fun registerSession(
        context: GrpcRequestContext,
        request: RegisterSessionRequest,
    ) = meterRegistry.trackResponse("lockers.push.register", RegisterSessionResponse::result, telemetry) {
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
            if (!pushSessionStore.applyCredential(serverSessionId, backend.protoValue, registration.toByteArray(), request.credentialRevision)) {
                return@trackResponse RegisterSessionResponse { result = RegisterSessionResponse.Result.UNKNOWN_ERROR }
            }
            counter("lockers.push.registrations", backend).increment()

            RegisterSessionResponse { result = RegisterSessionResponse.Result.OK }
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (e: Exception) {
            logger.error("Failed to register session", e)
            RegisterSessionResponse { result = RegisterSessionResponse.Result.UNKNOWN_ERROR }
        }
    }

    override suspend fun unregisterSession(
        context: GrpcRequestContext,
        request: UnregisterSessionRequest,
    ) = meterRegistry.trackResponse("lockers.push.unregister", UnregisterSessionResponse::result, telemetry) {
        try {
            val rawSessionId = request.sessionId?.rawValue
            if (rawSessionId == null || rawSessionId.isEmpty()) {
                return@trackResponse UnregisterSessionResponse { result = UnregisterSessionResponse.Result.UNKNOWN_ERROR }
            }
            if (!pushSessionStore.applyCredential(ServerSessionId(rawSessionId), request.backend.value, null, request.credentialRevision)) {
                return@trackResponse UnregisterSessionResponse { result = UnregisterSessionResponse.Result.UNKNOWN_ERROR }
            }
            UnregisterSessionResponse { result = UnregisterSessionResponse.Result.OK }
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (e: Exception) {
            logger.error("Failed to unregister session", e)
            UnregisterSessionResponse { result = UnregisterSessionResponse.Result.UNKNOWN_ERROR }
        }
    }

    override suspend fun getPushConfig(
        context: GrpcRequestContext,
        request: GetPushConfigRequest,
    ) = meterRegistry.trackResponse("lockers.push.config", GetPushConfigResponse::result, telemetry) {
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
    ) = meterRegistry.trackResponse("lockers.push.sendpush", SendPushResponse::result, telemetry) {
        try {
            val rawSessionId = request.sessionId?.rawValue
            val push = request.push
            if (rawSessionId == null || rawSessionId.isEmpty() || push == null ||
                (request.deliveryId.isNotEmpty() && request.deliveryId.size !in 16..64)) {
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
                val stableId = if (request.deliveryId.isEmpty()) Random.nextBytes(PUSH_ID_BYTES)
                    else com.latenighthack.ktcrypto.SHA256.digest("lockers.push.delivery.v1".encodeToByteArray() +
                        java.nio.ByteBuffer.allocate(4).putInt(rawSessionId.size).array() + rawSessionId +
                        java.nio.ByteBuffer.allocate(4).putInt(registration.backend).array() + request.deliveryId)
                val serverPush = ServerPush {
                    this.pushId = ServerPushId(stableId)
                    this.sessionId = serverSessionId
                    this.backend = registration.backend
                    this.encodedPush = encodedPush
                }
                if (!pushQueueStore.enqueue(serverPush, stableIdentity = request.deliveryId.isNotEmpty())) continue
                incQueue(backend)
                backend?.let { counter("lockers.push.enqueued", it).increment() }
                workAvailable.trySend(Unit)
            }

            SendPushResponse { result = SendPushResponse.Result.OK }
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (e: Exception) {
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
        workAvailable.trySend(Unit)
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
            workAvailable.trySend(Unit)
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
