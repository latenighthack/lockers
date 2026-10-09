package com.latenighthack.lockers.connector

import com.latenighthack.lockers.observability.*

import com.latenighthack.ktstore.*
import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.rpc.repeatWithBackoff
import com.latenighthack.ktbuf.rpc.RetryLimitExceeded
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import com.latenighthack.ktbuf.bytes.toBase64String
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.v1.SessionProof
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.connector.storage.v1.*
import com.latenighthack.lockers.push.v1.*

/** The push backends a client can register a credential for. */
class PushRegistrationException(message: String, cause: Throwable? = null) : Exception(message, cause)

enum class PushBackendType(internal val protoValue: Int) {
    APNS(1),
    FCM(2),
    WEB_PUSH(3),
}

/** Factories for the backend-specific credentials an app hands to [LockersClient.registerPush]. */
object PushRegistrations {
    fun apns(deviceToken: ByteArray, topic: String = "", production: Boolean = false): PushRegistration =
        PushRegistration {
            backend.apns {
                this.deviceToken = deviceToken
                this.topic = topic
                this.production = production
            }
        }

    fun fcm(registrationToken: String): PushRegistration =
        PushRegistration {
            backend.fcm { this.registrationToken = registrationToken }
        }

    fun webPush(endpoint: String, p256dh: ByteArray, auth: ByteArray): PushRegistration =
        PushRegistration {
            backend.webPush {
                this.endpoint = endpoint
                this.p256Dh = p256dh
                this.auth = auth
            }
        }
}

internal fun backendOf(registration: PushRegistration): Int? = when (registration.backend) {
    is PushRegistration.OneOfBackend.apns -> PushBackendType.APNS.protoValue
    is PushRegistration.OneOfBackend.fcm -> PushBackendType.FCM.protoValue
    is PushRegistration.OneOfBackend.webPush -> PushBackendType.WEB_PUSH.protoValue
    null -> null
}



interface PushRegistrationStore {
    suspend fun getAllIntents(): List<PushRegistrationIntent> = throw UnsupportedOperationException("Durable push intent revisions required")
    suspend fun nextIntent(backend: Int, encodedRegistration: ByteArray): PushRegistrationIntent = throw UnsupportedOperationException("Durable push intent revisions required")
    suspend fun confirmIntent(intent: PushRegistrationIntent): Boolean = throw UnsupportedOperationException("Durable push intent revisions required")
    suspend fun getAllRegistrations(): List<StoredPushRegistration>
    suspend fun saveRegistration(registration: StoredPushRegistration)
    suspend fun getRegistration(backend: Int): StoredPushRegistration?
    suspend fun deleteRegistration(backend: Int)
}

class PushRegistrationStoreImpl(private val database: Database) : PushRegistrationStore, Store<StoredPushRegistration>(database, PushRegistrationStoreImplDefinitionV1) {
    private val intents = PushIntentStore(database)
    override suspend fun getAllIntents(): List<PushRegistrationIntent> {
        prepare(); intents.prepare()
        return database.transaction("connector-push-intent") {
            getAll().forEach { stored -> if (intents.intent(stored.backend) == null)
                intents.put(PushRegistrationIntent(stored.backend, 1, stored.encodedRegistration, stored.isPending)) }
            intents.intents()
        }
    }
    override suspend fun nextIntent(backend: Int, encodedRegistration: ByteArray): PushRegistrationIntent {
        val encodedRegistration = encodedRegistration.copyOf()
        prepare(); intents.prepare()
        return database.transaction("connector-push-intent") {
            val previous = intents.intent(backend)?.revision ?: 0
            check(previous < Long.MAX_VALUE) { "Push credential revision exhausted" }
            PushRegistrationIntent(backend, previous + 1, encodedRegistration.copyOf()).also { persistIntent(it) }
        }
    }
    override suspend fun confirmIntent(intent: PushRegistrationIntent): Boolean {
        val intent = intent.copy(encodedRegistration = intent.encodedRegistration.copyOf())
        prepare(); intents.prepare()
        return database.transaction("connector-push-intent") {
            val current = intents.intent(intent.backend)
            if (current == null || current.revision != intent.revision || !current.encodedRegistration.contentEquals(intent.encodedRegistration)) false
            else { persistIntent(current.copy(pending = false)); true }
        }
    }
    private suspend fun persistIntent(intent: PushRegistrationIntent) {
        intents.put(intent)
        if (intent.encodedRegistration.isEmpty()) deleteRegistration(intent.backend)
        else saveRegistration(StoredPushRegistration(intent.backend, intent.encodedRegistration, intent.pending))
    }
    private val backendKey = PushRegistrationStoreImplDefinitionV1.backendKey

    override suspend fun getAllRegistrations(): List<StoredPushRegistration> = getAll().map { StoredPushRegistration.fromByteArray(it.toByteArray()) }

    override suspend fun saveRegistration(registration: StoredPushRegistration) = save(StoredPushRegistration.fromByteArray(registration.toByteArray()))

    override suspend fun getRegistration(backend: Int): StoredPushRegistration? = get(backendKey.eq(intToBytes(backend)))?.let { StoredPushRegistration.fromByteArray(it.toByteArray()) }

    override suspend fun deleteRegistration(backend: Int) = delete(backendKey.eq(intToBytes(backend)))
}

/**
 * Keeps the server's device-token registration in step with the client. Mirrors
 * [SubscriptionController]: the desired credential per backend is persisted, then
 * (re)sent to the server on every session (re)open — so a fresh session id always
 * relearns the token, and a send that failed while offline is retried on the next
 * connect. Token acquisition itself stays the app's responsibility; the app hands
 * over opaque credentials via [register].
 */
class PushRegistrationController(
    rpcClient: RpcClient,
    private val store: PushRegistrationStore,
    private val sessionIdSource: StateFlow<SessionId?>,
    private val telemetry: LockersTelemetry = LockersTelemetry.NONE,
    coroutineContext: kotlin.coroutines.CoroutineContext = Dispatchers.Default,
    private val connectionSource: StateFlow<StreamConnectionState>? = null,
    private val signRequest: (suspend (String, SessionId, ByteArray) -> SessionProof)? = null,
) {
    private val job = SupervisorJob(coroutineContext[Job])
    private val scope = CoroutineScope(coroutineContext + job)
    private val started = MutableStateFlow(false)
    private val pushService = PushServiceRpc(rpcClient)

    private data class State(val session: SessionId? = null, val epoch: Long = 0,
        val desired: Map<Int, PushRegistrationIntent> = emptyMap(), val loading: Boolean = true,
        val reconciled: Set<Int> = emptySet(), val confirmed: Map<Int, Long> = emptyMap(),
        val errors: Map<Int, Throwable> = emptyMap(), val closed: Boolean = false)
    private data class Effect(val session: SessionId?, val epoch: Long, val revision: Long, val bytes: String)
    private val state = MutableStateFlow(State())
    private sealed interface Command {
        data class Desired(val backend: Int, val encoded: ByteArray, val applied: CompletableDeferred<Unit>) : Command
        data class Session(val session: SessionId?, val epoch: Long, val failure: Throwable? = null) : Command
        data class Completed(val backend: Int, val effect: Effect, val intent: PushRegistrationIntent, val failure: Throwable?) : Command
    }
    private val commands = Channel<Command>(64)
    init { job.invokeOnCompletion { state.update { it.copy(closed = true) }; commands.close() } }

    fun start() {
        check(job.isActive) { "PushRegistrationController is closed" }
        if (!started.compareAndSet(false, true)) return
        scope.launch {
            try {
                val loaded = store.getAllIntents().associateBy { it.backend }
                state.update { it.copy(desired = loaded, loading = false) }
                for (command in commands) when (command) {
                    is Command.Desired -> {
                        try {
                            val intent = store.nextIntent(command.backend, command.encoded)
                            state.update { it.copy(desired = it.desired + (command.backend to intent), errors = it.errors - command.backend, reconciled = it.reconciled - command.backend, confirmed = it.confirmed - command.backend) }
                            command.applied.complete(Unit)
                        } catch (failure: Throwable) { command.applied.completeExceptionally(failure); if (failure is CancellationException) throw failure }
                    }
                    is Command.Session -> {
                        state.update { it.copy(session = command.session, epoch = command.epoch, reconciled = emptySet(), confirmed = emptyMap(), errors = if (command.failure == null) emptyMap() else PushBackendType.entries.associate { backend -> backend.protoValue to command.failure }) }
                    }
                    is Command.Completed -> {
                        val current = effect(command.backend)
                        if (current != command.effect) continue
                        if (command.failure != null) { state.update { it.copy(errors = it.errors + (command.backend to command.failure)) }; continue }
                        if (!store.confirmIntent(command.intent)) continue
                        state.update { it.copy(desired = it.desired + (command.backend to command.intent.copy(pending = false)), errors = it.errors - command.backend, confirmed = it.confirmed + (command.backend to command.intent.revision), reconciled = if (command.intent.encodedRegistration.isEmpty()) it.reconciled - command.backend else it.reconciled + command.backend) }
                    }
                }
            } catch (exhausted: RetryLimitExceeded) {
                currentCoroutineContext().ensureActive()
                state.update { it.copy(errors = PushBackendType.entries.associate { backend -> backend.protoValue to exhausted }) }; stop()
            } catch (cancelled: CancellationException) { throw cancelled }
            catch (failure: Throwable) { state.update { it.copy(errors = PushBackendType.entries.associate { backend -> backend.protoValue to failure }) }; stop() }
        }
        scope.launch {
            if (connectionSource != null) connectionSource.collect { connection ->
                when (connection) {
                    is StreamConnectionState.Connected -> commands.send(Command.Session(connection.sessionId.canonical(), connection.epoch))
                    is StreamConnectionState.Failed -> commands.send(Command.Session(null, 0, StreamFailedException(connection.error)))
                    is StreamConnectionState.Closed -> commands.send(Command.Session(null, 0, StreamClosedException()))
                    else -> commands.send(Command.Session(null, 0))
                }
            } else {
                var epoch = 0L
                sessionIdSource.collect { session -> commands.send(Command.Session(session?.canonical(), if (session == null) epoch else ++epoch)) }
            }
        }
        PushBackendType.entries.forEach { backend -> scope.launch {
            state.map { effect(backend.protoValue, it) }.distinctUntilChanged().collectLatest { target ->
                val session = target.session ?: return@collectLatest
                val intent = state.value.desired[backend.protoValue]?.takeIf { it.revision == target.revision } ?: return@collectLatest
                val failure = try { reconcile(session, intent); null }
                catch (exhausted: RetryLimitExceeded) { currentCoroutineContext().ensureActive(); exhausted }
                catch (cancelled: CancellationException) { throw cancelled }
                catch (error: Exception) { retainedProtocolFailure(error) }
                commands.send(Command.Completed(backend.protoValue, target, intent, failure))
            }
        } }
    }
    private fun effect(backend: Int, snapshot: State = state.value): Effect {
        val intent = snapshot.desired[backend]
        return Effect(snapshot.session, snapshot.epoch, intent?.revision ?: 0, intent?.encodedRegistration?.toBase64String() ?: "")
    }
    private suspend fun reconcile(session: SessionId, intent: PushRegistrationIntent) {
        repeatWithBackoff(exceptionHandler = { failure ->
            if (failure is CancellationException) throw failure
            when (failure) { is PushRegistrationException -> false; is RpcResponseException -> failure.retriable(); else -> isRetryableProtocolFailure(failure) }
        }) {
            if (intent.encodedRegistration.isEmpty()) {
                val unsigned = UnregisterSessionRequest(sessionId = session, backend = PushBackend.fromInt(intent.backend), credentialRevision = intent.revision)
                val request = unsigned.copy(proof = signRequest?.invoke(SessionSigning.UNREGISTER_PUSH, session, unsigned.toByteArray()))
                val response = telemetry.observe(TelemetryOperation.CONNECTOR_PUSH_UNREGISTER) { pushService.unregisterSession(request) }
                if (!response.result.isOk()) throw PushRegistrationException("Push revocation rejected: ${response.result}")
            } else {
                val unsigned = RegisterSessionRequest(sessionId = session, registration = PushRegistration.fromByteArray(intent.encodedRegistration), credentialRevision = intent.revision)
                val request = unsigned.copy(proof = signRequest?.invoke(SessionSigning.REGISTER_PUSH, session, unsigned.toByteArray()))
                val response = telemetry.observe(TelemetryOperation.CONNECTOR_PUSH_REGISTER) { pushService.registerSession(request) }
                if (!response.result.isOk()) throw PushRegistrationException("Push registration rejected: ${response.result}")
            }
            Unit
        }
    }
    private suspend fun desired(backend: Int, encoded: ByteArray) {
        start()
        val applied = CompletableDeferred<Unit>()
        val completion = job.invokeOnCompletion { applied.completeExceptionally(StreamClosedException()) }
        try { commands.send(Command.Desired(backend, encoded.copyOf(), applied)); applied.await() }
        finally { completion.dispose() }
    }
    suspend fun register(registration: PushRegistration) = desired(backendOf(registration) ?: throw IllegalArgumentException("push registration has no backend set"), registration.toByteArray())
    suspend fun unregister(backend: PushBackendType) = desired(backend.protoValue, byteArrayOf())
    suspend fun getPushConfig(): PushConfig? = pushService.getPushConfig(GetPushConfigRequest()).config
    /** Every outcome is a projection of the serialized desired/session/revision reducer. */
    val registrations: StateFlow<Map<PushBackendType, PushRegistrationStatus>> = MappedStateFlow(state) { current ->
        PushBackendType.entries.associateWith { backend ->
            val intent = current.desired[backend.protoValue]
            PushRegistrationStatus(intent?.revision ?: 0, current.session?.canonical(), current.epoch,
                intent?.encodedRegistration?.isNotEmpty() == true,
                current.confirmed[backend.protoValue] == intent?.revision && intent != null,
                current.errors[backend.protoValue], current.closed, current.loading)
        }
    }
    suspend fun awaitRegistered(backend: PushBackendType) {
        state.first { current ->
            current.errors[backend.protoValue]?.let { throw it }
            if (current.closed) throw StreamClosedException()
            if (!current.loading && current.desired[backend.protoValue]?.encodedRegistration?.isNotEmpty() != true)
                throw PushRegistrationException("Backend $backend is not desired")
            backend.protoValue in current.reconciled
        }
    }
    suspend fun awaitUnregistered(backend: PushBackendType) {
        state.first { current ->
            current.errors[backend.protoValue]?.let { throw it }
            if (current.closed) throw StreamClosedException()
            val intent = current.desired[backend.protoValue]
            if (intent?.encodedRegistration?.isNotEmpty() == true) throw PushRegistrationException("Backend $backend is still desired")
            !current.loading && (intent == null || current.confirmed[backend.protoValue] == intent.revision)
        }
    }
    suspend fun closeAndJoin() { stop(); job.join() }
    fun stop() { state.update { it.copy(closed = true) }; commands.close(); job.cancel() }
}

/** A credential-free view of desired push delivery state. */
data class PushRegistrationStatus(val revision: Long, val sessionId: SessionId?, val connectionEpoch: Long, val desired: Boolean, val confirmed: Boolean, val failure: Throwable?, val closed: Boolean, val loading: Boolean)
