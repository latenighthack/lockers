package com.latenighthack.lockers.connector

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktstore.Store
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.connector.storage.v1.StoredPushRegistration
import com.latenighthack.lockers.connector.storage.v1.fromByteArray
import com.latenighthack.lockers.connector.storage.v1.toByteArray
import com.latenighthack.lockers.push.v1.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch

/** The push backends a client can register a credential for. */
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

private fun intToBytes(value: Int): ByteArray = byteArrayOf(
    (value ushr 24).toByte(),
    (value ushr 16).toByte(),
    (value ushr 8).toByte(),
    value.toByte(),
)

interface PushRegistrationStore {
    suspend fun getAllRegistrations(): List<StoredPushRegistration>
    suspend fun saveRegistration(registration: StoredPushRegistration)
    suspend fun getRegistration(backend: Int): StoredPushRegistration?
    suspend fun deleteRegistration(backend: Int)
}

class PushRegistrationStoreImpl(delegate: StoreDelegate) : PushRegistrationStore, Store<StoredPushRegistration>(
    delegate,
    "push_registrations",
    StoredPushRegistration::toByteArray,
    StoredPushRegistration.Companion::fromByteArray,
) {
    private val backendKey = serializedIndex(StoredPushRegistration::backend, ::intToBytes).also { primaryKey(it) }

    override suspend fun getAllRegistrations(): List<StoredPushRegistration> = getAll()

    override suspend fun saveRegistration(registration: StoredPushRegistration) = save(registration)

    override suspend fun getRegistration(backend: Int): StoredPushRegistration? = get(backendKey.eq(intToBytes(backend)))

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
) {
    private val job = SupervisorJob()
    private val scope = CoroutineScope(Dispatchers.Default + job)
    private val pushService = PushServiceRpc(rpcClient)

    private val registrationChanges = MutableSharedFlow<PushRegistration>(extraBufferCapacity = 16)
    private val reconciled = MutableStateFlow<Set<Int>>(emptySet())

    fun start() {
        // Resend every stored registration whenever a session opens; a new session
        // id needs the token relearned, a resumed one is an idempotent no-op.
        scope.launch {
            sessionIdSource
                .filterNotNull()
                .distinctUntilChanged()
                .collect { sessionId ->
                    reconciled.value = emptySet()
                    for (stored in store.getAllRegistrations()) {
                        sendRegister(sessionId, stored.backend, PushRegistration.fromByteArray(stored.encodedRegistration))
                    }
                }
        }

        // Push a freshly-supplied credential to the server as soon as a session exists.
        scope.launch {
            registrationChanges.collect { registration ->
                val backend = backendOf(registration) ?: return@collect
                val sessionId = sessionIdSource.filterNotNull().first()
                sendRegister(sessionId, backend, registration)
            }
        }
    }

    /** Registers or rotates the credential for its backend. */
    suspend fun register(registration: PushRegistration) {
        val backend = backendOf(registration)
            ?: throw IllegalArgumentException("push registration has no backend set")
        store.saveRegistration(StoredPushRegistration {
            this.backend = backend
            this.encodedRegistration = registration.toByteArray()
            this.isPending = true
        })
        reconciled.value = reconciled.value - backend
        registrationChanges.emit(registration)
    }

    /** Drops the credential for [backend] locally and, if a session is open, on the server. */
    suspend fun unregister(backend: PushBackendType) {
        val sessionId = sessionIdSource.value
        if (sessionId != null) {
            runCatching {
                pushService.unregisterSession(UnregisterSessionRequest {
                    this.sessionId = sessionId
                    this.backend = PushBackend.fromInt(backend.protoValue)
                })
            }
        }
        store.deleteRegistration(backend.protoValue)
        reconciled.value = reconciled.value - backend.protoValue
    }

    /** Fetches server push capabilities (e.g. the VAPID public key for web subscriptions). */
    suspend fun getPushConfig(): PushConfig? = pushService.getPushConfig(GetPushConfigRequest {}).config

    /** Suspends until [backend] has been acknowledged for the current session. */
    suspend fun awaitRegistered(backend: PushBackendType) {
        if (backend.protoValue in reconciled.value) return
        reconciled.filter { backend.protoValue in it }.first()
    }

    private suspend fun sendRegister(sessionId: SessionId, backend: Int, registration: PushRegistration) {
        val response = runCatching {
            pushService.registerSession(RegisterSessionRequest {
                this.sessionId = sessionId
                this.registration = registration
            })
        }.getOrNull() ?: return

        if (response.result is RegisterSessionResponse.Result.OK) {
            store.saveRegistration(StoredPushRegistration {
                this.backend = backend
                this.encodedRegistration = registration.toByteArray()
                this.isPending = false
            })
            reconciled.value = reconciled.value + backend
        }
    }

    fun stop() {
        job.cancel()
    }
}
