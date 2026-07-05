package com.latenighthack.lockers.connector

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktstore.KeyValueStore
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.common.v1.Version
import com.latenighthack.lockers.connector.internal.LockerStoreImpl
import com.latenighthack.lockers.push.v1.PushConfig
import com.latenighthack.lockers.push.v1.PushRegistration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlin.reflect.KFunction1

/**
 * The entry point for the lockers client. Assembles the persistent stores, the
 * session [Stream] and the [LockerClient], owning store-preparation order and the
 * start/close lifecycle so callers don't wire the parts together by hand.
 *
 * ```
 * val client = LockersClient.create(rpcClient, storeDelegate, keyValueStore, keySource, appVersion)
 * client.awaitConnected()
 * val chat = client.typed(CHAT_KEYSPACE, ChatMessage::toByteArray, ChatMessage.Companion::fromByteArray)
 * ```
 */
class LockersClient private constructor(
    private val stream: Stream,
    val lockers: LockerClient,
    private val pushRegistrations: PushRegistrationController,
) {
    /** Emits `true` while the session stream is connected. */
    val isConnected: Flow<Boolean> get() = stream.isConnected

    /** Emits a non-null value when the stream hits a terminal, non-retryable error. */
    val fatalError: Flow<StreamFatalError?> get() = stream.fatalError

    /** The current session id once the session has opened, else null. */
    val sessionId: StateFlow<SessionId?> get() = stream.sessionId

    /** Suspends until the stream connects at least once. */
    suspend fun awaitConnected() {
        isConnected.filter { it }.first()
    }

    /** Creates a keyspace-scoped, typed view over the shared [LockerClient]. */
    fun <V> typed(
        keyspace: LockerKeyspace,
        writer: KFunction1<V, ByteArray>,
        reader: KFunction1<ByteArray, V>,
    ): TypedLockerClient<V> = TypedLockerClient(lockers, keyspace, writer, reader)

    /** Every locker in the local cache, across all rooms and keyspaces (for introspection). */
    suspend fun getAllKnownLockers(): List<LockerClient.LockerUpdate> = lockers.getAllKnownLockers()

    /** The stream of locker changes (adds, updates, deletes) across every keyspace. */
    val lockerChanges: Flow<LockerClient.LockerUpdate> get() = lockers.changes

    /**
     * Registers (or rotates) this device's push credential for its backend. The
     * credential is persisted and re-sent automatically on every reconnect;
     * acquiring the token/subscription stays the app's responsibility. Build the
     * argument with [PushRegistrations].
     */
    suspend fun registerPush(registration: PushRegistration) = pushRegistrations.register(registration)

    /** Alias of [registerPush] for the key-rotation case (same upsert-by-backend semantics). */
    suspend fun updatePushKeys(registration: PushRegistration) = pushRegistrations.register(registration)

    /** Drops this device's credential for [backend] locally and on the server. */
    suspend fun unregisterPush(backend: PushBackendType) = pushRegistrations.unregister(backend)

    /** Suspends until [backend]'s credential has been acknowledged for the current session. */
    suspend fun awaitPushRegistered(backend: PushBackendType) = pushRegistrations.awaitRegistered(backend)

    /** Server push capabilities — notably the VAPID public key a web client needs to subscribe. */
    suspend fun getPushConfig(): PushConfig? = pushRegistrations.getPushConfig()

    /** Tears down the stream and background processing. */
    fun close() {
        pushRegistrations.stop()
        lockers.stop()
        stream.stop()
    }

    companion object {
        /**
         * Builds and starts a client. The result is started but not necessarily
         * connected yet — call [awaitConnected] to wait for the first successful
         * session open.
         */
        suspend fun create(
            rpcClient: RpcClient,
            storeDelegate: StoreDelegate,
            keyValueStore: KeyValueStore,
            keySource: AuthenticationKeySource,
            appVersion: Version,
            lockKeySource: LockKeySource? = null,
            codecs: NotificationCodecs = NotificationCodecs.identity(),
        ): LockersClient {
            val sessionStore = SessionStoreImpl(keyValueStore, storeDelegate)
            val subscriptionStore = SubscriptionStoreImpl(storeDelegate)
            val lockerStore = LockerStoreImpl(storeDelegate)
            val pushRegistrationStore = PushRegistrationStoreImpl(storeDelegate)

            sessionStore.prepare()
            subscriptionStore.prepare()
            lockerStore.prepare()
            pushRegistrationStore.prepare()
            storeDelegate.createStores()

            val stream = Stream(rpcClient, keySource, sessionStore, subscriptionStore, appVersion)
            val lockerClient = LockerClient(rpcClient, stream, lockerStore, lockKeySource, codecs)
            val pushRegistrations = PushRegistrationController(rpcClient, pushRegistrationStore, stream.sessionId)

            stream.start()
            lockerClient.start()
            pushRegistrations.start()

            return LockersClient(stream, lockerClient, pushRegistrations)
        }
    }
}
