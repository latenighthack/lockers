package com.latenighthack.lockers.connector

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktstore.KeyValueStore
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.common.v1.Version
import com.latenighthack.lockers.connector.internal.LockerStoreImpl
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

    /** Tears down the stream and background processing. */
    fun close() {
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
        ): LockersClient {
            val sessionStore = SessionStoreImpl(keyValueStore, storeDelegate)
            val subscriptionStore = SubscriptionStoreImpl(storeDelegate)
            val lockerStore = LockerStoreImpl(storeDelegate)

            sessionStore.prepare()
            subscriptionStore.prepare()
            lockerStore.prepare()
            storeDelegate.createStores()

            val stream = Stream(rpcClient, keySource, sessionStore, subscriptionStore, appVersion)
            val lockerClient = LockerClient(rpcClient, stream, lockerStore, lockKeySource)

            stream.start()
            lockerClient.start()

            return LockersClient(stream, lockerClient)
        }
    }
}
