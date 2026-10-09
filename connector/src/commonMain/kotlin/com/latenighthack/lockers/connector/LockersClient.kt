package com.latenighthack.lockers.connector

import com.latenighthack.lockers.observability.*

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktstore.KeyValueStore
import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.common.v1.Version
import com.latenighthack.lockers.connector.internal.LockerStoreImpl
import com.latenighthack.lockers.push.v1.PushConfig
import com.latenighthack.lockers.push.v1.PushRegistration
import kotlinx.coroutines.*
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
 * val client = LockersClient.create(rpcClient, database, keyValueStore, keySource, appVersion)
 * client.awaitConnected()
 * val chat = client.typed(CHAT_KEYSPACE, ChatMessage::toByteArray, ChatMessage.Companion::fromByteArray)
 * ```
 */
class LockersClient private constructor(
    private val stream: Stream,
    val lockers: LockerClient,
    private val pushRegistrations: PushRegistrationController,
    private val clientJob: Job,
) {
    /** Emits `true` while the session stream is connected. */
    val isConnected: Flow<Boolean> get() = stream.isConnected
    val connection: StateFlow<StreamConnectionState> get() = stream.connection

    /** Emits a non-null value when the stream hits a terminal, non-retryable error. */
    val fatalError: Flow<StreamFatalError?> get() = stream.fatalError

    /** The current session id once the session has opened, else null. */
    val sessionId: StateFlow<SessionId?> get() = stream.sessionId

    /** Suspends until the stream connects at least once. */
    suspend fun awaitConnected() {
        stream.awaitConnected()
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

    /** Recover accepted changes after an application-owned persisted cursor. */
    fun lockerChangesAfter(cursor: Long): Flow<AcceptedLockerChange> = lockers.changesAfter(cursor)
    /** Raw accepted session events, including notification metadata, for durable consumption. */
    fun eventsAfter(cursor: Long): Flow<AcceptedSessionEvent> = stream.eventsAfter(cursor)
    val broadcasts: Flow<IncomingBroadcast> get() = lockers.broadcasts
    fun broadcastsAfter(cursor: Long): Flow<IncomingBroadcast> = lockers.broadcastsAfter(cursor)
    /** Advance only through the minimum persisted cursor of every independent application consumer. */
    suspend fun pruneAcceptedEventsThrough(cursor: Long) = lockers.pruneAcceptedEventsThrough(cursor)
    /** Cutoff must precede the server's maximum event replay horizon. Legacy confirmations start a conservative age on first maintenance. */
    suspend fun pruneConfirmedAcksBefore(cutoffMillis: Long) = stream.pruneConfirmedAcksBefore(cutoffMillis)

    fun writeOutcomes(roomId: com.latenighthack.lockers.common.v1.RoomId, writeRequestId: ByteArray, pollIntervalMillis: Long = 1_000, timeoutMillis: Long = 300_000): Flow<WriteOutcomeObservation> =
        lockers.writeOutcomes(roomId, writeRequestId, pollIntervalMillis, timeoutMillis)
    suspend fun awaitWriteOutcome(roomId: com.latenighthack.lockers.common.v1.RoomId, writeRequestId: ByteArray, timeoutMillis: Long = 300_000): WriteOutcomeObservation =
        lockers.awaitWriteOutcome(roomId, writeRequestId, timeoutMillis)

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
    suspend fun awaitPushUnregistered(backend: PushBackendType) = pushRegistrations.awaitUnregistered(backend)
    val pushRegistrationStates: StateFlow<Map<PushBackendType, PushRegistrationStatus>> get() = pushRegistrations.registrations

    /** Server push capabilities — notably the VAPID public key a web client needs to subscribe. */
    suspend fun getPushConfig(): PushConfig? = pushRegistrations.getPushConfig()

    /** Authenticates revocation of the connected server session before closing this client. */
    suspend fun destroySession() { stream.destroySession(); closeAndJoin() }

    /** Tears down the stream and background processing. */
    fun close() {
        pushRegistrations.stop()
        lockers.stop()
        stream.stop()
        clientJob.cancel()
    }

    /** Suspends until transports, reducers and pending owned work have stopped. */
    suspend fun closeAndJoin() { close(); clientJob.join() }

    companion object {
        /**
         * Builds and starts a client. The result is started but not necessarily
         * connected yet — call [awaitConnected] to wait for the first successful
         * session open. When ratchets are enabled, this database retains current authority private keys
         * for crash recovery. Use an application-trusted encrypted/secure delegate and restrict backups
         * and access; ordinary locker/watch/broadcast APIs never expose those keys. The latest key per
         * scope is retained until an authoritative newer epoch proves it obsolete.
         */
        suspend fun create(
            rpcClient: RpcClient,
            database: Database,
            keyValueStore: KeyValueStore,
            keySource: AuthenticationKeySource,
            appVersion: Version,
            lockKeySource: LockKeySource? = null,
            codecs: NotificationCodecs = NotificationCodecs.identity(),
            telemetry: LockersTelemetry = LockersTelemetry.NONE,
            coroutineContext: kotlin.coroutines.CoroutineContext = kotlin.coroutines.EmptyCoroutineContext,
            broadcastCodecs: BroadcastCodecs = BroadcastCodecs.identity(),
            retentionPolicy: ConnectorRetentionPolicy = ConnectorRetentionPolicy(),
        ): LockersClient {
            database.open()
            val sessionStore = SessionStoreImpl(keyValueStore, database, retentionPolicy)
            val subscriptionStore = SubscriptionStoreImpl(database)
            val lockerStore = LockerStoreImpl(database, retentionPolicy)
            val pushRegistrationStore = PushRegistrationStoreImpl(database)

            sessionStore.prepare()
            subscriptionStore.prepare()
            lockerStore.prepare()
            pushRegistrationStore.prepare()

            val parentContext = currentCoroutineContext() + coroutineContext
            val clientJob = SupervisorJob(parentContext[Job])
            val ownedContext = parentContext + clientJob
            val stream = Stream(rpcClient, keySource, sessionStore, subscriptionStore, appVersion, telemetry, ownedContext)
            val lockerClient = LockerClient(rpcClient, stream, lockerStore, lockKeySource, codecs, telemetry = telemetry, coroutineContext = ownedContext, broadcastCodecs = broadcastCodecs)
            val pushRegistrations = PushRegistrationController(rpcClient, pushRegistrationStore, stream.sessionId, telemetry, ownedContext, stream.connection, stream::signSessionRequest)
            try {
                lockerClient.start()
                stream.start()
                pushRegistrations.start()
                return LockersClient(stream, lockerClient, pushRegistrations, clientJob)
            } catch (failure: Throwable) {
                withContext(NonCancellable) {
                    pushRegistrations.stop(); lockerClient.stop(); stream.stop()
                    clientJob.cancelAndJoin()
                }
                throw failure
            }
        }
    }
}
