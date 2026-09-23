package com.latenighthack.lockers.connector

import com.diamondedge.logging.KmLog
import com.diamondedge.logging.logging

import com.latenighthack.ktbuf.bytes.toBase64String
import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.rpc.repeatWithBackoff
import com.latenighthack.ktcrypto.Secp256r1KeyPair
import com.latenighthack.ktcrypto.encode
import com.latenighthack.ktstore.BoundStoreKey
import com.latenighthack.lockers.session.v1.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.random.Random
import com.latenighthack.ktstore.KeyValueStore
import com.latenighthack.ktstore.Store
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.internal.ShardedRoomServiceRpc
import com.latenighthack.lockers.connector.internal.ShardedSessionServiceRpc
import com.latenighthack.lockers.connector.storage.v1.*
import com.latenighthack.lockers.room.v1.*

interface SessionStore {
    suspend fun getSessionId(): SessionId?
    suspend fun updateSessionId(sessionId: SessionId?)

    suspend fun getNextSequenceBytes(): ByteArray?
    suspend fun updateNextSequenceBytes(rawBytes: ByteArray?)

    suspend fun getPendingAcks(): List<StoredAck>
    suspend fun addAck(ack: StoredAck)
    suspend fun clearAck(ack: StoredAck)
}

fun byteArrayIdentity(bytes: ByteArray): ByteArray {
    return bytes
}

class SessionStoreImpl(private val keyValueStore: KeyValueStore, delegate: StoreDelegate) : Store<StoredAck>(
    delegate,
    "acks",
    StoredAck::toByteArray,
    StoredAck::fromByteArray
), SessionStore {
    private val roomIdKey = serializedIndex(StoredAck::roomIdRawValue, ::byteArrayIdentity)
    private val eventIdKey = serializedIndex(StoredAck::eventIdRawValue, ::byteArrayIdentity)
    private val roomIdEventIdKey = compositeIndex(roomIdKey, eventIdKey).also { primaryKey(it) }

    companion object {
        private val SESSION_ID_KEY = "session_id"
        private val NEXT_SEQUENCE_KEY = "next_sequence_bytes"
    }

    override suspend fun getSessionId(): SessionId? = keyValueStore.get(SESSION_ID_KEY, SessionId.Companion::fromByteArray)
    override suspend fun updateSessionId(sessionId: SessionId?) = sessionId?.let {
        keyValueStore.save(SESSION_ID_KEY, sessionId, SessionId::toByteArray)
    } ?: keyValueStore.delete<SessionId>(SESSION_ID_KEY)

    override suspend fun getNextSequenceBytes(): ByteArray? = keyValueStore.get(NEXT_SEQUENCE_KEY, ::byteArrayIdentity)
    override suspend fun updateNextSequenceBytes(rawBytes: ByteArray?) = rawBytes?.let {
        keyValueStore.save(NEXT_SEQUENCE_KEY, rawBytes, ::byteArrayIdentity)
    } ?: keyValueStore.delete<ByteArray>(NEXT_SEQUENCE_KEY)

    override suspend fun getPendingAcks(): List<StoredAck> = getAll()

    override suspend fun addAck(ack: StoredAck) = save(ack)

    override suspend fun clearAck(ack: StoredAck) = delete(roomIdEventIdKey.eq(listOf(
        BoundStoreKey.SerializedKey(roomIdKey.name, ack.roomIdRawValue),
        BoundStoreKey.SerializedKey(eventIdKey.name, ack.eventIdRawValue)
    )))
}

interface AuthenticationKeySource {
    suspend fun getSessionKeyPair(): Secp256r1KeyPair
    suspend fun hasSessionKeyPair(): Boolean
    suspend fun generateSessionKeyPair()
    suspend fun revokeKeys()
}

interface SubscriptionStore {
    suspend fun getAllSubscriptions(): List<StoredSubscription>
    suspend fun updateSubscription(subscription: StoredSubscription)
    suspend fun getSubscription(roomId: RoomId): StoredSubscription?
    suspend fun deleteSubscription(roomId: RoomId)
}

class SubscriptionStoreImpl(delegate: StoreDelegate) : SubscriptionStore, Store<StoredSubscription>(
    delegate,
    "subscriptions",
    StoredSubscription::toByteArray,
    StoredSubscription.Companion::fromByteArray
) {
    private val roomIdKey = serializedIndex(StoredSubscription::roomIdRawValue, ::byteArrayIdentity).also { primaryKey(it) }

    override suspend fun getAllSubscriptions() = getAll()

    override suspend fun updateSubscription(subscription: StoredSubscription) = save(subscription)

    override suspend fun getSubscription(roomId: RoomId): StoredSubscription? = get(roomIdKey.eq(roomId.rawValue))

    override suspend fun deleteSubscription(roomId: RoomId) = delete(roomIdKey.eq(roomId.rawValue))
}

class SubscriptionController(
    private val rpcClient: RpcClient,
    private val subscriptionStore: SubscriptionStore,
    @Suppress("UNUSED_PARAMETER") sessionStore: SessionStore,
    private val sessionIdSource: Flow<SessionId?>,
    private val log: KmLog = logging()
) {
    private val controllerJob = SupervisorJob()
    private val controllerScope = CoroutineScope(Dispatchers.Default + controllerJob)
    private val roomService = ShardedRoomServiceRpc(rpcClient)

    // Persisted subscriptions describe intent, not confirmation for a particular session.
    // Serialize intent, session changes and acknowledgments so a late RPC cannot confirm
    // an obsolete session or overwrite a newer unsubscribe. RPCs still run per room.
    private sealed interface Change {
        data class Desired(val roomId: RoomId, val subscribed: Boolean) : Change
        data class Session(val sessionId: SessionId?) : Change
        data object Refresh : Change
        data class Confirmed(val roomId: RoomId, val sessionId: SessionId,
                             val generation: Long, val subscribed: Boolean) : Change
    }

    private data class Confirmations(val sessionId: SessionId? = null, val rooms: Set<RoomId> = emptySet())
    private val changes = kotlinx.coroutines.channels.Channel<Change>(kotlinx.coroutines.channels.Channel.UNLIMITED)
    private val confirmations = MutableStateFlow(Confirmations())
    private val newSubscriptions = MutableSharedFlow<RoomId>(extraBufferCapacity = 64)

    suspend fun resendSubscriptions() {
        changes.send(Change.Refresh)
    }

    suspend fun startWatchingSubscriptions() {
        val desired = subscriptionStore.getAllSubscriptions().associate {
            RoomId(it.roomIdRawValue) to !it.isPendingRemove
        }.toMutableMap()

        controllerScope.launch {
            var sessionId: SessionId? = null
            var nextGeneration = 0L
            val generations = mutableMapOf<RoomId, Long>()
            val jobs = mutableMapOf<RoomId, Job>()

            fun reconcile(roomId: RoomId, subscribed: Boolean) {
                jobs.remove(roomId)?.cancel()
                val generation = ++nextGeneration
                generations[roomId] = generation
                val targetSession = sessionId ?: return
                jobs[roomId] = controllerScope.launch {
                    repeatWithBackoff(exceptionHandler = { it !is CancellationException }) {
                        val response = roomService.subscription(SubscriptionRequest {
                            this.sessionId = targetSession
                            this.roomId = roomId
                            if (subscribed) kind.subscribe { } else kind.unsubscribe { }
                        })
                        check(response.result.isOk()) { "subscription rejected: ${response.result}" }
                    }
                    changes.send(Change.Confirmed(roomId, targetSession, generation, subscribed))
                }
            }

            for (change in changes) {
                when (change) {
                    is Change.Desired -> {
                        if (desired[change.roomId] == change.subscribed) continue
                        desired[change.roomId] = change.subscribed
                        confirmations.update { it.copy(rooms = it.rooms - change.roomId) }
                        subscriptionStore.updateSubscription(StoredSubscription {
                            roomIdRawValue = change.roomId.rawValue
                            isPendingAdd = change.subscribed
                            isPendingRemove = !change.subscribed
                        })
                        reconcile(change.roomId, change.subscribed)
                    }
                    is Change.Session, Change.Refresh -> {
                        if (change is Change.Session) {
                            if (sessionId == change.sessionId) continue
                            sessionId = change.sessionId
                        }
                        confirmations.value = Confirmations(sessionId)
                        jobs.values.forEach { it.cancel() }
                        jobs.clear()
                        // Reconcile every persisted intent for the new session, including rooms
                        // previously confirmed on an old one. Never deduplicate this as a user ask.
                        desired.forEach { (room, subscribed) -> reconcile(room, subscribed) }
                    }
                    is Change.Confirmed -> {
                        if (change.sessionId != sessionId || generations[change.roomId] != change.generation) continue
                        jobs.remove(change.roomId)
                        if (change.subscribed) {
                            subscriptionStore.updateSubscription(StoredSubscription {
                                roomIdRawValue = change.roomId.rawValue
                            })
                            confirmations.update { it.copy(rooms = it.rooms + change.roomId) }
                            newSubscriptions.emit(change.roomId)
                        } else {
                            subscriptionStore.deleteSubscription(change.roomId)
                            desired.remove(change.roomId)
                        }
                    }
                }
            }
        }
        controllerScope.launch {
            sessionIdSource.distinctUntilChanged().collect { changes.send(Change.Session(it)) }
        }
    }

    suspend fun awaitSubscription(roomId: RoomId) {
        combine(sessionIdSource, confirmations) { session, confirmed ->
            session != null && session == confirmed.sessionId && roomId in confirmed.rooms
        }.first { it }
    }

    fun watchNewSubscriptions(): Flow<RoomId> = newSubscriptions

    suspend fun subscribe(roomId: RoomId) {
        changes.send(Change.Desired(roomId, true))
    }

    suspend fun unsubscribe(roomId: RoomId) {
        changes.send(Change.Desired(roomId, false))
    }

    fun stop() {
        controllerJob.cancel()
        changes.close()
    }
}

/**
 * A terminal, non-retryable session-open failure. When one occurs the stream
 * stops reconnecting and surfaces the reason via [Stream.fatalError] so the app
 * can react (e.g. regenerate keys or prompt for an upgrade).
 */
sealed class StreamFatalError(val reason: String) {
    object InvalidPublicKey : StreamFatalError("session public key was rejected by the server")
    object InvalidSessionId : StreamFatalError("session id was rejected by the server")
    object UpgradeRequired : StreamFatalError("client version is no longer supported; upgrade required")
    object TransportExhausted : StreamFatalError("transport retries exhausted")
}

private class FatalStreamException(val error: StreamFatalError) : CancellationException(error.reason)

private class RetryableStreamException(message: String) : Exception(message)

class Stream(
    private val rpcClient: RpcClient,
    private val keySource: AuthenticationKeySource,
    private val sessionStore: SessionStore,
    private val subscriptionStore: SubscriptionStore,
    private val appVersion: Version
) {
    companion object {
        val PING_TIMEOUT = 60_000L
        val RECONNECT_DELAY_MILLIS = 1_000L

        // Consecutive INVALID_SEQUENCE opens tolerated before abandoning the session and
        // re-creating. Re-creating orphans the old session's queued events, so it is a last
        // resort — but a permanent lockout is worse.
        const val INVALID_SEQUENCE_WIPE_THRESHOLD = 3
    }

    // Consecutive INVALID_SEQUENCE count across reconnects; reset by a successful open.
    private var invalidSequenceStreak = 0

    private val streamJob = SupervisorJob()
    private val streamScope = CoroutineScope(Dispatchers.Default + streamJob)
    private val sessionIdSource = MutableStateFlow<SessionId?>(null)
    private val sessionService = ShardedSessionServiceRpc(rpcClient)

    // When routing through a [RoutingRpcClient], an EPOCH_STALE + redirect on session open is
    // recorded here so the reconnect re-targets the owning node. A plain client (monolith) never
    // sees EPOCH_STALE, so this stays a no-op.
    private val routing = rpcClient as? RoutingRpcClient
    private val subscriptionController = SubscriptionController(rpcClient, subscriptionStore, sessionStore, sessionIdSource)
    private val outgoingAcks = MutableSharedFlow<List<StoredAck>>()

    private val incomingEvents = MutableSharedFlow<Event>()
    val events: Flow<Event>
        get() {
            return incomingEvents
        }

    private val onConnected = MutableStateFlow(false)
    val isConnected: Flow<Boolean>
        get() {
            return onConnected
        }

    private val fatalErrorState = MutableStateFlow<StreamFatalError?>(null)

    /**
     * Emits a non-null value when the stream hits a terminal session-open error
     * and stops reconnecting. Stays null during normal operation and transient,
     * automatically-retried failures.
     */
    val fatalError: Flow<StreamFatalError?>
        get() {
            return fatalErrorState
        }

    /** The current session id once the session has opened, else null. */
    val sessionId: StateFlow<SessionId?>
        get() = sessionIdSource

    private suspend fun connect() {
        // Outer loop: a stream that ends WITHOUT throwing (server closed the socket cleanly —
        // LB idle timeout, deploy, session takeover) must reconnect too; previously only
        // exceptions retried and a clean close ended the stream permanently. Each pass restarts
        // repeatWithBackoff with a fresh budget, so TransportExhausted still fires on
        // *consecutive* transport failures.
        while (true) {
            onConnected.value = false

            repeatWithBackoff(exceptionHandler = { it !is CancellationException }) {
                connectInternal()
                onConnected.value = false
            }

            // Clean close: brief pause so a same-session takeover fight can't tight-loop.
            delay(RECONNECT_DELAY_MILLIS)
        }
    }

    private fun failFatally(error: StreamFatalError): Nothing {
        fatalErrorState.value = error
        throw FatalStreamException(error)
    }

    fun watchNewSubscriptions(): Flow<RoomId> {
        return subscriptionController.watchNewSubscriptions()
    }

    suspend fun start() {
        subscriptionController.startWatchingSubscriptions()

        streamScope.launch {
            // connect() throws on fatal errors and on retry exhaustion; either way the
            // outcome belongs in fatalError, not an uncaught scope crash
            try {
                connect()
            } catch (e: FatalStreamException) {
                // fatalErrorState already set by failFatally
            } catch (e: kotlinx.coroutines.CancellationException) {
                throw e
            } catch (e: Exception) {
                fatalErrorState.value = StreamFatalError.TransportExhausted
            }
        }
    }

    private suspend fun connectInternal() {
        val (currentSessionId, isNew) = (
            sessionStore.getSessionId()?.let { Pair(it, false) } ?: Pair(SessionId(Random.nextBytes(32)), true)
        )
        val nextSequenceBytes = sessionStore.getNextSequenceBytes()
        val keyPair = keySource.getSessionKeyPair()
        val encodedPublicKey = keyPair.publicKey.encode()
        val nextSequenceSignature = nextSequenceBytes?.let { keyPair.privateKey.sign(it) }

        sessionService
            .watchSession(flow {
                emit(WatchSessionRequest {
                    if (isNew) {
                        request.create {
                            sessionId = currentSessionId
                            publicKey { rawValue = encodedPublicKey }
                        }
                    } else {
                        request.open {
                            sessionId = currentSessionId
                            sequenceKeySignature {
                                publicKey { rawValue = encodedPublicKey }
                                signature = nextSequenceSignature ?: byteArrayOf()
                            }
                        }
                    }
                })

                emitAll(
                    merge(outgoingAcks
                        .onEach {
                            for (ack in it) {
                                sessionStore.addAck(ack)
                            }
                        }
                        .onStart {
                            val storedAcks = sessionStore.getPendingAcks()

                            emit(storedAcks)
                        }
                        .filter { it.isNotEmpty() }
                        .map { acksToSend ->
                            WatchSessionRequest {
                                request.ack {
                                    acks {
                                        for (ack in acksToSend) {
                                            addEventAck {
                                                roomId { rawValue = ack.roomIdRawValue }
                                                eventId { rawValue = ack.eventIdRawValue }
                                            }
                                        }
                                    }
                                }
                            }
                        }, flow {
                            while (true) {
                                delay(PING_TIMEOUT)
                                emit(WatchSessionRequest {
                                    request.ping { }
                                })
                            }
                        }
                    )
                )

                awaitCancellation()
            })
            .collect { response ->
                when (val oneOf = response.response) {
                    is WatchSessionResponse.OneOfResponse.open -> {
                        val open = oneOf.getOpen()!!

                        when (open.result) {
                            is WatchSessionResponse.Open.Result.OK -> {
                                invalidSequenceStreak = 0
                                sessionStore.updateNextSequenceBytes(open.nextSequenceKey)
                                sessionStore.updateSessionId(currentSessionId)

                                onConnected.value = true
                                sessionIdSource.value = currentSessionId

                                processIncomingEvents(open.queuedEvents)
                            }
                            is WatchSessionResponse.Open.Result.INVALID_SEQUENCE -> {
                                // Sequence desync (e.g. killed between the server's key rotation and
                                // our persist). The server returns the material to re-sign; persist
                                // it and reconnect. NEVER store an empty key — doing so poisons every
                                // future open (we would sign empty bytes forever).
                                invalidSequenceStreak += 1
                                if (open.nextSequenceKey.isNotEmpty()) {
                                    sessionStore.updateNextSequenceBytes(open.nextSequenceKey)
                                }
                                if (invalidSequenceStreak >= INVALID_SEQUENCE_WIPE_THRESHOLD) {
                                    // Not converging (old server without recovery material, or a
                                    // key mismatch): abandon the session; the reconnect creates a
                                    // fresh one and resendSubscriptions restores its state.
                                    invalidSequenceStreak = 0
                                    sessionStore.updateSessionId(null)
                                    sessionStore.updateNextSequenceBytes(null)
                                }
                                throw RetryableStreamException("sequence desync; reconnecting")
                            }
                            is WatchSessionResponse.Open.Result.UNKNOWN_SESSION -> {
                                sessionStore.updateSessionId(null)
                                sessionStore.updateNextSequenceBytes(null)
                                throw RetryableStreamException("unknown session; re-creating")
                            }
                            is WatchSessionResponse.Open.Result.SERVICE_UNAVAILABLE ->
                                throw RetryableStreamException("session service unavailable")
                            is WatchSessionResponse.Open.Result.EPOCH_STALE -> {
                                // The session's shard moved to another node. Cache the server's
                                // redirect toward the owner (keyed by this session's `s` metadata)
                                // then reconnect: the routing client re-targets the owner on the
                                // next open, and resendSubscriptions + inbox replay recover state.
                                routing?.recordRedirect(
                                    currentSessionId.rawValue.toBase64String(),
                                    open.redirect?.ownerAddress ?: "",
                                    open.redirect?.epoch ?: 0L,
                                )
                                throw RetryableStreamException("session shard moved; reconnecting")
                            }
                            is WatchSessionResponse.Open.Result.UNKNOWN_ERROR ->
                                throw RetryableStreamException("unknown error opening session")
                            is WatchSessionResponse.Open.Result.SESSION_EXISTS -> {
                                // Our freshly generated session id collided; drop it so the
                                // next reconnect generates a new one.
                                sessionStore.updateSessionId(null)
                                throw RetryableStreamException("session already exists; regenerating id")
                            }
                            is WatchSessionResponse.Open.Result.INVALID_PUBLIC_KEY ->
                                failFatally(StreamFatalError.InvalidPublicKey)
                            is WatchSessionResponse.Open.Result.INVALID_SESSION_ID ->
                                failFatally(StreamFatalError.InvalidSessionId)
                            is WatchSessionResponse.Open.Result.UPGRADE_REQUIRED ->
                                failFatally(StreamFatalError.UpgradeRequired)
                            is WatchSessionResponse.Open.Result.UNKNOWN_ -> {
                                throw Exception("Failed to open session: ${open.result}")
                            }
                        }
                    }
                    is WatchSessionResponse.OneOfResponse.events -> {
                        val events = oneOf.getEvents()!!

                        processIncomingEvents(events.event)
                    }
                    is WatchSessionResponse.OneOfResponse.ack -> {
                        val ack = oneOf.getAck()!!

                        for (confirmedAck in ack.confirmedAcks) {
                            sessionStore.clearAck(
                                StoredAck(
                                confirmedAck.roomId!!.rawValue,
                                confirmedAck.eventId!!.rawValue,
                            )
                            )
                        }
                    }
                    is WatchSessionResponse.OneOfResponse.pong -> {
                    }
                    null -> {
                        throw Exception("Received null response")
                    }
                }
            }
    }

    private suspend fun processIncomingEvents(queuedEvents: List<Event>) {
        for (event in queuedEvents) {
            processEvent(event)
        }

        outgoingAcks.emit(queuedEvents.map { event ->
            StoredAck(event.roomId!!.rawValue, event.eventId!!.rawValue)
        })
    }

    private suspend fun processEvent(event: Event) {
        incomingEvents.emit(event)
    }

    suspend fun subscribe(roomId: RoomId, waitForSubscription: Boolean = false) {
        subscriptionController.subscribe(roomId)

        if (waitForSubscription) {
            subscriptionController.awaitSubscription(roomId)
        }
    }

    suspend fun unsubscribe(roomId: RoomId) {
        subscriptionController.unsubscribe(roomId)
    }

    fun stop() {
        subscriptionController.stop()
        streamJob.cancel()
    }
}
