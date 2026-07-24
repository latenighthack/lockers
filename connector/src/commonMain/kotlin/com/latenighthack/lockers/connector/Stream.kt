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
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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
    private val sessionStore: SessionStore,
    private val sessionIdSource: Flow<SessionId?>,
    private val log: KmLog = logging()
) {
    private val controllerJob = SupervisorJob()
    private val controllerScope = CoroutineScope(Dispatchers.Default + controllerJob)
    private val roomService = ShardedRoomServiceRpc(rpcClient)

    suspend fun resendSubscriptions() {
        val changes = mutableListOf<SubscriptionChange>()
        for (subscription in subscriptionStore.getAllSubscriptions()) {
            if (subscription.isPendingAdd || !subscription.isPendingRemove) {
                // resend
                changes.add(SubscriptionChange.Subscribed(RoomId(subscription.roomIdRawValue)))
            } else {
                // clear
                subscriptionStore.deleteSubscription(RoomId(subscription.roomIdRawValue))
            }
        }

        subscriptionChanges.emitAll(changes.asFlow())
    }

    // Reconcile a single room's subscription against the server, retrying the RPC in-session
    // with backoff instead of parking the room (pending in the store) until the next reconnect.
    // Runs per-room (see the fan-out in startWatchingSubscriptions), so a room whose RPC keeps
    // failing retries on its own without blocking other rooms. CancellationException — including
    // supersession by a newer change, RetryLimitExceeded, and stop() — is never retried and
    // propagates. On success the reconciled state is finalized in the store.
    private suspend fun reconcile(subscriptionChange: SubscriptionChange) {
        repeatWithBackoff(exceptionHandler = { it !is CancellationException }) {
            val sessionId = sessionIdSource.filter { it != null }.first()

            roomService.subscription(SubscriptionRequest {
                this.sessionId = sessionId

                when (subscriptionChange) {
                    is SubscriptionChange.Subscribed -> {
                        roomId = subscriptionChange.roomId
                        kind.subscribe { }
                    }

                    is SubscriptionChange.Unsubscribed -> {
                        roomId = subscriptionChange.roomId
                        kind.unsubscribe { }
                    }
                }
            })
        }

        when (subscriptionChange) {
            is SubscriptionChange.Subscribed -> {
                subscriptionStore.updateSubscription(StoredSubscription {
                    roomIdRawValue = subscriptionChange.roomId.rawValue
                })
                // concurrent per-room reconciles mutate this set, so update atomically
                reconciledSubscriptions.update { it + subscriptionChange.roomId }
                newSubscriptions.emit(subscriptionChange.roomId)
            }
            is SubscriptionChange.Unsubscribed -> {
                subscriptionStore.deleteSubscription(subscriptionChange.roomId)
                reconciledSubscriptions.update { it - subscriptionChange.roomId }
            }
        }
    }

    suspend fun startWatchingSubscriptions() {
        val subsReady = CompletableDeferred<Unit>()

        controllerScope.launch {
            sessionIdSource
                .onStart {
                    emit(sessionStore.getSessionId())
                }
                .distinctUntilChanged()
                .filterNotNull()
                .collect {
                    resendSubscriptions()
                }
        }

        controllerScope.launch {
            subscriptionChanges
                .mapNotNull {
                    when (it) {
                        is SubscriptionChange.Subscribed -> {
                            val existingSubscription = subscriptionStore.getSubscription(it.roomId)

                            if (existingSubscription != null) {
                                if (existingSubscription.isPendingAdd || !existingSubscription.isPendingRemove) {
                                    // we already know about this
                                    return@mapNotNull null
                                }
                            }

                            subscriptionStore.updateSubscription(StoredSubscription {
                                roomIdRawValue = it.roomId.rawValue
                                isPendingAdd = true
                            })
                        }
                        is SubscriptionChange.Unsubscribed -> {
                            val existingSubscription = subscriptionStore.getSubscription(it.roomId)

                            if (existingSubscription != null) {
                                if (existingSubscription.isPendingRemove) {
                                    // we already know about this
                                    return@mapNotNull null
                                }
                            }

                            subscriptionStore.updateSubscription(StoredSubscription {
                                roomIdRawValue = it.roomId.rawValue
                                isPendingRemove = true
                            })
                        }
                    }

                    it
                }
                .onStart {
                    subsReady.complete(Unit)

                    val subscriptions = subscriptionStore.getAllSubscriptions()

                    subscriptionState.update {
                        it + subscriptions.mapNotNull { storedSubscription ->
                            if (storedSubscription.isPendingRemove) {
                                null
                            } else {
                                RoomId(storedSubscription.roomIdRawValue)
                            }
                        }
                    }

                    reconciledSubscriptions.update {
                        it + subscriptions.mapNotNull { storedSubscription ->
                            if (storedSubscription.isPendingAdd || storedSubscription.isPendingRemove) {
                                null
                            } else {
                                RoomId(storedSubscription.roomIdRawValue)
                            }
                        }
                    }

                    val pendingSubscriptions = subscriptions
                        .filter {
                            it.isPendingAdd || it.isPendingRemove
                        }

                    emitAll(pendingSubscriptions.mapNotNull {
                        if (it.isPendingAdd) {
                            SubscriptionChange.Subscribed(RoomId(it.roomIdRawValue))
                        } else if (it.isPendingRemove) {
                            SubscriptionChange.Unsubscribed(RoomId(it.roomIdRawValue))
                        } else {
                            null
                        }
                    }.asFlow())
                }
                .collect { subscriptionChange ->
                    // Per-room fan-out: reconcile each room on its own job so a failing/slow
                    // room retries in-session (see reconcile) without head-of-line-blocking the
                    // reconciles of other rooms. A newer change for the same room supersedes
                    // (cancels) the in-flight retry so a stale Subscribe can't clobber an
                    // Unsubscribe (or vice versa).
                    val roomId = subscriptionChange.roomId
                    reconcileMutex.withLock {
                        reconcileJobs.remove(roomId)?.cancel()
                        reconcileJobs[roomId] = controllerScope.launch {
                            val self = coroutineContext[Job]
                            try {
                                reconcile(subscriptionChange)
                            } finally {
                                reconcileMutex.withLock {
                                    if (reconcileJobs[roomId] === self) {
                                        reconcileJobs.remove(roomId)
                                    }
                                }
                            }
                        }
                    }
                }
        }

        subsReady.await()
    }

    private sealed class SubscriptionChange {
        abstract val roomId: RoomId
        data class Subscribed(override val roomId: RoomId) : SubscriptionChange()
        data class Unsubscribed(override val roomId: RoomId) : SubscriptionChange()
    }

    private val subscriptionState = MutableStateFlow(emptySet<RoomId>())
    private val subscriptionChanges = MutableSharedFlow<SubscriptionChange>()
    private val reconciledSubscriptions = MutableStateFlow(emptySet<RoomId>())
    private val newSubscriptions = MutableSharedFlow<RoomId>(extraBufferCapacity = 64)

    // In-flight per-room reconcile jobs, so a newer change can supersede an outstanding retry
    // for the same room. Guarded by reconcileMutex.
    private val reconcileMutex = Mutex()
    private val reconcileJobs = mutableMapOf<RoomId, Job>()

    suspend fun awaitSubscription(roomId: RoomId) {
        if (reconciledSubscriptions.value.contains(roomId)) {
            return
        }
        reconciledSubscriptions
            .filter { it.contains(roomId) }
            .first()
    }

    fun watchNewSubscriptions(): Flow<RoomId> {
        return newSubscriptions
    }

    suspend fun subscribe(roomId: RoomId) {
        subscriptionState.update {
            it + roomId
        }
        subscriptionChanges.emit(SubscriptionChange.Subscribed(roomId))
    }

    suspend fun unsubscribe(roomId: RoomId) {
        subscriptionState.update {
            it - roomId
        }
        subscriptionChanges.emit(SubscriptionChange.Unsubscribed(roomId))
    }

    fun stop() {
        controllerJob.cancel()
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
                        sessionIdSource.value = currentSessionId
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
