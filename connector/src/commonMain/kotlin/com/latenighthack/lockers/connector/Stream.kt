package com.latenighthack.lockers.connector

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
    private val sessionStore: SessionStore,
    private val sessionIdSource: Flow<SessionId?>
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

                    when (subscriptionChange) {
                        is SubscriptionChange.Subscribed -> {
                            subscriptionStore.updateSubscription(StoredSubscription {
                                roomIdRawValue = subscriptionChange.roomId.rawValue
                            })
                            reconciledSubscriptions.value += subscriptionChange.roomId
                            newSubscriptions.emit(subscriptionChange.roomId)
                        }
                        is SubscriptionChange.Unsubscribed -> {
                            subscriptionStore.deleteSubscription(subscriptionChange.roomId)
                            reconciledSubscriptions.value -= subscriptionChange.roomId
                        }
                    }
                }
        }

        subsReady.await()
    }

    private sealed class SubscriptionChange {
        data class Subscribed(val roomId: RoomId) : SubscriptionChange()
        data class Unsubscribed(val roomId: RoomId) : SubscriptionChange()
    }

    private val subscriptionState = MutableStateFlow(emptySet<RoomId>())
    private val subscriptionChanges = MutableSharedFlow<SubscriptionChange>()
    private val reconciledSubscriptions = MutableStateFlow(emptySet<RoomId>())
    private val newSubscriptions = MutableSharedFlow<RoomId>(extraBufferCapacity = 64)

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
    }

    private val streamJob = SupervisorJob()
    private val streamScope = CoroutineScope(Dispatchers.Default + streamJob)
    private val sessionIdSource = MutableStateFlow<SessionId?>(null)
    private val sessionService = ShardedSessionServiceRpc(rpcClient)
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
        onConnected.value = false

        repeatWithBackoff(exceptionHandler = { it !is CancellationException }) {
            connectInternal()
            onConnected.value = false
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
            connect()
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
                                sessionStore.updateNextSequenceBytes(open.nextSequenceKey)
                                sessionStore.updateSessionId(currentSessionId)

                                onConnected.value = true
                                sessionIdSource.value = currentSessionId

                                processIncomingEvents(open.queuedEvents)
                            }
                            is WatchSessionResponse.Open.Result.INVALID_SEQUENCE -> {
                                sessionStore.updateNextSequenceBytes(open.nextSequenceKey)
                            }
                            is WatchSessionResponse.Open.Result.UNKNOWN_SESSION -> {
                                sessionStore.updateSessionId(null)
                            }
                            is WatchSessionResponse.Open.Result.SERVICE_UNAVAILABLE ->
                                throw RetryableStreamException("session service unavailable")
                            is WatchSessionResponse.Open.Result.EPOCH_STALE ->
                                // The session's shard moved to another node; reconnect so the
                                // routing tier (or a redirect) re-resolves the current owner.
                                throw RetryableStreamException("session shard moved; reconnecting")
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
