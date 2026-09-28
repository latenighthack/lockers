package com.latenighthack.lockers.connector

import com.diamondedge.logging.KmLog
import com.diamondedge.logging.logging
import com.latenighthack.ktbuf.bytes.toBase64String
import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.rpc.RetryLimitExceeded
import com.latenighthack.ktbuf.rpc.repeatWithBackoff
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.LockerSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.internal.LockerStore
import com.latenighthack.lockers.connector.internal.ShardedRoomServiceRpc
import com.latenighthack.lockers.connector.storage.v1.StoredLocker
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import kotlin.reflect.KFunction1

/**
 * The application bytes of a locker, regardless of envelope. Open lockers carry them
 * directly; signed/sealed lockers carry them in the (cleartext) enclosure. Callers at
 * the high level never see the envelope — they only ever get these bytes.
 */
internal fun Locker.plaintextPayload(): ByteArray =
    open?.encodedPayload ?: sealed?.payload?.enclosure?.innerPayload ?: byteArrayOf()

private fun IdentifiedLocker.toUpdate(roomId: RoomId, roomSequence: Long = 0L) =
    LockerClient.LockerUpdate(roomId, lockerId!!, version, locker?.plaintextPayload() ?: byteArrayOf(), deleted = locker == null, roomSequence = roomSequence)

private fun StoredLocker.toIdentifiedLocker(): IdentifiedLocker {
    val storedVersion = version
    val storedLockerId = LockerId(lockerIdRawValue, LockerKeyspace { value = lockerKeyspace })
    val storedPayload = lockerPayload

    return IdentifiedLocker {
        lockerId = storedLockerId
        locker = Locker { open { encodedPayload = storedPayload } }
        version = storedVersion
    }
}

private fun LockerId.keyspaceOrDefault() = keyspace ?: LockerKeyspace { value = 0L }

private fun LockerClient.LockerUpdate.toStored() = StoredLocker {
    roomIdRawValue = roomId.rawValue
    lockerIdRawValue = lockerId.rawValue
    lockerKeyspace = lockerId.keyspace?.value ?: 0L
    lockerPayload = payload
    version = this@toStored.version
    deleted = this@toStored.deleted
    roomSequence = this@toStored.roomSequence
}

/**
 * Supplies the signing keys for locked lockers. This is the client's sole opt-in to
 * locking: return a keypair for a locker and every write to it is automatically
 * wrapped in a signed envelope; return null and writes stay open. The source owns the
 * scope→key mapping (a per-locker, per-keyspace, or per-room key can back many
 * lockers) so the low-level protocol never needs to know the scope for a plain write.
 */
interface LockKeySource {
    suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId): Secp256r1KeyPair?

    /** Called after a ratchet write succeeds so the source can adopt the rotated key. */
    suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) {}
}

/**
 * A typed view of a locker change. [Present] carries the decoded value; [Deleted]
 * signals the locker was removed (locally or remotely) so watchers can drop it.
 */
sealed interface TypedLockerUpdate<out V> {
    val roomId: RoomId
    val lockerId: LockerId

    data class Present<V>(
        override val roomId: RoomId,
        override val lockerId: LockerId,
        val value: V,
    ) : TypedLockerUpdate<V>

    data class Deleted(
        override val roomId: RoomId,
        override val lockerId: LockerId,
    ) : TypedLockerUpdate<Nothing>
}

class TypedLockerClient<ValueType>(
    private val lockerClient: LockerClient,
    private val keyspace: LockerKeyspace,
    private val writer: KFunction1<ValueType, ByteArray>,
    private val reader: KFunction1<ByteArray, ValueType>
) {
    private fun LockerId.scoped(): LockerId {
        val existing = keyspace
        require(existing == null || existing == this@TypedLockerClient.keyspace) {
            "LockerId keyspace ${existing?.value} does not match this client's keyspace ${this@TypedLockerClient.keyspace.value}"
        }
        return copy(keyspace = this@TypedLockerClient.keyspace)
    }

    suspend fun getLocker(roomId: RoomId, lockerId: LockerId, revalidate: Boolean = true): ValueType? {
        val fetched = lockerClient.getLocker(roomId, lockerId.scoped(), revalidate)
        return fetched?.locker?.let { reader(it.plaintextPayload()) }
    }

    suspend fun getAllLockers(roomId: RoomId, revalidate: Boolean = true): Map<LockerId, ValueType> =
        lockerClient.getAllLockers(roomId, keyspace, revalidate)
            .associate { it.lockerId!! to reader(it.locker?.plaintextPayload() ?: byteArrayOf()) }

    suspend fun deleteLocker(
        roomId: RoomId,
        lockerId: LockerId,
        notificationBuilder: NotificationBuilder.() -> Unit = {}
    ) {
        lockerClient.deleteLocker(roomId, lockerId.scoped(), notificationBuilder)
    }

    suspend fun updateLocker(
        roomId: RoomId,
        lockerId: LockerId,
        notificationBuilder: NotificationBuilder.(Locker?) -> Unit = {},
        ratchet: Boolean = false,
        builder: (ValueType) -> ValueType
    ): ValueType? {
        val scopedId = lockerId.scoped()
        val updated = lockerClient.updateLocker(roomId, scopedId, notificationBuilder, ratchet) { existing ->
            writer(builder(reader(existing)))
        }

        return updated?.let { reader(it.plaintextPayload()) }
    }

    suspend fun lockLocker(
        roomId: RoomId,
        scope: LockScope,
        keyPair: Secp256r1KeyPair,
        parentKeyPair: Secp256r1KeyPair? = null,
        parentLockVersion: Long = 0L
    ) = lockerClient.lockLocker(roomId, scope, keyPair, parentKeyPair, parentLockVersion)

    suspend fun unlockLocker(
        roomId: RoomId,
        scope: LockScope,
        keyPair: Secp256r1KeyPair,
        parentLockVersion: Long
    ) = lockerClient.unlockLocker(roomId, scope, keyPair, parentLockVersion)

    fun watchAll(roomId: RoomId, includeHistory: Boolean = true): Flow<Map<LockerId, ValueType>> =
        watchAllIn(roomId, keyspace, includeHistory)

    fun watchAll(roomId: RoomId, keyspace: LockerKeyspace, includeHistory: Boolean = true): Flow<Map<LockerId, ValueType>> =
        watchAllIn(roomId, keyspace, includeHistory)

    // The stored keyspace is one getAllLockers read, emitted as ONE snapshot map that live
    // updates then fold onto. (Hydration used to replay each cached locker as a separate update
    // event through the fold, so every subscriber observed the map re-assemble {} -> {a} -> {a,b}.)
    // A failed subscribe/hydrate must not tear down the watcher; live updates still flow.
    // No ACK wait: offline, cached lockers must still hydrate (reconnect reconciles the sub).
    // The shared coordinator installs live collection before hydration and merges versions.
    private fun watchAllIn(roomId: RoomId, keyspace: LockerKeyspace, includeHistory: Boolean): Flow<Map<LockerId, ValueType>> =
        if (includeHistory) lockerClient.watchSnapshot(roomId, keyspace).map { items ->
            items.associate { it.lockerId!! to reader(it.locker!!.plaintextPayload()) }
        } else allUpdates.filter { it.lockerId.keyspace == keyspace && it.roomId == roomId }
            .onStart { lockerClient.subscribeToRoom(roomId, false) }
            .runningFold(emptyMap()) { acc, value -> acc.applyUpdate(value) }

    fun watch(roomId: RoomId, lockerId: LockerId, includeHistory: Boolean = true): Flow<TypedLockerUpdate<ValueType>> {
        val scopedId = lockerId.scoped()
        if (!includeHistory) return allUpdates.filter { it.roomId == roomId && it.lockerId == scopedId }
            .onStart { lockerClient.subscribeToRoom(roomId, false) }
        return lockerClient.watchSnapshot(roomId, keyspace).map { items ->
            items.firstOrNull { it.lockerId == scopedId }?.toTyped(roomId)
                ?: TypedLockerUpdate.Deleted(roomId, scopedId)
        }.distinctUntilChanged()
    }

    val allUpdates: Flow<TypedLockerUpdate<ValueType>>
        get() {
            return lockerClient
                .changes
                .filter { it.lockerId.keyspace == keyspace }
                .map { it.toTyped() }
        }

    val notifications: Flow<IncomingNotification>
        get() {
            return lockerClient
                .notifications
                .filter { it.lockerId.keyspace == keyspace }
        }

    suspend fun subscribeToRoom(roomId: RoomId, waitForSubscription: Boolean = true) {
        lockerClient.subscribeToRoom(roomId, waitForSubscription)
    }

    suspend fun unsubscribeFromRoom(roomId: RoomId) {
        lockerClient.unsubscribeFromRoom(roomId)
    }

    private fun IdentifiedLocker.toTyped(roomId: RoomId): TypedLockerUpdate<ValueType> =
        TypedLockerUpdate.Present(roomId, lockerId!!, reader(locker?.plaintextPayload() ?: byteArrayOf()))

    private fun LockerClient.LockerUpdate.toTyped(): TypedLockerUpdate<ValueType> =
        if (deleted) {
            TypedLockerUpdate.Deleted(roomId, lockerId)
        } else {
            TypedLockerUpdate.Present(roomId, lockerId, reader(payload))
        }

    private fun Map<LockerId, ValueType>.applyUpdate(update: TypedLockerUpdate<ValueType>): Map<LockerId, ValueType> =
        when (update) {
            is TypedLockerUpdate.Deleted -> this - update.lockerId
            is TypedLockerUpdate.Present -> this + (update.lockerId to update.value)
        }
}

class IncomingNotification(
    val roomId: RoomId,
    val lockerId: LockerId,
    val payload: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is IncomingNotification) return false

        return roomId == other.roomId &&
            lockerId == other.lockerId &&
            payload.contentEquals(other.payload)
    }

    override fun hashCode(): Int {
        var result = roomId.hashCode()
        result = 31 * result + lockerId.hashCode()
        result = 31 * result + payload.contentHashCode()
        return result
    }
}

private const val WRITE_RETRY_LIMIT = 8

/**
 * A [LockerWriteException] is terminal (a bad/absent signing key, or an unauthorized
 * write) and must not be retried — otherwise the write would burn the whole retry
 * budget before surfacing. RPC errors keep their usual transient/terminal split.
 */
private val WRITE_EXCEPTION_HANDLER: (Throwable) -> Boolean = { e ->
    writeRetryLog.debug { "locker write attempt failed (${e::class.simpleName}: ${e.message})\n${e.stackTraceToString()}" }
    when (e) {
        is LockerWriteException -> false
        is RpcResponseException -> e.retriable()
        else -> true
    }
}

private val writeRetryLog = com.diamondedge.logging.logging("LockerWriteRetry")

/**
 * Thrown when a locker write cannot be completed — the server reported a terminal
 * error or the optimistic-concurrency retry budget ([WRITE_RETRY_LIMIT]) was
 * exhausted. Distinguishes a failed write from a merely slow one; without it a
 * permanent server error would retry forever.
 */
class LockerWriteException(message: String, cause: Throwable? = null) : Exception(message, cause)

class LockerClient(
    rpcClient: RpcClient,
    private val stream: Stream,
    private val lockerStore: LockerStore,
    private val lockKeySource: LockKeySource? = null,
    private val codecs: NotificationCodecs = NotificationCodecs.identity(),
    internal val log: KmLog = logging()
) {
    private val processingJob = SupervisorJob()
    private val processingScope = GlobalScope + processingJob
    private val sync = LockerSyncCoordinator(processingScope)
    private val acceptance = Mutex()
    private val watched = mutableMapOf<Pair<RoomId, LockerKeyspace>, Flow<List<IdentifiedLocker>>>()
    private val watchMutex = Mutex()

    class LockerUpdate(
        val roomId: RoomId,
        val lockerId: LockerId,
        val version: Long,
        val payload: ByteArray,
        val deleted: Boolean = false,
        val roomSequence: Long = 0L,
    ) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is LockerUpdate) return false

            return roomId == other.roomId &&
                lockerId == other.lockerId &&
                version == other.version &&
                deleted == other.deleted &&
                payload.contentEquals(other.payload)
        }

        override fun hashCode(): Int {
            var result = roomId.hashCode()
            result = 31 * result + lockerId.hashCode()
            result = 31 * result + version.hashCode()
            result = 31 * result + deleted.hashCode()
            result = 31 * result + payload.contentHashCode()
            return result
        }
    }

    private val internalChanges = MutableSharedFlow<LockerUpdate>(extraBufferCapacity = 64)
    private val incomingNotifications = MutableSharedFlow<IncomingNotification>(extraBufferCapacity = 64)

    val changes: Flow<LockerUpdate>
        get() {
            return internalChanges.asSharedFlow()
        }

    val notifications: Flow<IncomingNotification>
        get() {
            return incomingNotifications.asSharedFlow()
        }

    private val roomService = ShardedRoomServiceRpc(rpcClient)

    // When routing through a [RoutingRpcClient], a NOT_OWNER + redirect is recorded here so the
    // next write attempt (inside the same repeatWithBackoff loop) re-targets the owning node. With
    // a plain client (e.g. a monolith) this is a no-op and writes never see NOT_OWNER.
    private val routing = rpcClient as? RoutingRpcClient

    private fun recordRoomRedirect(roomId: RoomId, redirect: ShardRedirect?) {
        val owner = redirect?.ownerAddress ?: return
        routing?.recordRedirect(roomId.rawValue.toBase64String(), owner, redirect.epoch)
    }

    suspend fun start() {
        stream.acceptEvent = { processEvent(it) }

        stream.hydrateSubscription = { room, session -> hydrateRoom(room, session) }
    }

    private suspend fun hydrateRoom(room: RoomId, session: SessionId? = stream.sessionId.value): List<IdentifiedLocker> {
        val capabilities = capabilities()
        // A replacement session must register itself even while an older hydration is in flight.
        return sync.read(room to session) {
        var watermark = 0L
        val lockers = if (capabilities.subscribeAndSnapshot && session != null) {
            val response = roomService.subscribeAndSnapshot(SubscribeAndSnapshotRequest(roomId = room, sessionId = session))
            check(response.result.isOk()) { "subscribe and snapshot failed" }
            watermark = response.roomSequence
            response.lockers
        } else {
            if (session != null) roomService.subscription(SubscriptionRequest {
                roomId = room; sessionId = session; kind.subscribe { }
            })
            roomService.getAllLockers(GetAllLockersRequest(roomId = room)).lockers
        }
        lockers.forEach { accept(it.toUpdate(room, watermark)) }
        lockers
        }
    }

    private var cachedCapabilities: CapabilitiesResponse? = null
    private suspend fun capabilities(): CapabilitiesResponse {
        // A warm write must not queue behind hydration merely to read cached capabilities.
        cachedCapabilities?.let { return it }
        return sync.read("capabilities") {
        cachedCapabilities ?: try { roomService.capabilities(CapabilitiesRequest()) }
        catch (e: RpcResponseException) {
            if (e.code == com.latenighthack.ktbuf.proto.Codes.UNIMPLEMENTED || e.code == com.latenighthack.ktbuf.proto.Codes.NOT_FOUND) CapabilitiesResponse() else throw e
        }.also { cachedCapabilities = it }
        }
    }

    class Change(val lockerId: LockerId, val transform: suspend (ByteArray) -> ByteArray)

    /** Atomic on capable servers. Legacy fallback is selected before submitting any write. */
    suspend fun updateLockers(roomId: RoomId, changes: List<Change>, initialKey: Secp256r1KeyPair? = null) {
        require(changes.isNotEmpty() && changes.size <= 64 && changes.map { it.lockerId }.distinct().size == changes.size)
        // Room batch lock and stable locker ordering prevent overlapping single writes interleaving.
        suspend fun locked(index: Int, action: suspend () -> Unit) {
            if (index == changes.size) action()
            else sync.mutate(roomId to changes.sortedBy { it.lockerId.toByteArray().toBase64String() }[index].lockerId) { locked(index + 1, action) }
        }
        locked(0) {
            val caps = capabilities()
            if (!caps.postLockerChanges) {
                if (initialKey != null) {
                    val lock = lockLocker(roomId, LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), initialKey, initialKey)
                    if (!lock.result.isOk()) throw LockerWriteException("initial room lock rejected: ${lock.result}")
                }
                coroutineScope { changes.map { change -> async {
                    updateLockerSerialized(roomId, change.lockerId, transform = change.transform)
                } }.awaitAll() }
                return@locked
            }
            val current = changes.associate { change ->
                val stored = lockerStore.getLocker(roomId, change.lockerId.keyspaceOrDefault(), change.lockerId)
                change.lockerId to ((stored?.version ?: 0L) to (stored?.lockerPayload ?: byteArrayOf()))
            }.toMutableMap()
            // Explicit pair construction avoids conflating version zero with a missing body.
            changes.forEach { change ->
                val stored = lockerStore.getLocker(roomId, change.lockerId.keyspaceOrDefault(), change.lockerId)
                current[change.lockerId] = (stored?.version ?: 0L) to (stored?.lockerPayload ?: byteArrayOf())
            }
            val scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM)
            val grant = initialKey?.let { key ->
                val public = key.publicKey.encode()
                LockGrant(scope, Secp256R1Key.PublicKey(public), signatureOf(key, LockerSigning.grantContext(roomId, scope, public)))
            }
            var submitted: PostLockerChangesRequest? = null
            try {
                repeatWithBackoff(retryLimit = WRITE_RETRY_LIMIT, exceptionHandler = WRITE_EXCEPTION_HANDLER) {
                    val request = submitted ?: PostLockerChangesRequest(roomId = roomId, initialLock = grant,
                        writeRequestId = kotlin.random.Random.nextBytes(32), changes = changes.map { change ->
                            val (version, plaintext) = current.getValue(change.lockerId)
                            val body = buildWriteBody(initialKey ?: lockKeySource?.writeKeyFor(roomId, change.lockerId),
                                roomId, change.lockerId, version, change.transform(plaintext))
                            PostLockerChangeRequest(roomId = roomId, lockerId = change.lockerId, locker = body.locker,
                                parentVersion = version, writeSignature = body.signature)
                        }).also {
                            if (it.toByteArray().size > minOf(8 * 1024 * 1024, caps.maxBatchBytes))
                                throw LockerWriteException("atomic locker batch exceeds encoded request limit")
                            submitted = it
                        }
                    val response = sync.network { roomService.postLockerChanges(request) }
                    when (response.result) {
                        is PostLockerChangesResponse.Result.OK -> {
                            if (response.agentFailed || response.agentPending) throw LockerWriteException("batch committed; agent ${if (response.agentPending) "pending" else "failed"}")
                            request.changes.zip(response.changes).forEach { (change, result) ->
                                accept(LockerUpdate(roomId, change.lockerId!!, result.version, change.locker!!.plaintextPayload()))
                            }
                        }
                        is PostLockerChangesResponse.Result.CONFLICT -> {
                            response.changes.forEachIndexed { index, item ->
                                if (item.result is PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION)
                                    current[changes[index].lockerId] = item.version to (item.existingLocker?.plaintextPayload() ?: byteArrayOf())
                                else if (!item.result.isOk()) throw LockerWriteException("batch rejected: ${item.result}")
                            }
                            if (response.changes.isEmpty()) throw LockerWriteException("initial lock conflict")
                            submitted = null
                            retry()
                        }
                        is PostLockerChangesResponse.Result.NOT_OWNER -> { recordRoomRedirect(roomId, response.redirect); retry() }
                        else -> throw LockerWriteException("batch rejected: ${response.result}")
                    }
                }
            } catch (e: RetryLimitExceeded) { throw LockerWriteException("batch retry budget exhausted: $e") }
        }
    }

    private suspend fun accept(update: LockerUpdate) = acceptance.withLock {
        val stored = lockerStore.getLocker(update.roomId, update.lockerId.keyspaceOrDefault(), update.lockerId)
        if (stored != null && (stored.version > update.version ||
            (stored.roomSequence > 0 && update.roomSequence > 0 && stored.roomSequence >= update.roomSequence) ||
            (stored.version == update.version && (stored.deleted || stored.lockerPayload.contentEquals(update.payload))))) return@withLock false
        lockerStore.saveLocker(update.toStored())
        internalChanges.emit(update)
        true
    }

    internal fun watchSnapshot(roomId: RoomId, keyspace: LockerKeyspace): Flow<List<IdentifiedLocker>> = flow {
        val shared = watchMutex.withLock { watched.getOrPut(roomId to keyspace) {
            channelFlow {
                val state = mutableMapOf<LockerId, LockerUpdate>()
                val stateMutex = Mutex()
                suspend fun merge(update: LockerUpdate) = stateMutex.withLock {
                    val previous = state[update.lockerId]
                    if (previous == null || update.version > previous.version ||
                        (update.version == previous.version && update.deleted && !previous.deleted)) {
                        state[update.lockerId] = update
                        send(state.values.filterNot { it.deleted }.map { value -> IdentifiedLocker(value.lockerId,
                            Locker { open { encodedPayload = value.payload } }, value.version) })
                    }
                }
                // Install live collector before starting hydration, buffering while the snapshot loads.
                val live = launch(start = CoroutineStart.UNDISPATCHED) {
                    changes.filter { it.roomId == roomId && it.lockerId.keyspaceOrDefault() == keyspace }.collect { merge(it) }
                }
                try {
                    subscribeToRoom(roomId, false)
                    getAllLockers(roomId, keyspace).forEach { merge(it.toUpdate(roomId)) }
                    if (state.isEmpty()) send(emptyList())
                } catch (e: CancellationException) { throw e }
                catch (e: Exception) { log.error { "watch hydration failed: $e" } }
                live.join()
            }.shareIn(processingScope, SharingStarted.WhileSubscribed(5_000), replay = 1)
        } }
        emitAll(shared)
    }

    private suspend fun processEvent(event: Event): Boolean {
        val roomId = event.roomId ?: return true
        val identified = event.locker
        val lockerId = identified?.lockerId
        val version = identified?.version ?: 0L
        val body = identified?.locker
        val hasBody = body != null && (body.open != null || body.sealed != null)
        val notificationPayload = event.notification?.payload?.rawValue

        if (lockerId != null) {
            if (hasBody) {
                accept(LockerUpdate(roomId, lockerId, version, body!!.plaintextPayload(), deleted = false, roomSequence = event.roomSequence))
            } else {
                // A body-less locker event is a tombstone: the server signals a
                // delete by sending lockerId + version with no Locker body.
                accept(LockerUpdate(roomId, lockerId, version, byteArrayOf(), deleted = true, roomSequence = event.roomSequence))
            }
        }

        if (lockerId != null && notificationPayload != null) {
            val keyspace = lockerId.keyspaceOrDefault()
            val decoded = if (codecs.isEmpty(keyspace)) {
                notificationPayload
            } else {
                val push = event.notification?.push
                codecs.decode(
                    NotificationContext(roomId, lockerId, keyspace, push?.title, push?.body),
                    notificationPayload,
                )
            }
            // A codec may drop the notification by returning null.
            if (decoded != null) {
                incomingNotifications.emit(IncomingNotification(roomId, lockerId, decoded))
            }
        }

        return true
    }

    /**
     * Builds the write-side [Notification] from the caller's [build] block, then
     * runs its payload through the encode chain so a consumer's codec round-trips
     * with [processEvent]'s decode.
     */
    private suspend fun encodedNotification(
        roomId: RoomId,
        lockerId: LockerId,
        configure: NotificationBuilder.() -> Unit,
    ): Notification {
        // Pass `configure` to the factory directly; `Notification { configure() }`
        // would instead resolve to NotificationBuilder.build()'s sibling and drop it.
        val built = Notification(configure)
        val payload = built.payload?.rawValue
        val keyspace = lockerId.keyspaceOrDefault()
        if (payload == null || payload.isEmpty() || codecs.isEmpty(keyspace)) {
            return built
        }
        val encoded = codecs.encode(
            NotificationContext(roomId, lockerId, keyspace, built.push?.title, built.push?.body),
            payload,
        )
        return built.copy { payload { rawValue = encoded } }
    }

    fun stop() {
        processingJob.cancel()
    }

    suspend fun subscribeToRoom(roomId: RoomId, waitForSubscription: Boolean = true) {
        stream.subscribe(roomId, waitForSubscription)
    }

    suspend fun unsubscribeFromRoom(roomId: RoomId) {
        stream.unsubscribe(roomId)
    }

    suspend fun getAllLockers(roomId: RoomId, keyspace: LockerKeyspace, revalidate: Boolean = true): List<IdentifiedLocker> {
        val cached = lockerStore.getAllLockers(roomId, keyspace)

        if (cached.isNotEmpty()) {
            if (revalidate) {
                processingScope.launch {
                    // background revalidation is best-effort; a network failure must not
                    // surface as an uncaught crash
                    try {
                        fetchAllLockers(roomId, keyspace)
                    } catch (e: kotlinx.coroutines.CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        log.error { "revalidate failed for ${roomId.toLogString()}: $e" }
                    }
                }
            }
            return cached.filterNot { it.deleted }.map { it.toIdentifiedLocker() }
        }

        return fetchAllLockers(roomId, keyspace)
    }

    /**
     * Every locker currently in the local cache, across all rooms and keyspaces — the
     * client's whole known-locker set, for introspection/debugging. Reads the cache only
     * (no server round-trip); each entry carries its room, keyspaced id, version, and
     * plaintext bytes.
     */
    suspend fun getAllKnownLockers(): List<LockerUpdate> =
        lockerStore.getAllLockers().filterNot { it.deleted }.map { stored ->
            LockerUpdate(
                RoomId(stored.roomIdRawValue),
                LockerId(stored.lockerIdRawValue, LockerKeyspace { value = stored.lockerKeyspace }),
                stored.version,
                stored.lockerPayload,
            )
        }

    private suspend fun fetchAllLockers(roomId: RoomId, keyspace: LockerKeyspace): List<IdentifiedLocker> =
        hydrateRoom(roomId).filter { it.locker != null && it.lockerId?.keyspace == keyspace }

    suspend fun getLocker(roomId: RoomId, lockerId: LockerId, revalidate: Boolean = true): IdentifiedLocker? {
        val cached = lockerStore.getLocker(roomId, lockerId.keyspaceOrDefault(), lockerId)

        if (cached != null) {
            if (revalidate) {
                processingScope.launch {
                    // background revalidation is best-effort; a network failure must not
                    // surface as an uncaught crash
                    try {
                        fetchLocker(roomId, lockerId)
                    } catch (e: kotlinx.coroutines.CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        log.error { "revalidate failed for ${roomId.toLogString()}: $e" }
                    }
                }
            }
            return cached.takeUnless { it.deleted }?.toIdentifiedLocker()
        }

        return fetchLocker(roomId, lockerId)
    }

    suspend fun getLockers(roomId: RoomId, lockerIds: List<LockerId>): List<IdentifiedLocker?> {
        require(lockerIds.size <= 64)
        val caps = capabilities()
        if (!caps.getLockers) return coroutineScope { lockerIds.map { id -> async { fetchLocker(roomId, id) } }.awaitAll() }
        return sync.read(Triple("lockers", roomId, lockerIds)) {
            roomService.getLockers(GetLockersRequest(roomId, lockerIds)).results.map { item ->
                item.locker?.also { accept(it.toUpdate(roomId)) }
            }
        }
    }

    private suspend fun fetchLocker(roomId: RoomId, lockerId: LockerId): IdentifiedLocker? = sync.read(roomId to lockerId) {
        val fetchedLocker = roomService.getLocker(GetLockerRequest {
            this.lockerId = lockerId
            this.roomId = roomId
        }).locker

        fetchedLocker?.let {
            accept(it.toUpdate(roomId))
        }

        fetchedLocker
    }

    suspend fun deleteLocker(roomId: RoomId, lockerId: LockerId, notificationBuilder: NotificationBuilder.() -> Unit = {}) =
        sync.mutate(roomId to lockerId) { deleteLockerSerialized(roomId, lockerId, notificationBuilder) }

    private suspend fun deleteLockerSerialized(
        roomId: RoomId,
        lockerId: LockerId,
        notificationBuilder: NotificationBuilder.() -> Unit = {}
    ) {
        val cached = lockerStore.getLocker(roomId, lockerId.keyspaceOrDefault(), lockerId)
        var parentVersion = cached?.version ?: 0L
        val signingKey = lockKeySource?.writeKeyFor(roomId, lockerId)

        val deletedVersion = try {
            repeatWithBackoff(retryLimit = WRITE_RETRY_LIMIT, exceptionHandler = WRITE_EXCEPTION_HANDLER) {
                val signature = signingKey?.let { signWrite(it, roomId, lockerId, parentVersion, ByteArray(0)) }

                val notif = encodedNotification(roomId, lockerId) { this.notificationBuilder() }

                val result = sync.network { roomService.deleteLocker(DeleteLockerRequest {
                    this.roomId = roomId
                    this.lockerId = lockerId
                    this.parentVersion = parentVersion
                    if (signature != null) this.writeSignature = signature

                    this.notification = notif
                }) }

                when (result.result) {
                    is DeleteLockerResponse.Result.OK -> result.version
                    is DeleteLockerResponse.Result.UPDATE_LOCAL_VERSION -> {
                        parentVersion = result.version
                        retry()
                    }
                    is DeleteLockerResponse.Result.NOT_OWNER -> {
                        // This node doesn't own the room's shard; cache the redirect and retry so
                        // the routing client re-targets the owner on the next attempt.
                        recordRoomRedirect(roomId, result.redirect)
                        retry()
                    }
                    is DeleteLockerResponse.Result.SIGNATURE_REQUIRED ->
                        throw LockerWriteException("locker is locked; a signing key is required to delete")
                    is DeleteLockerResponse.Result.SIGNATURE_INVALID ->
                        throw LockerWriteException("locker delete signature was rejected")
                    is DeleteLockerResponse.Result.NOT_AUTHORIZED ->
                        throw LockerWriteException("locker delete not authorized")
                    else -> {
                        // See updateLocker: rebase on server state before retrying an unknown result.
                        fetchLocker(roomId, lockerId)?.let { parentVersion = it.version }
                        retry()
                    }
                }
            }
        } catch (e: RetryLimitExceeded) {
            throw LockerWriteException("locker delete failed after $WRITE_RETRY_LIMIT attempts: $e")
        }

        deletedVersion?.let {
            accept(LockerUpdate(roomId, lockerId, it, byteArrayOf(), deleted = true))
        }
    }

    /**
     * Update a locker's plaintext content. [transform] receives the current plaintext
     * and returns the new plaintext; when the locker is signed (a key is available from
     * the [LockKeySource]) the result is wrapped in a signed envelope and the write is
     * signed. The signature binds the parent version, so a version conflict re-runs
     * [transform] and re-signs against the fresh version — preserving the fair-read
     * retry. Set [ratchet] to rotate the signing key atomically with this write.
     *
     * Throws [LockerWriteException] for retry exhaustion, terminal rejection, or a committed
     * source whose synchronous agent did not complete successfully.
     */
    suspend fun updateLocker(
        roomId: RoomId, lockerId: LockerId,
        notificationBuilder: NotificationBuilder.(Locker?) -> Unit = {}, ratchet: Boolean = false,
        transform: suspend (ByteArray) -> ByteArray,
    ): Locker? = sync.mutate(roomId to lockerId) { updateLockerSerialized(roomId, lockerId, notificationBuilder, ratchet, transform) }

    private suspend fun updateLockerSerialized(
        roomId: RoomId,
        lockerId: LockerId,
        notificationBuilder: NotificationBuilder.(Locker?) -> Unit = {},
        ratchet: Boolean = false,
        transform: suspend (ByteArray) -> ByteArray,
    ): Locker? {
        val cached = lockerStore.getLocker(roomId, lockerId.keyspaceOrDefault(), lockerId)
        var currentPlaintext = cached?.toIdentifiedLocker()?.locker?.plaintextPayload() ?: byteArrayOf()
        var parentVersion = cached?.version ?: 0L

        val signingKey = lockKeySource?.writeKeyFor(roomId, lockerId)
        val pendingRatchetKey = if (ratchet && signingKey != null) Secp256r1KeyPair.generate() else null

        // Names the write in rejection messages: a REQUIRED verdict means the key chain returned no
        // signing key for this room (signed=false), which is otherwise invisible from the message.
        val writeContext = "room=${roomId.toLogString()} locker=${lockerId.toLogString()} signed=${signingKey != null}"

        val supportsReceipts = capabilities().writeReceipts
        var submitted: PostLockerChangeRequest? = null
        val updatedLocker = try {
            repeatWithBackoff(retryLimit = WRITE_RETRY_LIMIT, exceptionHandler = WRITE_EXCEPTION_HANDLER) {
                val newPlaintext = transform(currentPlaintext)
                val body = buildWriteBody(signingKey, roomId, lockerId, parentVersion, newPlaintext)
                val ratchetMsg = if (pendingRatchetKey != null && signingKey != null) {
                    buildRatchet(signingKey, pendingRatchetKey, roomId, lockerId, parentVersion)
                } else {
                    null
                }
                log.debug { "updating locker=${lockerId.toLogString()}" }

                val notif = encodedNotification(roomId, lockerId) { this.notificationBuilder(body.locker) }

                val request = submitted ?: PostLockerChangeRequest {
                    if (supportsReceipts) writeRequestId = kotlin.random.Random.nextBytes(32)
                    this.roomId = roomId
                    this.lockerId = lockerId
                    this.parentVersion = parentVersion
                    this.locker = body.locker
                    if (body.signature != null) this.writeSignature = body.signature
                    if (ratchetMsg != null) this.ratchet = ratchetMsg

                    this.notification = notif
                }.also { submitted = it }
                val result = sync.network { roomService.postLockerChange(request) }

                if (result.agentFailed || result.agentPending) throw LockerWriteException("source write committed, but game agent failed ($writeContext)")
                log.debug { "updated locker=${lockerId.toLogString()} result=${result.result} version=${result.version}" }

                val locker = when (result.result) {
                    is PostLockerChangeResponse.Result.OK -> request.locker!!
                    is PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION -> {
                        submitted = null
                        currentPlaintext = result.existingLocker?.plaintextPayload() ?: byteArrayOf()
                        parentVersion = result.version
                        retry()
                    }
                    is PostLockerChangeResponse.Result.NOT_OWNER -> {
                        // This node doesn't own the room's shard; cache the redirect and retry so
                        // the routing client re-targets the owner on the next attempt.
                        recordRoomRedirect(roomId, result.redirect)
                        retry()
                    }
                    is PostLockerChangeResponse.Result.SIGNATURE_REQUIRED ->
                        throw LockerWriteException("locker is locked; a signing key is required ($writeContext)")
                    is PostLockerChangeResponse.Result.SIGNATURE_INVALID ->
                        throw LockerWriteException("locker write signature was rejected ($writeContext)")
                    is PostLockerChangeResponse.Result.NOT_AUTHORIZED ->
                        throw LockerWriteException("locker write not authorized ($writeContext)")
                    else -> {
                        // Unknown/transient server result: the write may have persisted before the
                        // server failed (e.g. a fan-out error), so a blind retry with the same
                        // parentVersion would just bounce off UPDATE_LOCAL_VERSION and burn two
                        // attempts per round trip. Rebase on the server's current state first.
                        if (supportsReceipts) throw LockerWriteException("write rejected: ${result.result}")
                        submitted = null
                        fetchLocker(roomId, lockerId)?.let { fetched ->
                            currentPlaintext = fetched.locker?.plaintextPayload() ?: byteArrayOf()
                            parentVersion = fetched.version
                        }
                        retry()
                    }
                }

                IdentifiedLocker {
                    this.locker = locker
                    this.lockerId = lockerId
                    this.version = result.version
                }
            }
        } catch (e: RetryLimitExceeded) {
            throw LockerWriteException("locker update failed after $WRITE_RETRY_LIMIT attempts ($writeContext): $e")
        }

        val result = updatedLocker ?: return null
        if (pendingRatchetKey != null) {
            lockKeySource?.onRatcheted(roomId, lockerId, pendingRatchetKey)
        }
        val update = result.toUpdate(roomId)
        accept(update)

        return result.locker
    }

    /** Establish a lock at [scope] with [keyPair]. Sign the grant with [parentKeyPair]
     *  (the parent-scope or room key); pass null for a TOFU root in a non-public-keyed room. */
    suspend fun lockLocker(
        roomId: RoomId,
        scope: LockScope,
        keyPair: Secp256r1KeyPair,
        parentKeyPair: Secp256r1KeyPair? = null,
        parentLockVersion: Long = 0L
    ): LockLockerResponse {
        val publicKeyBytes = keyPair.publicKey.encode()
        val parentSignature = parentKeyPair?.let {
            val context = LockerSigning.grantContext(roomId, scope, publicKeyBytes)
            signatureOf(it, context)
        }

        return roomService.lockLocker(LockLockerRequest {
            this.roomId = roomId
            this.parentLockVersion = parentLockVersion
            grant = LockGrant(
                scope = scope,
                publicKey = Secp256R1Key.PublicKey(rawValue = publicKeyBytes),
                parentSignature = parentSignature,
            )
        }).also {
            if (it.result is LockLockerResponse.Result.NOT_OWNER) recordRoomRedirect(roomId, it.redirect)
        }
    }

    /** Remove the lock at [scope], authorized by its current [keyPair]. */
    suspend fun unlockLocker(
        roomId: RoomId,
        scope: LockScope,
        keyPair: Secp256r1KeyPair,
        parentLockVersion: Long
    ): UnlockLockerResponse {
        val context = LockerSigning.unlockContext(roomId, scope)
        return roomService.unlockLocker(UnlockLockerRequest {
            this.roomId = roomId
            this.scope = scope
            this.parentLockVersion = parentLockVersion
            signature = signatureOf(keyPair, context)
        }).also {
            if (it.result is UnlockLockerResponse.Result.NOT_OWNER) recordRoomRedirect(roomId, it.redirect)
        }
    }

    private class WriteBody(val locker: Locker, val signature: Signature?)

    private suspend fun buildWriteBody(
        signingKey: Secp256r1KeyPair?,
        roomId: RoomId,
        lockerId: LockerId,
        parentVersion: Long,
        plaintext: ByteArray,
    ): WriteBody {
        if (signingKey == null) {
            return WriteBody(Locker { open { encodedPayload = plaintext } }, null)
        }
        val hash = SHA256.digest(plaintext)
        val signature = signWrite(signingKey, roomId, lockerId, parentVersion, hash)
        val locker = Locker {
            sealed {
                payload {
                    this.checksum = hash
                    enclosure {
                        this.signature = signature
                        innerPayload = plaintext
                    }
                }
            }
        }
        return WriteBody(locker, signature)
    }

    private suspend fun buildRatchet(
        oldKey: Secp256r1KeyPair,
        newKey: Secp256r1KeyPair,
        roomId: RoomId,
        lockerId: LockerId,
        parentVersion: Long,
    ): PostLockerChangeRequest.Ratchet {
        val newPublicKeyBytes = newKey.publicKey.encode()
        val context = LockerSigning.ratchetContext(roomId, lockerId, parentVersion, newPublicKeyBytes)
        return PostLockerChangeRequest.Ratchet(
            newPublicKey = Secp256R1Key.PublicKey(rawValue = newPublicKeyBytes),
            newSharedKeys = emptyList(),
            signature = signatureOf(oldKey, context),
        )
    }

    private suspend fun signWrite(
        keyPair: Secp256r1KeyPair,
        roomId: RoomId,
        lockerId: LockerId,
        parentVersion: Long,
        contentHash: ByteArray,
    ): Signature =
        signatureOf(keyPair, LockerSigning.writeContext(roomId, lockerId, parentVersion, contentHash))

    private suspend fun signatureOf(keyPair: Secp256r1KeyPair, message: ByteArray): Signature =
        Signature(
            publicKey = Secp256R1Key.PublicKey(rawValue = keyPair.publicKey.encode()),
            // ktcrypto's sign() returns the raw r‖s the server expects on every platform.
            signature = keyPair.privateKey.sign(message),
        )
}

private fun RoomId.toLogString() = "r+" + (this?.rawValue?.toBase64String()?.take(6) ?: "(nul)")

private fun LockerId?.toLogString() = "l+" + (this?.rawValue?.toBase64String()?.take(6) ?: "(nul)") +
    "/ks" + (this?.keyspace?.value?.toString() ?: "0")
