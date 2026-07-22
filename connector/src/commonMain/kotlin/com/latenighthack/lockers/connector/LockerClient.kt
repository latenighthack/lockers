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

private fun IdentifiedLocker.toUpdate(roomId: RoomId) =
    LockerClient.LockerUpdate(roomId, lockerId!!, version, locker?.plaintextPayload() ?: byteArrayOf())

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

    fun watchAll(roomId: RoomId, includeHistory: Boolean = true): Flow<Map<LockerId, ValueType>> = allUpdates
        .onStart {
            // a failed subscribe/hydrate must not tear down the watcher; live updates still flow.
            // no ACK wait: offline, cached lockers must still hydrate (reconnect reconciles the sub)
            try {
                lockerClient.subscribeToRoom(roomId, waitForSubscription = false)
                if (includeHistory) {
                    emitAll(hydrate(roomId, keyspace))
                }
            } catch (e: kotlinx.coroutines.CancellationException) {
                throw e
            } catch (e: Exception) {
                lockerClient.log.error { "watchAll hydrate failed for ${roomId.toLogString()}: $e" }
            }
        }
        .filter { it.roomId == roomId }
        .runningFold(emptyMap()) { acc, value -> acc.applyUpdate(value) }

    fun watchAll(roomId: RoomId, keyspace: LockerKeyspace, includeHistory: Boolean = true): Flow<Map<LockerId, ValueType>> = allUpdates
        .onStart {
            try {
                lockerClient.subscribeToRoom(roomId, waitForSubscription = false)
                if (includeHistory) {
                    emitAll(hydrate(roomId, keyspace))
                }
            } catch (e: kotlinx.coroutines.CancellationException) {
                throw e
            } catch (e: Exception) {
                lockerClient.log.error { "watchAll hydrate failed for ${roomId.toLogString()}: $e" }
            }
        }
        .filter { it.lockerId.keyspace == keyspace && it.roomId == roomId }
        .runningFold(emptyMap()) { acc, value -> acc.applyUpdate(value) }

    fun watch(roomId: RoomId, lockerId: LockerId, includeHistory: Boolean = true): Flow<TypedLockerUpdate<ValueType>> {
        val scopedId = lockerId.scoped()

        return allUpdates
            .onStart {
                try {
                    lockerClient.subscribeToRoom(roomId, waitForSubscription = false)
                    if (includeHistory) {
                        lockerClient.getLocker(roomId, scopedId)?.let {
                            emit(it.toTyped(roomId))
                        }
                    }
                } catch (e: kotlinx.coroutines.CancellationException) {
                    throw e
                } catch (e: Exception) {
                    lockerClient.log.error { "watch hydrate failed for ${roomId.toLogString()}: $e" }
                }
            }
            .filter { it.roomId == roomId && it.lockerId == scopedId }
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

    private suspend fun hydrate(roomId: RoomId, keyspace: LockerKeyspace): Flow<TypedLockerUpdate<ValueType>> =
        lockerClient.getAllLockers(roomId, keyspace)
            .map { it.toTyped(roomId) }
            .asFlow()

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
    private val processingJob = Job()
    private val processingScope = GlobalScope + processingJob

    class LockerUpdate(
        val roomId: RoomId,
        val lockerId: LockerId,
        val version: Long,
        val payload: ByteArray,
        val deleted: Boolean = false,
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
        processingScope.launch {
            internalChanges
                .onEach { update ->
                    if (update.deleted) {
                        lockerStore.deleteLocker(update.roomId, update.lockerId.keyspaceOrDefault(), update.lockerId)
                    } else {
                        lockerStore.saveLocker(update.toStored())
                    }
                }
                .collect()
        }

        processingScope.launch {
            stream.events
                .collect {
                    processEvent(it)
                }
        }

        processingScope.launch {
            stream.watchNewSubscriptions().collect { subscribedRoomId ->
                // a failed fetch must not kill the loop — log and keep serving later subscriptions
                try {
                    log.debug { "fetching all lockers for ${subscribedRoomId.toLogString()}" }
                    val fetchedLockers = roomService.getAllLockers(GetAllLockersRequest {
                        roomId = subscribedRoomId
                    }).lockers

                    log.debug {"got ${fetchedLockers.size} lockers for ${subscribedRoomId.toLogString()}" }

                    internalChanges.emitAll(fetchedLockers
                        .map {
                            it.toUpdate(subscribedRoomId)
                        }
                        .asFlow()
                    )
                } catch (e: kotlinx.coroutines.CancellationException) {
                    throw e
                } catch (e: Exception) {
                    log.error { "failed to fetch lockers for ${subscribedRoomId.toLogString()}: $e" }
                }
            }
        }
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
                internalChanges.emit(LockerUpdate(roomId, lockerId, version, body!!.plaintextPayload(), deleted = false))
            } else {
                // A body-less locker event is a tombstone: the server signals a
                // delete by sending lockerId + version with no Locker body.
                internalChanges.emit(LockerUpdate(roomId, lockerId, version, byteArrayOf(), deleted = true))
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
            return cached.map { it.toIdentifiedLocker() }
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
        lockerStore.getAllLockers().map { stored ->
            LockerUpdate(
                RoomId(stored.roomIdRawValue),
                LockerId(stored.lockerIdRawValue, LockerKeyspace { value = stored.lockerKeyspace }),
                stored.version,
                stored.lockerPayload,
            )
        }

    private suspend fun fetchAllLockers(roomId: RoomId, keyspace: LockerKeyspace): List<IdentifiedLocker> {
        val fetchedLockers = roomService.getAllLockers(GetAllLockersRequest {
            this.roomId = roomId
            this.keyspace = keyspace
        }).lockers

        internalChanges.emitAll(fetchedLockers
            .map {
                it.toUpdate(roomId)
            }
            .asFlow()
        )

        return fetchedLockers
    }

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
            return cached.toIdentifiedLocker()
        }

        return fetchLocker(roomId, lockerId)
    }

    private suspend fun fetchLocker(roomId: RoomId, lockerId: LockerId): IdentifiedLocker? {
        val fetchedLocker = roomService.getLocker(GetLockerRequest {
            this.lockerId = lockerId
            this.roomId = roomId
        }).locker

        fetchedLocker?.let {
            internalChanges.emit(it.toUpdate(roomId))
        }

        return fetchedLocker
    }

    suspend fun deleteLocker(
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

                val result = roomService.deleteLocker(DeleteLockerRequest {
                    this.roomId = roomId
                    this.lockerId = lockerId
                    this.parentVersion = parentVersion
                    if (signature != null) this.writeSignature = signature

                    this.notification = notif
                })

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
                    else -> retry()
                }
            }
        } catch (e: RetryLimitExceeded) {
            throw LockerWriteException("locker delete exceeded $WRITE_RETRY_LIMIT attempts", e)
        }

        deletedVersion?.let {
            lockerStore.deleteLocker(roomId, lockerId.keyspaceOrDefault(), lockerId)
            internalChanges.emit(LockerUpdate(roomId, lockerId, it, byteArrayOf(), deleted = true))
        }
    }

    /**
     * Update a locker's plaintext content. [transform] receives the current plaintext
     * and returns the new plaintext; when the locker is signed (a key is available from
     * the [LockKeySource]) the result is wrapped in a signed envelope and the write is
     * signed. The signature binds the parent version, so a version conflict re-runs
     * [transform] and re-signs against the fresh version — preserving the fair-read
     * retry. Set [ratchet] to rotate the signing key atomically with this write.
     */
    suspend fun updateLocker(
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

                val result = roomService.postLockerChange(PostLockerChangeRequest {
                    this.roomId = roomId
                    this.lockerId = lockerId
                    this.parentVersion = parentVersion
                    this.locker = body.locker
                    if (body.signature != null) this.writeSignature = body.signature
                    if (ratchetMsg != null) this.ratchet = ratchetMsg

                    this.notification = notif
                })

                log.debug { "updated locker=${lockerId.toLogString()} result=${result.result} version=${result.version}" }

                val locker = when (result.result) {
                    is PostLockerChangeResponse.Result.OK -> body.locker
                    is PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION -> {
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
                    else -> retry()
                }

                IdentifiedLocker {
                    this.locker = locker
                    this.lockerId = lockerId
                    this.version = result.version
                }
            }
        } catch (e: RetryLimitExceeded) {
            throw LockerWriteException("locker update exceeded $WRITE_RETRY_LIMIT attempts", e)
        }

        val result = updatedLocker ?: return null
        if (pendingRatchetKey != null) {
            lockKeySource?.onRatcheted(roomId, lockerId, pendingRatchetKey)
        }
        val update = result.toUpdate(roomId)
        lockerStore.saveLocker(update.toStored())
        internalChanges.emit(update)

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

private fun RoomId.toLogString() = "r+" + (this?.rawValue?.toBase64String()?.substring(0..5) ?: "(nul)")

private fun LockerId?.toLogString() = "l+" + (this?.rawValue?.toBase64String()?.substring(0..5) ?: "(nul)") +
    "/ks" + (this?.keyspace?.value?.toString() ?: "0")
