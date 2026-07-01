package com.latenighthack.lockers.connector

import com.diamondedge.logging.KmLog
import com.diamondedge.logging.logging
import com.latenighthack.ktbuf.bytes.toBase64String
import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.rpc.RetryLimitExceeded
import com.latenighthack.ktbuf.rpc.repeatWithBackoff
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

private fun IdentifiedLocker.toUpdate(roomId: RoomId) =
    LockerClient.LockerUpdate(roomId, lockerId!!, version, locker?.open?.encodedPayload ?: byteArrayOf())

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
        return fetched?.locker?.open?.encodedPayload?.let { reader(it) }
    }

    suspend fun getAllLockers(roomId: RoomId, revalidate: Boolean = true): Map<LockerId, ValueType> =
        lockerClient.getAllLockers(roomId, keyspace, revalidate)
            .associate { it.lockerId!! to reader(it.locker?.open?.encodedPayload ?: byteArrayOf()) }

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
        builder: (ValueType) -> ValueType
    ): ValueType? {
        val scopedId = lockerId.scoped()
        val updated = lockerClient.updateLocker(roomId, scopedId, notificationBuilder) {
            open {
                val existingValue = reader(encodedPayload)

                encodedPayload = writer(builder(existingValue))
            }
        }

        return updated?.open?.encodedPayload?.let { reader(it) }
    }

    fun watchAll(roomId: RoomId, includeHistory: Boolean = true): Flow<Map<LockerId, ValueType>> = allUpdates
        .onStart {
            lockerClient.subscribeToRoom(roomId)
            if (includeHistory) {
                emitAll(hydrate(roomId, keyspace))
            }
        }
        .filter { it.roomId == roomId }
        .runningFold(emptyMap()) { acc, value -> acc.applyUpdate(value) }

    fun watchAll(roomId: RoomId, keyspace: LockerKeyspace, includeHistory: Boolean = true): Flow<Map<LockerId, ValueType>> = allUpdates
        .onStart {
            lockerClient.subscribeToRoom(roomId)
            if (includeHistory) {
                emitAll(hydrate(roomId, keyspace))
            }
        }
        .filter { it.lockerId.keyspace == keyspace && it.roomId == roomId }
        .runningFold(emptyMap()) { acc, value -> acc.applyUpdate(value) }

    fun watch(roomId: RoomId, lockerId: LockerId, includeHistory: Boolean = true): Flow<TypedLockerUpdate<ValueType>> {
        val scopedId = lockerId.scoped()

        return allUpdates
            .onStart {
                lockerClient.subscribeToRoom(roomId)
                if (includeHistory) {
                    lockerClient.getLocker(roomId, scopedId)?.let {
                        emit(it.toTyped(roomId))
                    }
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
        TypedLockerUpdate.Present(roomId, lockerId!!, reader(locker?.open?.encodedPayload ?: byteArrayOf()))

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
    private val log: KmLog = logging()
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
            }
        }
    }

    private suspend fun processEvent(event: Event): Boolean {
        val roomId = event.roomId ?: return true
        val identified = event.locker
        val lockerId = identified?.lockerId
        val version = identified?.version ?: 0L
        val open = identified?.locker?.open
        val notificationPayload = event.notification?.payload?.rawValue

        if (lockerId != null) {
            if (open != null) {
                internalChanges.emit(LockerUpdate(roomId, lockerId, version, open.encodedPayload, deleted = false))
            } else {
                // A body-less locker event is a tombstone: the server signals a
                // delete by sending lockerId + version with no Locker body.
                internalChanges.emit(LockerUpdate(roomId, lockerId, version, byteArrayOf(), deleted = true))
            }
        }

        if (lockerId != null && notificationPayload != null) {
            incomingNotifications.emit(IncomingNotification(roomId, lockerId, notificationPayload))
        }

        return true
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
                processingScope.launch { fetchAllLockers(roomId, keyspace) }
            }
            return cached.map { it.toIdentifiedLocker() }
        }

        return fetchAllLockers(roomId, keyspace)
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
                processingScope.launch { fetchLocker(roomId, lockerId) }
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
        var originalLocker = cached?.toIdentifiedLocker()?.locker ?: Locker()
        var parentVersion = cached?.version ?: 0L

        val deletedLocker = try {
            repeatWithBackoff(retryLimit = WRITE_RETRY_LIMIT) {
                val result = roomService.deleteLocker(DeleteLockerRequest {
                    this.roomId = roomId
                    this.lockerId = lockerId
                    this.parentVersion = parentVersion

                    notification {
                        this.notificationBuilder()
                    }
                })

                val locker = when (result.result) {
                    is DeleteLockerResponse.Result.OK -> originalLocker
                    is DeleteLockerResponse.Result.UPDATE_LOCAL_VERSION -> {
                        originalLocker = result.existingLocker!!
                        parentVersion = result.version

                        retry()
                    }
                    else -> retry()
                }

                IdentifiedLocker {
                    this.locker = locker
                    this.lockerId = lockerId
                    this.version = result.version
                }
            }
        } catch (e: RetryLimitExceeded) {
            throw LockerWriteException("locker delete exceeded $WRITE_RETRY_LIMIT attempts", e)
        }

        deletedLocker?.let {
            lockerStore.deleteLocker(roomId, lockerId.keyspaceOrDefault(), lockerId)
            internalChanges.emit(LockerUpdate(roomId, lockerId, it.version, byteArrayOf(), deleted = true))
        }
    }

    suspend fun updateLocker(
        roomId: RoomId,
        lockerId: LockerId,
        notificationBuilder: NotificationBuilder.(Locker?) -> Unit = {},
        builder: suspend LockerBuilder.() -> Unit,
    ): Locker? {
        val cached = lockerStore.getLocker(roomId, lockerId.keyspaceOrDefault(), lockerId)
        var originalLocker = cached?.toIdentifiedLocker()?.locker ?: Locker()
        var parentVersion = cached?.version ?: 0L

        val updatedLocker = try {
            repeatWithBackoff(retryLimit = WRITE_RETRY_LIMIT) {
                val updatedLocker = originalLocker.copy {
                    builder()
                }
                log.debug { "updating locker=${lockerId.toLogString()}" }

                val result = roomService.postLockerChange(PostLockerChangeRequest {
                    this.roomId = roomId
                    this.lockerId = lockerId
                    this.parentVersion = parentVersion
                    this.locker = updatedLocker

                    notification {
                        this.notificationBuilder(updatedLocker)
                    }
                })

                log.debug { "updated locker=${lockerId.toLogString()}" }

                val locker = when (result.result) {
                    is PostLockerChangeResponse.Result.OK -> updatedLocker
                    is PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION -> {
                        originalLocker = result.existingLocker!!
                        parentVersion = result.version

                        retry()
                    }
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
        val update = result.toUpdate(roomId)
        lockerStore.saveLocker(update.toStored())
        internalChanges.emit(update)

        return result.locker
    }
}

private fun RoomId.toLogString() = "r+" + (this?.rawValue?.toBase64String()?.substring(0..5) ?: "(nul)")

private fun LockerId?.toLogString() = "l+" + (this?.rawValue?.toBase64String()?.substring(0..5) ?: "(nul)")
