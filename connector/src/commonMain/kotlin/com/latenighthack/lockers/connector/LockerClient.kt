package com.latenighthack.lockers.connector

import com.diamondedge.logging.KmLog
import com.diamondedge.logging.logging
import com.latenighthack.ktbuf.bytes.toBase64String
import com.latenighthack.ktbuf.net.RpcClient
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

private fun IdentifiedLocker.toUpdate(roomId: RoomId) = LockerClient.LockerUpdate(roomId, lockerId!!, version,locker?.open?.encodedPayload ?: byteArrayOf())

class TypedLockerClient<ValueType>(
    private val lockerClient: LockerClient,
    private val keyspace: LockerKeyspace,
    private val writer: KFunction1<ValueType, ByteArray>,
    private val reader: KFunction1<ByteArray, ValueType>
) {
    inner class TypedLockerUpdate(val roomId: RoomId, val lockerId: LockerId, val value: ValueType)

    inner class TypedLocker(val value: ValueType)

    inner class TypedIdentifiedLocker(val lockerId: LockerId, val value: ValueType)

    suspend fun deleteLocker(
        roomId: RoomId,
        lockerId: LockerId,
        notificationBuilder: NotificationBuilder.() -> Unit = {}
    ) {
        lockerClient.deleteLocker(roomId, lockerId, notificationBuilder)
    }

    suspend fun updateLocker(
        roomId: RoomId,
        lockerId: LockerId,
        notificationBuilder: NotificationBuilder.(Locker?) -> Unit = {},
        builder: (ValueType) -> ValueType
    ): ValueType? {
        val lockerId = lockerId.copy(keyspace = keyspace)
        val updated = lockerClient.updateLocker(roomId, lockerId, notificationBuilder) {
            open {
                val existingValue = reader(encodedPayload)

                encodedPayload = writer(builder(existingValue))
            }
        }

        return updated?.open?.encodedPayload?.let { reader(it) }
    }

    fun watchAll(roomId: RoomId): Flow<Map<LockerId, ValueType>> = allUpdates
        .onStart { lockerClient.subscribeToRoom(roomId) }
        .filter { it.roomId == roomId }
        .runningFold(emptyMap()) { acc, value ->
            if (value.value == null) {
                acc - value.lockerId
            } else {
                acc + Pair(value.lockerId, value.value)
            }
        }

    fun watchAll(roomId: RoomId, keyspace: LockerKeyspace): Flow<Map<LockerId, ValueType>> = allUpdates
        .onStart {
            lockerClient.subscribeToRoom(roomId)
            emitAll(
                lockerClient.getAllLockers(roomId, keyspace)
                    .map {
                        TypedLockerUpdate(roomId, it.lockerId!!, reader(it.locker?.open?.encodedPayload ?: byteArrayOf()))
                    }
                    .asFlow()
            )
        }
        .filter { it.lockerId.keyspace == keyspace && it.roomId == roomId }
        .runningFold(emptyMap()) { acc, value ->
            if (value.value == null) {
                acc - value.lockerId
            } else {
                acc + Pair(value.lockerId, value.value)
            }
        }

    fun watch(roomId: RoomId, lockerId: LockerId): Flow<TypedLocker> {
        val lockerId = lockerId.copy(keyspace = keyspace)

        return allUpdates
            .onStart { lockerClient.subscribeToRoom(roomId) }
            .filter { it.roomId == roomId && it.lockerId == lockerId }
            .map {
                TypedLocker(it.value)
            }
    }

    val allUpdates: Flow<TypedLockerUpdate>
        get() {
            return lockerClient
                .changes
                .filter { it.lockerId.keyspace == keyspace }
                .map {
                    TypedLockerUpdate(it.roomId, it.lockerId, reader(it.payload))
                }
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
}

data class IncomingNotification(
    val roomId: RoomId,
    val lockerId: LockerId,
    val payload: ByteArray
)

class LockerClient(
    rpcClient: RpcClient,
    private val stream: Stream,
    private val lockerStore: LockerStore,
    private val log: KmLog = logging()
) {
    private val processingJob = Job()
    private val processingScope = GlobalScope + processingJob

    data class LockerUpdate(val roomId: RoomId, val lockerId: LockerId, val version: Long, val payload: ByteArray)

    private val internalChanges = MutableSharedFlow<LockerUpdate>()
    private val incomingNotifications = MutableSharedFlow<IncomingNotification>()

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
                    lockerStore.saveLocker(StoredLocker {
                        roomIdRawValue = update.roomId.rawValue
                        lockerIdRawValue = update.lockerId.rawValue
                        lockerKeyspace = update.lockerId.keyspace?.value ?: 0L
                        lockerPayload = update.payload
                        version = update.version
                    })
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
        val lockerId = event.locker?.lockerId ?: return true
        val payload = event.locker?.locker?.open?.encodedPayload ?: return true
        val version = event.locker?.version ?: 0L
        val payloadBytes = event.notification?.payload?.rawValue

        internalChanges.emit(LockerUpdate(roomId, lockerId, version, payload))

        if (payloadBytes != null) {
            incomingNotifications.emit(IncomingNotification(roomId, lockerId, payloadBytes))
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

    suspend fun getAllLockers(roomId: RoomId, keyspace: LockerKeyspace): List<IdentifiedLocker> {
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

    suspend fun getLocker(roomId: RoomId, lockerId: LockerId): IdentifiedLocker? {
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
        val existingLocker = getLocker(roomId, lockerId)
        var originalLocker = existingLocker?.locker ?: Locker()
        var parentVersion = existingLocker?.version ?: 0L

        val deleteLocker = repeatWithBackoff {
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

        // todo:
//        internalChanges.emit(it.toUpdate(roomId))
    }

    suspend fun updateLocker(
        roomId: RoomId,
        lockerId: LockerId,
        notificationBuilder: NotificationBuilder.(Locker?) -> Unit = {},
        builder: suspend LockerBuilder.() -> Unit,
    ): Locker? {
        val existingLocker = getLocker(roomId, lockerId)
        var originalLocker = existingLocker?.locker ?: Locker()
        var parentVersion = existingLocker?.version ?: 0L

        val updatedLocker = repeatWithBackoff {
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

        return updatedLocker
            ?.also {
                internalChanges.emit(it.toUpdate(roomId))
            }
            ?.locker
    }
}

private fun RoomId.toLogString() = "r+" + (this?.rawValue?.toBase64String()?.substring(0..5) ?: "(nul)")

private fun LockerId?.toLogString() = "l+" + (this?.rawValue?.toBase64String()?.substring(0..5) ?: "(nul)")
