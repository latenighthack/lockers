package com.latenighthack.lockers.connector.internal

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.connector.byteArrayIdentity
import com.latenighthack.lockers.connector.storage.v1.StoredLocker
import com.latenighthack.lockers.connector.storage.v1.fromByteArray
import com.latenighthack.lockers.connector.storage.v1.toByteArray

interface LockerStore {
    suspend fun pendingRatchets(): List<PendingRatchet> = emptyList()
    suspend fun saveRatchet(value: PendingRatchet): Unit = throw UnsupportedOperationException("Durable ratchet journal required")
    suspend fun clearRatchet(request: com.latenighthack.lockers.room.v1.PostLockerChangeRequest): Unit = throw UnsupportedOperationException("Durable ratchet journal required")

    suspend fun saveLocker(locker: StoredLocker)

    suspend fun getAllLockers(): List<StoredLocker>

    suspend fun getAllLockers(roomId: RoomId, keyspace: LockerKeyspace): List<StoredLocker>

    suspend fun getAllLockers(roomId: RoomId): List<StoredLocker>

    suspend fun getLocker(roomId: RoomId, keyspace: LockerKeyspace, lockerId: LockerId): StoredLocker?

    suspend fun deleteLocker(roomId: RoomId, keyspace: LockerKeyspace, lockerId: LockerId)
}

class LockerStoreImpl(delegate: Database) : LockerStore, Store<StoredLocker>(delegate, LockerStoreImplDefinitionV1) {
    private val ratchetJournal = RatchetJournal(delegate)
    override suspend fun pendingRatchets() = ratchetJournal.pending()
    override suspend fun saveRatchet(value: PendingRatchet) = ratchetJournal.put(value)
    override suspend fun clearRatchet(request: com.latenighthack.lockers.room.v1.PostLockerChangeRequest) = ratchetJournal.remove(request)
    private val roomIdKey = LockerStoreImplDefinitionV1.roomIdKey
    private val lockerIdKey = LockerStoreImplDefinitionV1.lockerIdKey
    private val lockerKeyspaceKey = LockerStoreImplDefinitionV1.lockerKeyspaceKey
    private val roomIdLockerKeyspaceKey = LockerStoreImplDefinitionV1.roomIdLockerKeyspaceKey
    private val roomIdLockerIdLockerKeyspaceKey = LockerStoreImplDefinitionV1.roomIdLockerIdLockerKeyspaceKey

    override suspend fun saveLocker(locker: StoredLocker) = save(locker)

    override suspend fun getAllLockers() = getAll()

    override suspend fun getAllLockers(roomId: RoomId, keyspace: LockerKeyspace) = getAll(roomIdLockerKeyspaceKey.eq(listOf(
        BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.rawValue),
        BoundStoreKey.LongKey(lockerKeyspaceKey.name.value, keyspace.value)
    )))

    override suspend fun getAllLockers(roomId: RoomId) = getAll(roomIdKey.eq(roomId.rawValue))

    override suspend fun getLocker(roomId: RoomId, keyspace: LockerKeyspace, lockerId: LockerId) = get(roomIdLockerIdLockerKeyspaceKey.eq(listOf(
        BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.rawValue),
        BoundStoreKey.SerializedKey(lockerIdKey.name.value, lockerId.rawValue),
        BoundStoreKey.LongKey(lockerKeyspaceKey.name.value, keyspace.value)
    )))

    override suspend fun deleteLocker(roomId: RoomId, keyspace: LockerKeyspace, lockerId: LockerId) = delete(roomIdLockerIdLockerKeyspaceKey.eq(listOf(
        BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.rawValue),
        BoundStoreKey.SerializedKey(lockerIdKey.name.value, lockerId.rawValue),
        BoundStoreKey.LongKey(lockerKeyspaceKey.name.value, keyspace.value)
    )))
}
