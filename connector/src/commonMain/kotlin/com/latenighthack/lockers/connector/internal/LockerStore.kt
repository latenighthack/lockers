package com.latenighthack.lockers.connector.internal

import com.latenighthack.ktstore.BoundStoreKey
import com.latenighthack.ktstore.Store
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.connector.byteArrayIdentity
import com.latenighthack.lockers.connector.storage.v1.StoredLocker
import com.latenighthack.lockers.connector.storage.v1.fromByteArray
import com.latenighthack.lockers.connector.storage.v1.toByteArray

interface LockerStore {
    suspend fun saveLocker(locker: StoredLocker)

    suspend fun getAllLockers(roomId: RoomId, keyspace: LockerKeyspace): List<StoredLocker>

    suspend fun getAllLockers(roomId: RoomId): List<StoredLocker>

    suspend fun getLocker(roomId: RoomId, keyspace: LockerKeyspace, lockerId: LockerId): StoredLocker?

    suspend fun deleteLocker(roomId: RoomId, keyspace: LockerKeyspace, lockerId: LockerId)
}

class LockerStoreImpl(delegate: StoreDelegate) : LockerStore, Store<StoredLocker>(
    delegate,
    "lockers",
    StoredLocker::toByteArray,
    StoredLocker.Companion::fromByteArray
) {
    private val roomIdKey = serializedIndex(StoredLocker::roomIdRawValue, ::byteArrayIdentity)
    private val lockerIdKey = serializedIndex(StoredLocker::lockerIdRawValue, ::byteArrayIdentity)
    private val lockerKeyspaceKey = longIndex(StoredLocker::lockerKeyspace)
    private val roomIdLockerKeyspaceKey = compositeIndex(roomIdKey, lockerKeyspaceKey)
    private val roomIdLockerIdLockerKeyspaceKey = compositeIndex(roomIdKey, lockerIdKey, lockerKeyspaceKey).also { primaryKey(it) }

    override suspend fun saveLocker(locker: StoredLocker) = save(locker)

    override suspend fun getAllLockers(roomId: RoomId, keyspace: LockerKeyspace) = getAll(roomIdLockerKeyspaceKey.eq(listOf(
        BoundStoreKey.SerializedKey(roomIdKey.name, roomId.rawValue),
        BoundStoreKey.LongKey(lockerKeyspaceKey.name, keyspace.value)
    )))

    override suspend fun getAllLockers(roomId: RoomId) = getAll(roomIdKey.eq(roomId.rawValue))

    override suspend fun getLocker(roomId: RoomId, keyspace: LockerKeyspace, lockerId: LockerId) = get(roomIdLockerIdLockerKeyspaceKey.eq(listOf(
        BoundStoreKey.SerializedKey(roomIdKey.name, roomId.rawValue),
        BoundStoreKey.SerializedKey(lockerIdKey.name, lockerId.rawValue),
        BoundStoreKey.LongKey(lockerKeyspaceKey.name, keyspace.value)
    )))

    override suspend fun deleteLocker(roomId: RoomId, keyspace: LockerKeyspace, lockerId: LockerId) = delete(roomIdLockerIdLockerKeyspaceKey.eq(listOf(
        BoundStoreKey.SerializedKey(roomIdKey.name, roomId.rawValue),
        BoundStoreKey.SerializedKey(lockerIdKey.name, lockerId.rawValue),
        BoundStoreKey.LongKey(lockerKeyspaceKey.name, keyspace.value)
    )))
}
