package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.BoundStoreKey
import com.latenighthack.ktstore.Store
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.server.storage.v1.*

interface LockerStore {
    suspend fun getAllLockers(roomId: ServerRoomId): List<ServerLocker>
    suspend fun getAllLockersInKeyspace(roomId: ServerRoomId, keyspace: Long): List<ServerLocker>
    suspend fun getLocker(roomId: ServerRoomId, keyspace: Long, lockerId: ServerLockerId): ServerLocker?
    suspend fun updateLocker(locker: ServerLocker)
    suspend fun deleteLocker(roomId: ServerRoomId, keyspace: Long, lockerId: ServerLockerId)
}

class LockerStoreImpl(delegate: StoreDelegate) : LockerStore, Store<ServerLocker>(
    delegate,
    "lockers",
    ServerLocker::toByteArray,
    ServerLocker.Companion::fromByteArray
) {
    private val roomIdKey = serializedIndex(
        ServerLocker::roomId,
        ServerRoomId::toByteArray
    )
    private val lockerIdKey = serializedIndex(
        ServerLocker::lockerId,
        ServerLockerId::toByteArray
    )
    private val keyspaceKey = longIndex(ServerLocker::keyspace)
    private val roomIdAndKeyspace = compositeIndex(
        roomIdKey,
        keyspaceKey
    )
    private val roomIdAndKeyspaceAndLockerIdKey = compositeIndex(
        roomIdKey,
        keyspaceKey,
        lockerIdKey
    ).also { primaryKey(it) }

    override suspend fun getAllLockers(roomId: ServerRoomId): List<ServerLocker> = getAll(roomIdKey.eq(roomId.toByteArray()))

    override suspend fun getAllLockersInKeyspace(roomId: ServerRoomId, keyspace: Long): List<ServerLocker> = getAll(roomIdAndKeyspace.eq(
        listOf(
            BoundStoreKey.SerializedKey(roomIdKey.name, roomId.toByteArray()),
            BoundStoreKey.LongKey(keyspaceKey.name, keyspace)
        )
    ))

    override suspend fun getLocker(roomId: ServerRoomId, keyspace: Long, lockerId: ServerLockerId): ServerLocker? = get(roomIdAndKeyspaceAndLockerIdKey.eq(
        listOf(
            BoundStoreKey.SerializedKey(roomIdKey.name, roomId.toByteArray()),
            BoundStoreKey.LongKey(keyspaceKey.name, keyspace),
            BoundStoreKey.SerializedKey(lockerIdKey.name, lockerId.toByteArray())
        )
    ))

    override suspend fun updateLocker(locker: ServerLocker) = save(locker)

    override suspend fun deleteLocker(roomId: ServerRoomId, keyspace: Long, lockerId: ServerLockerId) = delete(roomIdAndKeyspaceAndLockerIdKey.eq(
        listOf(
            BoundStoreKey.SerializedKey(roomIdKey.name, roomId.toByteArray()),
            BoundStoreKey.LongKey(keyspaceKey.name, keyspace),
            BoundStoreKey.SerializedKey(lockerIdKey.name, lockerId.toByteArray())
        )
    ))
}
