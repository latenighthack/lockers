package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

interface LockerStore {
    suspend fun getLockers(roomId: ServerRoomId, ids: List<Pair<Long, ServerLockerId>>): List<ServerLocker> =
        ids.mapNotNull { (space, id) -> getLocker(roomId, space, id) }

    suspend fun getAllLockers(roomId: ServerRoomId): List<ServerLocker>
    suspend fun getAllLockersInKeyspace(roomId: ServerRoomId, keyspace: Long): List<ServerLocker>
    suspend fun getLocker(roomId: ServerRoomId, keyspace: Long, lockerId: ServerLockerId): ServerLocker?
    suspend fun updateLockers(lockers: List<ServerLocker>) { lockers.forEach { updateLocker(it) } }
    suspend fun updateLocker(locker: ServerLocker)
    suspend fun deleteLocker(roomId: ServerRoomId, keyspace: Long, lockerId: ServerLockerId)
}

class LockerStoreImpl(delegate: Database) : LockerStore, Store<ServerLocker>(delegate, LockerStoreImplDefinitionV1) {
    private val roomIdKey = LockerStoreImplDefinitionV1.roomIdKey
    private val lockerIdKey = LockerStoreImplDefinitionV1.lockerIdKey
    private val keyspaceKey = LockerStoreImplDefinitionV1.keyspaceKey
    private val roomIdAndKeyspace = LockerStoreImplDefinitionV1.roomIdAndKeyspace
    private val roomIdAndKeyspaceAndLockerIdKey = LockerStoreImplDefinitionV1.roomIdAndKeyspaceAndLockerIdKey

    override suspend fun getAllLockers(roomId: ServerRoomId): List<ServerLocker> = getAll(roomIdKey.eq(roomId.toByteArray()))

    override suspend fun getAllLockersInKeyspace(roomId: ServerRoomId, keyspace: Long): List<ServerLocker> = getAll(roomIdAndKeyspace.eq(
        listOf(
            BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.toByteArray()),
            BoundStoreKey.LongKey(keyspaceKey.name.value, keyspace)
        )
    ))

    override suspend fun getLocker(roomId: ServerRoomId, keyspace: Long, lockerId: ServerLockerId): ServerLocker? = get(roomIdAndKeyspaceAndLockerIdKey.eq(
        listOf(
            BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.toByteArray()),
            BoundStoreKey.LongKey(keyspaceKey.name.value, keyspace),
            BoundStoreKey.SerializedKey(lockerIdKey.name.value, lockerId.toByteArray())
        )
    ))

    override suspend fun getLockers(roomId: ServerRoomId, ids: List<Pair<Long, ServerLockerId>>) = getMany(ids.map { (space, id) ->
        roomIdAndKeyspaceAndLockerIdKey.eq(listOf(
            BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.toByteArray()),
            BoundStoreKey.LongKey(keyspaceKey.name.value, space),
            BoundStoreKey.SerializedKey(lockerIdKey.name.value, id.toByteArray())
        ))
    })

    override suspend fun updateLocker(locker: ServerLocker) = save(locker)
    override suspend fun updateLockers(lockers: List<ServerLocker>) = saveAll(lockers)

    override suspend fun deleteLocker(roomId: ServerRoomId, keyspace: Long, lockerId: ServerLockerId) = delete(roomIdAndKeyspaceAndLockerIdKey.eq(
        listOf(
            BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.toByteArray()),
            BoundStoreKey.LongKey(keyspaceKey.name.value, keyspace),
            BoundStoreKey.SerializedKey(lockerIdKey.name.value, lockerId.toByteArray())
        )
    ))
}
