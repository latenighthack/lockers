package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.BoundStoreKey
import com.latenighthack.ktstore.Store
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.server.storage.v1.*

/**
 * Persistence for per-scope locks. A lock exists at one of three scopes of a room
 * (room / keyspace / locker); the narrower scopes leave the unused key slots at
 * sentinel values (keyspace 0, empty locker id) so the composite primary key stays
 * uniform. The stored [ServerLock.lockState] is a serialized common.v1.LockState.
 */
interface LockStore {
    suspend fun getLock(roomId: ServerRoomId, scopeKind: Long, keyspace: Long, lockerId: ServerLockerId): ServerLock?
    suspend fun getAllLocksInRoom(roomId: ServerRoomId): List<ServerLock>
    suspend fun saveLock(lock: ServerLock)
    suspend fun deleteLock(roomId: ServerRoomId, scopeKind: Long, keyspace: Long, lockerId: ServerLockerId)
}

class LockStoreImpl(delegate: StoreDelegate) : LockStore, Store<ServerLock>(
    delegate,
    "locks",
    ServerLock::toByteArray,
    ServerLock.Companion::fromByteArray
) {
    private val roomIdKey = serializedIndex(
        ServerLock::roomId,
        ServerRoomId::toByteArray
    )
    private val scopeKindKey = longIndex(ServerLock::scopeKind)
    private val keyspaceKey = longIndex(ServerLock::keyspace)
    private val lockerIdKey = serializedIndex(
        ServerLock::lockerId,
        ServerLockerId::toByteArray
    )
    private val primary = compositeIndex(
        roomIdKey,
        scopeKindKey,
        keyspaceKey,
        lockerIdKey
    ).also { primaryKey(it) }

    override suspend fun getLock(
        roomId: ServerRoomId,
        scopeKind: Long,
        keyspace: Long,
        lockerId: ServerLockerId
    ): ServerLock? = get(primary.eq(
        listOf(
            BoundStoreKey.SerializedKey(roomIdKey.name, roomId.toByteArray()),
            BoundStoreKey.LongKey(scopeKindKey.name, scopeKind),
            BoundStoreKey.LongKey(keyspaceKey.name, keyspace),
            BoundStoreKey.SerializedKey(lockerIdKey.name, lockerId.toByteArray())
        )
    ))

    override suspend fun getAllLocksInRoom(roomId: ServerRoomId): List<ServerLock> =
        getAll(roomIdKey.eq(roomId.toByteArray()))

    override suspend fun saveLock(lock: ServerLock) = save(lock)

    override suspend fun deleteLock(
        roomId: ServerRoomId,
        scopeKind: Long,
        keyspace: Long,
        lockerId: ServerLockerId
    ) = delete(primary.eq(
        listOf(
            BoundStoreKey.SerializedKey(roomIdKey.name, roomId.toByteArray()),
            BoundStoreKey.LongKey(scopeKindKey.name, scopeKind),
            BoundStoreKey.LongKey(keyspaceKey.name, keyspace),
            BoundStoreKey.SerializedKey(lockerIdKey.name, lockerId.toByteArray())
        )
    ))
}
