package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.storage.v1.*

/**
 * Persistence for per-scope locks. A lock exists at one of three scopes of a room
 * (room / keyspace / locker); the narrower scopes leave the unused key slots at
 * sentinel values (keyspace 0, empty locker id) so the composite primary key stays
 * uniform. The stored [ServerLock.lockState] is a serialized common.v1.LockState.
 */
interface LockStore {
    /** Must serialize the entire room hierarchy with content and outbox commits. */
    suspend fun <T> atomic(roomId: ServerRoomId, block: suspend () -> T): T =
        throw UnsupportedOperationException("LockStore requires atomic room transactions")
    fun deliveryOutbox(): DeliveryOutboxStore? = null
    suspend fun getLock(roomId: ServerRoomId, scopeKind: Long, keyspace: Long, lockerId: ServerLockerId): ServerLock?
    suspend fun getAllLocksInRoom(roomId: ServerRoomId): List<ServerLock>
    suspend fun saveLock(lock: ServerLock)
    suspend fun deleteLock(roomId: ServerRoomId, scopeKind: Long, keyspace: Long, lockerId: ServerLockerId)
}

class LockStoreImpl(private val database: Database, private val limits: com.latenighthack.lockers.server.ServerResourceLimits = com.latenighthack.lockers.server.ServerResourceLimits()) : LockStore, Store<ServerLock>(database, LockStoreImplDefinitionV1) {
    override suspend fun <T> atomic(roomId: ServerRoomId, block: suspend () -> T): T = database.transaction(roomMutationKey(roomId), block)
    override fun deliveryOutbox() = DeliveryOutboxStore(database)
    private val roomIdKey = LockStoreImplDefinitionV1.roomIdKey
    private val scopeKindKey = LockStoreImplDefinitionV1.scopeKindKey
    private val keyspaceKey = LockStoreImplDefinitionV1.keyspaceKey
    private val lockerIdKey = LockStoreImplDefinitionV1.lockerIdKey
    private val primary = LockStoreImplDefinitionV1.primary

    override suspend fun getLock(
        roomId: ServerRoomId,
        scopeKind: Long,
        keyspace: Long,
        lockerId: ServerLockerId
    ): ServerLock? = get(primary.eq(
        listOf(
            BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.toByteArray()),
            BoundStoreKey.LongKey(scopeKindKey.name.value, scopeKind),
            BoundStoreKey.LongKey(keyspaceKey.name.value, keyspace),
            BoundStoreKey.SerializedKey(lockerIdKey.name.value, lockerId.toByteArray())
        )
    ))

    override suspend fun getAllLocksInRoom(roomId: ServerRoomId): List<ServerLock> = atomic(roomId) {
        val count = database.count(LockStoreImplDefinitionV1.storeName, roomIdKey.query(1, lower = roomId.toByteArray(), upper = roomId.toByteArray()))
        if (count > limits.maxLocksPerRoom) protocolCapacityExceeded("Historical room lock set exceeds finite capacity")
        getAll(roomIdKey.eq(roomId.toByteArray()))
    }

    override suspend fun saveLock(lock: ServerLock) = atomic(requireNotNull(lock.roomId)) {
        database.transaction("lockers.lock-admission") {
            val room = requireNotNull(lock.roomId)
            if (getLock(room, lock.scopeKind, lock.keyspace, requireNotNull(lock.lockerId)) == null) {
                val total = database.count(LockStoreImplDefinitionV1.storeName, roomIdKey.query(1))
                val count = database.count(LockStoreImplDefinitionV1.storeName, roomIdKey.query(1, lower = room.toByteArray(), upper = room.toByteArray()))
                if (total >= limits.maxLocks || count >= limits.maxLocksPerRoom) namespaceExhausted("Permanent lock history namespace exhausted")
            }
            save(lock)
        }
    }

    /** Retire a scope while preserving the epoch needed to reject stale authority. */
    override suspend fun deleteLock(roomId: ServerRoomId, scopeKind: Long, keyspace: Long, lockerId: ServerLockerId) = atomic(roomId) {
        val current = getLock(roomId, scopeKind, keyspace, lockerId) ?: return@atomic
        val state = com.latenighthack.lockers.common.v1.LockState.fromByteArray(current.lockState)
        saveLock(current.copy(lockState = state.copy(locked = false).toByteArray()))
    }
}

fun roomMutationKey(roomId: ServerRoomId): String = "lockers.room." + roomId.rawValue.joinToString("") { "%02x".format(it) }

internal fun namespaceExhausted(message: String): Nothing = throw com.latenighthack.ktbuf.net.RpcResponseException("", "RPC", com.latenighthack.ktbuf.proto.Codes.FAILED_PRECONDITION, message)

internal fun protocolCapacityExceeded(message: String): Nothing = throw com.latenighthack.ktbuf.net.RpcResponseException("", "RPC", com.latenighthack.ktbuf.proto.Codes.OUT_OF_RANGE, message)
internal fun resourceExhausted(message: String): Nothing = throw com.latenighthack.ktbuf.net.RpcResponseException("", "RPC", com.latenighthack.ktbuf.proto.Codes.RESOURCE_EXHAUSTED, message)
