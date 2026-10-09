package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import com.latenighthack.lockers.server.storage.v1.*

interface LockerStore {
    fun supportsSnapshotPaging(): Boolean = false
    fun lockerPages(roomId: ServerRoomId): Flow<List<ServerLocker>> =
        throw UnsupportedOperationException("Locker extension requires bounded indexed pages")
    fun snapshotStore(limits: com.latenighthack.lockers.server.ServerResourceLimits): SnapshotStore =
        throw UnsupportedOperationException("Locker extension requires durable snapshot leases")
    suspend fun getLockers(roomId: ServerRoomId, ids: List<Pair<Long, ServerLockerId>>): List<ServerLocker> =
        ids.mapNotNull { (space, id) -> getLocker(roomId, space, id) }

    suspend fun getAllLockers(roomId: ServerRoomId): List<ServerLocker>
    suspend fun getAllLockersInKeyspace(roomId: ServerRoomId, keyspace: Long): List<ServerLocker>
    suspend fun getLocker(roomId: ServerRoomId, keyspace: Long, lockerId: ServerLockerId): ServerLocker?
    suspend fun updateLockers(lockers: List<ServerLocker>) { lockers.forEach { updateLocker(it) } }
    suspend fun updateLocker(locker: ServerLocker)
    suspend fun deleteLocker(roomId: ServerRoomId, keyspace: Long, lockerId: ServerLockerId)
}

class LockerStoreImpl(private val database: Database, private val limits: com.latenighthack.lockers.server.ServerResourceLimits = com.latenighthack.lockers.server.ServerResourceLimits()) : LockerStore, Store<ServerLocker>(database, LockerStoreDefinitionV2) {
    private val roomIdKey = LockerStoreDefinitionV2.roomIdKey
    private val lockerIdKey = LockerStoreDefinitionV2.lockerIdKey
    private val keyspaceKey = LockerStoreDefinitionV2.keyspaceKey
    private val roomIdAndKeyspace = LockerStoreDefinitionV2.roomIdAndKeyspace
    private val roomIdAndKeyspaceAndLockerIdKey = LockerStoreDefinitionV2.roomIdAndKeyspaceAndLockerIdKey

    override fun supportsSnapshotPaging() = true
    override fun snapshotStore(limits: com.latenighthack.lockers.server.ServerResourceLimits) = SnapshotStore(database, limits)
    override fun lockerPages(roomId: ServerRoomId): Flow<List<ServerLocker>> = flow {
        var after: LocalContinuation? = null
        do {
            val page = database.query(LockerStoreDefinitionV2.storeName, roomIdKey.query(64,
                lower = roomId.toByteArray(), upper = roomId.toByteArray(), after = after))
            if (page.records.isNotEmpty()) emit(page.records.map { when (it) {
                is ServerLocker -> it; is ByteArray -> LockerStoreDefinitionV2.decode(it); else -> error("Invalid locker row")
            } })
            after = page.continuation
        } while (after != null)
    }
    override suspend fun getAllLockers(roomId: ServerRoomId): List<ServerLocker> = getAll(roomIdKey.eq(roomId.toByteArray()))

    override suspend fun getAllLockersInKeyspace(roomId: ServerRoomId, keyspace: Long): List<ServerLocker> = getAll(roomIdAndKeyspace.eq(
        listOf(
            BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.toByteArray()),
            BoundStoreKey.SerializedKey(keyspaceKey.name.value, lockerKeyspaceKey(keyspace))
        )
    ))

    override suspend fun getLocker(roomId: ServerRoomId, keyspace: Long, lockerId: ServerLockerId): ServerLocker? = get(roomIdAndKeyspaceAndLockerIdKey.eq(
        listOf(
            BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.toByteArray()),
            BoundStoreKey.SerializedKey(keyspaceKey.name.value, lockerKeyspaceKey(keyspace)),
            BoundStoreKey.SerializedKey(lockerIdKey.name.value, lockerId.toByteArray())
        )
    ))

    override suspend fun getLockers(roomId: ServerRoomId, ids: List<Pair<Long, ServerLockerId>>) = getMany(ids.map { (space, id) ->
        roomIdAndKeyspaceAndLockerIdKey.eq(listOf(
            BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.toByteArray()),
            BoundStoreKey.SerializedKey(keyspaceKey.name.value, lockerKeyspaceKey(space)),
            BoundStoreKey.SerializedKey(lockerIdKey.name.value, id.toByteArray())
        ))
    })

    override suspend fun updateLocker(locker: ServerLocker) = updateLockers(listOf(locker))
    override suspend fun updateLockers(lockers: List<ServerLocker>) {
        if (lockers.isEmpty()) return
        require(lockers.size <= 1024) { "Locker store batch exceeds bounded work" }
        val rooms = lockers.map { requireNotNull(it.roomId) }.distinct().sortedBy { roomMutationKey(it) }
        suspend fun owned(index: Int) {
            if (index < rooms.size) { database.transaction(roomMutationKey(rooms[index])) { owned(index + 1) }; return }
            database.transaction("lockers.locker-admission") {
                val unique = lockers.distinctBy { Triple(it.roomId, it.keyspace, it.lockerId) }
                val added = unique.filter { getLocker(requireNotNull(it.roomId), it.keyspace, requireNotNull(it.lockerId)) == null }
                if (added.isNotEmpty()) {
                    val total = database.count(LockerStoreDefinitionV2.storeName, roomIdKey.query(1))
                    if (total + added.size > limits.maxLockers) namespaceExhausted("Permanent locker namespace exhausted")
                    for ((room, rows) in added.groupBy { requireNotNull(it.roomId) }) {
                        val count = database.count(LockerStoreDefinitionV2.storeName, roomIdKey.query(1, lower = room.toByteArray(), upper = room.toByteArray()))
                        if (count + rows.size > limits.maxLockersPerRoom) namespaceExhausted("Room locker namespace exhausted")
                    }
                }
                saveAll(lockers)
            }
        }
        owned(0)
    }

    /** Retire content without freeing its permanent version/namespace reservation. */
    override suspend fun deleteLocker(roomId: ServerRoomId, keyspace: Long, lockerId: ServerLockerId) = database.transaction(roomMutationKey(roomId)) {
        val current = getLocker(roomId, keyspace, lockerId) ?: return@transaction
        if (current.deleted) return@transaction
        if (current.version == Long.MAX_VALUE) namespaceExhausted("Locker version space exhausted")
        updateLocker(current.copy(locker = byteArrayOf(), version = current.version + 1, deleted = true))
    }
}
