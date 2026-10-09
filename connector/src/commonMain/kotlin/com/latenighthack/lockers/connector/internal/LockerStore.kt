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
    suspend fun archivedRatchets(): List<ArchivedRatchet> = emptyList()
    suspend fun hasArchivedRatchet(room: RoomId): Boolean = archivedRatchets().any { it.room == room }
    suspend fun archiveRatchet(value: ArchivedRatchet): Unit = throw UnsupportedOperationException("Durable ratchet archive required")
    suspend fun matchingRatchet(room: RoomId, publicKey: ByteArray): ArchivedRatchet? = null
    suspend fun forgetArchivedRatchet(value: ArchivedRatchet): Unit = throw UnsupportedOperationException("Durable ratchet archive required")
    suspend fun pruneEventsThrough(cursor: Long): Unit = throw UnsupportedOperationException("Application cursor retention required")
    suspend fun acceptAtomically(action: suspend () -> Unit) = action()
    suspend fun forgetLocker(expected: StoredLocker) {
        deleteLocker(RoomId(expected.roomIdRawValue), LockerKeyspace(expected.lockerKeyspace), LockerId(expected.lockerIdRawValue))
    }
    suspend fun acceptLocker(locker: StoredLocker) = saveLocker(locker)
    fun changesAfter(cursor: Long): kotlinx.coroutines.flow.Flow<ConnectorJournalEntry> = throw UnsupportedOperationException("Durable change journal required")
    fun liveChanges(): kotlinx.coroutines.flow.Flow<ConnectorJournalEntry> = throw UnsupportedOperationException("Durable change journal required")

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

class LockerStoreImpl(private val database: Database, private val policy: com.latenighthack.lockers.connector.ConnectorRetentionPolicy = com.latenighthack.lockers.connector.ConnectorRetentionPolicy()) : LockerStore, Store<StoredLocker>(database, LockerStoreImplDefinitionV1) {
    private val archive = RatchetArchive(database)
    override suspend fun archivedRatchets() = archive.archives()
    override suspend fun hasArchivedRatchet(room: RoomId) = archive.hasRoom(room)
    override suspend fun archiveRatchet(value: ArchivedRatchet) = archive.put(value)
    override suspend fun matchingRatchet(room: RoomId, publicKey: ByteArray) = archive.matching(room, publicKey)
    override suspend fun forgetArchivedRatchet(value: ArchivedRatchet) = archive.remove(value)
    private val eventJournal = ConnectorEventJournal(database, policy)
    override suspend fun acceptAtomically(action: suspend () -> Unit) { prepare(); database.transaction("connector-accept") { action() } }
    override suspend fun forgetLocker(expected: StoredLocker) {
        prepare(); database.transaction("connector-accept") {
            val room = RoomId(expected.roomIdRawValue); val id = LockerId(expected.lockerIdRawValue, LockerKeyspace(expected.lockerKeyspace))
            val current = getLocker(room, id.keyspace!!, id)
            if (current != expected) return@transaction
            deleteLocker(room, id.keyspace!!, id)
            eventJournal.append(1, expected.copy(deleted = true, lockerPayload = byteArrayOf()).toByteArray(), kotlin.random.Random.nextBytes(32))
        }
    }
    override suspend fun pruneEventsThrough(cursor: Long) = eventJournal.pruneThrough(cursor)
    override fun changesAfter(cursor: Long) = eventJournal.after(cursor)
    override fun liveChanges() = eventJournal.live()
    override suspend fun acceptLocker(locker: StoredLocker) {
        prepare(); database.transaction("connector-accept") {
            if (getLocker(RoomId(locker.roomIdRawValue), LockerKeyspace(locker.lockerKeyspace), LockerId(locker.lockerIdRawValue)) == null && database.count(LockerStoreImplDefinitionV1.storeName, IndexedQuery(LockerStoreImplDefinitionV1.roomIdKey.key, 1)) >= policy.maxCachedLockers)
                throw com.latenighthack.lockers.connector.ConnectorRetentionExceededException("Locker cache admission limit exceeded")
            save(locker)
            eventJournal.append(1, locker.toByteArray(), kotlin.random.Random.nextBytes(32))
        }
    }
    private val ratchetJournal = RatchetJournal(database)
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
