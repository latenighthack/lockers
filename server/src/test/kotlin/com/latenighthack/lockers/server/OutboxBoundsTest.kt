package com.latenighthack.lockers.server

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class OutboxBoundsTest {
    private class SequenceSeed(db: Database): Store<ServerRoomSequence>(db, RoomSequencesDefinitionV1("delivery")) {
        suspend fun set(row: ServerRoomSequence) = save(row)
    }
    private val room = RoomId(byteArrayOf(1))
    private fun event(id: Int) = Event(roomId = room, eventId = EventId(java.nio.ByteBuffer.allocate(4).putInt(id).array()))

    @Test fun `sequence exhaustion rolls back source writes and never wraps`(): Unit = runBlocking {
        val db = ServerStorage.inMemory()
        val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
        val seed = SequenceSeed(db).also { it.prepare() }
        val lockers = LockerStoreImpl(db).also { it.prepare() }
        db.open()
        seed.set(ServerRoomSequence(ServerRoomId(room.rawValue), Long.MAX_VALUE))
        assertFailsWith<IllegalStateException> {
            outbox.commit(room, emptyList(), listOf(event(1))) {
                lockers.updateLocker(ServerLocker(roomId = ServerRoomId(room.rawValue), lockerId = ServerLockerId(byteArrayOf(1)), version = 1))
            }
        }
        assertEquals(Long.MAX_VALUE, outbox.watermark(room))
        assertEquals(0, outbox.pendingCount())
        assertTrue(lockers.getAllLockers(ServerRoomId(room.rawValue)).isEmpty())
    }

    @Test fun `claim discovers a bounded indexed room prefix without full outbox reads`(): Unit = runBlocking {
        val memory = FencedMemoryDelegate(InMemoryStoreDelegate())
        val bounded = object: LifecycleStoreDelegate by memory, ScopedStoreDelegate, IndexedQueryDelegate {
            override val supportsTransactions get() = memory.supportsTransactions
            override suspend fun <T> transaction(block: suspend () -> T) = memory.transaction(block)
            override suspend fun <T> transaction(lockKey: String, block: suspend () -> T) = memory.transaction(lockKey, block)
            override suspend fun <T> transaction(stores: Set<String>, mode: TransactionMode, block: suspend () -> T) = memory.transaction(stores, mode, block)
            override suspend fun query(tableName: String, query: IndexedQuery, identity: String, version: Int) = memory.query(tableName, query, identity, version)
            override suspend fun count(tableName: String, query: IndexedQuery) = memory.count(tableName, query)
            override suspend fun deleteBatch(tableName: String, query: IndexedQuery, identity: String, version: Int) = memory.deleteBatch(tableName, query, identity, version)
            override suspend fun getAll(tableName: String, relation: StoreRelation?): List<Any> {
                check(tableName != "delivery_outbox") { "outbox discovery performed an unbounded read" }
                return memory.getAll(tableName, relation)
            }
        }
        val db = Database(ServerStorage.configuration("outbox-bounded-${java.util.UUID.randomUUID()}"), bounded)
        val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
        db.open()
        outbox.commit(room, emptyList(), (1..300).map(::event)) {}
        val claimed = outbox.claim("owner", 100, limit = 1, eventsPerRoom = 64)
        assertEquals(64, claimed.size)
        assertEquals((1L..64L).toList(), claimed.map { it.roomSequence })
    }
    @Test fun `expired intent is parked durably and does not block a newer room prefix`(): Unit = runBlocking {
        var now = 1L
        val db = ServerStorage.inMemory()
        val outbox = DeliveryOutboxStore(db, policy = OutboxPolicy(retryWindowMs = 10, completedRetentionMs = 20), clock = { now }).also { it.prepareStores() }
        db.open()
        val session = SessionId(byteArrayOf(9))
        outbox.commit(room, listOf(session), listOf(event(1))) {}
        now = 9
        outbox.commit(room, listOf(session), listOf(event(2))) {}
        val claim = outbox.claim("owner", 11, eventsPerRoom = 64).single()
        assertEquals(2L, claim.roomSequence)
        val parked = outbox.parked().single()
        assertEquals(1L, parked.sequence)
        assertTrue(parked.parkedReason.isNotEmpty())
        assertEquals(listOf(ServerSessionId(session.rawValue)), ServerDeliveryIntent.fromByteArray(parked.parkedIntent).pendingSessions)
        now = 100
        assertEquals(1, outbox.parked().size) // Parked records are never age-pruned.
        assertTrue(outbox.resolveParked(parked.eventId))
        assertTrue(outbox.parked().isEmpty())
    }

    @Test fun `global capacity spans rooms and failure rolls back source mutation`(): Unit = runBlocking {
        val db = ServerStorage.inMemory()
        val outbox = DeliveryOutboxStore(db, policy = OutboxPolicy(retainedPerRoom = 1, retainedGlobal = 1)).also { it.prepareStores() }
        val lockers = LockerStoreImpl(db).also { it.prepare() }
        db.open()
        outbox.commit(room, emptyList(), listOf(event(1))) {}
        val other = RoomId(byteArrayOf(2))
        assertFailsWith<IllegalStateException> {
            outbox.commit(other, emptyList(), listOf(event(2).copy(roomId = other))) {
                lockers.updateLocker(ServerLocker(roomId = ServerRoomId(other.rawValue), lockerId = ServerLockerId(byteArrayOf(1)), version = 1))
            }
        }
        assertEquals(1, outbox.pendingCount())
        assertTrue(lockers.getAllLockers(ServerRoomId(other.rawValue)).isEmpty())
        assertEquals(0L, outbox.watermark(other))
    }

}
