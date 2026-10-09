package com.latenighthack.lockers.server

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class StorageFetchBudgetTest {
    private class Guard(private val backing: InMemoryStoreDelegate = InMemoryStoreDelegate()) :
        IndexedQueryDelegate by backing, ScopedStoreDelegate, LifecycleStoreDelegate {
        val fetched = mutableListOf<Pair<String, Int>>()
        override suspend fun query(tableName: String, query: IndexedQuery, identity: String, version: Int): QueryPage {
            if (tableName in setOf("lockers", "inbox")) {
                // A valid body may approach8MiB: more than4 records exceeds a32MiB raw fetch budget.
                check(query.limit <= 4) { "Storage fetch can retain more than32MiB of valid bodies" }
                fetched.add(tableName to query.limit)
            }
            return backing.query(tableName, query, identity, version)
        }
        override suspend fun <T> transaction(block: suspend () -> T): T = backing.transaction(block)
        override suspend fun <T> transaction(lockKey: String, block: suspend () -> T): T = backing.transaction(lockKey, block)
        override suspend fun <T> transaction(stores: Set<String>, mode: TransactionMode, block: suspend () -> T): T = backing.transaction(stores, mode, block)
        override suspend fun close() = backing.close()
        override suspend fun deleteDatabase() = backing.deleteDatabase()
    }
    @Test fun rawFetchBudgetDoesNotChangeReplayPageContractOrCompleteLockerOrder() = runBlocking {
        val guard = Guard(); val db = Database(ServerStorage.configuration("fetch-budget-${System.nanoTime()}"), guard); db.open()
        try {
            val room = ServerRoomId(byteArrayOf(1)); val sid = ServerSessionId(byteArrayOf(1))
            val lockers = LockerStoreImpl(db); val inbox = SessionInboxStoreImpl(db)
            lockers.updateLockers((1..150).map { ServerLocker(room, 0, ServerLockerId(byteArrayOf(it.toByte())), version = it.toLong()) })
            inbox.acceptClientEvents((1..150).map { n ->
                val event = Event(roomId = RoomId(room.rawValue), eventId = EventId(byteArrayOf(n.toByte())), roomSequence = n.toLong())
                ServerSessionEvent(sid, room, ServerEventId(event.eventId!!.rawValue), roomSequence = n.toLong()) to event
            })
            val stored = lockers.lockerPages(room).toList().flatten()
            assertEquals(150, stored.size); assertEquals(150, stored.map { it.version }.distinct().size)
            val emitted = inbox.clientEventPages(sid).toList()
            assertEquals(listOf(64, 64, 22), emitted.map { it.size })
            assertEquals((1L..150L).toList(), emitted.flatten().map { it.roomSequence })
            assertTrue(guard.fetched.any { it.first == "lockers" }); assertTrue(guard.fetched.any { it.first == "inbox" })
        } finally { db.close() }
    }
}
