package com.latenighthack.lockers.server

import com.latenighthack.ktstore.*
import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import java.io.File
import kotlinx.coroutines.*
import kotlin.test.*

class InboxByteAdmissionTest {
    private fun pair(id: Int, session: Int = 1, size: Int = 1024): Pair<ServerSessionEvent, Event> {
        val event = Event(eventId = EventId(byteArrayOf(id.toByte())), roomId = RoomId(byteArrayOf(9)), roomSequence = id.toLong(),
            notification = Notification { payload { rawValue = ByteArray(size) { 7 } } })
        return ServerSessionEvent(ServerSessionId(byteArrayOf(session.toByte())), ServerRoomId(byteArrayOf(9)),
            ServerEventId(byteArrayOf(id.toByte())), event.notification!!.payload!!.rawValue, roomSequence = id.toLong()) to event
    }
    private fun bytes(pair: Pair<ServerSessionEvent, Event>): Long {
        val row = pair.first.copy(enqueuedAt = 100)
        return row.toByteArray().size.toLong() + row.copy(encodedPayload = pair.second.toByteArray(),
            encodedLocker = byteArrayOf()).toByteArray().size
    }
    private class RawInbox(db: Database) : Store<ServerSessionEvent>(db, SessionInboxStoreDefinitionV3) {
        suspend fun insert(rows: List<ServerSessionEvent>) = saveAll(rows)
    }
    @Test fun exactRowAccountingIncludesUnknownBytesAndNegativeSequences() {
        for (sequence in listOf(0L, 1L, 128L, -1L, Long.MAX_VALUE)) {
            val row = pair(1).first.copy(roomSequence = sequence, unknownFields = byteArrayOf(0x78, 0x01))
            assertEquals(row.toByteArray().size.toLong(), inboxRowBytes(row))
        }
    }
    @Test fun inMemoryAcceptanceAndReadResultsCannotMutateDurablePayloadOrIndexes() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        try {
            val inbox = SessionInboxStoreImpl(db)
            val sid = ServerSessionId(byteArrayOf(1))
            val accepted = inbox.acceptClientEvents(listOf(pair(1))).single()
            accepted.first.encodedPayload.fill(9); accepted.first.eventId!!.rawValue.fill(9)
            accepted.second.notification!!.payload!!.rawValue.fill(8)
            val loaded = inbox.getAllEvents(sid).single()
            assertEquals(7.toByte(), loaded.encodedPayload.first())
            assertContentEquals(byteArrayOf(1), loaded.eventId!!.rawValue)
            loaded.encodedPayload.fill(3); loaded.sessionId!!.rawValue.fill(3)
            assertEquals(7.toByte(), inbox.getAllEvents(sid).single().encodedPayload.first())
            assertEquals(7.toByte(), inbox.getAllClientEvents(sid).single().notification!!.payload!!.rawValue.first())
            assertTrue(inbox.acceptClientEvents(listOf(pair(1))).isEmpty())
        } finally { db.close() }
    }
    @Test fun quotaAndAckDestroyAreAtomicAndSurviveReopen() = runBlocking {
        val file = File.createTempFile("inbox-bytes", ".db")
        val configuration = ServerStorage.configuration(file.name)
        val cost = bytes(pair(1)); val limits = ServerResourceLimits(maxInboxBytes = cost * 2, maxInboxBytesPerSession = cost)
        var db = createDatabase(configuration, file.absolutePath); db.open()
        try {
            var inbox = SessionInboxStoreImpl(db, limits, clock = { 100 })
            inbox.acceptClientEvents(listOf(pair(1)))
            assertFailsWith<ResourceLimitException> { inbox.acceptClientEvents(listOf(pair(2))) }
            inbox.acceptClientEvents(listOf(pair(2, 2)))
            assertFailsWith<ResourceLimitException> { inbox.acceptClientEvents(listOf(pair(3, 3))) }
            db.close(); db = createDatabase(configuration, file.absolutePath); db.open(); inbox =
                SessionInboxStoreImpl(db, limits, clock = { 100 })
            assertFailsWith<ResourceLimitException> { inbox.acceptClientEvents(listOf(pair(3, 3))) }
            inbox.deleteEvent(ServerEventId(byteArrayOf(1)), ServerSessionId(byteArrayOf(1)))
            inbox.acceptClientEvents(listOf(pair(3, 3)))
            inbox.deleteAllEvents(ServerSessionId(byteArrayOf(2)))
            inbox.acceptClientEvents(listOf(pair(4, 4)))
            assertTrue(inbox.acceptClientEvents(listOf(pair(1))).isEmpty()) // retained ACK receipt
            assertFailsWith<ResourceLimitException> { inbox.acceptClientEvents(listOf(pair(5, 4), pair(6, 5))) }
            assertTrue(inbox.getAllEvents(ServerSessionId(byteArrayOf(5))).isEmpty())
        } finally { db.close(); file.delete() }
    }
    @Test fun legacyBackfillIsBoundedResumableAndAckDuringBackfillDoesNotUnderflow() = runBlocking {
        val file = File.createTempFile("inbox-byte-backfill", ".db"); val configuration = ServerStorage.configuration(file.name)
        var db = createDatabase(configuration, file.absolutePath); db.open()
        val cost = bytes(pair(1)); val limits = ServerResourceLimits(maxInboxBytes = cost * 3, maxInboxBytesPerSession = cost * 3)
        try {
            RawInbox(db).insert((1..9).map { pair(it).first })
            var inbox = SessionInboxStoreImpl(db, limits, clock = { 100 })
            assertFalse(inbox.backfillByteLedger()) // exactly four rows
            inbox.deleteEvent(ServerEventId(byteArrayOf(1)), ServerSessionId(byteArrayOf(1))) // charged
            inbox.deleteEvent(ServerEventId(byteArrayOf(9)), ServerSessionId(byteArrayOf(1))) // not charged yet
            db.close(); db = createDatabase(configuration, file.absolutePath); db.open(); inbox =
                SessionInboxStoreImpl(db, limits, clock = { 100 })
            inbox.initializeAdmission()
            assertFailsWith<ResourceLimitException> { inbox.acceptClientEvents(listOf(pair(10))) }
            inbox.deleteAllEvents(ServerSessionId(byteArrayOf(1)))
            inbox.acceptClientEvents(listOf(pair(10)))
            assertEquals(1, inbox.getAllEvents(ServerSessionId(byteArrayOf(1))).size)
        } finally { db.close(); file.delete() }
    }
    @Test fun independentPostgresHandlesShareTheTransactionalByteBudget() = runBlocking {
        val base = com.latenighthack.lockers.server.claim.PgTestGate.urlOrSkip()
        val schema = "inbox_bytes_${System.nanoTime()}"
        java.sql.DriverManager.getConnection(base).use { it.createStatement().use { statement -> statement.execute("CREATE SCHEMA $schema") } }
        val location = base + (if (base.contains('?')) "&" else "?") + "currentSchema=$schema"
        val a = ServerStorage.postgres(location); val b = ServerStorage.postgres(location)
        val cost = bytes(pair(1)); val limits = ServerResourceLimits(maxInboxBytes = cost * 3, maxInboxBytesPerSession = cost)
        try {
            a.open(); b.open(); val stores = listOf(SessionInboxStoreImpl(a, limits, clock = { 100 }),
                SessionInboxStoreImpl(b, limits, clock = { 100 }))
            val results = coroutineScope { (1..12).map { n -> async(Dispatchers.Default) {
                try { stores[n % 2].acceptClientEvents(listOf(pair(n, n))); true }
                catch (_: ResourceLimitException) { false }
            } }.awaitAll() }
            assertEquals(3, results.count { it })
            val rows = (1..12).flatMap { stores[0].getAllEvents(ServerSessionId(byteArrayOf(it.toByte()))) }
            assertEquals(3, rows.size)
            stores[0].deleteAllEvents(requireNotNull(rows.first().sessionId))
            assertEquals(1, stores[1].acceptClientEvents(listOf(pair(99, 99))).size)
        } finally {
            a.close(); b.close()
            java.sql.DriverManager.getConnection(base).use { it.createStatement().use { statement -> statement.execute("DROP SCHEMA $schema CASCADE") } }
        }
    }
    @Test fun smallWireFanoutCannotAllocateUnboundedDurableRows() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        try {
            val event = pair(1, size = 1024 * 1024).second
            val offered = (1..40).map { pair(1, it, 0).first to event }
            val rejected = assertFailsWith<RpcResponseException> { SessionInboxStoreImpl(db).acceptClientEvents(offered) }
            assertEquals(Codes.OUT_OF_RANGE, rejected.code)
            assertTrue(SessionInboxStoreImpl(db).getAllEvents(ServerSessionId(byteArrayOf(1))).isEmpty())
        } finally { db.close() }
    }
}
