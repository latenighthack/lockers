package com.latenighthack.lockers.server

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import java.io.File
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class InboxReceiptPersistenceTest {
    @Test fun ackReceiptsSurviveSqliteReopenAndCapacityCannotDeleteUnexpiredHistory() = runBlocking {
        val file = File.createTempFile("lockers-inbox-receipt", ".db")
        val config = ServerStorage.configuration("receipt-reopen-${file.name}")
        val limits = ServerResourceLimits(maxInboxReceipts = 1, maxInboxReceiptsPerSession = 1)
        var now = 1_000_000L
        val sid = ServerSessionId(byteArrayOf(1))
        fun pair(id: Byte): Pair<ServerSessionEvent, Event> {
            val event = Event(roomId = RoomId(byteArrayOf(9)), eventId = EventId(byteArrayOf(id)),
                notification = Notification { push { body = "Metadata" } })
            return ServerSessionEvent(sessionId = sid, roomId = ServerRoomId(byteArrayOf(9)), eventId = ServerEventId(byteArrayOf(id))) to event
        }
        var db = createDatabase(config, file.absolutePath); db.open()
        try {
            var inbox = SessionInboxStoreImpl(db, limits) { now }
            assertEquals(1, inbox.acceptClientEvents(listOf(pair(1))).size)
            assertTrue(inbox.acceptClientEvents(listOf(pair(1))).isEmpty())
            inbox.deleteEvent(ServerEventId(byteArrayOf(1)), sid)
            db.close(); db = createDatabase(config, file.absolutePath); db.open()
            inbox = SessionInboxStoreImpl(db, limits) { now }
            assertTrue(inbox.acceptClientEvents(listOf(pair(1))).isEmpty())
            assertFailsWith<ResourceLimitException> { inbox.acceptClientEvents(listOf(pair(2))) }
            assertTrue(inbox.getAllEvents(sid).isEmpty())
            now += INBOX_RECEIPT_RETENTION_MILLIS
            assertTrue(inbox.acceptClientEvents(listOf(pair(1))).isEmpty())
            assertFailsWith<ResourceLimitException> { inbox.acceptClientEvents(listOf(pair(2))) }
            now++
            assertEquals(1, inbox.acceptClientEvents(listOf(pair(2))).size)
            assertEquals("Metadata", inbox.getAllClientEvents(sid).single().notification?.push?.body)
        } finally { db.close(); file.delete() }
    }
}
