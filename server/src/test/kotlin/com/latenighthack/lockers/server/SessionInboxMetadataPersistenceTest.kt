package com.latenighthack.lockers.server

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.*

class SessionInboxMetadataPersistenceTest {
    @Test fun fullEventSurvivesReopenAndAckAndRevocationClearBothRecords() = runBlocking {
        val file = File.createTempFile("lockers-inbox-metadata", ".db")
        val config = ServerStorage.configuration("metadata-reopen-${file.name}")
        val sid = ServerSessionId(byteArrayOf(7))
        fun row(id: Byte) = ServerSessionEvent(sessionId = sid, eventId = ServerEventId(byteArrayOf(id)),
            roomId = ServerRoomId(byteArrayOf(9)), encodedPayload = byteArrayOf(3), roomSequence = id.toLong())
        fun event(id: Byte) = Event(eventId = EventId(byteArrayOf(id)), roomId = RoomId(byteArrayOf(9)),
            notification = Notification { push { body = "Only body" }; payload { rawValue = byteArrayOf(3) } },
            unknownFields = byteArrayOf(0xA0.toByte(), 0x06, 0x01))
        var db = createDatabase(config, file.absolutePath)
        db.open()
        try {
            var inbox = SessionInboxStoreImpl(db)
            inbox.saveClientEvents(listOf(row(1) to event(1), row(2) to event(2)))
            db.close()
            db = createDatabase(config, file.absolutePath); db.open()
            inbox = SessionInboxStoreImpl(db)
            assertContentEquals(event(1).toByteArray(), inbox.clientEvent(row(1)).toByteArray())
            inbox.deleteEvent(row(1).eventId!!, sid)
            assertEquals(1, inbox.getAllEvents(sid).size)
            inbox.deleteAllEvents(sid)
            assertTrue(inbox.getAllEvents(sid).isEmpty())
            val definition = SessionInboxMetadataDefinitionV2
            assertEquals(0, db.count(definition.storeName, IndexedQuery(definition.declaration.keys.first(), 1)))
            // Pre-upgrade rows cannot recover metadata which V1 never stored.
            inbox.saveEvent(row(3))
            assertNull(inbox.clientEvent(row(3)).notification?.push)
        } finally { db.close(); file.delete() }
    }
}
