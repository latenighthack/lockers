package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class InboxReplayPagingTest {
    @Test fun replayIsBoundedFiniteAndDoesNotIncludeOtherSessionsWhileAckAdvancesCursor() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val inbox = SessionInboxStoreImpl(db)
        val sid = ServerSessionId(byteArrayOf(1, -1)); val other = ServerSessionId(byteArrayOf(2, 0))
        fun pair(session: ServerSessionId, sequence: Long): Pair<ServerSessionEvent, Event> {
            val id = byteArrayOf((sequence ushr 8).toByte(), sequence.toByte())
            val event = Event(eventId = EventId(id), roomId = RoomId(byteArrayOf(9)), roomSequence = sequence)
            return ServerSessionEvent(sessionId = session, eventId = ServerEventId(id), roomId = ServerRoomId(byteArrayOf(9)), roomSequence = sequence) to event
        }
        try {
            inbox.acceptClientEvents((1L..150L).map { pair(sid, it) } + listOf(pair(other, 900)))
            val delivered = mutableListOf<Long>(); var pages = 0
            inbox.clientEventPages(sid).collect { page ->
                assertTrue(page.size in 1..64)
                pages++; delivered.addAll(page.map { it.roomSequence })
                // Every emission must release the DB owner: these writes and ACKs
                // would deadlock or invalidate the transaction if emission retained it.
                if (pages == 1) {
                    inbox.deleteEvents(page.map { ServerEventId(it.eventId!!.rawValue) }, sid)
                    inbox.acceptClientEvents(listOf(pair(sid, 1000)))
                }
            }
            assertEquals(3, pages)
            assertEquals((1L..150L).toList(), delivered)
            assertEquals(1000, inbox.getAllClientEvents(sid).maxOf { it.roomSequence })
        } finally { db.close() }
    }
    @Test fun replayAlsoSplitsEncodedBytesBelowTransportEnvelope() = runBlocking {
        val db = ServerStorage.inMemory(); db.open(); val inbox = SessionInboxStoreImpl(db)
        val sid = ServerSessionId(byteArrayOf(7))
        try {
            val events = (1..12).map { n ->
                val event = Event(eventId = EventId(byteArrayOf(n.toByte())), roomId = RoomId(byteArrayOf(9)),
                    notification = Notification { payload { rawValue = ByteArray(1024 * 1024) } })
                ServerSessionEvent(sessionId = sid, eventId = ServerEventId(byteArrayOf(n.toByte())), roomId = ServerRoomId(byteArrayOf(9))) to event
            }
            inbox.acceptClientEvents(events)
            val sizes = mutableListOf<Int>(); var count = 0
            inbox.clientEventPages(sid).collect { page ->
                sizes.add(page.sumOf { it.toByteArray().size }); count += page.size
            }
            assertEquals(12, count); assertEquals(2, sizes.size)
            assertTrue(sizes.all { it < ProtocolValidation.MAX_ENVELOPE_BYTES - 1024 })
        } finally { db.close() }
    }
}
