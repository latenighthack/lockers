package com.latenighthack.lockers.connector.test
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.connector.internal.*
import com.latenighthack.lockers.connector.storage.v1.StoredLocker
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.test.*
class ReviewAcceptanceCursorTests {
    @Test fun `one consumer can advance through locker and session cursors before pruning`(): Unit = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        LockerStoreImpl(db).acceptLocker(StoredLocker(roomIdRawValue = byteArrayOf(1), lockerIdRawValue = byteArrayOf(2), version = 1))
        SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).receive(Event(roomId = RoomId(byteArrayOf()), eventId = EventId(byteArrayOf(3)))) { true }
        val client = reviewClient(ReviewRpc { _, _ -> error("network unused") }, db = db)
        try {
            val events = client.acceptedEventsAfter(0).take(2).toList()
            assertEquals(listOf(1L, 2L), events.map { it.cursor })
            assertTrue(events[0] is AcceptedConnectorEvent.LockerChanged)
            assertTrue(events[1] is AcceptedConnectorEvent.SessionEvent)
            client.pruneAcceptedEventsThrough(events.last().cursor)
            assertFailsWith<ConnectorCursorExpiredException> { client.acceptedEventsAfter(0).first() }
        } finally { client.closeAndJoin() }
    }
    @Test fun `a failed notification decoder preserves ACK and recoverable typed cursor`(): Unit = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db)
        val event = Event(roomId = RoomId(byteArrayOf(1)), eventId = EventId(byteArrayOf(2)), locker = IdentifiedLocker(lockerId = LockerId(byteArrayOf(3))),
            notification = com.latenighthack.lockers.common.v1.Notification(payload = Payload(byteArrayOf(4))))
        session.receive(event) { true }
        val bad = reviewClient(ReviewRpc { _, _ -> error("network unused") }, db = db, codecs = NotificationCodecs.of(object : NotificationCodec {
            override suspend fun decode(context: NotificationContext, payload: ByteArray): ByteArray? = error("key unavailable")
        }))
        try { assertFailsWith<IllegalStateException> { bad.notificationsAfter(0).first() } } finally { bad.closeAndJoin() }
        assertTrue(session.hasReceived(com.latenighthack.lockers.connector.storage.v1.StoredAck(byteArrayOf(1), byteArrayOf(2))))
        val recovered = reviewClient(ReviewRpc { _, _ -> error("network unused") }, db = db)
        try {
            val accepted = recovered.notificationsAfter(0).first()
            assertEquals(1, accepted.cursor)
            assertContentEquals(byteArrayOf(4), accepted.notification.payload)
        } finally { recovered.closeAndJoin() }
    }
}
