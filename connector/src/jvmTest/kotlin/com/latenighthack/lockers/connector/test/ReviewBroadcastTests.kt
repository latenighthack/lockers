package com.latenighthack.lockers.connector.test

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.test.*

class ReviewBroadcastTests {
    @Test fun `lockerless broadcasts survive acceptance without a collector and replacement`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db)
        val event = Event(roomId = RoomId(byteArrayOf(1)), eventId = EventId(byteArrayOf(2)),
            notification = Notification(payload = Payload(byteArrayOf(3)), push = Push(body = "body")))
        session.receive(event) { true }
        val client = reviewClient(ReviewRpc { _, _ -> error("broadcast replay must not use the network") }, db = db, broadcastCodecs = BroadcastCodecs.of(object : BroadcastCodec {
            override suspend fun decode(context: BroadcastContext, payload: ByteArray): ByteArray {
                assertEquals("", context.title); assertEquals("body", context.body)
                return payload + byteArrayOf(4)
            }
        }))
        try {
            val broadcast = withTimeout(2_000) { client.broadcastsAfter(0).first() }
            assertContentEquals(byteArrayOf(3, 4), broadcast.payload)
            assertEquals("", broadcast.context.title)
            assertEquals("body", broadcast.context.body)
            assertEquals(event.eventId, broadcast.context.eventId)
        } finally { client.closeAndJoin() }
    }
}
