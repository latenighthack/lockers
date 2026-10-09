package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.common.InboxAdmission
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.session.v1.*
import com.latenighthack.lockers.server.storage.v1.toByteArray
import kotlinx.coroutines.*
import kotlin.test.*

class DeliveryFrameBoundsTest {
    private val room = RoomId(byteArrayOf(1))
    private fun event(id: Int, bytes: Int) = Event(roomId = room, eventId = EventId(ByteArray(32) { id.toByte() }),
        locker = IdentifiedLocker(LockerId(byteArrayOf(1)), Locker { open { encodedPayload = ByteArray(bytes) } }, 1))
    @Test fun `one large event splits its recipients into complete protocol sized frames`(): Unit = runBlocking {
        val db = ServerStorage.inMemory(); val store = DeliveryOutboxStore(db).also { it.prepareStores() }; db.open()
        val recipients = (0 until 1024).map { n -> SessionId(ByteArray(128) { 7 }.also { java.nio.ByteBuffer.wrap(it).putInt(n) }) }
        val accepted = mutableSetOf<List<Byte>>()
        var calls = 0
        val gateway = object: SessionGatewayService {
            override suspend fun postEvent(request: PostEventRequest): PostEventResponse {
                check(request.toByteArray().size <= ProtocolValidation.MAX_ENVELOPE_BYTES)
                check(request.sessionIds.size <= ProtocolValidation.MAX_RECIPIENTS)
                accepted.addAll(request.sessionIds.map { it.rawValue.toList() })
                return PostEventResponse()
            }
            override suspend fun postEvents(request: PostEventsRequest): PostEventsResponse {
                check(request.toByteArray().size <= ProtocolValidation.MAX_ENVELOPE_BYTES)
                calls++
                if (calls == 2) throw java.io.IOException("second recipient frame failed before acceptance")
                return PostEventsResponse(request.groups.map { postEvent(it) })
            }
        }
        val worker = DeliveryWorker(store, object: SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService = gateway
            override suspend fun resolveGroups(sessionIds: List<SessionId>) = listOf(SessionGatewayGroup(sessionIds, gateway))
        })
        try {
            store.commit(room, recipients, listOf(event(1, ProtocolValidation.MAX_ENVELOPE_BYTES - 64 * 1024))) {}
            worker.drainOnce()
            assertEquals(1, store.pendingCount())
            assertTrue(accepted.size in 1..1023)
            withTimeout(5000) { while (store.pendingCount() > 0) { delay(20); worker.drainOnce() } }
            assertEquals(0, store.pendingCount())
            assertEquals(1024, accepted.size)
            assertTrue(calls >= 2)
        } finally { worker.close(); db.close() }
    }
    @Test fun `gateway batches bound durable recipient expansion as well as wire bytes`(): Unit = runBlocking {
        val db = ServerStorage.inMemory(); val store = DeliveryOutboxStore(db).also { it.prepareStores() }; db.open()
        val recipients = (0 until 1024).map { n -> SessionId(java.nio.ByteBuffer.allocate(4).putInt(n).array()) }
        var accepted = 0; var calls = 0
        val gateway = object: SessionGatewayService {
            override suspend fun postEvent(request: PostEventRequest): PostEventResponse { accepted += request.sessionIds.size; return PostEventResponse() }
            override suspend fun postEvents(request: PostEventsRequest): PostEventsResponse {
                check(request.toByteArray().size <= ProtocolValidation.MAX_ENVELOPE_BYTES)
                val expansion = request.groups.sumOf { group ->
                    val eventBytes = group.event!!.toByteArray().size
                    group.sessionIds.sumOf { InboxAdmission.recipientExpansionBytes(eventBytes, it.toByteArray().size) }
                }
                check(expansion <= InboxAdmission.MAX_BATCH_EXPANSION_BYTES)
                calls++
                return PostEventsResponse(request.groups.map { postEvent(it) })
            }
        }
        val worker = DeliveryWorker(store, object: SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService = gateway
            override suspend fun resolveGroups(sessionIds: List<SessionId>) = listOf(SessionGatewayGroup(sessionIds, gateway))
        })
        try {
            store.commit(room, recipients, listOf(event(2, 1024 * 1024))) {}
            worker.drainOnce()
            assertEquals(0, store.pendingCount()); assertEquals(1024, accepted); assertTrue(calls > 1)
        } finally { worker.close(); db.close() }
    }

    @Test fun `claimed room prefixes obey a byte bound as well as an item count`(): Unit = runBlocking {
        val db = ServerStorage.inMemory(); val store = DeliveryOutboxStore(db).also { it.prepareStores() }; db.open()
        try {
            repeat(3) { store.commit(room, emptyList(), listOf(event(it + 1, 6 * 1024 * 1024))) {} }
            val claimed = store.claim("owner", 100, eventsPerRoom = 64)
            assertTrue(claimed.sumOf { it.toByteArray().size.toLong() } <= 16L * 1024 * 1024)
            assertEquals(listOf(1L, 2L), claimed.map { it.roomSequence })
            claimed.forEach { store.accepted(it, emptyList()) }
            assertEquals(3L, store.claim("owner", 101).single().roomSequence)
        } finally { db.close() }
    }
}
