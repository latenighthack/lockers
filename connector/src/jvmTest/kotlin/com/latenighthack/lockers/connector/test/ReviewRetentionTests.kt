package com.latenighthack.lockers.connector.test

import com.latenighthack.lockers.connector.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.connector.internal.*
import com.latenighthack.lockers.connector.storage.v1.*
import com.latenighthack.lockers.common.v1.*
import kotlinx.coroutines.flow.*
import java.io.File
import kotlinx.coroutines.*
import kotlin.test.*

class ReviewRetentionTests {
    @Test fun `completed mutation identities are evicted`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val coordinator = LockerSyncCoordinator(scope)
        try {
            repeat(1_000) { coordinator.mutate(it) {} }
            val field = LockerSyncCoordinator::class.java.getDeclaredField("mutations").apply { isAccessible = true }
            assertEquals(0, (field.get(coordinator) as Map<*, *>).size)
        } finally { scope.cancel() }
    }
    @Test fun `explicit consumer watermark prunes pages and stale cursors fail`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db, ConnectorRetentionPolicy(maxAcceptedEvents = 1))
        fun event(id: Int) = Event(roomId = RoomId(byteArrayOf(1)), eventId = EventId(byteArrayOf(id.toByte())))
        session.receive(event(1)) { true }
        assertFailsWith<ConnectorRetentionExceededException> { session.receive(event(2)) { true } }
        assertFalse(session.hasReceived(StoredAck(byteArrayOf(1), byteArrayOf(2))))
        session.pruneEventsThrough(1)
        assertFailsWith<ConnectorCursorExpiredException> { session.eventsAfter(0).first() }
        session.receive(event(2)) { true }
        assertEquals(2, session.eventsAfter(1).first().cursor)
    }
    @Test fun `pending ACK pages exclude confirmed history and horizon pruning preserves pending`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db)
        repeat(130) { index ->
            val ack = StoredAck(byteArrayOf(1), byteArrayOf(index.toByte()))
            session.addAck(ack)
            if (index < 60) session.clearAck(ack)
        }
        val pages = session.pendingAckBatches().toList()
        assertEquals(listOf(64, 6), pages.map { it.size })
        assertEquals(70, pages.flatten().size)
        session.pruneConfirmedAcksBefore(Long.MAX_VALUE)
        assertFalse(session.hasReceived(StoredAck(byteArrayOf(1), byteArrayOf(0))))
        assertTrue(session.hasReceived(StoredAck(byteArrayOf(1), byteArrayOf(100))))
        assertEquals(70, session.getPendingAcks().size)
    }
    @Test fun `configured version three upgrades recovery schemas without changing old wire bytes`() = runBlocking {
        val file = File.createTempFile("connector-v3", ".db")
        val original = definitionDatabaseConfiguration(file.name, ConnectorStorage.definitionsV3)
        val bytes = byteArrayOf(0x0a, 1, 1, 0x12, 1, 2, 0xa0.toByte(), 6, 7)
        val legacy = createDatabase(original, file.absolutePath)
        try {
            legacy.open()
            legacy.transaction(setOf(SessionStoreImplDefinitionV1.storeName)) {
                val row = SessionStoreImplDefinitionV1.encodeRow(StoredAck.fromByteArray(bytes))
                save(SessionStoreImplDefinitionV1.storeName, StoreRow(bytes, row.keys))
            }
        } finally { legacy.close() }
        val migrated = createDatabase(ConnectorStorage.configuration(file.name), file.absolutePath)
        try {
            migrated.open()
            migrated.transaction(setOf(SessionStoreImplDefinitionV2.storeName), TransactionMode.READ_ONLY) {
                assertContentEquals(bytes, getAll(SessionStoreImplDefinitionV2.storeName).single() as ByteArray)
            }
            val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), migrated)
            assertEquals(1, session.getPendingAcks().size)
            session.clearAck(StoredAck(byteArrayOf(1), byteArrayOf(2)))
            migrated.transaction(setOf(SessionStoreImplDefinitionV2.storeName), TransactionMode.READ_ONLY) {
                val ack = StoredAck.fromByteArray(getAll(SessionStoreImplDefinitionV2.storeName).single() as ByteArray)
                assertContentEquals(byteArrayOf(0xa0.toByte(), 6, 7), ack.unknownFields)
            }
        } finally { migrated.close(); file.delete() }
    }
    @Test fun `cancelling a queued mutation preserves serialization for the remaining waiter`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val coordinator = LockerSyncCoordinator(scope)
        val held = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
        val first = launch { coordinator.mutate("key") { held.complete(Unit); release.await() } }
        held.await()
        val cancelled = launch { coordinator.mutate("key") { fail("cancelled waiter ran") } }
        yield(); cancelled.cancelAndJoin()
        var entered = false
        val last = launch { coordinator.mutate("key") { entered = true } }
        yield(); assertFalse(entered)
        release.complete(Unit); first.join(); last.join()
        assertTrue(entered)
        scope.cancel()
    }
}
