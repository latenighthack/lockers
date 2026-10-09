package com.latenighthack.lockers.server

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.claim.PgTestGate
import com.latenighthack.lockers.server.services.room.v1.DeliveryOutboxStore
import kotlinx.coroutines.*
import java.sql.DriverManager
import kotlin.test.*

class DeliveryOutboxPgTest {
    @Test fun twoConnectionsClaimExclusivelyAndRollbackPersistsNothing() = runBlocking {
        val base = PgTestGate.urlOrSkip()
        val schema = "delivery_test_${System.nanoTime()}"
        DriverManager.getConnection(base).use { it.createStatement().use { statement -> statement.execute("CREATE SCHEMA $schema") } }
        val url = base + (if ('?' in base) "&" else "?") + "currentSchema=$schema"
        val handles = mutableListOf<Database>()
        try {
            suspend fun store(): Pair<Database, DeliveryOutboxStore> {
                val db = ServerStorage.postgres(url).also { handles.add(it) }
                val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
                db.open()
                return db to outbox
            }
            val (_, a) = store(); val (_, b) = store()
            val room = RoomId(byteArrayOf(1))
            val events = (1..3).map { Event(roomId = room, eventId = EventId(byteArrayOf(it.toByte()))) }
            assertFailsWith<IllegalStateException> { a.commit(room, emptyList(), events) { error("rollback") } }
            assertEquals(0, b.pendingCount())
            a.commit(room, listOf(SessionId(byteArrayOf(1))), events) { }
            val claims = coroutineScope {
                listOf(async(Dispatchers.IO) { a.claim("a", 100) }, async(Dispatchers.IO) { b.claim("b", 100) }).awaitAll().flatten()
            }
            assertEquals(1, claims.size)
            val claim = claims.single()
            assertEquals(1L, claim.roomSequence)
            b.accepted(claim, listOf(SessionId(byteArrayOf(1))))
            assertEquals(2L, a.claim("c", 101).single().roomSequence)
        } finally {
            handles.forEach { it.close() }
            DriverManager.getConnection(base).use { it.createStatement().use { statement -> statement.execute("DROP SCHEMA $schema CASCADE") } }
        }
    }
}
