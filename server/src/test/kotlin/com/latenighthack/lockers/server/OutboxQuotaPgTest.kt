package com.latenighthack.lockers.server

import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.claim.PgTestGate
import com.latenighthack.lockers.server.services.room.v1.*
import kotlinx.coroutines.*
import java.sql.DriverManager
import kotlin.test.*

class OutboxQuotaPgTest {
    @Test fun `distinct room transactions share one physical global quota lock`(): Unit = runBlocking {
        val base = PgTestGate.urlOrSkip()
        val schema = "outbox_quota_${System.nanoTime()}"
        DriverManager.getConnection(base).use { it.createStatement().use { statement -> statement.execute("CREATE SCHEMA $schema") } }
        val url = base + (if ('?' in base) "&" else "?") + "currentSchema=$schema"
        val handles = mutableListOf<Database>()
        val release = CompletableDeferred<Unit>()
        try {
            suspend fun open(): DeliveryOutboxStore {
                val db = ServerStorage.postgres(url).also { handles.add(it) }
                val store = DeliveryOutboxStore(db, policy = OutboxPolicy(retainedPerRoom = 1, retainedGlobal = 1)).also { it.prepareStores() }
                db.open()
                return store
            }
            val a = open(); val b = open()
            val firstEntered = CompletableDeferred<Unit>(); val secondEntered = CompletableDeferred<Unit>()
            val firstRoom = RoomId(byteArrayOf(1)); val secondRoom = RoomId(byteArrayOf(2))
            val first = async(Dispatchers.IO) {
                a.commit(firstRoom, emptyList(), listOf(Event(roomId = firstRoom, eventId = EventId(byteArrayOf(1))))) {
                    firstEntered.complete(Unit); release.await()
                }
            }
            withTimeout(1000) { firstEntered.await() }
            val second = async(Dispatchers.IO) {
                runCatching { b.commit(secondRoom, emptyList(), listOf(Event(roomId = secondRoom, eventId = EventId(byteArrayOf(2))))) {
                    secondEntered.complete(Unit)
                } }
            }
            delay(100)
            assertFalse(secondEntered.isCompleted, "second physical transaction must wait on nested global lock")
            release.complete(Unit)
            first.await()
            assertIs<IllegalStateException>(second.await().exceptionOrNull())
            assertEquals(1, a.pendingCount())
            assertEquals(0L, b.watermark(secondRoom))
        } finally {
            release.complete(Unit)
            handles.forEach { it.close() }
            DriverManager.getConnection(base).use { it.createStatement().use { statement -> statement.execute("DROP SCHEMA $schema CASCADE") } }
        }
    }
}
