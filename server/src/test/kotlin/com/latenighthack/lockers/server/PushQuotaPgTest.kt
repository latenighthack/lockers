package com.latenighthack.lockers.server

import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.server.claim.PgTestGate
import com.latenighthack.lockers.server.services.push.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.*
import java.sql.DriverManager
import kotlin.test.*

class PushQuotaPgTest {
    @Test fun `two independent handles cannot exceed retained queue capacity`(): Unit = runBlocking {
        val base = PgTestGate.urlOrSkip()
        val schema = "push_quota_${System.nanoTime()}"
        DriverManager.getConnection(base).use { it.createStatement().use { s -> s.execute("CREATE SCHEMA $schema") } }
        val handles = mutableListOf<Database>()
        try {
            val url = base + (if ('?' in base) "&" else "?") + "currentSchema=$schema"
            suspend fun open(): PushQueueStoreImpl {
                val db = ServerStorage.postgres(url).also { handles.add(it) }
                val queue = PushQueueStoreImpl(db, PushRetentionPolicy(retainedGlobal = 1)).also { it.prepare() }
                db.open(); return queue
            }
            val a = open(); val b = open()
            val start = CompletableDeferred<Unit>()
            val attempts = listOf(a, b).mapIndexed { index, queue -> async(Dispatchers.IO) {
                start.await()
                runCatching { queue.enqueue(ServerPush(pushId = ServerPushId(byteArrayOf(index.toByte())), sessionId = ServerSessionId(byteArrayOf(index.toByte())), backend = 1), true) }
            } }
            start.complete(Unit)
            val outcomes = attempts.awaitAll()
            assertEquals(1, outcomes.count { it.isSuccess })
            assertIs<IllegalStateException>(outcomes.single { it.isFailure }.exceptionOrNull())
            assertEquals(1L, a.pendingCounts().values.sum())
            val lease = b.claim(1, "worker", System.currentTimeMillis(), 1).single()
            assertTrue(a.finish(lease))
            assertFalse(b.finish(lease))
            assertEquals(0L, a.pendingCounts().values.sum())
        } finally {
            handles.forEach { it.close() }
            DriverManager.getConnection(base).use { it.createStatement().use { s -> s.execute("DROP SCHEMA $schema CASCADE") } }
        }
    }
}
