package com.latenighthack.lockers.server

import com.latenighthack.lockers.server.services.push.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class PushRetentionTest {
    private fun push(id: Int) = ServerPush(pushId = ServerPushId(byteArrayOf(id.toByte())), sessionId = ServerSessionId(byteArrayOf(1)), backend = 1)
    @Test fun `completion expires after retry horizon and parked work remains durable`(): Unit = runBlocking {
        var now = 1L
        val db = ServerStorage.inMemory()
        val queue = PushQueueStoreImpl(db, PushRetentionPolicy(10, 20, 3), { now }).also { it.prepare() }
        val dead = PushDeadLetterStoreImpl(db).also { it.prepare() }
        db.open()
        try {
            assertTrue(queue.enqueue(push(1), true))
            assertTrue(queue.finish(queue.claim(1, "worker", now, 1).single()))
            assertFalse(queue.enqueue(push(1), true))
            assertTrue(queue.enqueue(push(2), true))
            now = 11
            assertTrue(queue.claim(1, "worker", now, 1).isEmpty())
            assertEquals(1L, dead.counts()[1])
            assertFalse(queue.enqueue(push(2), true))
            assertFailsWith<IllegalStateException> { queue.savePush(push(2)) }
            now = 22
            queue.maintain(now)
            assertTrue(queue.enqueue(push(1), true)) // Old completed identity is now outside its documented horizon.
            assertEquals(1L, dead.counts()[1]) // Pending/parked work is never silently removed by age.
            queue.clearForSession(ServerSessionId(byteArrayOf(1)))
            assertEquals(0L, queue.pendingCounts().values.sum())
            assertEquals(0L, dead.counts().values.sum())
        } finally { db.close() }
    }
    @Test fun `global retained capacity includes parked and completion records`(): Unit = runBlocking {
        val db = ServerStorage.inMemory()
        val queue = PushQueueStoreImpl(db, PushRetentionPolicy(retainedGlobal = 1)).also { it.prepare() }
        db.open()
        try {
            queue.enqueue(push(1), true)
            queue.finish(queue.claim(1, "worker", System.currentTimeMillis(), 1).single(), "operator reconciliation")
            assertFailsWith<IllegalStateException> { queue.enqueue(push(2), true) }
            assertTrue(queue.pendingCounts().values.all { it == 0L })
        } finally { db.close() }
    }
}
