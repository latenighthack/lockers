package com.latenighthack.lockers.server

import com.latenighthack.lockers.server.services.push.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class PushQueueClaimsTest {
    private suspend fun queue(): PushQueueStoreImpl {
        val db = ServerStorage.inMemory()
        return PushQueueStoreImpl(db).also { it.prepare(); db.open() }
    }
    private fun push(id: Int, session: Int = 1) = ServerPush(
        pushId = ServerPushId(java.nio.ByteBuffer.allocate(4).putInt(id).array()),
        sessionId = ServerSessionId(byteArrayOf(session.toByte())), backend = 1,
        encodedPush = byteArrayOf(id.toByte()),
    )
    @Test fun `expired claim cannot complete or renew a replacement lease`(): Unit = runBlocking {
        val queue = queue()
        queue.enqueue(push(1), true)
        val old = queue.claim(1, "old", 100, 1, 10).single()
        assertTrue(queue.claim(1, "new", 109, 1, 10).isEmpty())
        val current = queue.claim(1, "new", 111, 1, 10).single()
        assertFalse(queue.finish(old))
        assertFalse(queue.renew(old, 112))
        assertTrue(queue.finish(current))
        assertFalse(queue.enqueue(push(1), true))
        assertFailsWith<IllegalArgumentException> { queue.enqueue(push(1).copy(encodedPush = byteArrayOf(99)), true) }
    }
    @Test fun `session cleanup deletes multiple bounded pages and preserves other sessions`(): Unit = runBlocking {
        val queue = queue()
        repeat(260) { queue.savePush(push(it)) }
        queue.savePush(push(999, 2))
        queue.clearForSession(ServerSessionId(byteArrayOf(1)))
        assertEquals(listOf(push(999, 2)), queue.getPendingPushes())
    }
    @Test fun `session quota counts pending deliveries across backends`(): Unit = runBlocking {
        val queue = queue()
        queue.enqueue(push(1), true, 1)
        assertFailsWith<IllegalStateException> { queue.enqueue(push(2).copy(backend = 2), true, 1) }
        assertEquals(1, queue.getPendingPushes().size)
    }
}
