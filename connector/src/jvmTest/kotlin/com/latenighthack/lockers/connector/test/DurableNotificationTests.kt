package com.latenighthack.lockers.connector.test

import com.latenighthack.ktstore.InMemoryKeyValueStoreDelegate
import com.latenighthack.ktstore.KeyValueStore
import com.latenighthack.lockers.common.v1.Event
import com.latenighthack.lockers.common.v1.EventId
import com.latenighthack.lockers.common.v1.IdentifiedLocker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.Notification
import com.latenighthack.lockers.common.v1.Payload
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.connector.ConnectorCursorExpiredException
import com.latenighthack.lockers.connector.ConnectorStorage
import com.latenighthack.lockers.connector.LockerClient
import com.latenighthack.lockers.connector.SessionStoreImpl
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.flow.first
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** Durable consumers resume from a saved cursor; the journal is pruned behind every reader. */
class DurableNotificationTests {
    private val offline = ReviewRpc { _, _ -> error("network unused") }

    private fun notification(id: Int) = Event(
        roomId = RoomId(byteArrayOf(1)),
        eventId = EventId(byteArrayOf(id.toByte(), (id shr 8).toByte())),
        locker = IdentifiedLocker(lockerId = LockerId(byteArrayOf(3))),
        notification = Notification(payload = Payload(byteArrayOf(id.toByte()))),
    )

    private suspend fun SessionStoreImpl.accept(vararg ids: Int) = ids.forEach { receive(notification(it)) { true } }

    private suspend fun LockerClient.collectDurable(consumer: String, count: Int): List<Int> = coroutineScope {
        val received = mutableListOf<Int>()
        val done = CompletableDeferred<Unit>()
        val job = launch {
            durableNotifications(consumer).collect {
                received += it.payload.single().toInt() and 0xFF
                if (received.size == count) done.complete(Unit)
            }
        }
        withTimeout(10_000) { done.await() }
        delay(200)
        job.cancelAndJoin()
        received
    }

    @Test fun `a durable consumer resumes after what it handled and sees what arrived meanwhile`(): Unit = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db)
        session.accept(1, 2)
        val first = reviewClient(offline, db = db)
        try { assertEquals(listOf(1, 2), first.collectDurable("messages", 2)) } finally { first.closeAndJoin() }
        session.accept(3)
        val second = reviewClient(offline, db = db)
        try { assertEquals(listOf(3), second.collectDurable("messages", 1)) } finally { second.closeAndJoin() }
    }

    @Test fun `catching up prunes the journal, but never past another consumer's saved cursor`(): Unit = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db)
        val client = reviewClient(offline, db = db)
        try {
            // "pinned" registers first and stops after entry 1, pinning the rest of the journal.
            session.accept(1)
            assertEquals(listOf(1), client.collectDurable("pinned", 1))
            session.accept(*(2..200).toList().toIntArray())
            // A new consumer starts from the oldest retained entry.
            assertEquals((1..200).toList(), client.collectDurable("messages", 200))
            // Pinned at cursor 1: entry 2 onward is still there for it.
            assertEquals(2, client.notificationsAfter(1).first().notification.payload.single().toInt().and(0xFF))
            // Once the pinned consumer catches up too, everything it and "messages" handled is pruned.
            assertEquals((2..200).toList(), client.collectDurable("pinned", 199))
            assertFailsWith<ConnectorCursorExpiredException> { client.notificationsAfter(1).first() }
        } finally { client.closeAndJoin() }
    }

    @Test fun `pruning keeps entries a live reader has not reached`(): Unit = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db)
        val client = reviewClient(offline, db = db)
        try {
            val gate = CompletableDeferred<Unit>()
            val live = mutableListOf<Int>()
            // A live reader that stalls on its first notification, so it falls behind.
            val reader = launch(Dispatchers.Default) {
                client.notifications.collect {
                    live += it.payload.single().toInt() and 0xFF
                    if (live.size == 1) gate.await()
                }
            }
            delay(300)
            session.accept(*(1..150).toList().toIntArray())
            assertEquals(150, client.collectDurable("messages", 150).size)
            gate.complete(Unit)
            withTimeout(10_000) { while (live.size < 150) delay(50) }
            assertEquals((1..150).toList(), live)
            reader.cancelAndJoin()
        } finally { client.closeAndJoin() }
    }

    @Test fun `live notifications start from now`(): Unit = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db)
        session.accept(1)
        val client = reviewClient(offline, db = db)
        try {
            val next = async(Dispatchers.Default) { client.notifications.first().payload.single().toInt() }
            delay(300)
            session.accept(2)
            assertEquals(2, withTimeout(10_000) { next.await() })
        } finally { client.closeAndJoin() }
    }
}
