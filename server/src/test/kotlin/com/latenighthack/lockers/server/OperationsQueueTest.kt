package com.latenighthack.lockers.server

import com.latenighthack.ktstore.createDatabase
import com.latenighthack.lockers.common.v1.Event
import com.latenighthack.lockers.common.v1.EventId
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.server.services.push.v1.PushQueueStoreImpl
import com.latenighthack.lockers.server.services.room.v1.DeliveryOutboxStore
import com.latenighthack.lockers.server.services.session.v1.SessionInboxStoreDefinitionV2
import com.latenighthack.lockers.server.services.session.v1.SessionInboxStoreImpl
import com.latenighthack.lockers.server.services.session.v1.inboxRowBytes
import com.latenighthack.lockers.server.storage.v1.ServerEventId
import com.latenighthack.lockers.server.storage.v1.ServerPush
import com.latenighthack.lockers.server.storage.v1.ServerPushId
import com.latenighthack.lockers.server.storage.v1.ServerRoomId
import com.latenighthack.lockers.server.storage.v1.ServerSessionEvent
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import com.latenighthack.lockers.server.storage.v1.toByteArray
import com.latenighthack.lockers.server.storage.v2.toByteArray
import com.latenighthack.lockers.server.tools.QueueMetrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class OperationsQueueTest {
    @Test fun retriesKeepEnqueueAgeAndStalePushLeaseCannotDeleteNewClaim() = runBlocking {
        val db = ServerStorage.inMemory()
        var now = 100L
        val store = PushQueueStoreImpl(db, clock = { now })
        try {
            db.open()
            store.enqueue(ServerPush(ServerPushId(byteArrayOf(1)), ServerSessionId(byteArrayOf(2)), 1), false)
            assertEquals(100, store.snapshotsByBackend().getValue(1).oldestAt)
            val first = store.claim(1, "first", now, 1, 30).single()
            now = 130
            val next = store.claim(1, "next", now, 1, 30).single()
            assertFalse(store.finish(first))
            assertTrue(store.retry(next, 1, 500))
            assertEquals(100, store.snapshotsByBackend().getValue(1).oldestAt)
            assertEquals(0, store.snapshotsByBackend().getValue(1).eligibleDepth)
            now = 500
            assertEquals(1, store.snapshotsByBackend().getValue(1).eligibleDepth)
            assertTrue(store.finish(store.claim(1, "retry", now, 1).single()))
            assertEquals(0, store.snapshotsByBackend().getValue(1).depth)
        } finally {
            db.close()
        }
    }

    @Test fun outboxRecipientCountsAndProgressFollowFencedAcceptance() = runBlocking {
        val db = ServerStorage.inMemory()
        val registry = SimpleMeterRegistry()
        val metrics = QueueMetrics(registry, "room_delivery")
        val store = DeliveryOutboxStore(db, clock = { 100 }, metrics = metrics)
        try {
            db.open()
            val room = RoomId(byteArrayOf(1))
            val recipients = listOf(SessionId(byteArrayOf(2)), SessionId(byteArrayOf(3)))
            store.commit(room, recipients, listOf(Event(eventId = EventId(byteArrayOf(4)), roomId = room))) { }
            assertEquals(2, store.snapshot().recipients)
            val first = store.claim("a", 100, leaseMs = 30).single()
            val second = store.claim("b", 130, leaseMs = 30).single()
            store.accepted(first, recipients)
            assertEquals(2, store.snapshot().recipients)
            store.accepted(second, recipients.take(1))
            assertEquals(1, store.snapshot().recipients)
            store.accepted(second, recipients.drop(1))
            metrics.update(store.snapshot())
            assertEquals(
                0.0,
                registry
                    .get("fullhouse.queue.depth")
                    .tag("queue", "room_delivery")
                    .gauge()
                    .value(),
            )
            assertEquals(
                1.0,
                registry
                    .get("fullhouse.queue.events")
                    .tags("queue", "room_delivery", "event", "completion")
                    .counter()
                    .count(),
            )
        } finally {
            db.close()
            registry.close()
        }
    }

    @Test fun schemaFiveUpgradePreservesStoredBytesAndUnknownAgesAcrossReopen() = runBlocking {
        val file = File.createTempFile("operations-migration", ".db")
        val current = ServerStorage.configuration(file.name)
        val previous = legacyConfiguration(current)
        val row =
            ServerSessionEvent(
                sessionId = ServerSessionId(byteArrayOf(1)),
                eventId = ServerEventId(byteArrayOf(2)),
                roomSequence = 7,
                unknownFields = byteArrayOf(0xa0.toByte(), 6, 7),
            )
        val raw = row.toByteArray()
        seedVersionFive(previous, file, row)
        try {
            repeat(2) {
                val db = createDatabase(current, file.absolutePath)
                try {
                    db.open()
                    val inbox = SessionInboxStoreImpl(db)
                    val snapshot = inbox.snapshot()
                    assertEquals((1 + it).toLong(), snapshot.depth)
                    assertEquals(1, snapshot.unknownAge)
                    if (it == 0) assertNull(snapshot.oldestAt) else assertTrue(snapshot.oldestAt!! > 0)
                    assertContentEquals(
                        raw,
                        inbox.getAllEvents(requireNotNull(row.sessionId)).single().toByteArray(),
                    )
                    val session = ServerSessionId(byteArrayOf(3))
                    inbox.saveEvent(
                        ServerSessionEvent(
                            sessionId = session,
                            roomId = ServerRoomId(byteArrayOf(4)),
                            eventId =
                            ServerEventId(
                                byteArrayOf(
                                    (
                                        10 +
                                            it
                                        ).toByte(),
                                ),
                            ),
                        ),
                    )
                    assertTrue(inbox.snapshot().oldestAt!! > 0)
                } finally {
                    db.close()
                }
            }
        } finally {
            file.delete()
        }
    }

    @Test fun wireByteAccountingIncludesOperationalMetadata() {
        val row =
            ServerSessionEvent(
                sessionId = ServerSessionId(byteArrayOf(1)),
                eventId = ServerEventId(byteArrayOf(2)),
                encodedPayload = ByteArray(128),
                enqueuedAt = 123456789,
                traceparent = "00-" + "a".repeat(32) + "-" + "b".repeat(16) + "-01",
            )
        assertEquals(row.toByteArray().size.toLong(), inboxRowBytes(row))
    }

    private suspend fun seedVersionFive(
        previous: com.latenighthack.ktstore.DatabaseConfiguration,
        file: File,
        row: ServerSessionEvent,
    ) {
        val legacy = createDatabase(previous, file.absolutePath)
        try {
            legacy.open()
            legacy.transaction(setOf(SessionInboxStoreDefinitionV2.storeName)) {
                save(SessionInboxStoreDefinitionV2.storeName, SessionInboxStoreDefinitionV2.encodeRow(row))
            }
        } finally {
            legacy.close()
        }
    }

    private fun legacyConfiguration(current: com.latenighthack.ktstore.DatabaseConfiguration) =
            current.copy(
                version = 5,
                stores = ServerStorage.definitionsV5.map { it.declaration },
                migrations =
                current.migrations.filter {
                    it.toVersion <=
                        5
                },
            )

}
