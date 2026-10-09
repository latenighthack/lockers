package com.latenighthack.lockers.server

import com.latenighthack.lockers.server.claim.PgTestGate
import com.latenighthack.lockers.server.services.session.v1.SessionInboxStoreImpl
import com.latenighthack.lockers.server.storage.v1.ServerEventId
import com.latenighthack.lockers.server.storage.v1.ServerRoomId
import com.latenighthack.lockers.server.storage.v1.ServerSessionEvent
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class InboxOperationsPgTest {
    @Test fun replayPagesAreBoundedOrderedAndDuplicateDeliveryKeepsAge() = runBlocking {
        val base = PgTestGate.urlOrSkip()
        val schema = "inbox_test_${System.nanoTime()}"
        DriverManager.getConnection(base).use {
            it.createStatement().use { s ->
                s.execute("CREATE SCHEMA $schema")
            }
        }
        val url = base + (if ('?' in base) "&" else "?") + "currentSchema=$schema"
        val handles = mutableListOf<com.latenighthack.ktstore.Database>()
        try {
            suspend fun store(): SessionInboxStoreImpl {
                val db = ServerStorage.postgres(url).also { handles.add(it) }
                return SessionInboxStoreImpl(db).also {
                    it.prepare()
                    db.open()
                }
            }
            val a = store()
            val b = store()
            val session = ServerSessionId(byteArrayOf(1))
            val events =
                (1..700).reversed().map { n ->
                    ServerSessionEvent(
                        sessionId = session,
                        roomId = ServerRoomId(byteArrayOf(9)),
                        eventId =
                        ServerEventId(
                            java.nio.ByteBuffer
                                .allocate(4)
                                .putInt(n)
                                .array(),
                        ),
                        roomSequence = n.toLong(),
                    )
                }
            a.saveEvents(events)
            val oldest = a.snapshot().oldestAt
            delay(2)
            b.saveEvents(events.take(100))
            assertEquals(oldest, b.snapshot().oldestAt)
            val pages = store().clientEventPages(session).toList()
            assertEquals(List(10) { 64 } + 60, pages.map { it.size })
            assertEquals((1L..700L).toList(), pages.flatten().map { it.roomSequence })
            assertEquals(700, b.snapshot().depth)
            assertEquals(0, b.snapshot().unknownAge)
            a.deleteEvents(events.take(100).map { it.eventId!! }, session)
            assertEquals(600, store().snapshot().depth)
            val remainingOldest = b.snapshot().oldestAt
            assertTrue(store().clientEventPages(ServerSessionId(byteArrayOf(2))).toList().isEmpty())
            // A historical event has no enqueue timestamp. Classify it from the
            // shared gateway registry without inventing an age or decoding bodies.
            seedHistoricalEvent(url)
            verifyPresence(url, session, remainingOldest)
        } finally {
            handles.asReversed().forEach { it.close() }
            DriverManager.getConnection(base).use {
                it.createStatement().use { s ->
                    s.execute("DROP SCHEMA $schema CASCADE")
                }
            }
        }
    }

    private suspend fun verifyPresence(url: String, session: ServerSessionId, remainingOldest: Long?) {
        com.latenighthack.lockers.server.claim.ClaimJdbcPool(url, size = 1).use { pool ->
            val gateway =
                com.latenighthack.lockers.server.claim
                    .JdbcSessionGatewayStore(pool, pool)
                    .also { it.prepare() }
            gateway.upsert(
                com.latenighthack.lockers.common.v1
                    .SessionId(session.rawValue),
                "node-a",
                "peer-a",
                60_000,
            )
            val online = gateway.inboxPresenceSnapshot()!!
            assertEquals(600, online.getValue("online").depth)
            assertEquals(remainingOldest, online.getValue("online").oldestAt)
            assertEquals(1, online.getValue("offline").depth)
            assertEquals(1, online.getValue("offline").unknownAge)
            assertNull(online.getValue("offline").oldestAt)
            gateway.upsert(
                com.latenighthack.lockers.common.v1
                    .SessionId(session.rawValue),
                "node-b",
                "peer-b",
                60_000,
            )
            assertEquals(
                600,
                com.latenighthack.lockers.server.claim
                    .JdbcSessionGatewayStore(
                        pool,
                        pool,
                    ).inboxPresenceSnapshot()!!
                    .getValue("online")
                    .depth,
                "Moving a gateway and reopening a reporter must not double count shared backlog",
            )
            pool.withConnection { conn ->
                conn.createStatement().use {
                    it.execute("UPDATE session_gateway SET expires_at = now() - interval '1 second'")
                }
            }
            val offline = gateway.inboxPresenceSnapshot()!!
            assertEquals(0, offline.getValue("online").depth)
            assertEquals(601, offline.getValue("offline").depth)
            assertEquals(1, offline.getValue("offline").unknownAge)
            assertEquals(remainingOldest, offline.getValue("offline").oldestAt)
            verifyPresenceMeters(offline)
        }
    }

    private fun verifyPresenceMeters(offline: Map<String, com.latenighthack.lockers.server.tools.QueueSnapshot>) {
        val registry =
            io.micrometer.core.instrument.simple
                .SimpleMeterRegistry()
        val metrics =
            com.latenighthack.lockers.server.tools
                .InboxPresenceMetrics(registry, true)
        assertTrue(
            registry
                .get("fullhouse.session.inbox.depth")
                .tag("presence", "online")
                .gauge()
                .value()
                .isNaN(),
        )
        metrics.update(offline)
        assertEquals(
            0.0,
            registry
                .get("fullhouse.session.inbox.depth")
                .tag("presence", "online")
                .gauge()
                .value(),
        )
        assertEquals(
            601.0,
            registry
                .get("fullhouse.session.inbox.depth")
                .tag("presence", "offline")
                .gauge()
                .value(),
        )
    }

    private suspend fun seedHistoricalEvent(url: String) {
            val legacyDatabase = ServerStorage.postgres(url)
            val historical =
                object : com.latenighthack.ktstore.Store<ServerSessionEvent>(
                    legacyDatabase,
                    com.latenighthack.lockers.server.services.session.v1.SessionInboxStoreDefinitionV3,
                ) {
                    suspend fun seed(value: ServerSessionEvent) = save(value)
                }
            historical.prepare()
            legacyDatabase.open()
            historical.seed(
                ServerSessionEvent(
                    sessionId = ServerSessionId(byteArrayOf(3)),
                    eventId =
                    ServerEventId(byteArrayOf(1)),
                ),
            )
            legacyDatabase.close()
    }
}
