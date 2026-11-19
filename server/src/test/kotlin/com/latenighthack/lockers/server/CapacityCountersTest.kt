package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktstore.Database
import com.latenighthack.ktstore.JdbcDriver
import com.latenighthack.ktstore.ReadCacheConfiguration
import com.latenighthack.ktstore.SqlStoreDelegate
import com.latenighthack.lockers.common.v1.Event
import com.latenighthack.lockers.common.v1.EventId
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.room.v1.PostLockerChangesRequest
import com.latenighthack.lockers.room.v1.PostLockerChangesResponse
import com.latenighthack.lockers.server.claim.PgTestGate
import com.latenighthack.lockers.server.services.room.v1.AgentWorkDefinitionV3
import com.latenighthack.lockers.server.services.room.v1.AgentWorkState
import com.latenighthack.lockers.server.services.room.v1.AgentWorkStore
import com.latenighthack.lockers.server.services.room.v1.DeliveryOutboxStore
import com.latenighthack.lockers.server.services.room.v1.FencedSqlDriver
import com.latenighthack.lockers.server.services.room.v1.LockerStoreDefinitionV2
import com.latenighthack.lockers.server.services.room.v1.LockerStoreImpl
import com.latenighthack.lockers.server.services.room.v1.OutboxPolicy
import com.latenighthack.lockers.server.services.room.v1.agentRoomPrefix
import com.latenighthack.lockers.server.storage.v1.ServerLocker
import com.latenighthack.lockers.server.storage.v1.ServerLockerId
import com.latenighthack.lockers.server.storage.v1.ServerRoomId
import com.latenighthack.lockers.server.storage.v2.ServerAgentWork
import kotlinx.coroutines.runBlocking
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Admission counters must reject at exactly the same boundaries as the COUNT scans they replace. */
class CapacityCountersTest {
    private val room = RoomId(byteArrayOf(1))
    private val other = RoomId(byteArrayOf(2))
    private val third = RoomId(byteArrayOf(3))
    private val session = SessionId(byteArrayOf(9))
    private val externalTables =
        setOf("room_claim", "room_claim_capacity", "session_gateway", "shard_map", "shard_fence")
    private fun event(target: RoomId, id: Int) =
        Event(roomId = target, eventId = EventId(java.nio.ByteBuffer.allocate(4).putInt(id).array()))
    private fun write(id: Int, target: RoomId = room) =
        PostLockerChangesRequest(roomId = target, writeRequestId = ByteArray(16) { id.toByte() })
    private fun locker(target: RoomId, id: Int) =
        ServerLocker(ServerRoomId(target.rawValue), 0, ServerLockerId(byteArrayOf(id.toByte())), version = 1)
    private fun schemaUrl(base: String, schema: String) =
        base + (if ('?' in base) "&" else "?") + "currentSchema=$schema"
    private fun sql(base: String, statement: String) =
        DriverManager.getConnection(base).use { it.createStatement().use { s -> s.execute(statement) } }
    private suspend fun DeliveryOutboxStore.send(target: RoomId, vararg ids: Int) =
        commit(target, listOf(session), ids.map { event(target, it) }) {}
    private suspend fun AgentWorkStore.add(id: Int, target: RoomId = room, applied: Boolean = false) =
        create(target, write(id, target), PostLockerChangesResponse(), "v1", id.toLong(), applied)

    /** Runs [block] against the in-memory store, then against Postgres when it is configured. */
    private suspend fun withDatabases(block: suspend (suspend () -> Database) -> Unit) {
        block { ServerStorage.inMemory() }
        val base = System.getenv("LOCKERS_TEST_PG_URL")?.takeIf { it.isNotBlank() } ?: return
        val schema = "capacity_counters_${System.nanoTime()}"
        sql(base, "CREATE SCHEMA $schema")
        val handles = mutableListOf<Database>()
        try { block { ServerStorage.postgres(schemaUrl(base, schema)).also { handles.add(it) } } }
        finally { handles.forEach { it.close() }; sql(base, "DROP SCHEMA $schema CASCADE") }
    }

    @Test fun `outbox retention rejects at room and global boundaries and frees on delivery`(): Unit = runBlocking {
        withDatabases { open ->
            val db = open()
            val policy = OutboxPolicy(retainedPerRoom = 2, retainedGlobal = 3)
            val outbox = DeliveryOutboxStore(db, policy = policy).also { it.prepareStores() }
            db.open()
            outbox.send(room, 1, 2)
            assertEquals("Room delivery capacity exceeded",
                assertFailsWith<IllegalStateException> { outbox.send(room, 3) }.message)
            outbox.send(other, 4)
            assertEquals("Global delivery capacity exceeded",
                assertFailsWith<IllegalStateException> { outbox.send(third, 5) }.message)
            // A completed delivery releases both the room's and the global retention.
            val claimed = outbox.claim("owner", System.currentTimeMillis(), limit = 4, eventsPerRoom = 1)
            outbox.accepted(claimed.first { it.roomId == ServerRoomId(room.rawValue) }, listOf(session))
            outbox.send(room, 6)
            assertFailsWith<IllegalStateException> { outbox.send(third, 7) }
            // Forgetting the counters recounts the same retained rows.
            outbox.recountCapacity()
            assertFailsWith<IllegalStateException> { outbox.send(room, 8) }
            assertFailsWith<IllegalStateException> { outbox.send(third, 9) }
        }
    }

    @Test fun `parked deliveries stay retained until resolved`(): Unit = runBlocking {
        withDatabases { open ->
            var now = 1L
            val db = open()
            val policy = OutboxPolicy(retryWindowMs = 10, completedRetentionMs = 20, retainedPerRoom = 1,
                retainedGlobal = 2)
            val outbox = DeliveryOutboxStore(db, policy = policy, clock = { now }).also { it.prepareStores() }
            db.open()
            outbox.send(room, 1)
            now = 50
            assertTrue(outbox.claim("owner", now).isEmpty())
            val parked = outbox.parked().single()
            assertFailsWith<IllegalStateException> { outbox.send(room, 2) }
            assertTrue(outbox.resolveParked(parked.eventId))
            // Resolution frees the room; the resolved entry keeps global retention until pruned.
            outbox.send(room, 3)
            assertFailsWith<IllegalStateException> { outbox.send(other, 4) }
            now = 100_000
            outbox.claim("owner", now)
            outbox.send(other, 5)
            assertFailsWith<IllegalStateException> { outbox.send(third, 6) }
        }
    }

    @Test fun `locker admission counts only new lockers and survives a recount`(): Unit = runBlocking {
        withDatabases { open ->
            val db = open()
            val lockers = LockerStoreImpl(db, ServerResourceLimits(maxLockers = 4, maxLockersPerRoom = 2))
            val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
            db.open()
            fun rejected(error: RpcResponseException, message: String) {
                assertEquals(Codes.FAILED_PRECONDITION, error.code); assertContains(error.message.orEmpty(), message)
            }
            lockers.updateLockers(listOf(locker(room, 1), locker(room, 2)))
            lockers.updateLocker(locker(room, 1).copy(version = 2))
            rejected(assertFailsWith { lockers.updateLocker(locker(room, 3)) }, "Room locker namespace exhausted")
            lockers.updateLocker(locker(other, 1))
            outbox.recountCapacity()
            rejected(assertFailsWith { lockers.updateLocker(locker(room, 4)) }, "Room locker namespace exhausted")
            lockers.updateLocker(locker(third, 1))
            rejected(assertFailsWith { lockers.updateLocker(locker(third, 2)) }, "Permanent locker namespace exhausted")
        }
    }

    @Test fun `agent journal rejects at its room boundary and pruning releases capacity`(): Unit = runBlocking {
        withDatabases { open ->
            var now = 1L
            val db = open()
            val work = AgentWorkStore(db, clock = { now }, retentionMs = 10, globalCapacity = 3, roomCapacity = 2)
            db.open()
            work.add(1, applied = true)
            work.add(2, applied = true)
            assertEquals("Agent journal room capacity exceeded",
                assertFailsWith<IllegalStateException> { work.add(3) }.message)
            work.add(4, other)
            assertEquals("Agent journal global capacity exceeded",
                assertFailsWith<IllegalStateException> { work.add(5, third) }.message)
            now = 100
            work.prune { _, _ -> }
            work.add(6)
            work.add(7)
            assertFailsWith<IllegalStateException> { work.add(8) }
        }
    }

    @Test fun `postgres v6 database initialises counters from existing rows`(): Unit = runBlocking {
        val base = PgTestGate.urlOrSkip(); val schema = "capacity_upgrade_${System.nanoTime()}"
        sql(base, "CREATE SCHEMA $schema")
        val url = schemaUrl(base, schema)
        try {
            val current = ServerStorage.configuration("lockers-server").copy(externalTables = externalTables)
            val v6 = current.copy(version = 6, stores = ServerStorage.definitionsV6.map { it.declaration },
                migrations = current.migrations.filter { it.toVersion <= 6 })
            val driver = FencedSqlDriver(JdbcDriver(url.removePrefix("jdbc:postgresql:"), "postgresql"))
            val legacy = Database(v6, SqlStoreDelegate(driver, "BYTEA", v6), ReadCacheConfiguration.Disabled)
            try {
                legacy.open()
                legacy.transaction(setOf(AgentWorkDefinitionV3.storeName, LockerStoreDefinitionV2.storeName)) {
                    for (n in 1..2) {
                        val id = ByteArray(16) { n.toByte() }
                        val row = ServerAgentWork(key = agentRoomPrefix(room.rawValue) + id, roomId = room.rawValue,
                            writeRequestId = id, state = AgentWorkState.APPLIED, completedAt = 1, createdAt = 1)
                        save(AgentWorkDefinitionV3.storeName, AgentWorkDefinitionV3.encodeRow(row))
                        save(LockerStoreDefinitionV2.storeName, LockerStoreDefinitionV2.encodeRow(locker(room, n)))
                    }
                }
            } finally { legacy.close() }
            val db = ServerStorage.postgres(url)
            try {
                val work = AgentWorkStore(db, roomCapacity = 3)
                val lockers = LockerStoreImpl(db, ServerResourceLimits(maxLockersPerRoom = 3))
                db.open()
                work.add(3)
                assertFailsWith<IllegalStateException> { work.add(4) }
                lockers.updateLocker(locker(room, 3))
                assertFailsWith<RpcResponseException> { lockers.updateLocker(locker(room, 4)) }
            } finally { db.close() }
        } finally { sql(base, "DROP SCHEMA $schema CASCADE") }
    }
}
