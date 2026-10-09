package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.common.v1.RoomId
import kotlinx.coroutines.*
import java.sql.DriverManager
import kotlin.test.*

class ClaimCapacityPgTest {
    @Test fun productionClaimContextHonorsTheConfiguredPermanentCapacity(): Unit = runBlocking {
        val base = PgTestGate.urlOrSkip()
        val schema = "claim_config_capacity_${System.nanoTime()}"
        DriverManager.getConnection(base).use { it.createStatement().use { s -> s.execute("CREATE SCHEMA $schema") } }
        val url = base + (if ('?' in base) "&" else "?") + "currentSchema=$schema"
        val values = mapOf("LOCKERS_ROOM_OWNERSHIP" to "claim", "LOCKERS_DB_URL" to url,
            "LOCKERS_NODE_ID" to "one", "LOCKERS_ADVERTISE_ADDR" to "127.0.0.1:1", "LOCKERS_MAX_ROOM_CLAIMS" to "1")
        val config = com.latenighthack.lockers.server.LockersConfig.fromEnv(values::get)
        val context = ClaimContext.fromConfig(config, io.micrometer.core.instrument.simple.SimpleMeterRegistry())!!
        try {
            context.roomClaims.claim(RoomId(byteArrayOf(1)), context.nodeId, context.advertiseAddr, context.ttlMs)
            assertFailsWith<RoomClaimCapacityExceeded> {
                context.roomClaims.claim(RoomId(byteArrayOf(2)), context.nodeId, context.advertiseAddr, context.ttlMs)
            }
        } finally {
            context.close()
            DriverManager.getConnection(base).use { it.createStatement().use { s -> s.execute("DROP SCHEMA $schema CASCADE") } }
        }
    }
    @Test fun permanentClaimHistoryHasOneAtomicCapacityAcrossReplicas(): Unit = runBlocking {
        val base = PgTestGate.urlOrSkip()
        val schema = "claim_capacity_${System.nanoTime()}"
        DriverManager.getConnection(base).use { it.createStatement().use { s -> s.execute("CREATE SCHEMA $schema") } }
        val url = base + (if ('?' in base) "&" else "?") + "currentSchema=$schema"
        val pools = List(2) { ClaimJdbcPool(url) }
        val stores = pools.map { JdbcRoomClaimStore(it, maxRoomClaims = 2).also { store -> store.prepare() } }
        try {
            val results = (1..3).map { id -> async(Dispatchers.IO) {
                runCatching { stores[id % 2].claim(RoomId(byteArrayOf(id.toByte())), "node$id", "node$id:1", 60_000) }
            } }.awaitAll()
            assertEquals(2, results.count { it.isSuccess })
            assertTrue(results.single { it.isFailure }.exceptionOrNull() is RoomClaimCapacityExceeded)
            val accepted = results.indexOfFirst { it.isSuccess } + 1
            val room = RoomId(byteArrayOf(accepted.toByte()))
            val previous = results[accepted - 1].getOrThrow()
            stores[0].release(room, previous.nodeId)
            assertFailsWith<RoomClaimCapacityExceeded> { stores[1].claim(RoomId(byteArrayOf(4)), "new", "new:1", 60_000) }
            assertEquals(previous.epoch + 1, stores[1].claim(room, "replacement", "replacement:1", 60_000).epoch)
        } finally {
            pools.forEach { it.close() }
            DriverManager.getConnection(base).use { it.createStatement().use { s -> s.execute("DROP SCHEMA $schema CASCADE") } }
        }
    }

    @Test fun inMemoryCapacityPreservesReleasedAuthorityHistory(): Unit = runBlocking {
        val store = InMemoryRoomClaimStore(maxRoomClaims = 1)
        val room = RoomId(byteArrayOf(1))
        val first = store.claim(room, "first", "first:1", 60_000)
        store.release(room, "first")
        assertFailsWith<RoomClaimCapacityExceeded> { store.claim(RoomId(byteArrayOf(2)), "new", "new:1", 60_000) }
        assertEquals(first.epoch + 1, store.claim(room, "second", "second:1", 60_000).epoch)
    }
}
