package com.latenighthack.lockers.server

import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.claim.*
import com.latenighthack.lockers.server.cluster.JdbcAdvisoryLockGateway
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.SessionGatewayService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import java.sql.DriverManager
import kotlin.test.*

class RoomMutationFencePgTest {
    private val room = RoomId(byteArrayOf(1))
    private val id = LockerId(byteArrayOf(2))

    private suspend fun fixture(test: suspend (String, Database, LockerStoreImpl, DeliveryOutboxStore) -> Unit) {
        val base = PgTestGate.urlOrSkip()
        val schema = "room_fence_${System.nanoTime()}"
        DriverManager.getConnection(base).use { it.createStatement().use { sql -> sql.execute("CREATE SCHEMA $schema") } }
        val url = base + (if ('?' in base) "&" else "?") + "currentSchema=$schema"
        val database = ServerStorage.postgres(url)
        database.open()
        try { test(url, database, LockerStoreImpl(database), DeliveryOutboxStore(database)) }
        finally {
            database.close()
            DriverManager.getConnection(base).use { it.createStatement().use { sql -> sql.execute("DROP SCHEMA $schema CASCADE") } }
        }
    }

    @Test fun staleClaimCannotCommitWithItsCachedEpoch() = runBlocking {
        fixture { url, _, lockers, outbox ->
            val pool = ClaimJdbcPool(url)
            try {
                val claims = JdbcRoomClaimStore(pool).also { it.prepare() }
                val first = claims.claim(room, "old", "old:1", 60_000)
                val proof = claims.mutationFence(room, "old", first.epoch)
                claims.release(room, "old")
                val successor = claims.claim(room, "new", "new:1", 60_000)
                assertTrue(successor.epoch > first.epoch)
                assertFailsWith<RoomOwnershipLost> {
                    withContext(proof) { outbox.commit(room, emptyList(), listOf(Event(roomId = room, eventId = EventId(byteArrayOf(8))))) {
                        lockers.updateLocker(ServerLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue), Locker(open = Locker.OpenLocker(byteArrayOf(9))).toByteArray(), 1))
                    } }
                }
                assertNull(lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue)))
                assertEquals(0, outbox.pendingCount())
            } finally { pool.close() }
        }
    }

    @Test fun expiryDuringTheSourceTransactionRollsBackContentAndIntent() = runBlocking {
        fixture { url, _, lockers, outbox ->
            val pool = ClaimJdbcPool(url)
            try {
                val claims = JdbcRoomClaimStore(pool).also { it.prepare() }
                val claim = claims.claim(room, "old", "old:1", 1_000)
                var bodyRan = false
                assertFailsWith<RoomOwnershipLost> {
                    withContext(claims.mutationFence(room, "old", claim.epoch)) {
                        outbox.commit(room, emptyList(), listOf(Event(roomId = room, eventId = EventId(byteArrayOf(8))))) {
                            lockers.updateLocker(ServerLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue), byteArrayOf(7), 1))
                            bodyRan = true
                            delay(1_100)
                        }
                    }
                }
                assertTrue(bodyRan)
                assertNull(lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue)))
                assertEquals(0, outbox.pendingCount())
            } finally { pool.close() }
        }
    }

    @Test fun ringFenceTokensAdvanceAndRejectAnOldTokenEvenWithStaleLiveness() = runBlocking {
        fixture { url, _, lockers, outbox ->
            val gateway = JdbcAdvisoryLockGateway(url)
            val key = 912_347L
            val old = assertNotNull(gateway.tryLock(key))
            val token = old.fencingToken
            old.close()
            val current = assertNotNull(gateway.tryLock(key))
            try {
                assertTrue(current.fencingToken > token)
                assertFailsWith<RoomOwnershipLost> {
                    withContext(LeaseMutationFence(key, token) { true }) { outbox.commit(room, emptyList(), emptyList()) {
                        lockers.updateLocker(ServerLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue), byteArrayOf(7), 1))
                    } }
                }
                assertNull(lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue)))
            } finally { current.close() }
        }
    }

    @Test fun twoDatabaseHandlesCannotCommitTheSameParentVersion() = runBlocking {
        fixture { url, database, lockers, outbox ->
            fun service(db: Database, store: LockerStoreImpl, delivery: DeliveryOutboxStore) = RoomServiceImpl(
                SubscriptionStoreImpl(db), store, LockStoreImpl(db), object : SessionGatewayDiscovery {
                    override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
                }, LocalRoomOwnership(), LockerAgentRegistry.None, SimpleMeterRegistry(),
                LockersConfig.defaults().copy(shardMultiplier = 0, roomWritesPerSecond = 0, deliveryWorkerEnabled = false), delivery,
            )
            val other = ServerStorage.postgres(url).also { it.open() }
            val first = service(database, lockers, outbox)
            val second = service(other, LockerStoreImpl(other), DeliveryOutboxStore(other))
            try {
                val request = PostLockerChangeRequest(roomId = room, lockerId = id, locker = Locker(open = Locker.OpenLocker(byteArrayOf(1))))
                val initial = LocalRoomServiceRpc(first).postLockerChange(request)
                val results = listOf(first, second).map { implementation -> async(Dispatchers.IO) {
                    LocalRoomServiceRpc(implementation).postLockerChange(request.copy(parentVersion = initial.version))
                } }.awaitAll()
                assertEquals(1, results.count { it.result.isOk() })
                assertEquals(1, results.count { it.result is PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION })
                assertEquals(2, lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue))!!.version)
                assertEquals(2, outbox.pendingCount())
            } finally { first.close(); second.close(); other.close() }
        }
    }
}
