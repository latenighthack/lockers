package com.latenighthack.lockers.server

import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.common.LockerSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.push.v1.*
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.services.push.v1.*
import com.latenighthack.lockers.server.services.push.v1.providers.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.server.cluster.OwnerLifecycle
import com.latenighthack.lockers.sharding.*
import com.latenighthack.lockers.sharding.spi.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.*

/** Security regressions for the reviewed trust boundaries. */
class CanonicalBatchRegressionTest {
    private val room = RoomId(byteArrayOf(1))
    private val id = LockerId(byteArrayOf(2), LockerKeyspace(0))
    private fun body(n: Int) = Locker { open { encodedPayload = byteArrayOf(n.toByte()) } }
    private class Stores(val db: Database, val lockers: LockerStoreImpl, val locks: LockStoreImpl, val subs: SubscriptionStoreImpl, val outbox: DeliveryOutboxStore)
    private suspend fun stores(): Stores {
        val db = ServerStorage.inMemory()
        val s = Stores(db, LockerStoreImpl(db), LockStoreImpl(db), SubscriptionStoreImpl(db), DeliveryOutboxStore(db))
        s.lockers.prepare(); s.locks.prepare(); s.subs.prepare(); s.outbox.prepareStores(); db.open()
        return s
    }
    private fun service(s: Stores, fast: Boolean = false, lockers: LockerStore = s.lockers, discovery: SessionGatewayDiscovery = object : SessionGatewayDiscovery {
        override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
    }) = RoomServiceImpl(s.subs, lockers, s.locks, discovery, LocalRoomOwnership(), object : LockerAgentRegistry {
        override suspend fun processPayload(roomId: RoomId, lockerId: LockerId, locker: Locker) = emptyList<LockerAgentRegistry.LockerWrite>()
    }, SimpleMeterRegistry(), LockersConfig.defaults().copy(shardMultiplier = 0, roomWritesPerSecond = 0, deliveryOutboxEnabled = fast, deliveryWorkerEnabled = false), s.outbox)

    @Test fun keyspaceAliasesCannotTargetTheSameLockerTwiceInABatch(): Unit = runBlocking {
        val s = stores(); val service = service(s, fast = true); val rpc = LocalRoomServiceRpc(service)
        try {
            val result = rpc.postLockerChanges(PostLockerChangesRequest(roomId = room, writeRequestId = ByteArray(16) { 9 },
                changes = listOf(
                    PostLockerChangeRequest(roomId = room, lockerId = id.copy(keyspace = null), locker = body(1)),
                    PostLockerChangeRequest(roomId = room, lockerId = id, locker = body(2)))))
            assertFalse(result.result.isOk())
            assertTrue(s.lockers.getAllLockers(ServerRoomId(room.rawValue)).isEmpty())
            assertEquals(0, s.outbox.pendingCount())
        } finally { service.close() }
    }
}
