package com.latenighthack.lockers.server

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import kotlin.test.*

class AgentRecoveryTest {
    @Test fun `restart recovers a default agent source committed before request cancellation`(): Unit = runBlocking {
        val memory = FencedMemoryDelegate(InMemoryStoreDelegate())
        var depth = 0
        var receiptWritten = false
        var armed = false
        suspend fun <T> physicallyCommitted(block: suspend () -> T): T {
            depth++
            val result: T
            try { result = block() } finally { depth-- }
            if (depth == 0 && armed && receiptWritten) { armed = false; throw CancellationException("request lost after physical commit") }
            return result
        }
        val delegate = object: LifecycleStoreDelegate by memory, ScopedStoreDelegate, IndexedQueryDelegate {
            override val supportsTransactions get() = memory.supportsTransactions
            override suspend fun <T> transaction(block: suspend () -> T) = physicallyCommitted { memory.transaction(block) }
            override suspend fun <T> transaction(lockKey: String, block: suspend () -> T) = physicallyCommitted { memory.transaction(lockKey, block) }
            override suspend fun <T> transaction(stores: Set<String>, mode: TransactionMode, block: suspend () -> T) = physicallyCommitted { memory.transaction(stores, mode, block) }
            override suspend fun save(tableName: String, data: Any, keys: List<BoundStoreKey>) { memory.save(tableName, data, keys); if (tableName == "delivery_write_receipts") receiptWritten = true }
            override suspend fun query(tableName: String, query: IndexedQuery, identity: String, version: Int) = memory.query(tableName, query, identity, version)
            override suspend fun count(tableName: String, query: IndexedQuery) = memory.count(tableName, query)
            override suspend fun deleteBatch(tableName: String, query: IndexedQuery, identity: String, version: Int) = memory.deleteBatch(tableName, query, identity, version)
        }
        val db = Database(ServerStorage.configuration("agent-restart-${System.nanoTime()}"), delegate)
        val lockers = LockerStoreImpl(db).also { it.prepare() }
        val locks = LockStoreImpl(db).also { it.prepare() }
        val subs = SubscriptionStoreImpl(db).also { it.prepare() }
        val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
        db.open()
        fun service() = RoomServiceImpl(subs, lockers, locks, object: SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
        }, LocalRoomOwnership(), LockerAgentRegistry.None, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryWorkerEnabled = false), outbox)
        val first = service()
        val room = RoomId(byteArrayOf(1)); val writeId = ByteArray(16) { 9 }
        val request = PostLockerChangesRequest(roomId = room, writeRequestId = writeId, changes = listOf(
            PostLockerChangeRequest(roomId = room, lockerId = LockerId(byteArrayOf(2)), locker = Locker { open { encodedPayload = byteArrayOf(3) } })))
        armed = true
        assertFailsWith<CancellationException> { LocalRoomServiceRpc(first).postLockerChanges(request) }
        assertNotNull(outbox.receipt(room, writeId))
        first.closeAndJoin()
        val second = service()
        try {
            second.start()
            withTimeout(1500) {
                while (LocalRoomServiceRpc(second).getWriteOutcome(GetWriteOutcomeRequest(roomId = room, writeRequestId = writeId)).outcome?.agentState != WriteOutcome.AgentState.APPLIED) delay(20)
            }
        } finally { second.closeAndJoin(); db.close() }
    }
}
