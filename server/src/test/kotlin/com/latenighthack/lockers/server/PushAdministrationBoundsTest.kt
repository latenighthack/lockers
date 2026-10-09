package com.latenighthack.lockers.server

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.services.room.v1.FencedMemoryDelegate
import com.latenighthack.lockers.server.services.push.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.push.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class PushAdministrationBoundsTest {
    @Test fun `queue statistics and capped dead letter listing never load whole stores`(): Unit = runBlocking {
        val memory = FencedMemoryDelegate(InMemoryStoreDelegate())
        val bounded = object: LifecycleStoreDelegate by memory, ScopedStoreDelegate, IndexedQueryDelegate {
            override val supportsTransactions get() = memory.supportsTransactions
            override suspend fun <T> transaction(block: suspend () -> T) = memory.transaction(block)
            override suspend fun <T> transaction(lockKey: String, block: suspend () -> T) = memory.transaction(lockKey, block)
            override suspend fun <T> transaction(stores: Set<String>, mode: TransactionMode, block: suspend () -> T) = memory.transaction(stores, mode, block)
            override suspend fun query(tableName: String, query: IndexedQuery, identity: String, version: Int) = memory.query(tableName, query, identity, version)
            override suspend fun count(tableName: String, query: IndexedQuery) = memory.count(tableName, query)
            override suspend fun deleteBatch(tableName: String, query: IndexedQuery, identity: String, version: Int) = memory.deleteBatch(tableName, query, identity, version)
            override suspend fun getAll(tableName: String, relation: StoreRelation?): List<Any> {
                check(tableName != "push" && tableName != "push_deadletter") { "push administration performed an unbounded read" }
                return memory.getAll(tableName, relation)
            }
        }
        val db = Database(ServerStorage.configuration("outbox-bounded-${java.util.UUID.randomUUID()}"), bounded)
        val queue = PushQueueStoreImpl(db).also { it.prepare() }
        val dead = PushDeadLetterStoreImpl(db).also { it.prepare() }
        val sessions = PushSessionStoreImpl(db).also { it.prepare() }
        db.open()
        queue.enqueue(ServerPush(pushId = ServerPushId(byteArrayOf(1)), sessionId = ServerSessionId(byteArrayOf(1)), backend = 1), true)
        repeat(300) { i -> dead.saveDeadLetter(ServerDeadLetter(pushId = ServerPushId(java.nio.ByteBuffer.allocate(4).putInt(i).array()), sessionId = ServerSessionId(byteArrayOf(1)), backend = 1)) }
        val service = PushServiceImpl(sessions, queue, dead, SimpleMeterRegistry(), emptyList())
        try {
            val admin = LocalPushAdminServiceRpc(service)
            assertEquals(1L, admin.getQueueStats(GetQueueStatsRequest()).queued)
            assertEquals(300L, admin.getQueueStats(GetQueueStatsRequest()).deadLettered)
            assertEquals(256, admin.listDeadLetters(ListDeadLettersRequest(limit = Int.MAX_VALUE)).deadLetters.size)
            assertEquals(1L, admin.drainQueue(DrainQueueRequest()).drained)
        } finally { service.stopAndJoin(); db.close() }
    }
}
