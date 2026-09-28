package com.latenighthack.lockers.server.tools

import com.latenighthack.ktstore.*
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

/** Request-scoped counters: no payloads, signing material, or unbounded metric labels. */
class WriteTrace(val requestId: String, private val meters: MeterRegistry) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<WriteTrace>
    private val start = System.nanoTime()
    private val phases = linkedMapOf<String, Long>()
    var databaseOperations = 0
    var databaseBytes = 0L
    var recipients = 0
    var result = "exception"
    var requestBytes = 0
    suspend fun <T> phase(name: String, action: suspend () -> T): T {
        val began = System.nanoTime()
        try { return action() }
        finally {
            val elapsed = System.nanoTime() - began
            phases[name] = (phases[name] ?: 0) + elapsed
            meters.timer("lockers.write.phase", "phase", name).record(elapsed, TimeUnit.NANOSECONDS)
        }
    }
    suspend fun <T> run(action: suspend () -> T): T = withContext(this) {
        try { action() }
        finally {
            LoggerFactory.getLogger(WriteTrace::class.java).info(
                "locker_write request={} elapsed_ms={} db_ops={} db_bytes={} recipients={} phases_ms={} result={} request_bytes={}",
                requestId, (System.nanoTime() - start) / 1_000_000.0, databaseOperations, databaseBytes, recipients,
                phases.mapValues { it.value / 1_000_000.0 }, result, requestBytes)
        }
    }
}

class MeasuredStoreDelegate(private val inner: StoreDelegate) : TransactionalStoreDelegate, StoreDelegate by inner {
    override val supportsTransactions get() = (inner as? TransactionalStoreDelegate)?.supportsTransactions == true
    override suspend fun <T> transaction(block: suspend () -> T): T = (inner as TransactionalStoreDelegate).transaction(block)
    override suspend fun <T> transaction(lockKey: String, block: suspend () -> T): T = (inner as TransactionalStoreDelegate).transaction(lockKey, block)
    private suspend fun <T> measure(name: String, bytes: Long = 0, action: suspend () -> T): T {
        val trace = coroutineContext[WriteTrace] ?: return action()
        trace.databaseOperations++
        trace.databaseBytes += bytes
        return trace.phase(name, action)
    }
    override suspend fun save(tableName: String, data: Any, keys: List<BoundStoreKey>) = measure("db_write", (data as? ByteArray)?.size?.toLong() ?: 0) { inner.save(tableName, data, keys) }
    override suspend fun saveAll(tableName: String, rows: List<StoreRow>) = measure("db_write", rows.sumOf { (it.data as? ByteArray)?.size?.toLong() ?: 0 }) { inner.saveAll(tableName, rows) }
    override suspend fun getMany(tableName: String, relations: List<StoreRelation>) = measure("db_read") { inner.getMany(tableName, relations) }
    override suspend fun get(tableName: String, relation: StoreRelation?) = measure("db_read") { inner.get(tableName, relation) }
    override suspend fun getAll(tableName: String, relation: StoreRelation?) = measure("db_read") { inner.getAll(tableName, relation) }
    override suspend fun delete(tableName: String, relation: StoreRelation) = measure("db_write") { inner.delete(tableName, relation) }
    override suspend fun deleteMany(tableName: String, relations: List<StoreRelation>) = measure("db_write") { inner.deleteMany(tableName, relations) }
}
