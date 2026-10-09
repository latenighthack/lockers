package com.latenighthack.lockers.server.tools

import com.latenighthack.ktstore.*
import io.micrometer.core.instrument.MeterRegistry
import com.latenighthack.lockers.observability.*
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

/** Request-scoped counters: no payloads, signing material, or unbounded metric labels. */
class WriteTrace(val requestId: String, private val meters: MeterRegistry, private val telemetry: LockersTelemetry = LockersTelemetry.NONE) : AbstractCoroutineContextElement(Key) {
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
        val operation = TelemetryOperation.entries.firstOrNull { it.component == "write" && it.operation == name }
        try { return if (operation == null) action() else telemetry.observe(operation) { action() } }
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

class MeasuredStoreDelegate(private val inner: LifecycleStoreDelegate, private val registry: MeterRegistry? = null, private val telemetry: LockersTelemetry = LockersTelemetry.NONE) : LifecycleStoreDelegate by inner, ScopedStoreDelegate, IndexedQueryDelegate {
    override val supportsTransactions get() = (inner as? TransactionalStoreDelegate)?.supportsTransactions == true
    override suspend fun <T> transaction(block: suspend () -> T): T = (inner as TransactionalStoreDelegate).transaction(block)
    override suspend fun <T> transaction(lockKey: String, block: suspend () -> T): T = (inner as TransactionalStoreDelegate).transaction(lockKey, block)
    override suspend fun <T> transaction(stores: Set<String>, mode: TransactionMode, block: suspend () -> T): T =
        (inner as ScopedStoreDelegate).transaction(stores, mode, block)
    override suspend fun query(tableName: String, query: IndexedQuery, identity: String, version: Int) =
        measure("db_read", table = tableName) { (inner as IndexedQueryDelegate).query(tableName, query, identity, version) }
    override suspend fun count(tableName: String, query: IndexedQuery) =
        measure("db_read", table = tableName) { (inner as IndexedQueryDelegate).count(tableName, query) }
    override suspend fun deleteBatch(tableName: String, query: IndexedQuery, identity: String, version: Int) =
        measure("db_write", table = tableName) { (inner as IndexedQueryDelegate).deleteBatch(tableName, query, identity, version) }
    private suspend fun <T> measure(name: String, bytes: Long = 0, table: String? = null, action: suspend () -> T): T {
        val trace = coroutineContext[WriteTrace]
        if (trace != null) { trace.databaseOperations++; trace.databaseBytes += bytes }
        val known = table != null && com.latenighthack.lockers.server.ServerStorage.definitions.any { it.storeName.value == table }
        val start = System.nanoTime(); var outcome = "error"
        try {
            val operation = if (name == "db_read") TelemetryOperation.STORAGE_READ else TelemetryOperation.STORAGE_WRITE
            return (if (registry != null && known) telemetry.observe(operation) { if (trace == null) action() else trace.phase(name, action) }
                else if (trace == null) action() else trace.phase(name, action)).also { outcome = "ok" }
        } catch (cancelled: kotlinx.coroutines.CancellationException) { outcome = "cancelled"; throw cancelled }
        finally { if (registry != null && known) {
            val tags = arrayOf(
                "store", table, "operation", if (name == "db_read") "read" else "write", "outcome", outcome,
            )
            registry.safeMeters {
                counter("lockers.storage.operations", *tags).increment()
                timer("lockers.storage.duration", *tags).record(System.nanoTime() - start, TimeUnit.NANOSECONDS)
            }
        } }
    }
    override suspend fun save(tableName: String, data: Any, keys: List<BoundStoreKey>) = measure("db_write", (data as? ByteArray)?.size?.toLong() ?: 0, tableName) { inner.save(tableName, data, keys) }
    override suspend fun saveAll(tableName: String, rows: List<StoreRow>) = measure("db_write", rows.sumOf { (it.data as? ByteArray)?.size?.toLong() ?: 0 }, tableName) { inner.saveAll(tableName, rows) }
    override suspend fun getMany(tableName: String, relations: List<StoreRelation>) = measure("db_read", table = tableName) { inner.getMany(tableName, relations) }
    override suspend fun get(tableName: String, relation: StoreRelation?) = measure("db_read", table = tableName) { inner.get(tableName, relation) }
    override suspend fun getAll(tableName: String, relation: StoreRelation?) = measure("db_read", table = tableName) { inner.getAll(tableName, relation) }
    override suspend fun delete(tableName: String, relation: StoreRelation) = measure("db_write", table = tableName) { inner.delete(tableName, relation) }
    override suspend fun deleteMany(tableName: String, relations: List<StoreRelation>) = measure("db_write", table = tableName) { inner.deleteMany(tableName, relations) }
}
