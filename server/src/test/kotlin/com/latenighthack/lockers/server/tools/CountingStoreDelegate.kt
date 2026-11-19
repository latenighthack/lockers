package com.latenighthack.lockers.server.tools

import com.latenighthack.ktstore.BoundStoreKey
import com.latenighthack.ktstore.IndexedQuery
import com.latenighthack.ktstore.IndexedQueryDelegate
import com.latenighthack.ktstore.LifecycleStoreDelegate
import com.latenighthack.ktstore.QueryPage
import com.latenighthack.ktstore.ScopedStoreDelegate
import com.latenighthack.ktstore.StoreRelation
import com.latenighthack.ktstore.StoreRow
import com.latenighthack.ktstore.TransactionMode
import com.latenighthack.ktstore.TransactionalStoreDelegate
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.coroutineContext

/**
 * Test-only wrapper that counts every storage call the way [MeasuredStoreDelegate] defines a
 * "db op", split by whether a request [WriteTrace] is in scope. Indexed calls are keyed by
 * table and index. Logical lock transactions are counted separately: on PostgreSQL each one is
 * an extra `pg_advisory_xact_lock` statement.
 */
class CountingStoreDelegate(private val inner: LifecycleStoreDelegate, private val countLockRequests: Boolean = true) :
    LifecycleStoreDelegate by inner,
    ScopedStoreDelegate, IndexedQueryDelegate {
    data class Snapshot(
        val request: Map<String, Int>,
        val background: Map<String, Int>,
        val requestLocks: Int,
        val backgroundLocks: Int,
    ) {
        val requestOps get() = request.values.sum()
        val backgroundOps get() = background.values.sum()
        override fun toString() = "request=$requestOps (+$requestLocks advisory locks) ${request.toSortedMap()}\n" +
            "background=$backgroundOps (+$backgroundLocks advisory locks) ${background.toSortedMap()}"
    }
    private val request = ConcurrentHashMap<String, AtomicInteger>()
    private val background = ConcurrentHashMap<String, AtomicInteger>()
    private val requestLocks = AtomicInteger()
    private val backgroundLocks = AtomicInteger()
    private val indexed get() = inner as IndexedQueryDelegate

    fun reset() { request.clear(); background.clear(); requestLocks.set(0); backgroundLocks.set(0) }
    fun snapshot() = Snapshot(request.mapValues { it.value.get() }, background.mapValues { it.value.get() },
        requestLocks.get(), backgroundLocks.get())
    private suspend fun record(operation: String, table: String) {
        val target = if (coroutineContext[WriteTrace] != null) request else background
        target.computeIfAbsent("$operation:$table") { AtomicInteger() }.incrementAndGet()
    }

    override val supportsTransactions get() = (inner as? TransactionalStoreDelegate)?.supportsTransactions == true
    override suspend fun <T> transaction(block: suspend () -> T): T =
        (inner as TransactionalStoreDelegate).transaction(block)
    override suspend fun <T> transaction(lockKey: String, block: suspend () -> T): T {
        if (countLockRequests) recordLock()
        return (inner as TransactionalStoreDelegate).transaction(lockKey, block)
    }
    /** Counts one logical lock; a SQL driver wrapper calls this once per issued advisory lock. */
    suspend fun recordLock() {
        (if (coroutineContext[WriteTrace] != null) requestLocks else backgroundLocks).incrementAndGet()
    }
    override suspend fun <T> transaction(stores: Set<String>, mode: TransactionMode, block: suspend () -> T): T =
        (inner as ScopedStoreDelegate).transaction(stores, mode, block)
    override suspend fun query(tableName: String, query: IndexedQuery, identity: String, version: Int): QueryPage {
        record("query", "$tableName/${query.index.name}")
        return indexed.query(tableName, query, identity, version)
    }
    override suspend fun count(tableName: String, query: IndexedQuery): Long {
        record("count", "$tableName/${query.index.name}")
        return indexed.count(tableName, query)
    }
    override suspend fun deleteBatch(tableName: String, query: IndexedQuery, identity: String, version: Int): Int {
        record("deleteBatch", "$tableName/${query.index.name}")
        return indexed.deleteBatch(tableName, query, identity, version)
    }
    override suspend fun save(tableName: String, data: Any, keys: List<BoundStoreKey>) {
        record("save", tableName); inner.save(tableName, data, keys)
    }
    override suspend fun saveAll(tableName: String, rows: List<StoreRow>) {
        record("saveAll", tableName); inner.saveAll(tableName, rows)
    }
    override suspend fun getMany(tableName: String, relations: List<StoreRelation>): List<Any> {
        record("getMany", tableName); return inner.getMany(tableName, relations)
    }
    override suspend fun get(tableName: String, relation: StoreRelation?): Any? {
        record("get", tableName); return inner.get(tableName, relation)
    }
    override suspend fun getAll(tableName: String, relation: StoreRelation?): List<Any> {
        record("getAll", tableName); return inner.getAll(tableName, relation)
    }
    override suspend fun delete(tableName: String, relation: StoreRelation) {
        record("delete", tableName); inner.delete(tableName, relation)
    }
    override suspend fun deleteMany(tableName: String, relations: List<StoreRelation>) {
        record("deleteMany", tableName); inner.deleteMany(tableName, relations)
    }
}
