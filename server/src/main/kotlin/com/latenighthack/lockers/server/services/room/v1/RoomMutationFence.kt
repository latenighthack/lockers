package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.RoomId
import kotlinx.coroutines.withContext
import kotlin.coroutines.*

class RoomOwnershipLost : IllegalStateException("Room ownership changed before commit")

/** Captured at admission, enforced by the storage connection that commits the mutation. */
abstract class RoomMutationFence : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<RoomMutationFence>
    abstract suspend fun <T> guard(driver: SqlDriver?, block: suspend () -> T): T
}

class CheckedRoomMutationFence(private val allowPersistent: Boolean = false, private val valid: suspend () -> Boolean) : RoomMutationFence() {
    override suspend fun <T> guard(driver: SqlDriver?, block: suspend () -> T): T {
        require(driver == null || allowPersistent) { "Persistent ownership implementations must provide a storage-backed commit fence" }
        if (!valid()) throw RoomOwnershipLost()
        val result = block()
        if (!valid()) throw RoomOwnershipLost()
        return result
    }
}

class ClaimMutationFence(private val room: RoomId, private val nodeId: String, private val epoch: Long) : RoomMutationFence() {
    override suspend fun <T> guard(driver: SqlDriver?, block: suspend () -> T): T {
        val sql = requireNotNull(driver) { "JDBC claims require a fenced SQL storage driver" }
        suspend fun validate() {
            val query = sql.selectAll("SELECT epoch FROM room_claim WHERE room_id = ? AND node_id = ? AND epoch = ? AND expires_at >= clock_timestamp() FOR UPDATE")
            try {
                query.bindBytes(0, room.rawValue); query.bindText(1, nodeId); query.bindInt(2, epoch)
                if (!query.step()) throw RoomOwnershipLost()
            } finally { query.finalize() }
        }
        validate()
        val result = block()
        validate()
        return result
    }
}

class LeaseMutationFence(private val fenceKey: Long?, private val token: Long, private val valid: suspend () -> Boolean) : RoomMutationFence() {
    override suspend fun <T> guard(driver: SqlDriver?, block: suspend () -> T): T {
        if (!valid()) throw RoomOwnershipLost()
        if (driver != null) {
            require(fenceKey != null && token > 0) { "Persistent ring writes require a durable coordinator fence" }
            val query = driver.selectAll("SELECT token FROM shard_fence WHERE fence_key = ? AND token = ? FOR UPDATE")
            try { query.bindInt(0, fenceKey); query.bindInt(1, token); if (!query.step()) throw RoomOwnershipLost() }
            finally { query.finalize() }
        }
        val result = block()
        if (!valid()) throw RoomOwnershipLost()
        return result
    }
}

private class FenceChecked(val backend: Any) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<FenceChecked>
}

/** The guard executes inside JdbcDriver.transaction, including its final commit. */
class FencedSqlDriver(private val inner: ManagedSqlDriver) : ManagedSqlDriver by inner {
    private suspend fun <T> guarded(block: suspend () -> T): T {
        val proof = coroutineContext[RoomMutationFence] ?: return block()
        if (coroutineContext[FenceChecked]?.backend === this) return block()
        return withContext(FenceChecked(this)) { proof.guard(inner, block) }
    }
    override suspend fun <T> transaction(block: suspend () -> T): T = inner.transaction { guarded(block) }
    override suspend fun <T> transaction(lockKey: String, block: suspend () -> T): T = inner.transaction(lockKey) { guarded(block) }
}

/** In-memory embedding uses the same commit guard contract as SQL. */
class FencedMemoryDelegate(private val inner: LifecycleStoreDelegate) : LifecycleStoreDelegate by inner,
    ScopedStoreDelegate, IndexedQueryDelegate {
    override val supportsTransactions get() = (inner as TransactionalStoreDelegate).supportsTransactions
    private suspend fun <T> guarded(block: suspend () -> T): T {
        val proof = coroutineContext[RoomMutationFence] ?: return block()
        if (coroutineContext[FenceChecked]?.backend === this) return block()
        return withContext(FenceChecked(this)) { proof.guard(null, block) }
    }
    override suspend fun <T> transaction(block: suspend () -> T): T = (inner as TransactionalStoreDelegate).transaction { guarded(block) }
    override suspend fun <T> transaction(lockKey: String, block: suspend () -> T): T = (inner as TransactionalStoreDelegate).transaction(lockKey) { guarded(block) }
    override suspend fun <T> transaction(stores: Set<String>, mode: TransactionMode, block: suspend () -> T): T = (inner as ScopedStoreDelegate).transaction(stores, mode) { guarded(block) }
    override suspend fun query(tableName: String, query: IndexedQuery, identity: String, version: Int) = (inner as IndexedQueryDelegate).query(tableName, query, identity, version)
    override suspend fun count(tableName: String, query: IndexedQuery) = (inner as IndexedQueryDelegate).count(tableName, query)
    override suspend fun deleteBatch(tableName: String, query: IndexedQuery, identity: String, version: Int) = (inner as IndexedQueryDelegate).deleteBatch(tableName, query, identity, version)
}
