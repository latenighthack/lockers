package com.latenighthack.lockers.server.cluster

import com.latenighthack.lockers.sharding.Epoch
import com.latenighthack.lockers.sharding.Keyspace
import com.latenighthack.lockers.sharding.ShardId
import com.latenighthack.lockers.sharding.spi.OwnershipCoordinator
import com.latenighthack.lockers.sharding.spi.ShardLease
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.sql.Connection
import java.sql.DriverManager

/**
 * Postgres advisory locks as the cluster-wide fencing coordinator. `pg_try_advisory_lock(key)`
 * grants a **session-scoped** exclusive lock: at most one DB session holds a given key across the
 * whole cluster, and if that session dies (crash, network partition, connection drop) Postgres
 * releases the lock automatically. That last property is the whole point — a partitioned owner's
 * lease becomes invalid on its own without a heartbeat protocol.
 *
 * We derive one advisory key per `(keyspace, shard)` via [advisoryKey] and hold the lock on a
 * dedicated [Connection] for the lease's lifetime. `lease.isValid` is false the moment that
 * connection is closed or found dead ⇒ the losing node stops coordinating that shard.
 *
 * The JDBC is behind [AdvisoryLockGateway] so the key-derivation and the coordinator's
 * acquire/release protocol are unit-testable with an in-memory fake (no database).
 */
class AdvisoryLockCoordinator(private val gateway: AdvisoryLockGateway) : OwnershipCoordinator {
    override suspend fun acquire(keyspace: Keyspace, shard: ShardId, epoch: Epoch): ShardLease? {
        val key = advisoryKey(keyspace, shard)
        val session = gateway.tryLock(key) ?: return null
        val token = session.fencingToken.takeIf { it > 0 } ?: fakeTokens.incrementAndGet()
        return AdvisoryShardLease(keyspace, shard, epoch, token, key, session)
    }

    companion object {
        private val fakeTokens = java.util.concurrent.atomic.AtomicLong()
        /**
         * Maps a `(keyspace, shard)` pair onto the single 64-bit key `pg_try_advisory_lock(bigint)`
         * takes. Pure and deterministic so every node derives the *same* key for the same shard —
         * that identity is what makes the lock cluster-wide mutual exclusion. FNV-1a-64 over the
         * keyspace and shard bytes spreads pairs across the 64-bit space (collisions would falsely
         * couple two shards; 2^64 makes that negligible), and folding keyspace in keeps shard N of
         * two different keyspaces independent.
         */
        fun advisoryKey(keyspace: Keyspace, shard: ShardId): Long {
            var hash = FNV_OFFSET_BASIS
            fun mix(v: Long) {
                var x = v
                for (i in 0 until 8) {
                    hash = hash xor (x and 0xFF)
                    hash *= FNV_PRIME
                    x = x ushr 8
                }
            }
            mix(keyspace.value)
            mix(shard.value.toLong())
            return hash
        }

        private const val FNV_OFFSET_BASIS = -3750763034362895579L // 0xcbf29ce484222325
        private const val FNV_PRIME = 1099511628211L // 0x100000001b3
    }
}

/** A held advisory lock. [isValid] reflects the liveness of the underlying DB session. */
private class AdvisoryShardLease(
    override val keyspace: Keyspace,
    override val shard: ShardId,
    override val epoch: Epoch,
    override val fencingToken: Long,
    override val fencingKey: Long,
    private val session: AdvisoryLockSession,
) : ShardLease {
    @Volatile
    private var released = false
    private var closed = false
    private val releaseMutex = Mutex()

    override val isValid: Boolean get() = !released && session.isAlive()

    override suspend fun release() = withContext(NonCancellable) {
        releaseMutex.withLock {
            if (!closed) {
                released = true
                session.close()
                closed = true
            }
        }
    }
}

/**
 * Grants advisory-lock sessions. Production is [JdbcAdvisoryLockGateway]; tests supply a fake that
 * models "one holder per key" in memory so the coordinator's exclusivity and lease-validity
 * transitions are verified without Postgres.
 */
interface AdvisoryLockGateway {
    /** Attempts to acquire the advisory lock for [key]; returns its session, or null if held. */
    suspend fun tryLock(key: Long): AdvisoryLockSession?
}

/** A DB session holding one advisory lock. Closing it releases the lock (or the session dies). */
interface AdvisoryLockSession {
    val fencingToken: Long get() = 0
    /** False once the session/connection is closed or otherwise dead. */
    fun isAlive(): Boolean
    suspend fun close()
}

/**
 * Production [AdvisoryLockGateway]: `pg_try_advisory_lock` on a dedicated [Connection] per held
 * lock. Kept thin so it is compile-verified here and exercised for real only against Postgres
 * (`postgresql` is runtime-only on `:server:run`, so this uses only `java.sql`).
 */
class JdbcAdvisoryLockGateway(private val jdbcUrl: String) : AdvisoryLockGateway {
    override suspend fun tryLock(key: Long): AdvisoryLockSession? {
        // Ownership transfers only after the dispatcher handoff succeeds. Prompt cancellation
        // at that boundary otherwise loses the newly allocated connection and its advisory lock.
        var allocated: Connection? = null
        try {
            val session = withContext(Dispatchers.IO) {
                val conn = DriverManager.getConnection(jdbcUrl).also { allocated = it }
                val acquired = conn.prepareStatement("SELECT pg_try_advisory_lock(?)").use { st ->
                    st.setLong(1, key)
                    st.executeQuery().use { rs -> rs.next() && rs.getBoolean(1) }
                }
                if (!acquired) {
                    conn.close()
                    allocated = null
                    null
                } else {
                    conn.createStatement().use { it.execute("CREATE TABLE IF NOT EXISTS shard_fence (fence_key BIGINT PRIMARY KEY, token BIGINT NOT NULL)") }
                    val token = conn.prepareStatement("INSERT INTO shard_fence (fence_key, token) VALUES (?, 1) ON CONFLICT (fence_key) DO UPDATE SET token = shard_fence.token + 1 RETURNING token").use { statement ->
                        statement.setLong(1, key)
                        statement.executeQuery().use { result -> check(result.next()); result.getLong(1) }
                    }
                    JdbcAdvisoryLockSession(conn, token)
                }
            }
            allocated = null
            return session
        } catch (failure: Throwable) {
            allocated?.let { connection ->
                try { withContext(NonCancellable + Dispatchers.IO) { connection.close() } }
                catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
            }
            throw failure
        }
    }
}

/** Holds the advisory lock for as long as its [Connection] is open; closing releases the lock. */
private class JdbcAdvisoryLockSession(private val conn: Connection, override val fencingToken: Long) : AdvisoryLockSession {
    override fun isAlive(): Boolean = runCatching { !conn.isClosed && conn.isValid(1) }.getOrDefault(false)

    override suspend fun close() {
        // Closing the connection ends the session, which releases every advisory lock it held.
        withContext(NonCancellable + Dispatchers.IO) { conn.close() }
    }
}
