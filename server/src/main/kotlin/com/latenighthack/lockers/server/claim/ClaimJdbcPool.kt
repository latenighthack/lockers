package com.latenighthack.lockers.server.claim

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withContext
import kotlinx.coroutines.ensureActive
import kotlin.coroutines.coroutineContext
import java.sql.Connection

/**
 * A tiny bounded JDBC connection pool for the claim stores. The whole coordination workload is one
 * batched renew per node per interval plus a single-row upsert per cold room/session miss, so two
 * connections (one for the renew loop, one for foreground claims/lookups) is the ceiling by design —
 * this is what makes claim mode O(nodes) on Postgres connections instead of the ring's O(shards).
 *
 * Deliberately not Hikari: `postgresql` is a runtime-only dependency of `:server:run`, so this uses
 * only the JDK's `java.sql` API (the [JdbcShardMapGateway] precedent) and adds no dependency to the
 * published `lockers-server` artifact.
 *
 * Failure policy: a connection is validated on borrow and rebuilt once; an error inside [block]
 * closes the connection and propagates. Resilience (demote-on-renew-failure) lives in
 * [ClaimRenewalService], which must see DB unreachability rather than have it hidden here.
 */
class ClaimJdbcPool @JvmOverloads constructor(
    private val jdbcUrl: String,
    size: Int = 2,
    private val limits: com.latenighthack.ktstore.PostgresJdbcLimits = com.latenighthack.ktstore.PostgresJdbcLimits(),
) : AutoCloseable {
    // Each slot holds a connection or null (not yet created / discarded after a failure).
    private val slots = Channel<Connection?>(size.also { require(it > 0) { "ClaimJdbcPool size must be positive" } })
    private val closed = java.util.concurrent.atomic.AtomicBoolean(false)

    init {
        repeat(size) { check(slots.trySend(null).isSuccess) }
    }

    suspend fun <T> withConnection(block: (Connection) -> T): T {
        check(!closed.get()) { "ClaimJdbcPool is closed" }
        var conn = slots.receive()
        try {
            val result = withContext(Dispatchers.IO) {
                check(!closed.get()) { "ClaimJdbcPool is closed" }
                if (conn == null || !isUsable(conn!!)) {
                    runCatching { conn?.close() }
                    conn = com.latenighthack.lockers.server.tools.openPostgresConnection(jdbcUrl, limits)
                }
                block(conn!!)
            }
            // A slot send only fails after close(); don't leak the borrowed connection then.
            if (closed.get() || slots.trySend(conn).isFailure) runCatching { conn?.close() }
            return result
        } catch (t: Throwable) {
            runCatching { withContext(kotlinx.coroutines.NonCancellable) { conn?.close() } }
            slots.trySend(null)
            coroutineContext.ensureActive()
            throw t
        }
    }

    private fun isUsable(conn: Connection): Boolean =
        runCatching { !conn.isClosed && conn.isValid(1) }.getOrDefault(false)

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        slots.close()
        while (true) {
            val received = slots.tryReceive()
            if (received.isFailure) break
            runCatching { received.getOrNull()?.close() }
        }
    }
}
