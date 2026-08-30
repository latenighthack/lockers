package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.server.LockersConfig
import com.latenighthack.lockers.server.cluster.PeerConnectionPool
import io.micrometer.core.instrument.MeterRegistry

/**
 * Everything claim-based multi-node mode needs, bundled for `MonolithComponent`. Deliberately not
 * a `ClusterContext`: claim mode has no ring, no shard map and no coordinator — just the two
 * claim/registry stores, this node's identity, and the east-west connection pool. Constructed via
 * [fromConfig] (production, JDBC stores) or directly by the test harness (in-memory stores).
 */
class ClaimContext(
    val nodeId: String,
    val advertiseAddr: String,
    val roomClaims: RoomClaimStore,
    val sessionGateways: SessionGatewayStore,
    val pool: PeerConnectionPool,
    val ttlMs: Long,
    val renewMs: Long,
    val meters: ClaimMetrics,
    /** Set by [fromConfig] so [close] can release the coordination pool it created. */
    private val ownedJdbcPool: ClaimJdbcPool? = null,
) : AutoCloseable {
    /** Releases the east-west connection pool and (when [fromConfig]-built) the JDBC pool. */
    override fun close() {
        pool.close()
        ownedJdbcPool?.close()
    }

    companion object {
        /**
         * The canonical claim-mode boot path for embedders and the runnable server alike:
         * validates the mode's env contract (fail fast, messages name the env vars), builds the
         * 2-connection [ClaimJdbcPool] + both JDBC stores (idempotent DDL via `prepare()`), and
         * bundles the east-west [PeerConnectionPool].
         *
         * Returns null unless `LOCKERS_ROOM_OWNERSHIP=claim` — `local` (and the deprecated `ring`,
         * which has its own wiring) need no context. Throws [IllegalStateException] on a violated
         * requirement; callers with operator-facing boot UX should surface the message and exit.
         *
         * [fallbackJdbcUrl] backs `LOCKERS_DB_URL` when unset — embedders that already carry a
         * Postgres URL (e.g. an app's `DATABASE_URL`) pass its `jdbc:postgresql:` form here so
         * claim mode needs only the mode + node identity vars.
         */
        suspend fun fromConfig(
            config: LockersConfig,
            meterRegistry: MeterRegistry,
            fallbackJdbcUrl: String? = null,
        ): ClaimContext? {
            if (config.roomOwnership != "claim") return null
            val jdbcUrl = config.databaseUrl
                ?: fallbackJdbcUrl?.takeIf { it.isNotBlank() }
                ?: error("LOCKERS_ROOM_OWNERSHIP=claim requires LOCKERS_DB_URL.")
            val nodeId = config.sharding.nodeId?.takeIf { it.isNotBlank() }
                ?: error("LOCKERS_ROOM_OWNERSHIP=claim requires LOCKERS_NODE_ID.")
            val advertiseAddr = config.sharding.advertiseAddr?.takeIf { it.isNotBlank() }
                ?: error("LOCKERS_ROOM_OWNERSHIP=claim requires LOCKERS_ADVERTISE_ADDR (peer-reachable host:port).")
            check(config.claimRenewMs > 0 && config.claimRenewMs < config.claimTtlMs / 2) {
                "LOCKERS_CLAIM_RENEW_MS (${config.claimRenewMs}) must be > 0 and < half of " +
                    "LOCKERS_CLAIM_TTL_MS (${config.claimTtlMs})."
            }
            val jdbcPool = ClaimJdbcPool(jdbcUrl)
            return ClaimContext(
                nodeId = nodeId,
                advertiseAddr = advertiseAddr,
                roomClaims = JdbcRoomClaimStore(jdbcPool).also { it.prepare() },
                sessionGateways = JdbcSessionGatewayStore(jdbcPool).also { it.prepare() },
                pool = PeerConnectionPool(),
                ttlMs = config.claimTtlMs,
                renewMs = config.claimRenewMs,
                meters = ClaimMetrics(meterRegistry),
                ownedJdbcPool = jdbcPool,
            )
        }
    }
}
