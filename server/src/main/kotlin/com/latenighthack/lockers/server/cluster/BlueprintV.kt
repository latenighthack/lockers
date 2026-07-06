package com.latenighthack.lockers.server.cluster

import com.latenighthack.lockers.server.LockersConfig
import com.latenighthack.lockers.server.ShardingConfig
import com.latenighthack.lockers.sharding.Keyspace
import com.latenighthack.lockers.sharding.RingAssignment
import com.latenighthack.lockers.sharding.ShardCounts
import com.latenighthack.lockers.sharding.ShardRouter
import com.latenighthack.lockers.sharding.spi.Membership
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.sql.DriverManager

/**
 * Room + session shard counts folded into one [ShardCounts]. The session ring routes under the
 * reserved [Keyspace.SESSION] keyspace, so its independent count is expressed as a per-keyspace
 * override; the two rings then co-locate on one node set (single-fleet deployment) while still
 * sharding independently. User `LOCKERS_KEYSPACE_SHARD_COUNTS` overrides layer on top.
 */
private fun ShardingConfig.shardCounts(): ShardCounts {
    val base = ShardCounts.parse(shardCountDefault, keyspaceShardCounts)
    return ShardCounts(base.default, base.perKeyspace + (Keyspace.SESSION to sessionShardCount))
}

/**
 * Assembles a [ClusterContext] from the Blueprint V (VM/bare-metal + Postgres) implementations, or
 * returns `null` when sharding is not configured (no peers / no node id) so the caller stays a pure
 * monolith.
 *
 * Wiring (each maps a `:sharding-core` SPI to a concrete tech — see `deploy/README.md`):
 *  - [Membership] / `PeerLocator` -> [StaticMembership] / [StaticPeerLocator] over `LOCKERS_PEERS`.
 *  - `ShardMapSource`             -> [PostgresShardMapSource] over the `shard_map` table.
 *  - east-west transport          -> [PeerConnectionPool] + [HttpSessionGateways]/[HttpPushGateways].
 *
 * The `OwnershipCoordinator` ([AdvisoryLockCoordinator]) is built by [ownershipCoordinator] and
 * consumed by the per-shard owner lifecycle (M5), not by `ClusterContext` itself.
 *
 * Both rings are co-located on one membership + one shard-map source (the single-fleet topology
 * the Railway/StatefulSet blueprint uses); a separate gateway pool would call `ShardRouter.create`
 * with distinct session membership instead.
 */
object BlueprintV {
    class Wiring(
        val context: ClusterContext,
        val membership: Membership,
        val pool: PeerConnectionPool,
        val shardSource: PostgresShardMapSource,
        private val databaseUrl: String?,
    ) {
        /**
         * Ring-aware readiness for `/readyz`, kept here (not in `:server:run`, which can't see
         * `:sharding-core` types) so `Main` only handles a `String?`. The static-membership ring is
         * available from t=0, so readiness reduces to DB reachability when a DB is configured.
         * Returns `null` when ready, else a short human reason.
         */
        suspend fun notReadyReason(): String? {
            if (databaseUrl != null && !pingDb(databaseUrl)) return "database unreachable"
            return null
        }
    }

    /**
     * @return the assembled [Wiring], or `null` if [config] has no peer configuration.
     * @throws IllegalArgumentException on malformed peer/shard config (fail fast at boot).
     */
    suspend fun wire(config: LockersConfig, scope: CoroutineScope): Wiring? {
        val sharding = config.sharding
        val topology = ClusterTopology.fromEnv(sharding.peers, sharding.nodeId, sharding.advertiseAddr)
            ?: return null
        val databaseUrl = config.databaseUrl

        val membership = StaticMembership(topology)
        val locator = StaticPeerLocator(topology)

        // Fallback ring assignment: how shards map onto the static node set before/absent a
        // shard_map row. Once the control plane writes rows at a higher epoch, they take over.
        val fallback = RingAssignment(topology.nodes, sharding.ringVnodes)
        val source = PostgresShardMapSource(
            gateway = gatewayFor(databaseUrl),
            counts = sharding.shardCounts(),
            fallbackAssignment = fallback,
        )
        source.bind(scope)

        val router = ShardRouter.coLocated(membership, source, locator, scope)

        val pool = PeerConnectionPool()
        val context = ClusterContext(
            router = router,
            sessionGateways = HttpSessionGateways(pool),
            pushGateways = HttpPushGateways(pool),
            // Enable M5 fenced ownership when a DB is present (advisory locks need a Postgres
            // session); without a DB the cluster degrades to route-local gating.
            ownerCoordinator = ownershipCoordinator(config),
        )
        return Wiring(context, membership, pool, source, databaseUrl)
    }

    /**
     * The fencing coordinator for owned shards (advisory locks over the same Postgres). Returns
     * `null` when there is no DB URL — advisory locks require a real Postgres session, so a
     * DB-less cluster cannot fence (dev/monolith only). Consumed by the M5 owner lifecycle.
     */
    fun ownershipCoordinator(config: LockersConfig): AdvisoryLockCoordinator? {
        val url = config.databaseUrl ?: return null
        return AdvisoryLockCoordinator(JdbcAdvisoryLockGateway(url))
    }

    private fun gatewayFor(databaseUrl: String?): ShardMapGateway {
        // Without a DB the shard_map table can't be read; route purely off static membership by
        // returning no rows (the source then holds the ring computed from LOCKERS_PEERS).
        if (databaseUrl == null) return ShardMapGateway { emptyList() }
        return JdbcShardMapGateway(databaseUrl)
    }

    private fun ShardMapGateway(read: () -> List<ShardMapRow>): ShardMapGateway =
        object : ShardMapGateway {
            override suspend fun readRows(): List<ShardMapRow> = read()
        }

    /** Cheap DB liveness probe for `/readyz` (a valid session within 1s). */
    private suspend fun pingDb(jdbcUrl: String): Boolean = withContext(Dispatchers.IO) {
        runCatching { DriverManager.getConnection(jdbcUrl).use { it.isValid(1) } }.getOrDefault(false)
    }
}
