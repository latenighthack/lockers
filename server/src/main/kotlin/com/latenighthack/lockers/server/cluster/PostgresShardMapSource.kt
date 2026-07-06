package com.latenighthack.lockers.server.cluster

import com.latenighthack.lockers.sharding.Assignment
import com.latenighthack.lockers.sharding.Epoch
import com.latenighthack.lockers.sharding.HashPartitionFunction
import com.latenighthack.lockers.sharding.Keyspace
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.PartitionFunction
import com.latenighthack.lockers.sharding.ShardCounts
import com.latenighthack.lockers.sharding.ShardId
import com.latenighthack.lockers.sharding.ShardMap
import com.latenighthack.lockers.sharding.spi.ShardMapSource
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.sql.Connection
import java.sql.DriverManager
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * One row of the `shard_map(epoch, keyspace, shard, node_id)` control table. The table is the
 * durable, epoch-stamped source of truth for shard→node assignment in the Postgres blueprint; a
 * control-plane / reshard tool writes it, every node reads the highest epoch.
 */
data class ShardMapRow(val epoch: Long, val keyspace: Long, val shard: Int, val nodeId: String)

/**
 * Freezes the shard→node table read from [ShardMapRow]s into an [Assignment]. Because the table is
 * an explicit assignment (not a ring), this is an exact lookup table: `owner(keyspace, shard)` is
 * whatever the highest-epoch row says. Rows of lower epochs are ignored; the whole map advances
 * atomically at the max epoch. Unlisted `(keyspace, shard)` pairs fall back to [fallback] (the
 * ring assignment computed from static membership) so a partially-populated table still routes.
 */
class TableAssignment(
    rows: List<ShardMapRow>,
    epoch: Long,
    private val fallback: Assignment,
) : Assignment {
    private val owners: Map<Long, Map<Int, NodeId>> =
        rows.filter { it.epoch == epoch }
            .groupBy { it.keyspace }
            .mapValues { (_, ksRows) -> ksRows.associate { it.shard to NodeId(it.nodeId) } }

    override val nodes: Set<NodeId> =
        (owners.values.flatMap { it.values.toList() } + fallback.nodes).toSet()

    override fun owner(keyspace: Keyspace, shard: ShardId): NodeId =
        owners[keyspace.value]?.get(shard.value) ?: fallback.owner(keyspace, shard)
}

/**
 * Pure mapping from a set of table rows to a [ShardMap]. Factored out of the JDBC path so the
 * epoch selection + row→assignment logic is unit-testable with zero infrastructure. When the table
 * is empty (fresh cluster, control plane hasn't written yet), returns [empty] — the ring computed
 * from static membership — so the node still routes deterministically until the table populates.
 */
fun shardMapFromRows(
    rows: List<ShardMapRow>,
    counts: ShardCounts,
    fallback: Assignment,
    partition: PartitionFunction = HashPartitionFunction(),
    empty: ShardMap,
): ShardMap {
    if (rows.isEmpty()) return empty
    val epoch = rows.maxOf { it.epoch }
    return ShardMap(
        epoch = Epoch(epoch),
        counts = counts,
        assignment = TableAssignment(rows, epoch, fallback),
        partition = partition,
    )
}

/**
 * Reads shard-map rows out of a backing store. The production impl is [JdbcShardMapGateway] over a
 * Postgres connection; tests supply an in-memory fake so [PostgresShardMapSource]'s polling and
 * mapping are exercised without a database.
 */
interface ShardMapGateway {
    /** Every row of the shard_map table (all epochs); the source picks the max epoch. */
    suspend fun readRows(): List<ShardMapRow>
}

/**
 * A [ShardMapSource] backed by the Postgres `shard_map` table. On [bind] it polls (a `LISTEN`
 * upgrade is a drop-in optimization — see [JdbcShardMapGateway]) and, whenever the highest epoch
 * advances, publishes a fresh [ShardMap] to every routing decision on this node. Reuses
 * `LOCKERS_DB_URL`; no separate coordination store.
 *
 * The initial map is the ring computed from static membership (`fallbackAssignment`) so the node
 * routes correctly from t=0 even before the control plane has written any rows.
 */
class PostgresShardMapSource(
    private val gateway: ShardMapGateway,
    private val counts: ShardCounts,
    fallbackAssignment: Assignment,
    private val partition: PartitionFunction = HashPartitionFunction(),
    private val pollInterval: Duration = 5.seconds,
) : ShardMapSource {
    private val bootstrap: ShardMap = ShardMap(
        epoch = Epoch(0),
        counts = counts,
        assignment = fallbackAssignment,
        partition = partition,
    )
    private val fallback: Assignment = fallbackAssignment
    private val state = MutableStateFlow(bootstrap)

    @Volatile
    private var primed = false

    /** Starts polling the table; each higher-epoch snapshot is pushed onto [watch]. */
    fun bind(scope: CoroutineScope) {
        scope.launch {
            // Prime once immediately so /readyz reflects the real map without waiting a full poll.
            refresh()
            while (isActive) {
                delay(pollInterval)
                runCatching { refresh() }
                    .onFailure { System.err.println("shard_map poll failed (keeping last map): ${it.message}") }
            }
        }
    }

    private suspend fun refresh() {
        val next = shardMapFromRows(gateway.readRows(), counts, fallback, partition, bootstrap)
        // Publish only on a strictly higher epoch (epochs are monotonic, so equal-epoch reads carry
        // identical assignment). This adopts the first real read over the synthetic bootstrap, then
        // stays silent on steady-state polls so collectors aren't churned with no-op ShardMap swaps.
        if (!primed || next.epoch > state.value.epoch) {
            primed = true
            state.value = next
        }
    }

    override suspend fun current(): ShardMap = state.value
    override fun watch(): Flow<ShardMap> = state.asStateFlow()
}

/**
 * Production [ShardMapGateway]: reads the `shard_map` table over JDBC. Kept intentionally thin —
 * one prepared query — so it is compile-verified here and exercised for real only in a Postgres
 * environment. `postgresql` is a runtime-only dependency of `:server:run`, so this uses only the
 * JDK's `java.sql` API and the driver registers itself via the URL.
 *
 * LISTEN/NOTIFY upgrade: a control plane can `NOTIFY lockers_shard_map` on epoch change; a node can
 * hold a dedicated connection issuing `LISTEN lockers_shard_map` and refresh on notification rather
 * than on the poll timer. Polling is the correctness floor; LISTEN only lowers latency.
 */
class JdbcShardMapGateway(private val jdbcUrl: String) : ShardMapGateway {
    override suspend fun readRows(): List<ShardMapRow> = withContext(Dispatchers.IO) {
        connect().use { conn ->
            conn.prepareStatement(SELECT_ALL).use { st ->
                st.executeQuery().use { rs ->
                    buildList {
                        while (rs.next()) {
                            add(
                                ShardMapRow(
                                    epoch = rs.getLong("epoch"),
                                    keyspace = rs.getLong("keyspace"),
                                    shard = rs.getInt("shard"),
                                    nodeId = rs.getString("node_id"),
                                )
                            )
                        }
                    }
                }
            }
        }
    }

    private fun connect(): Connection = DriverManager.getConnection(jdbcUrl)

    companion object {
        const val TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS shard_map (
                epoch    BIGINT  NOT NULL,
                keyspace BIGINT  NOT NULL,
                shard    INTEGER NOT NULL,
                node_id  TEXT    NOT NULL,
                PRIMARY KEY (epoch, keyspace, shard)
            )
        """

        private const val SELECT_ALL =
            "SELECT epoch, keyspace, shard, node_id FROM shard_map"
    }
}
