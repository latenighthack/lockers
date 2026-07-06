package com.latenighthack.lockers.server.cluster

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.latenighthack.lockers.sharding.Epoch
import com.latenighthack.lockers.sharding.Keyspace
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.RingAssignment
import com.latenighthack.lockers.sharding.ShardCounts
import com.latenighthack.lockers.sharding.ShardId
import com.latenighthack.lockers.sharding.ShardMap
import kotlin.test.Test

/** Pure row->ShardMap mapping coverage (no JDBC): epoch selection, fallback, empty handling. */
class PostgresShardMapSourceTest {
    private val counts = ShardCounts(default = 4)
    private val fallbackNodes = setOf(NodeId("a"), NodeId("b"))
    private val fallback = RingAssignment(fallbackNodes)
    private val bootstrap = ShardMap(Epoch(0), counts, fallback)

    @Test
    fun emptyTableYieldsBootstrapRing() {
        val map = shardMapFromRows(emptyList(), counts, fallback, empty = bootstrap)
        assertThat(map).isEqualTo(bootstrap)
    }

    @Test
    fun tableRowsOverrideOwnerAtHighestEpoch() {
        val rows = listOf(
            ShardMapRow(epoch = 7, keyspace = 1, shard = 0, nodeId = "a"),
            ShardMapRow(epoch = 7, keyspace = 1, shard = 1, nodeId = "b"),
            ShardMapRow(epoch = 7, keyspace = 1, shard = 2, nodeId = "a"),
            ShardMapRow(epoch = 7, keyspace = 1, shard = 3, nodeId = "b"),
        )
        val map = shardMapFromRows(rows, counts, fallback, empty = bootstrap)
        assertThat(map.epoch).isEqualTo(Epoch(7))
        assertThat(map.assignment.owner(Keyspace(1), ShardId(0))).isEqualTo(NodeId("a"))
        assertThat(map.assignment.owner(Keyspace(1), ShardId(1))).isEqualTo(NodeId("b"))
        assertThat(map.assignment.owner(Keyspace(1), ShardId(3))).isEqualTo(NodeId("b"))
    }

    @Test
    fun onlyHighestEpochRowsAreApplied() {
        // Two epochs present; the stale epoch-2 rows for the same shard are ignored.
        val rows = listOf(
            ShardMapRow(epoch = 2, keyspace = 1, shard = 0, nodeId = "a"),
            ShardMapRow(epoch = 5, keyspace = 1, shard = 0, nodeId = "b"),
        )
        val map = shardMapFromRows(rows, counts, fallback, empty = bootstrap)
        assertThat(map.epoch).isEqualTo(Epoch(5))
        assertThat(map.assignment.owner(Keyspace(1), ShardId(0))).isEqualTo(NodeId("b"))
    }

    @Test
    fun unlistedShardFallsBackToRing() {
        // Table only assigns shard 0 of keyspace 1; every other (keyspace, shard) uses the ring.
        val rows = listOf(ShardMapRow(epoch = 9, keyspace = 1, shard = 0, nodeId = "a"))
        val map = shardMapFromRows(rows, counts, fallback, empty = bootstrap)
        val ringOwnerForShard2 = fallback.owner(Keyspace(1), ShardId(2))
        assertThat(map.assignment.owner(Keyspace(1), ShardId(0))).isEqualTo(NodeId("a"))
        assertThat(map.assignment.owner(Keyspace(1), ShardId(2))).isEqualTo(ringOwnerForShard2)
    }

    @Test
    fun sessionKeyspaceRowsRouteIndependently() {
        // The session ring routes under Keyspace.SESSION; a row there must not affect room keyspaces.
        val rows = listOf(
            ShardMapRow(epoch = 3, keyspace = Keyspace.SESSION.value, shard = 0, nodeId = "b"),
        )
        val map = shardMapFromRows(rows, counts, fallback, empty = bootstrap)
        assertThat(map.assignment.owner(Keyspace.SESSION, ShardId(0))).isEqualTo(NodeId("b"))
        // Room keyspace 1 shard 0 unaffected -> ring fallback.
        assertThat(map.assignment.owner(Keyspace(1), ShardId(0)))
            .isEqualTo(fallback.owner(Keyspace(1), ShardId(0)))
    }
}
