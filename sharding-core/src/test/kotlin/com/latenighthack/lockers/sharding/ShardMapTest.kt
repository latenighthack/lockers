package com.latenighthack.lockers.sharding

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isLessThan
import assertk.assertions.isNotEmpty
import kotlin.test.Test

class ShardMapTest {
    private fun mapFor(nodeCount: Int, counts: ShardCounts, epoch: Long): ShardMap =
        ShardMap(Epoch(epoch), counts, RingAssignment((0 until nodeCount).map { NodeId("n$it") }.toSet()))

    @Test
    fun `movedShards reports exactly the shards that changed owner`() {
        val counts = ShardCounts(256)
        val before = mapFor(4, counts, 0)
        val after = mapFor(5, counts, 1)
        val moved = after.movedShards(before, Keyspace(1))
        for (s in 0 until 256) {
            val shard = ShardId(s)
            val changed = before.assignment.owner(Keyspace(1), shard) !=
                after.assignment.owner(Keyspace(1), shard)
            assertThat(moved.contains(shard)).isEqualTo(changed)
        }
        assertThat(moved).isNotEmpty()
        assertThat(moved.size).isLessThan(256) // a rebalance, not a full reshuffle
    }

    @Test
    fun `changing one keyspace's shard count does not disturb other keyspaces`() {
        val before = mapFor(4, ShardCounts(256, mapOf(Keyspace(1) to 128)), 0)
        val after = mapFor(4, ShardCounts(256, mapOf(Keyspace(1) to 256)), 1)
        // keyspace 2 uses the default (256) in both maps over the same nodes ⇒ identical owners
        repeat(500) { i ->
            val room = "r$i".encodeToByteArray()
            assertThat(after.owner(Keyspace(2), room))
                .isEqualTo(before.owner(Keyspace(2), room))
        }
    }

    @Test
    fun `session axis routes deterministically on the same map`() {
        val map = mapFor(6, ShardCounts(128), 0)
        repeat(300) { i ->
            val session = "sess-$i".encodeToByteArray()
            assertThat(map.owner(Keyspace.SESSION, session)).isEqualTo(map.owner(Keyspace.SESSION, session))
        }
    }
}
