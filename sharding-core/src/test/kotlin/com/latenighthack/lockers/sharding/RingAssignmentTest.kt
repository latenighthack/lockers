package com.latenighthack.lockers.sharding

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isLessThan
import kotlin.test.Test

class RingAssignmentTest {
    private fun nodes(n: Int) = (0 until n).map { NodeId("node-$it") }.toSet()

    @Test
    fun `deterministic across instances with the same node set`() {
        val a = RingAssignment(nodes(5))
        val b = RingAssignment(nodes(5))
        repeat(1000) { s ->
            assertThat(b.owner(Keyspace(1), ShardId(s))).isEqualTo(a.owner(Keyspace(1), ShardId(s)))
        }
    }

    @Test
    fun `distributes shards roughly evenly across nodes`() {
        val ns = nodes(8)
        val ring = RingAssignment(ns)
        val shards = 4096
        val counts = HashMap<NodeId, Int>()
        for (s in 0 until shards) {
            counts.merge(ring.owner(Keyspace(1), ShardId(s)), 1, Int::plus)
        }
        val ideal = shards.toDouble() / ns.size
        assertThat(counts.values.maxOrNull()!!.toDouble()).isLessThan(ideal * 1.5)
        assertThat(counts.values.minOrNull()!!.toDouble()).isGreaterThan(ideal * 0.5)
    }

    @Test
    fun `adding a node moves only a minimal fraction of shards`() {
        val n = 5
        val before = RingAssignment(nodes(n))
        val after = RingAssignment(nodes(n + 1))
        val shards = 4096
        var moved = 0
        for (s in 0 until shards) {
            if (before.owner(Keyspace(1), ShardId(s)) != after.owner(Keyspace(1), ShardId(s))) moved++
        }
        val fraction = moved.toDouble() / shards
        val ideal = 1.0 / (n + 1) // ~0.167
        assertThat(fraction).isGreaterThan(0.0)
        assertThat(fraction).isLessThan(ideal * 2.0) // far below a naive full reshuffle (~0.83)
    }
}
