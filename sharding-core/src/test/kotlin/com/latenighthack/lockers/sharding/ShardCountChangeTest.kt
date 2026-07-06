package com.latenighthack.lockers.sharding

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isTrue
import com.latenighthack.lockers.sharding.inmem.SimCluster
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

/**
 * M7 — online per-keyspace shard-count change (Level-1 repartition). Proves the [ShardMapSource]
 * emits a higher-epoch [ShardMap] when only the counts change (node set fixed), that repartitioning
 * one keyspace leaves every other keyspace's routing byte-identical, and that only the rooms whose
 * shard actually moved are reported by [ShardMap.movedShards] — the exact set that must be handed off.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class ShardCountChangeTest {

    private val K = Keyspace(1L)
    private val OTHER = Keyspace(2L)

    @Test
    fun `changing a keyspace count bumps the epoch over an unchanged node set`() = runTest {
        val sim = SimCluster(
            listOf(NodeId("n0"), NodeId("n1"), NodeId("n2")),
            ShardCounts(default = 256),
        )
        val source = sim.roomSourceFor(NodeId("n0"), backgroundScope)
        runCurrent()
        val before = source.current()

        sim.setRoomCounts(ShardCounts(default = 256, perKeyspace = mapOf(K to 512)))
        runCurrent()
        val after = source.current()

        assertThat(after.epoch.value).isGreaterThan(before.epoch.value)
        assertThat(after.counts.forKeyspace(K)).isEqualTo(512)
        // Same node set — assignment node membership unchanged.
        assertThat(after.nodes).isEqualTo(before.nodes)
    }

    @Test
    fun `repartitioning one keyspace leaves other keyspaces byte-identical`() = runTest {
        val sim = SimCluster(
            listOf(NodeId("n0"), NodeId("n1"), NodeId("n2")),
            ShardCounts(default = 256),
        )
        val source = sim.roomSourceFor(NodeId("n0"), backgroundScope)
        runCurrent()
        val before = source.current()

        sim.setRoomCounts(ShardCounts(default = 256, perKeyspace = mapOf(K to 128)))
        runCurrent()
        val after = source.current()

        // Every OTHER-keyspace room resolves to the identical owner before and after (count for OTHER
        // is unchanged at 256, and the ring/node set is unchanged).
        repeat(1_000) { i ->
            val room = "room-$i".encodeToByteArray()
            assertThat(after.owner(OTHER, room)).isEqualTo(before.owner(OTHER, room))
        }
        // K's repartition (256 -> 128) actually moves a non-trivial fraction of K's rooms.
        var kMoved = 0
        repeat(1_000) { i ->
            val room = "room-$i".encodeToByteArray()
            if (after.owner(K, room) != before.owner(K, room)) kMoved++
        }
        assertThat(kMoved).isGreaterThan(0)
    }

    @Test
    fun `movedShards reports exactly the shards whose owner changed on a count change`() = runTest {
        val sim = SimCluster(
            listOf(NodeId("n0"), NodeId("n1")),
            ShardCounts(default = 64),
        )
        val source = sim.roomSourceFor(NodeId("n0"), backgroundScope)
        runCurrent()
        val before = source.current()

        sim.setRoomCounts(ShardCounts(default = 64, perKeyspace = mapOf(K to 96)))
        runCurrent()
        val after = source.current()

        val moved = after.movedShards(before, K)
        val now = after.counts.forKeyspace(K)   // 96
        val was = before.counts.forKeyspace(K)  // 64
        for (s in 0 until maxOf(now, was)) {
            val shard = ShardId(s)
            val nowOwner = if (s < now) after.assignment.owner(K, shard) else null
            val wasOwner = if (s < was) before.assignment.owner(K, shard) else null
            assertThat(moved.contains(shard)).isEqualTo(nowOwner != wasOwner)
        }
        // Growing 64 -> 96 introduces 32 brand-new shards, so at least those are "moved".
        assertThat(moved.size).isGreaterThan(0)
    }

    @Test
    fun `every node sees the same repartition at the same counts`() = runTest {
        val sim = SimCluster(
            listOf(NodeId("n0"), NodeId("n1"), NodeId("n2")),
            ShardCounts(default = 128),
        )
        val r0 = sim.routerFor(NodeId("n0"), backgroundScope)
        val r1 = sim.routerFor(NodeId("n1"), backgroundScope)
        runCurrent()

        sim.setRoomCounts(ShardCounts(default = 128, perKeyspace = mapOf(K to 256)))
        runCurrent()

        // Both nodes advanced to the same epoch and agree on every room's owner post-repartition.
        assertThat(r0.roomMap().epoch).isEqualTo(r1.roomMap().epoch)
        repeat(500) { i ->
            val room = "room-$i".encodeToByteArray()
            assertThat(r0.roomMap().owner(K, room) == r1.roomMap().owner(K, room)).isTrue()
        }
    }

    @Test
    fun `a redundant count change does not perturb the epoch`() = runTest {
        val sim = SimCluster(listOf(NodeId("n0"), NodeId("n1")), ShardCounts(default = 64))
        val source = sim.roomSourceFor(NodeId("n0"), backgroundScope)
        runCurrent()
        val before = source.current().epoch.value

        sim.setRoomCounts(ShardCounts(default = 64)) // identical to current
        runCurrent()

        assertThat(source.current().epoch.value).isEqualTo(before)
    }
}
