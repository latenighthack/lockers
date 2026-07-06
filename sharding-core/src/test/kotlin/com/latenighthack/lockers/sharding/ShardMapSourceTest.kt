package com.latenighthack.lockers.sharding

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import com.latenighthack.lockers.sharding.inmem.SimCluster
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

@OptIn(ExperimentalCoroutinesApi::class)
class ShardMapSourceTest {
    @Test
    fun `epoch increases monotonically on each membership change`() = runTest {
        val sim = SimCluster(listOf(NodeId("n0"), NodeId("n1")), ShardCounts(64))
        val source = sim.shardMapSourceFor(NodeId("n0"), backgroundScope)

        val epochs = mutableListOf<Long>()
        epochs.add(source.current().epoch.value)
        sim.addNode(NodeId("n2")); runCurrent(); epochs.add(source.current().epoch.value)
        sim.addNode(NodeId("n3")); runCurrent(); epochs.add(source.current().epoch.value)
        sim.removeNode(NodeId("n1")); runCurrent(); epochs.add(source.current().epoch.value)

        assertThat(epochs).isEqualTo(epochs.sorted())        // monotonic
        assertThat(epochs.toSet().size).isEqualTo(epochs.size) // strictly increasing (no dupes)
        assertThat(epochs.first()).isEqualTo(0L)
        assertThat(epochs.last()).isGreaterThan(0L)
    }

    @Test
    fun `every node resolves the same owner at the same topology`() = runTest {
        val sim = SimCluster(listOf(NodeId("n0"), NodeId("n1"), NodeId("n2")), ShardCounts(128))
        val r0 = sim.routerFor(NodeId("n0"), backgroundScope)
        val r1 = sim.routerFor(NodeId("n1"), backgroundScope)
        runCurrent()

        repeat(500) { i ->
            val room = "room-$i".encodeToByteArray()
            assertThat(r1.routeRoom(Keyspace(1), room).node)
                .isEqualTo(r0.routeRoom(Keyspace(1), room).node)
        }
    }

    @Test
    fun `a node routes its own owned rooms as local`() = runTest {
        val sim = SimCluster(listOf(NodeId("n0"), NodeId("n1")), ShardCounts(64))
        val r0 = sim.routerFor(NodeId("n0"), backgroundScope)
        runCurrent()

        var sawLocal = false
        var sawRemote = false
        repeat(200) { i ->
            val route = r0.routeRoom(Keyspace(1), "room-$i".encodeToByteArray())
            if (route.isLocal) {
                sawLocal = true
                assertThat(route.node).isEqualTo(NodeId("n0"))
            } else {
                sawRemote = true
                assertThat(route.node).isEqualTo(NodeId("n1"))
            }
        }
        assertThat(sawLocal).isEqualTo(true)
        assertThat(sawRemote).isEqualTo(true)
    }
}
