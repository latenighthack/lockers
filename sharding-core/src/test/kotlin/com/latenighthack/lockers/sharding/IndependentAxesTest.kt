package com.latenighthack.lockers.sharding

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import com.latenighthack.lockers.sharding.inmem.SimCluster
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

/**
 * The room (read/write) axis and the session/gateway axis shard independently: they can span
 * DIFFERENT node pools (separate WebSocket servers) and scale on their own — a room write fans
 * out across session-owning nodes by the session-id ring, exactly the original session/gateway
 * concept.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class IndependentAxesTest {
    private val roomNodes = listOf(NodeId("room-0"), NodeId("room-1"))
    private val gatewayNodes = listOf(NodeId("gw-0"), NodeId("gw-1"), NodeId("gw-2"))

    @Test
    fun `room and session rings resolve onto their own separate node pools`() = runTest {
        val sim = SimCluster(
            initialRoomNodes = roomNodes,
            roomCounts = ShardCounts(64),
            initialSessionNodes = gatewayNodes,
            sessionCounts = ShardCounts(128),
        )
        val router = sim.routerFor(NodeId("room-0"), backgroundScope)
        runCurrent()

        val roomOwners = mutableSetOf<NodeId>()
        val sessionOwners = mutableSetOf<NodeId>()
        repeat(400) { i ->
            roomOwners.add(router.routeRoom(Keyspace(1), "room-$i".encodeToByteArray()).node)
            sessionOwners.add(router.routeSession("sess-$i".encodeToByteArray()).node)
        }
        // Writes land only on the room tier; session subscription/delivery only on the gateway tier.
        assertThat(roomOwners).isEqualTo(roomNodes.toSet())
        assertThat(sessionOwners).isEqualTo(gatewayNodes.toSet())
    }

    @Test
    fun `a room node forwards all session delivery to the gateway tier`() = runTest {
        val sim = SimCluster(
            initialRoomNodes = roomNodes,
            roomCounts = ShardCounts(64),
            initialSessionNodes = gatewayNodes,
        )
        // This process is a room node; it is not in the gateway pool, so every session is remote.
        val router = sim.routerFor(NodeId("room-0"), backgroundScope)
        runCurrent()

        repeat(200) { i ->
            val route = router.routeSession("sess-$i".encodeToByteArray())
            assertThat(route.isLocal).isFalse()
        }
    }

    @Test
    fun `scaling the gateway tier does not move room ownership`() = runTest {
        val sim = SimCluster(
            initialRoomNodes = roomNodes,
            roomCounts = ShardCounts(64),
            initialSessionNodes = gatewayNodes,
        )
        val router = sim.routerFor(NodeId("room-0"), backgroundScope)
        runCurrent()

        val roomsBefore = (0 until 400).associateWith {
            router.routeRoom(Keyspace(1), "room-$it".encodeToByteArray()).node
        }
        val roomEpochBefore = router.roomEpoch

        sim.addSessionNode(NodeId("gw-3")); runCurrent()

        // Adding a WebSocket server bumped only the session ring; room ownership is untouched.
        assertThat(router.roomEpoch).isEqualTo(roomEpochBefore)
        (0 until 400).forEach {
            assertThat(router.routeRoom(Keyspace(1), "room-$it".encodeToByteArray()).node)
                .isEqualTo(roomsBefore.getValue(it))
        }
    }
}
