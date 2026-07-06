package com.latenighthack.lockers.sharding

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNull
import assertk.assertions.isTrue
import com.latenighthack.lockers.sharding.inmem.InMemoryMembership
import com.latenighthack.lockers.sharding.inmem.InMemoryShardMapSource
import com.latenighthack.lockers.sharding.inmem.SimCluster
import com.latenighthack.lockers.sharding.spi.PeerLocator
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

/**
 * The monolith — one process running the full set of services together, with no external sharding
 * infrastructure — is the degenerate case of the ring model: a single node owns every shard of both
 * rings, so all routing is local/in-process (exactly today's behavior). Sharding is opt-in.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class MonolithModeTest {
    private val self = NodeId("local")

    @Test
    fun `a single-node cluster routes every room and session locally`() = runTest {
        val sim = SimCluster(listOf(self), ShardCounts(256))
        val router = sim.routerFor(self, backgroundScope)
        runCurrent()

        repeat(300) { i ->
            val room = router.routeRoom(Keyspace(1), "room-$i".encodeToByteArray())
            assertThat(room.isLocal).isTrue()
            assertThat(room.node).isEqualTo(self)
            assertThat(room.address).isNull() // in-process; no peer hop

            val session = router.routeSession("sess-$i".encodeToByteArray())
            assertThat(session.isLocal).isTrue()
            assertThat(session.node).isEqualTo(self)
        }
    }

    @Test
    fun `coLocated router puts both axes on one node for a single-fleet monolith`() = runTest {
        val registry = MutableStateFlow(setOf(self))
        val membership = InMemoryMembership(self, registry)
        val source = InMemoryShardMapSource(membership, ShardCounts(256)).also { it.bind(backgroundScope) }
        val router = ShardRouter.coLocated(
            membership = membership,
            source = source,
            locator = PeerLocator { PeerAddress(it.value, 0) },
            scope = backgroundScope,
        )
        runCurrent()

        assertThat(router.routeRoom(Keyspace(30), "any-room".encodeToByteArray()).isLocal).isTrue()
        assertThat(router.routeSession("any-session".encodeToByteArray()).isLocal).isTrue()
        assertThat(router.roomEpoch).isEqualTo(router.sessionEpoch) // one shared ring/epoch
    }
}
