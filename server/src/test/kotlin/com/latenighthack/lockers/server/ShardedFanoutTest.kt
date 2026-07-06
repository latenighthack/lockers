package com.latenighthack.lockers.server

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isNotEmpty
import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.push.v1.PushGatewayServer
import com.latenighthack.lockers.push.v1.PushGatewayService
import com.latenighthack.lockers.push.v1.SendPushRequest
import com.latenighthack.lockers.push.v1.SendPushResponse
import com.latenighthack.lockers.server.cluster.ClusterContext
import com.latenighthack.lockers.server.cluster.HttpPushGateways
import com.latenighthack.lockers.server.cluster.HttpSessionGateways
import com.latenighthack.lockers.server.cluster.PeerConnectionPool
import com.latenighthack.lockers.server.cluster.RemoteGateway
import com.latenighthack.lockers.server.cluster.RingPushGatewayDiscovery
import com.latenighthack.lockers.server.cluster.RingSessionGatewayDiscovery
import com.latenighthack.lockers.session.v1.PostEventRequest
import com.latenighthack.lockers.session.v1.PostEventResponse
import com.latenighthack.lockers.session.v1.SessionGatewayServer
import com.latenighthack.lockers.session.v1.SessionGatewayService
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.ShardCounts
import com.latenighthack.lockers.sharding.inmem.SimCluster
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test

/**
 * M2: proves the ring discoveries route a session's gateway call to the node that owns that session
 * on the session ring — the exact hop `RoomServiceImpl` fan-out makes
 * (`findServer(sessionId).postEvent(...)`), which is what turns in-process delivery into cross-node
 * delivery. Also proves the monolith (single node) never leaves the process.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class ShardedFanoutTest {

    private class RecordingSessionGatewayServer(val node: NodeId, val hits: MutableList<NodeId>) : SessionGatewayServer {
        override suspend fun postEvent(context: GrpcRequestContext, request: PostEventRequest): PostEventResponse {
            hits.add(node)
            return PostEventResponse { }
        }
    }

    private class RecordingSessionGateway(val node: NodeId, val hits: MutableList<NodeId>) : SessionGatewayService {
        override suspend fun postEvent(request: PostEventRequest): PostEventResponse {
            hits.add(node)
            return PostEventResponse { }
        }
    }

    private class RecordingPushGatewayServer(val node: NodeId, val hits: MutableList<NodeId>) : PushGatewayServer {
        override suspend fun sendPush(context: GrpcRequestContext, request: SendPushRequest): SendPushResponse {
            hits.add(node)
            return SendPushResponse { result = SendPushResponse.Result.OK }
        }
    }

    private class RecordingPushGateway(val node: NodeId, val hits: MutableList<NodeId>) : PushGatewayService {
        override suspend fun sendPush(request: SendPushRequest): SendPushResponse {
            hits.add(node)
            return SendPushResponse { result = SendPushResponse.Result.OK }
        }
    }

    @Test
    fun `session gateway discovery routes postEvent to the session-ring owner`() = runTest {
        val nodes = listOf(NodeId("a"), NodeId("b"), NodeId("c"))
        val sim = SimCluster(nodes, ShardCounts(64))
        val router = sim.routerFor(NodeId("a"), backgroundScope)
        runCurrent()

        val hits = CopyOnWriteArrayList<NodeId>()
        val discovery = RingSessionGatewayDiscovery(
            router = router,
            local = RecordingSessionGatewayServer(NodeId("a"), hits),
            remote = RemoteGateway { node, _ -> RecordingSessionGateway(node, hits) },
        )

        val owners = mutableSetOf<NodeId>()
        repeat(300) { i ->
            val sid = SessionId("s$i".encodeToByteArray())
            hits.clear()
            discovery.findServer(sid)!!.postEvent(PostEventRequest { })
            val owner = router.routeSession(sid.rawValue).node
            owners.add(owner)
            // The gateway call landed on exactly the node the session ring assigns.
            assertThat(hits.toList()).isEqualTo(listOf(owner))
        }
        // Both the local path (node a) and remote peers were exercised.
        assertThat(owners).contains(NodeId("a"))
        assertThat(owners.size).isGreaterThan(1)
    }

    @Test
    fun `push gateway discovery routes sendPush to the session-ring owner`() = runTest {
        val nodes = listOf(NodeId("a"), NodeId("b"), NodeId("c"))
        val sim = SimCluster(nodes, ShardCounts(64))
        val router = sim.routerFor(NodeId("a"), backgroundScope)
        runCurrent()

        val hits = CopyOnWriteArrayList<NodeId>()
        val discovery = RingPushGatewayDiscovery(
            router = router,
            local = RecordingPushGatewayServer(NodeId("a"), hits),
            remote = RemoteGateway { node, _ -> RecordingPushGateway(node, hits) },
        )

        repeat(300) { i ->
            val sid = SessionId("s$i".encodeToByteArray())
            hits.clear()
            discovery.findServer(sid)!!.sendPush(SendPushRequest { })
            assertThat(hits.toList()).isEqualTo(listOf(router.routeSession(sid.rawValue).node))
        }
    }

    @Test
    fun `a single-node monolith never routes off the node`() = runTest {
        val sim = SimCluster(listOf(NodeId("solo")), ShardCounts(64))
        val router = sim.routerFor(NodeId("solo"), backgroundScope)
        runCurrent()

        val hits = CopyOnWriteArrayList<NodeId>()
        val remoteCalls = AtomicInteger(0)
        val discovery = RingSessionGatewayDiscovery(
            router = router,
            local = RecordingSessionGatewayServer(NodeId("solo"), hits),
            remote = RemoteGateway { _, _ -> remoteCalls.incrementAndGet(); null },
        )

        repeat(50) { i ->
            discovery.findServer(SessionId("x$i".encodeToByteArray()))!!.postEvent(PostEventRequest { })
        }
        assertThat(remoteCalls.get()).isEqualTo(0)
        assertThat(hits.size).isEqualTo(50)
    }

    @Test
    fun `monolith component builds, starts and stops with a single-node cluster context`() = runBlocking {
        val core = ServerCore::class.create(LockersConfig.defaults(), InMemoryStoreDelegate())
        core.setup()

        // Dedicated scope for the router's (never-completing) shard-map watch, so it does not
        // keep runBlocking alive; cancelled at the end.
        val ringScope = CoroutineScope(Dispatchers.Default)
        val sim = SimCluster(listOf(NodeId("solo")), ShardCounts(64))
        val router = sim.routerFor(NodeId("solo"), ringScope)
        val pool = PeerConnectionPool()
        val cluster = ClusterContext(router, HttpSessionGateways(pool), HttpPushGateways(pool))

        val component = MonolithComponent(core, cluster = cluster)
        component.start()
        assertThat(component.allServices).isNotEmpty()
        component.stop()
        pool.close()
        ringScope.cancel()
    }
}
