package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.server.cluster.*
import com.latenighthack.lockers.sharding.*
import com.latenighthack.lockers.sharding.inmem.SimCluster
import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import kotlin.test.*

@OptIn(ExperimentalCoroutinesApi::class)
class PublicSessionAddressContractTest {
    private val nodes = setOf(NodeId("a"), NodeId("b"))
    @Test fun `complete mapping accepts HTTPS base URLs and host port endpoints`() {
        val addresses = PublicSessionAddresses.parse("a=https://a.example/sessions/,b=b.example:8443,c=https://future.example", nodes)
        assertEquals("https://a.example/sessions/", addresses[NodeId("a")])
        assertEquals("b.example:8443", addresses[NodeId("b")])
        for (raw in listOf(null, "", "a=https://a.example", "a=https://a.example,a=https://other.example,b=b.example:80",
            "a=,b=b.example:80", "a=ws://a.example,b=b.example:80", "a=https://user:secret@a.example,b=b.example:80",
            "a=https://a.example?secret=1,b=b.example:80", "a=https://a.example/#fragment,b=b.example:80", "a=a.example:0,b=b.example:80")) {
            assertFailsWith<IllegalArgumentException>(raw.toString()) { PublicSessionAddresses.parse(raw, nodes) }
        }
    }
    @Test fun `authenticated production wiring rejects incomplete mapping before launching resources`(): Unit = runBlocking {
        val job = SupervisorJob(); val scope = CoroutineScope(job + Dispatchers.Default)
        try {
            val base = LockersConfig.fromEnv { name -> mapOf("LOCKERS_NODE_ID" to "a", "LOCKERS_PEERS" to "a=a.internal:8081,b=b.internal:8081",
                "LOCKERS_ADVERTISE_ADDR" to "a.internal:8081", "LOCKERS_PEER_TOKEN" to "secret", "LOCKERS_ROOM_OWNERSHIP" to "ring")[name] }
            for (raw in listOf(null, "a=https://a.example")) {
                assertFailsWith<IllegalArgumentException> { BlueprintV.wire(base.copy(sharding = base.sharding.copy(publicSessionAddresses = raw)), scope) }
                assertFalse(job.children.any(), "Rejected boot must not leave a shard-map watch or transport owner")
            }
        } finally { job.cancelAndJoin() }
    }
    @Test fun `credentialed embedder cannot accidentally select legacy private redirects`(): Unit = runTest {
        val sim = SimCluster(nodes, ShardCounts(8))
        val router = sim.routerFor(NodeId("a"), backgroundScope); runCurrent()
        val sessions = object : RemoteGateway<com.latenighthack.lockers.session.v1.SessionGatewayService> {
            override suspend fun connect(node: NodeId, address: PeerAddress?): com.latenighthack.lockers.session.v1.SessionGatewayService? = null
        }
        val push = object : RemoteGateway<com.latenighthack.lockers.push.v1.PushGatewayService> {
            override suspend fun connect(node: NodeId, address: PeerAddress?): com.latenighthack.lockers.push.v1.PushGatewayService? = null
        }
        val core = ServerCore::class.create(LockersConfig.defaults().copy(peerToken = "secret"), ServerStorage.inMemory())
        try {
            assertFailsWith<IllegalArgumentException> { MonolithComponent(core, cluster = ClusterContext(router, sessions, push)) }
        } finally { core.closeAndJoin() }
    }
    @Test fun `address book is immutable and unknown future owner fails closed`(): Unit = runTest {
        val sim = SimCluster(nodes, ShardCounts(8))
        val router = sim.routerFor(NodeId("a"), backgroundScope); runCurrent()
        val input = mutableMapOf(NodeId("a") to "https://a.example", NodeId("b") to "https://b.example")
        val ownership = RingSessionOwnership(router, input)
        input[NodeId("b")] = "private-b:8081"
        val remote = (0..1000).map { SessionId("sid-$it".encodeToByteArray()) }.first { router.routeSession(it.rawValue).node == NodeId("b") }
        assertEquals("https://b.example", (ownership.resolve(remote) as com.latenighthack.lockers.server.services.session.v1.SessionOwner.Remote).address)
        sim.addSessionNode(NodeId("c")); runCurrent()
        val added = (0..1000).map { SessionId("new-$it".encodeToByteArray()) }.first { router.routeSession(it.rawValue).node == NodeId("c") }
        val error = assertFailsWith<RpcResponseException> { ownership.resolve(added) }
        assertEquals(Codes.UNAVAILABLE, error.code)
        assertFalse(error.message.orEmpty().contains("c:0"))
    }
}
