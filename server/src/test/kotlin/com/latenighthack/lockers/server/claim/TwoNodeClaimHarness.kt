package com.latenighthack.lockers.server.claim

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.rpc.HttpRpcClient
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.lockers.room.v1.RoomServiceRpc
import com.latenighthack.lockers.server.LockersConfig
import com.latenighthack.lockers.server.MonolithComponent
import com.latenighthack.lockers.server.ServerCore
import com.latenighthack.lockers.server.cluster.PeerConnectionPool
import com.latenighthack.lockers.server.create
import com.latenighthack.lockers.server.monolith
import io.ktor.server.cio.CIO
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.ktor.server.application.install
import java.net.ServerSocket

/**
 * One claim-mode node of the in-process cluster: a real [MonolithComponent] wired with a
 * [ClaimContext] behind a real loopback Ktor server — client traffic and east-west
 * session-gateway RPC both go over actual HTTP through the production `PeerConnectionPool` stack.
 */
class ClaimNode(
    val nodeId: String,
    val port: Int,
    val component: MonolithComponent,
    val meterRegistry: SimpleMeterRegistry,
    private val server: EmbeddedServer<*, *>,
) {
    val addr: String = "127.0.0.1:$port"

    // Schemeless on purpose: HttpRpcClient prepends "http://" itself for non-https paths.
    val rpc: RpcClient by lazy { HttpRpcClient(addr) }

    fun roomClient() = RoomServiceRpc(rpc)

    /** Drain: releases claim/registry rows (successors need no TTL wait), then stops the listener. */
    fun stopGracefully() {
        component.stop()
        server.stop(gracePeriodMillis = 100, timeoutMillis = 1_000)
    }

    /** Crash: kill the listener and the renew loop WITHOUT releasing rows — they must expire. */
    fun crash() {
        component.claimRenewal?.stop()
        server.stop(gracePeriodMillis = 0, timeoutMillis = 200)
    }
}

class ClaimCluster(
    val nodes: List<ClaimNode>,
    val delegate: InMemoryStoreDelegate,
    val roomClaims: RoomClaimStore,
    val sessionGateways: SessionGatewayStore,
) : AutoCloseable {
    val addrs: List<String> get() = nodes.map { it.addr }

    override fun close() {
        for (node in nodes) {
            runCatching { node.stopGracefully() }
        }
    }
}

class TwoNodeClaimCluster(
    val node1: ClaimNode,
    val node2: ClaimNode,
    val delegate: InMemoryStoreDelegate,
    val roomClaims: RoomClaimStore,
    val sessionGateways: SessionGatewayStore,
) : AutoCloseable {
    override fun close() {
        for (node in listOf(node1, node2)) {
            runCatching { node.stopGracefully() }
        }
    }
}

private fun freePort(): Int = ServerSocket(0).use { it.localPort }

/**
 * Boots one claim-mode node over [delegate]. The claim/registry stores are the cluster's shared
 * coordination substrate (in-memory here; the PG contract run covers the SQL semantics), while
 * the locker/session/subscription stores share [delegate] the way production nodes share Postgres.
 */
suspend fun startClaimNode(
    nodeId: String,
    delegate: InMemoryStoreDelegate,
    roomClaims: RoomClaimStore,
    sessionGateways: SessionGatewayStore,
    ttlMs: Long,
    renewMs: Long,
    configureCore: (ServerCore) -> Unit = {},
): ClaimNode {
    val port = freePort()
    val registry = SimpleMeterRegistry()
    val core = ServerCore::class.create(LockersConfig.defaults(), delegate)
    core.overrideMeterRegistry = registry
    configureCore(core)
    core.setup()

    val context = ClaimContext(
        nodeId = nodeId,
        advertiseAddr = "127.0.0.1:$port",
        roomClaims = roomClaims,
        sessionGateways = sessionGateways,
        pool = PeerConnectionPool(),
        ttlMs = ttlMs,
        renewMs = renewMs,
        meters = ClaimMetrics(registry),
    )
    val component = MonolithComponent(core, emptyList(), cluster = null, claim = context)
    component.start()

    val server = embeddedServer(CIO, port = port) {
        install(WebSockets)
        routing { monolith(component) }
    }
    server.start(wait = false)
    return ClaimNode(nodeId, port, component, registry, server)
}

/** N claim-mode nodes sharing one store delegate + one claim substrate. */
suspend fun startClaimClusterOfSize(
    size: Int,
    ttlMs: Long = 500,
    renewMs: Long = 100,
): ClaimCluster {
    val delegate = InMemoryStoreDelegate()
    val roomClaims = InMemoryRoomClaimStore()
    val sessionGateways = InMemorySessionGatewayStore()
    val nodes = (1..size).map { i ->
        startClaimNode("node$i", delegate, roomClaims, sessionGateways, ttlMs, renewMs)
    }
    return ClaimCluster(nodes, delegate, roomClaims, sessionGateways)
}

/** A plain monolith (no ring, no claim) behind loopback HTTP — the baseline for load comparisons. */
suspend fun startLocalMonolithNode(delegate: InMemoryStoreDelegate): ClaimNode {
    val port = freePort()
    val registry = SimpleMeterRegistry()
    val core = ServerCore::class.create(LockersConfig.defaults(), delegate)
    core.overrideMeterRegistry = registry
    core.setup()
    val component = MonolithComponent(core)
    component.start()
    val server = embeddedServer(CIO, port = port) {
        install(WebSockets)
        routing { monolith(component) }
    }
    server.start(wait = false)
    return ClaimNode("monolith", port, component, registry, server)
}

/** Two claim-mode nodes sharing one store delegate + one claim substrate, with short test TTLs. */
suspend fun startTwoNodeClaimCluster(
    ttlMs: Long = 500,
    renewMs: Long = 100,
    roomClaimsForNode: (String, RoomClaimStore) -> RoomClaimStore = { _, shared -> shared },
    configureCore: (String, ServerCore) -> Unit = { _, _ -> },
): TwoNodeClaimCluster {
    val delegate = InMemoryStoreDelegate()
    val roomClaims = InMemoryRoomClaimStore()
    val sessionGateways = InMemorySessionGatewayStore()
    val node1 = startClaimNode(
        "node1", delegate, roomClaimsForNode("node1", roomClaims), sessionGateways, ttlMs, renewMs,
    ) { configureCore("node1", it) }
    val node2 = startClaimNode(
        "node2", delegate, roomClaimsForNode("node2", roomClaims), sessionGateways, ttlMs, renewMs,
    ) { configureCore("node2", it) }
    return TwoNodeClaimCluster(node1, node2, delegate, roomClaims, sessionGateways)
}
