package com.latenighthack.lockers.server.cluster

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.rpc.HttpRpcClient
import com.latenighthack.lockers.push.v1.PushGatewayService
import com.latenighthack.lockers.push.v1.PushGatewayServiceRpc
import com.latenighthack.lockers.session.v1.SessionGatewayService
import com.latenighthack.lockers.session.v1.SessionGatewayServiceRpc
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.PeerAddress
import java.util.concurrent.ConcurrentHashMap

/**
 * Produces a gateway service stub `S` that reaches a remote node's gateway (east-west RPC), or
 * `null` if the node is unreachable. The one seam that couples the ring to a transport — the
 * in-process test harness supplies a direct-dispatch implementation, production supplies an
 * HTTP-backed one ([HttpSessionGateways] / [HttpPushGateways]).
 */
fun interface RemoteGateway<S> {
    suspend fun connect(node: NodeId, address: PeerAddress?): S?
}

/**
 * Caches one [RpcClient] per peer address so cross-node calls reuse connections. Addresses come
 * from the ring's `PeerLocator`, never hardcoded.
 */
class PeerConnectionPool(
    private val scheme: String = "http",
    private val clientFactory: (String) -> RpcClient = { HttpRpcClient(it) },
) : AutoCloseable {
    private val clients = ConcurrentHashMap<String, RpcClient>()

    fun clientFor(address: PeerAddress): RpcClient =
        clients.computeIfAbsent(url(address), clientFactory)

    fun evict(address: PeerAddress) {
        clients.remove(url(address))
    }

    private fun url(address: PeerAddress): String = "$scheme://${address.host}:${address.port}"

    override fun close() {
        clients.clear()
    }
}

/** Production session-gateway transport: an HTTP RPC stub per peer address. */
class HttpSessionGateways(private val pool: PeerConnectionPool) : RemoteGateway<SessionGatewayService> {
    override suspend fun connect(node: NodeId, address: PeerAddress?): SessionGatewayService? =
        address?.let { SessionGatewayServiceRpc(pool.clientFor(it)) }
}

/** Production push-gateway transport: an HTTP RPC stub per peer address. */
class HttpPushGateways(private val pool: PeerConnectionPool) : RemoteGateway<PushGatewayService> {
    override suspend fun connect(node: NodeId, address: PeerAddress?): PushGatewayService? =
        address?.let { PushGatewayServiceRpc(pool.clientFor(it)) }
}
