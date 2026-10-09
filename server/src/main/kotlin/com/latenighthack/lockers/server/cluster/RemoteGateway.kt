package com.latenighthack.lockers.server.cluster

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.rpc.HttpRpcClient
import com.latenighthack.lockers.push.v1.PushGatewayService
import com.latenighthack.lockers.push.v1.PushGatewayServiceRpc
import com.latenighthack.lockers.session.v1.SessionGatewayService
import com.latenighthack.lockers.session.v1.SessionGatewayServiceRpc
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.PeerAddress
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Produces a gateway service stub `S` that reaches a remote node's gateway (east-west RPC), or
 * `null` if the node is unreachable. The one seam that couples the ring to a transport — the
 * in-process test harness supplies a direct-dispatch implementation, production supplies an
 * HTTP-backed one ([HttpSessionGateways] / [HttpPushGateways]).
 */
fun interface RemoteGateway<S> {
    suspend fun connect(node: NodeId, address: PeerAddress?): S?
}

class PeerConnectionCapacityException : IllegalStateException("All peer transports are active or awaiting disposal")

/**
 * Owns at most [maxClients] independently created transports. Returned stubs acquire a lease for
 * every call, so an evicted stub can safely reconnect and active calls are never evicted to admit
 * a new peer. Retired transports count toward capacity until disposal finishes. [close] stops
 * admission and cancels calls; the host awaits [closeAndJoin] to finish transport disposal.
 * Custom transports supply close/join hooks (AutoCloseable is supported by default).
 */
class PeerConnectionPool(
    private val scheme: String = "http",
    private val clientFactory: (String) -> RpcClient = { HttpRpcClient(it) },
    private val peerToken: String? = null,
    private val maxClients: Int = 64,
    private val closeClient: (RpcClient) -> Unit = { if (it is HttpRpcClient) it.close() else if (it is AutoCloseable) it.close() },
    private val joinClient: suspend (RpcClient) -> Unit = { if (it is HttpRpcClient) it.closeAndJoin() },
) : AutoCloseable {
    init { require(maxClients > 0); require(scheme == "http" || scheme == "https") }
    private class Client(val value: RpcClient, var users: Int = 0, var touched: Long = 0) {
        var retired = false
        val closeStarted = java.util.concurrent.atomic.AtomicBoolean()
        val joinStarted = java.util.concurrent.atomic.AtomicBoolean()
        val disposed = CompletableDeferred<Unit>()
        @Volatile var closeFailure: Throwable? = null
    }
    private data class Calls(val closed: Boolean = false, val jobs: Set<Job> = emptySet())
    private val monitor = Any()
    private val clients = mutableMapOf<String, Client>()
    private val retiring = mutableSetOf<Client>()
    private val acquisition = Mutex()
    private val closing = Mutex()
    private val calls = MutableStateFlow(Calls())
    private var clock = 0L

    fun clientFor(address: PeerAddress): RpcClient = clientFor("$scheme://${address.host}:${address.port}")
    fun clientFor(address: String): RpcClient {
        check(!calls.value.closed) { "Peer pool is closed" }
        val endpoint = normalize(address)
        return object : RpcClient {
            override suspend fun unaryCall(method: com.latenighthack.ktbuf.net.RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray) =
                leased(endpoint) { it.unaryCall(method, headers + listOfNotNull(peerToken?.let { token -> com.latenighthack.lockers.server.PEER_TOKEN_HEADER to token }).toMap(), request) }
            override suspend fun serverStreamingCall(method: com.latenighthack.ktbuf.net.RpcMethodSpecifier, block: suspend com.latenighthack.ktbuf.net.RpcServerStream.() -> Unit, readyCallback: () -> Unit) =
                leased(endpoint) { it.serverStreamingCall(method, block, readyCallback) }
        }
    }
    private fun normalize(address: String): String {
        val trimmed = address.trim().trimEnd('/')
        val endpoint = if (trimmed.contains("://")) trimmed else "$scheme://$trimmed"
        val uri = java.net.URI(endpoint)
        require(uri.scheme in listOf("http", "https") && !uri.host.isNullOrBlank() && uri.userInfo == null && uri.query == null && uri.fragment == null) { "Invalid peer endpoint" }
        return endpoint
    }
    private fun closeOnce(client: Client) = synchronized(client) {
        if (client.closeStarted.compareAndSet(false, true)) try { closeClient(client.value) } catch (failure: Throwable) { client.closeFailure = failure }
    }
    private suspend fun dispose(client: Client) = withContext(NonCancellable) {
        if (client.joinStarted.compareAndSet(false, true)) {
            closeOnce(client)
            var failure = client.closeFailure
            try { joinClient(client.value) } catch (error: Throwable) { if (failure == null) failure = error else failure.addSuppressed(error) }
            if (failure == null) { synchronized(monitor) { retiring.remove(client) }; client.disposed.complete(Unit) }
            else client.disposed.completeExceptionally(failure)
        }
        client.disposed.await()
    }
    private suspend fun acquire(endpoint: String): Client = acquisition.withLock {
        while (true) {
            var retired: Client? = null
            val available = synchronized(monitor) {
                check(!calls.value.closed) { "Peer pool is closed" }
                clients[endpoint]?.let { it.users++; it.touched = ++clock; return@synchronized it }
                retiring.firstOrNull { it.users == 0 }?.let { retired = it; return@synchronized null }
                if (clients.size + retiring.size >= maxClients) {
                    val idle = clients.entries.filter { it.value.users == 0 }.minByOrNull { it.value.touched } ?: throw PeerConnectionCapacityException()
                    clients.remove(idle.key); idle.value.retired = true; retiring.add(idle.value); retired = idle.value
                    null
                } else {
                    val value = clientFactory(endpoint)
                    check(clients.values.none { it.value === value } && retiring.none { it.value === value }) { "Peer factory must create independently owned transports" }
                    Client(value, users = 1, touched = ++clock).also { clients[endpoint] = it }
                }
            }
            if (available != null) return@withLock available
            dispose(requireNotNull(retired))
        }
        error("unreachable")
    }
    private suspend fun <T> leased(endpoint: String, operation: suspend (RpcClient) -> T): T = coroutineScope {
        val job = coroutineContext[Job]!!
        calls.update { check(!it.closed) { "Peer pool is closed" }; check(it.jobs.size < 1_024) { "Peer call admission exceeded" }; it.copy(jobs = it.jobs + job) }
        var client: Client? = null
        try { val acquired = acquire(endpoint); client = acquired; operation(acquired.value) }
        finally { withContext(NonCancellable) {
            try { client?.let { held ->
                val release = synchronized(monitor) { held.users--; held.retired && held.users == 0 }
                if (release) dispose(held)
            } } finally { calls.update { it.copy(jobs = it.jobs - job) } }
        } }
    }
    fun evict(address: PeerAddress) = evict("$scheme://${address.host}:${address.port}")
    fun evict(address: String) {
        val retired = synchronized(monitor) {
            clients.remove(normalize(address))?.also { it.retired = true; retiring.add(it) }?.takeIf { it.users == 0 }
        }
        retired?.let { closeOnce(it) }
    }
    override fun close() {
        while (true) {
            val previous = calls.value
            if (previous.closed) return
            if (calls.compareAndSet(previous, previous.copy(closed = true))) {
                previous.jobs.forEach { it.cancel(CancellationException("Peer pool closed")) }
                synchronized(monitor) { (clients.values + retiring).toList() }.forEach(::closeOnce)
                return
            }
        }
    }
    suspend fun closeAndJoin(): Unit = withContext(NonCancellable) {
        close()
        calls.first { it.jobs.isEmpty() }
        closing.withLock { withContext(NonCancellable) {
            val resources = synchronized(monitor) { (clients.values + retiring).distinct().also { clients.clear(); retiring.addAll(it) } }
            var failed: Throwable? = null
            resources.forEach { try { dispose(it) } catch (failure: Throwable) { if (failed == null) failed = failure else failed!!.addSuppressed(failure) } }
            failed?.let { throw it }
        } }
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
