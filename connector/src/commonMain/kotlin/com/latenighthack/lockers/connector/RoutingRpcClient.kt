package com.latenighthack.lockers.connector

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.rpc.HttpRpcClient
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class RoutingCapacityExceededException : IllegalStateException("All routed transport slots are active or awaiting disposal")

/** Server-directed routing. The factory returns one independently owned transport per address.
 * Room and session keys occupy separate namespaces. Delegates are created serially and retained
 * in a bounded cache; an active delegate is never evicted. Custom transports provide [disposeClient].
 * The seed remains caller-owned unless [ownsSeed] is requested. Call [closeAndJoin] to complete disposal.
 */
class RoutingRpcClient(
    private val seed: RpcClient,
    private val clientFactory: (address: String) -> RpcClient,
    private val normalizeAddress: (String) -> String = ::defaultNormalizeAddress,
    private val disposeClient: suspend (RpcClient) -> Unit = { if (it is HttpRpcClient) it.closeAndJoin() },
    private val maxClients: Int = 64,
    private val maxRoutes: Int = 2_048,
    private val ownsSeed: Boolean = false,
) : RpcClient {
    init { require(maxClients > 0 && maxRoutes > 0) }
    private enum class Namespace { ROOM, SESSION }
    private data class Key(val namespace: Namespace, val bytes: String)
    private data class Owner(val address: String, val epoch: Long)
    private data class Calls(val closed: Boolean = false, val jobs: Set<Job> = emptySet())
    private class Client(val value: RpcClient, var users: Int = 0, var touched: Long = 0)
    private val owners = MutableStateFlow<Map<Key, Owner>>(emptyMap())
    private val calls = MutableStateFlow(Calls())
    private val mutex = Mutex()
    private val clients = mutableMapOf<String, Client>()
    private val retiring = mutableSetOf<Client>()
    private val seedClient = Client(seed)
    private var clock = 0L
    private var closing: CompletableDeferred<Unit>? = null

    fun recordRedirect(routingKey: String, ownerAddress: String, epoch: Long = 0L) = record(Key(Namespace.ROOM, routingKey), ownerAddress, epoch)
    fun recordSessionRedirect(routingKey: String, ownerAddress: String, epoch: Long = 0L) = record(Key(Namespace.SESSION, routingKey), ownerAddress, epoch)
    private fun record(key: Key, address: String, epoch: Long) {
        if (calls.value.closed) return
        val owner = address.takeIf { it.isNotBlank() }?.let { Owner(normalizeAddress(it), epoch) }
        owners.update { current ->
            val existing = current[key]
            if (existing != null && existing.epoch > epoch) current
            else if (owner == null) current - key
            else {
                val retained = if (key !in current && current.size >= maxRoutes) current - current.keys.first() else current
                retained + (key to owner)
            }
        }
    }

    private suspend fun acquire(owner: Owner?): Client {
        if (owner == null) return mutex.withLock { check(!calls.value.closed) { "Routing client is closed" }; seedClient.also { it.users++ } }
        while (true) {
            val decision = mutex.withLock {
                check(!calls.value.closed) { "Routing client is closed" }
                clients[owner.address]?.let { it.users++; it.touched = ++clock; return@withLock it to null }
                if (clients.size + retiring.size < maxClients) {
                    val created = Client(clientFactory(owner.address), users = 1, touched = ++clock)
                    check(clients.values.none { it.value === created.value } && retiring.none { it.value === created.value } && created.value !== seed) {
                        "Routing factory must return an independently owned transport per address"
                    }
                    clients[owner.address] = created
                    created to null
                } else {
                    val idle = clients.entries.filter { it.value.users == 0 }.minByOrNull { it.value.touched } ?: throw RoutingCapacityExceededException()
                    clients.remove(idle.key); retiring.add(idle.value)
                    null to idle.value
                }
            }
            decision.first?.let { return it }
            val retired = requireNotNull(decision.second)
            // Cleanup must finish even when shutdown cancels the request which discovered eviction.
            withContext(NonCancellable) {
                disposeClient(retired.value)
                mutex.withLock { retiring.remove(retired) }
            }
        }
    }
    private suspend fun <T> routed(method: RpcMethodSpecifier, operation: suspend (RpcClient, RpcMethodSpecifier) -> T): T = coroutineScope {
        val job = coroutineContext[Job]!!
        calls.update { check(!it.closed) { "Routing client is closed" }; check(it.jobs.size < 1_024) { "Routing admission limit exceeded" }; it.copy(jobs = it.jobs + job) }
        var client: Client? = null
        try {
            val key = method.additionalParameters["rid"]?.let { Key(Namespace.ROOM, it) }
                ?: method.additionalParameters["s"]?.let { Key(Namespace.SESSION, it) }
            val owner = key?.let { owners.value[it] }
            val acquired = acquire(owner)
            client = acquired
            val stamped = if (owner == null) method else method.copy(additionalParameters = method.additionalParameters + ("e" to owner.epoch.toString()))
            operation(acquired.value, stamped)
        } finally {
            withContext(NonCancellable) {
                client?.let { leased -> mutex.withLock { leased.users-- } }
                calls.update { it.copy(jobs = it.jobs - job) }
            }
        }
    }
    override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse =
        routed(method) { client, stamped -> client.unaryCall(stamped, headers, request) }
    override suspend fun serverStreamingCall(method: RpcMethodSpecifier, block: suspend RpcServerStream.() -> Unit, readyCallback: () -> Unit): Unit =
        routed(method) { client, stamped -> client.serverStreamingCall(stamped, block, readyCallback) }

    fun close() {
        while (true) {
            val previous = calls.value
            if (previous.closed) return
            if (calls.compareAndSet(previous, previous.copy(closed = true))) { previous.jobs.forEach { it.cancel(CancellationException("Routing client closed")) }; return }
        }
    }
    suspend fun closeAndJoin() {
        close()
        calls.first { it.jobs.isEmpty() }
        val (done, owner) = mutex.withLock {
            closing?.let { it to false } ?: CompletableDeferred<Unit>().also { closing = it }.let { it to true }
        }
        if (!owner) return done.await()
        withContext(NonCancellable) {
            val resources = mutex.withLock { (clients.values + retiring + if (ownsSeed) listOf(seedClient) else emptyList()).also { clients.clear(); retiring.clear() } }
            var failure: Throwable? = null
            for (resource in resources) try { disposeClient(resource.value) } catch (error: Throwable) { if (failure == null) failure = error else failure.addSuppressed(error) }
            owners.value = emptyMap()
            if (failure == null) done.complete(Unit) else done.completeExceptionally(failure)
        }
        done.await()
    }
}
internal fun defaultNormalizeAddress(address: String): String {
    val trimmed = address.trim().trimEnd('/')
    val normalized = if (trimmed.contains("://")) trimmed else "http://$trimmed"
    require(trimmed.isNotEmpty() && (normalized.startsWith("http://") || normalized.startsWith("https://"))) { "Unsupported routing address" }
    return normalized
}
