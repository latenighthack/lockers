package com.latenighthack.lockers.server.cluster

import com.latenighthack.ktbuf.net.*
import com.latenighthack.lockers.sharding.PeerAddress
import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.*

class PeerConnectionOwnershipTest {
    private class Fake(val entered: CompletableDeferred<Unit>? = null, val release: CompletableDeferred<Unit>? = null, val closed: AtomicInteger = AtomicInteger()) : RpcClient, AutoCloseable {
        override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
            entered?.complete(Unit); release?.await(); return RpcResponse(request, headers)
        }
        override suspend fun serverStreamingCall(method: RpcMethodSpecifier, block: suspend RpcServerStream.() -> Unit, readyCallback: () -> Unit) = error("unused")
        override fun close() { closed.incrementAndGet() }
    }
    private val method = RpcMethodSpecifier("test", "Test", "Call")
    @Test fun `eviction and close dispose retained transports`() = runBlocking {
        val disposals = AtomicInteger()
        val owned = Fake(closed = disposals)
        val pool = PeerConnectionPool(clientFactory = { owned })
        pool.clientFor(PeerAddress("localhost", 1)).unaryCall(method, emptyMap(), byteArrayOf())
        // Baseline loses even HttpRpcClient ownership; AutoCloseable transport seam was ignored too.
        pool.evict(PeerAddress("localhost", 1)); pool.closeAndJoin()
        assertEquals(1, disposals.get(), "owned transport must be disposed")
    }
    @Test fun `concurrent authenticated calls create one transport and close it once`() = runBlocking {
        val created = AtomicInteger(); val disposed = AtomicInteger()
        val pool = PeerConnectionPool(peerToken = "secret", clientFactory = { created.incrementAndGet(); Thread.sleep(20); Fake(closed = disposed) })
        try {
            coroutineScope { repeat(32) { launch(Dispatchers.Default) {
                val reply = pool.clientFor(PeerAddress("localhost", 1)).unaryCall(method, emptyMap(), byteArrayOf())
                assertEquals("secret", reply.headers[com.latenighthack.lockers.server.PEER_TOKEN_HEADER])
            } } }
            assertEquals(1, created.get())
        } finally { pool.closeAndJoin() }
        assertEquals(1, disposed.get())
    }
    @Test fun `active eviction retains its lease and counts toward capacity until completion`() = runBlocking {
        val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>(); val disposed = AtomicInteger()
        val pool = PeerConnectionPool(maxClients = 1, clientFactory = { endpoint -> Fake(
            if (endpoint.endsWith(":1")) entered else null, if (endpoint.endsWith(":1")) release else null, disposed) })
        try {
            val stub = pool.clientFor(PeerAddress("localhost", 1))
            val held = async { stub.unaryCall(method, emptyMap(), byteArrayOf(1)) }
            entered.await(); pool.evict(PeerAddress("localhost", 1))
            assertEquals(0, disposed.get())
            assertFailsWith<PeerConnectionCapacityException> { pool.clientFor(PeerAddress("localhost", 2)).unaryCall(method, emptyMap(), byteArrayOf()) }
            release.complete(Unit); assertContentEquals(byteArrayOf(1), held.await().data)
            assertEquals(1, disposed.get())
            pool.clientFor(PeerAddress("localhost", 2)).unaryCall(method, emptyMap(), byteArrayOf())
        } finally { release.complete(Unit); pool.closeAndJoin() }
        assertEquals(2, disposed.get())
    }
    @Test fun `close cancels a held real HTTP call and joins socket disposal`() = runBlocking {
        val listener = java.net.ServerSocket(0)
        val accepted = CompletableDeferred<Unit>(); val disconnected = CompletableDeferred<Unit>()
        val worker = kotlin.concurrent.thread(isDaemon = true) { runCatching { listener.accept().use { socket ->
            socket.soTimeout = 5_000
            var suffix = ""
            while (!suffix.endsWith("\r\n\r\n")) {
                val byte = socket.getInputStream().read(); if (byte < 0) return@use
                suffix = (suffix + byte.toChar()).takeLast(4)
            }
            accepted.complete(Unit)
            while (socket.getInputStream().read() >= 0) { }
            disconnected.complete(Unit)
        } }.onFailure { disconnected.completeExceptionally(it) } }
        val pool = PeerConnectionPool(peerToken = "owned-token")
        try {
            val held = async { pool.clientFor(PeerAddress("127.0.0.1", listener.localPort)).unaryCall(method, emptyMap(), byteArrayOf()) }
            withTimeout(3_000) { accepted.await() }
            coroutineScope { listOf(async { pool.closeAndJoin() }, async { pool.closeAndJoin() }).awaitAll() }
            assertTrue(held.isCancelled)
            withTimeout(3_000) { disconnected.await() }
            assertFailsWith<IllegalStateException> { pool.clientFor(PeerAddress("localhost", 1)) }
        } finally { pool.closeAndJoin(); listener.close(); worker.join(2_000) }
        Unit
    }

}
