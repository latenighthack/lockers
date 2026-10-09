package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.rpc.HttpRpcClient
import com.latenighthack.lockers.connector.*
import kotlinx.coroutines.*
import java.net.ServerSocket
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.test.*

class ReviewRoutingOwnershipTests {
    private fun method(key: String) = RpcMethodSpecifier("p", "S", "M", mapOf(key to "same"))
    @Test fun `room and session owners with identical bytes occupy distinct namespaces`() = runBlocking {
        val seed = ReviewRpc { _, _ -> byteArrayOf(1) }
        val owner = ReviewRpc { _, _ -> byteArrayOf(2) }
        val routed = RoutingRpcClient(seed, { owner })
        routed.recordRedirect("same", "room:1")
        assertContentEquals(byteArrayOf(2), routed.unaryCall(method("rid"), emptyMap(), byteArrayOf()).data)
        assertContentEquals(byteArrayOf(1), routed.unaryCall(method("s"), emptyMap(), byteArrayOf()).data)
    }
    @Test fun `concurrent routing creates one owned transport per address`() = runBlocking {
        val factories = AtomicInteger()
        val routed = RoutingRpcClient(ReviewRpc { _, _ -> byteArrayOf() }, {
            factories.incrementAndGet(); Thread.sleep(20); ReviewRpc { _, _ -> byteArrayOf() }
        })
        routed.recordRedirect("same", "owner:1")
        coroutineScope { repeat(32) { launch(Dispatchers.Default) { routed.unaryCall(method("rid"), emptyMap(), byteArrayOf()) } } }
        assertEquals(1, factories.get())
    }
    @Test fun `the documented HTTP factory reaches real bare and full URL redirects`() = runBlocking {
        for (full in listOf(false, true)) {
            val listener = ServerSocket(0)
            val worker = thread(isDaemon = true) {
                runCatching { listener.accept().use { socket ->
                    var suffix = ""
                    while (!suffix.endsWith("\r\n\r\n")) {
                        val byte = socket.getInputStream().read(); if (byte < 0) return@use
                        suffix = (suffix + byte.toChar()).takeLast(4)
                    }
                    socket.getOutputStream().write("HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx".toByteArray())
                } }
            }
            var owned: RoutingRpcClient? = null
            try {
                val routed = RoutingRpcClient(ReviewRpc { _, _ -> error("redirect not used") }, { HttpRpcClient(it) }).also { owned = it }
                val host = "127.0.0.1:${listener.localPort}"
                routed.recordRedirect("same", if (full) "http://$host" else host)
                assertContentEquals(byteArrayOf('x'.code.toByte()), withTimeout(2_000) { routed.unaryCall(method("rid"), emptyMap(), byteArrayOf()).data })
            } finally { owned?.closeAndJoin(); listener.close(); worker.join(2_000) }
        }
    }
    @Test fun `bounded routing never evicts an active transport and disposes idle owners once`() = runBlocking {
        val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
        val disposed = mutableListOf<RpcClient>()
        val created = mutableMapOf<String, RpcClient>()
        val routed = RoutingRpcClient(ReviewRpc { _, _ -> byteArrayOf() }, { address ->
            ReviewRpc { _, _ -> if (address.endsWith("a:1")) { entered.complete(Unit); release.await(); byteArrayOf(1) } else byteArrayOf(2) }.also { created[address] = it }
        }, disposeClient = { disposed += it }, maxClients = 1)
        try {
            routed.recordRedirect("same", "a:1")
            val held = async { routed.unaryCall(method("rid"), emptyMap(), byteArrayOf()) }
            entered.await(); routed.recordRedirect("same", "b:1")
            assertFailsWith<RoutingCapacityExceededException> { routed.unaryCall(method("rid"), emptyMap(), byteArrayOf()) }
            assertTrue(disposed.isEmpty())
            release.complete(Unit); held.await()
            assertContentEquals(byteArrayOf(2), routed.unaryCall(method("rid"), emptyMap(), byteArrayOf()).data)
            assertEquals(listOf(created.getValue("http://a:1")), disposed)
        } finally { release.complete(Unit); routed.closeAndJoin() }
        assertEquals(2, disposed.size)
        assertEquals(created.getValue("http://b:1"), disposed.last())
        assertFailsWith<IllegalStateException> { routed.unaryCall(method("rid"), emptyMap(), byteArrayOf()) }
        Unit
    }
    @Test fun `closing routing cancels and joins active calls before disposing their transports`() = runBlocking {
        val entered = CompletableDeferred<Unit>(); var cleaned = false; var disposed = 0
        val routed = RoutingRpcClient(ReviewRpc { _, _ -> byteArrayOf() }, { ReviewRpc { _, _ ->
            entered.complete(Unit); try { awaitCancellation() } finally { cleaned = true }
        } }, disposeClient = { assertTrue(cleaned); disposed++ })
        routed.recordRedirect("same", "a:1")
        val held = async { routed.unaryCall(method("rid"), emptyMap(), byteArrayOf()) }
        entered.await()
        coroutineScope { listOf(async { routed.closeAndJoin() }, async { routed.closeAndJoin() }).awaitAll() }
        assertTrue(held.isCancelled); assertTrue(cleaned); assertEquals(1, disposed)
    }
}
