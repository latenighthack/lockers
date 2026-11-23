package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.net.RpcMethodSpecifier
import com.latenighthack.ktbuf.net.RpcResponse
import com.latenighthack.ktbuf.test.server.TestServer
import com.latenighthack.ktcrypto.Secp256r1KeyPair
import com.latenighthack.ktcrypto.generate
import com.latenighthack.ktstore.InMemoryKeyValueStoreDelegate
import com.latenighthack.ktstore.KeyValueStore
import com.latenighthack.lockers.common.LockerEnvelope
import com.latenighthack.lockers.common.v1.Event
import com.latenighthack.lockers.common.v1.IdentifiedLocker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.connector.AuthenticationKeySource
import com.latenighthack.lockers.connector.ConnectorStorage
import com.latenighthack.lockers.connector.LockerClient
import com.latenighthack.lockers.connector.internal.LockerStoreImpl
import com.latenighthack.lockers.connector.SessionStoreImpl
import com.latenighthack.lockers.connector.Stream
import com.latenighthack.lockers.connector.SubscriptionStoreImpl
import com.latenighthack.lockers.common.v1.Version
import com.latenighthack.lockers.server.attachFastpathTestServices
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

/** A subscribed room stays current through delivered events; watchers do not re-read it. */
class LiveRoomHydrationTests {
    private val keyspace = LockerKeyspace(5)

    private class Counting(private val inner: RpcClient) : RpcClient by inner {
        val calls = ConcurrentHashMap<String, AtomicInteger>()
        /** Holds SubscribeAndSnapshot replies, as a slow network would, after the server has answered. */
        @Volatile var snapshotReplyDelayMs = 0L
        fun count(method: String) = calls[method]?.get() ?: 0
        fun hydrations() = count("SubscribeAndSnapshot") + count("GetAllLockers")
        override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>,
            request: ByteArray): RpcResponse {
            calls.computeIfAbsent(method.methodName) { AtomicInteger() }.incrementAndGet()
            val response = inner.unaryCall(method, headers, request)
            if (method.methodName == "SubscribeAndSnapshot" && snapshotReplyDelayMs > 0) delay(snapshotReplyDelayMs)
            return response
        }
    }

    /** A started client whose delivered events can be selectively dropped, as a lost delivery would. */
    private class Subscriber(val client: LockerClient, val stream: Stream, val rpc: Counting) {
        @Volatile var drop: ((Event) -> Boolean)? = null
        suspend fun close() { client.closeAndJoin(); stream.closeAndJoin() }
    }

    private suspend fun subscriber(server: TestServer): Subscriber {
        val db = ConnectorStorage.inMemory().also { it.open() }
        val key = Secp256r1KeyPair.generate()
        val auth = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = key
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() = Unit
            override suspend fun revokeKeys() = Unit
        }
        val rpc = Counting(server.ownedRpcClient)
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val stream = Stream(rpc, auth, sessions, SubscriptionStoreImpl(db).also { it.prepare() }, Version())
        val client = LockerClient(rpc, stream, LockerStoreImpl(db).also { it.prepare() }, null)
        val subscriber = Subscriber(client, stream, rpc)
        client.start()
        val accept = requireNotNull(stream.acceptEvent)
        stream.acceptEvent = { event -> if (subscriber.drop?.invoke(event) == true) true else accept(event) }
        stream.start()
        stream.awaitConnected()
        return subscriber
    }

    private fun withServer(block: suspend (TestServer) -> Unit) =
        runOwnedTestWithServer({ attachFastpathTestServices() }) { server, _ ->
            withContext(Dispatchers.Default) { block(server) }
        }

    private suspend fun Subscriber.awaitVersion(room: RoomId, id: LockerId, version: Long): IdentifiedLocker =
        withTimeout(10_000) {
            client.watchSnapshot(room, keyspace).first { all -> all.any { it.lockerId == id && it.version == version } }
                .first { it.lockerId == id }
        }

    @Test(timeout = 60_000)
    fun `watchers and revalidation of a live room read only the local cache`() = withServer { server ->
        val room = RoomId(Random.nextBytes(32))
        val first = LockerId(byteArrayOf(1), keyspace); val second = LockerId(byteArrayOf(2), keyspace)
        val subscriber = subscriber(server)
        val writer = reviewClient(server.ownedRpcClient)
        try {
            writer.updateLocker(room, first) { byteArrayOf(1) }
            // The first watcher subscribes and hydrates the room once.
            subscriber.awaitVersion(room, first, 1)
            delay(200)
            subscriber.rpc.calls.clear()
            // Later watchers and revalidating reads use the cache that delivery keeps current.
            val watcher = CoroutineScope(Dispatchers.Default).launch {
                subscriber.client.watchSnapshot(room, keyspace).collect { it.size }
            }
            subscriber.client.getAllLockers(room, keyspace, revalidate = true)
            subscriber.client.getLocker(room, first, revalidate = true)
            writer.updateLocker(room, second) { byteArrayOf(2) }
            val delivered = subscriber.awaitVersion(room, second, 1)
            assertEquals(listOf(2), LockerEnvelope.payload(requireNotNull(delivered.locker)).map { it.toInt() })
            delay(200)
            watcher.cancel()
            assertEquals(0, subscriber.rpc.hydrations(), "room re-read while live: ${subscriber.rpc.calls}")
            assertEquals(0, subscriber.rpc.count("GetLocker"), "locker re-read while live: ${subscriber.rpc.calls}")
        } finally { subscriber.close(); writer.closeAndJoin() }
    }

    @Test(timeout = 60_000)
    fun `a missed delivery is detected at the next event and the room re-hydrates`() = withServer { server ->
        val room = RoomId(Random.nextBytes(32))
        val missed = LockerId(byteArrayOf(1), keyspace); val next = LockerId(byteArrayOf(2), keyspace)
        val subscriber = subscriber(server)
        val writer = reviewClient(server.ownedRpcClient)
        try {
            writer.updateLocker(room, missed) { byteArrayOf(1) }
            subscriber.awaitVersion(room, missed, 1)
            delay(200)
            subscriber.rpc.calls.clear()
            // Lose the next delivery for `missed`; the following event exposes the sequence gap.
            subscriber.drop = { it.locker?.lockerId == missed }
            writer.updateLocker(room, missed) { byteArrayOf(2) }
            delay(500)
            subscriber.drop = null
            writer.updateLocker(room, next) { byteArrayOf(3) }
            val healed = subscriber.awaitVersion(room, missed, 2)
            assertEquals(listOf(2), LockerEnvelope.payload(requireNotNull(healed.locker)).map { it.toInt() })
            assertEquals(1, subscriber.rpc.count("SubscribeAndSnapshot"), "${subscriber.rpc.calls}")
        } finally { subscriber.close(); writer.closeAndJoin() }
    }

    @Test(timeout = 60_000)
    fun `events delivered while the snapshot reply is in flight keep the room live`() = withServer { server ->
        val room = RoomId(Random.nextBytes(32))
        val first = LockerId(byteArrayOf(1), keyspace); val during = LockerId(byteArrayOf(2), keyspace)
        val after = LockerId(byteArrayOf(3), keyspace)
        val subscriber = subscriber(server)
        val writer = reviewClient(server.ownedRpcClient)
        try {
            writer.updateLocker(room, first) { byteArrayOf(1) }
            subscriber.rpc.snapshotReplyDelayMs = 1_000
            val watcher = CoroutineScope(Dispatchers.Default).launch {
                subscriber.client.watchSnapshot(room, keyspace).collect { it.size }
            }
            // The server has subscribed the session; these deliveries overtake the snapshot reply.
            delay(400)
            writer.updateLocker(room, during) { byteArrayOf(2) }
            writer.updateLocker(room, during) { byteArrayOf(3) }
            subscriber.awaitVersion(room, during, 2)
            subscriber.rpc.snapshotReplyDelayMs = 0
            delay(1_200)
            val hydrated = subscriber.rpc.hydrations()
            writer.updateLocker(room, after) { byteArrayOf(4) }
            subscriber.awaitVersion(room, after, 1)
            delay(300)
            watcher.cancel()
            assertEquals(hydrated, subscriber.rpc.hydrations(),
                "in-order delivery re-hydrated the room: ${subscriber.rpc.calls}")
        } finally { subscriber.close(); writer.closeAndJoin() }
    }
}
