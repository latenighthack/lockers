package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.connector.internal.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.session.v1.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import com.latenighthack.lockers.connector.storage.v1.StoredLocker
import kotlin.test.*

internal class ReviewRpc(val response: suspend (RpcMethodSpecifier, ByteArray) -> ByteArray) : RpcClient {
    override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray) = RpcResponse(response(method, request), emptyMap())
    override suspend fun serverStreamingCall(method: RpcMethodSpecifier, block: suspend RpcServerStream.() -> Unit, readyCallback: () -> Unit) = error("not used")
}

internal suspend fun reviewClient(rpc: RpcClient, keys: LockKeySource? = null, db: Database = ConnectorStorage.inMemory(), codecs: NotificationCodecs = NotificationCodecs.identity(), broadcastCodecs: BroadcastCodecs = BroadcastCodecs.identity()): LockerClient {
    db.open()
    val key = Secp256r1KeyPair.generate()
    val auth = object : AuthenticationKeySource {
        override suspend fun getSessionKeyPair() = key
        override suspend fun hasSessionKeyPair() = true
        override suspend fun generateSessionKeyPair() {}
        override suspend fun revokeKeys() {}
    }
    val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
    val subscriptions = SubscriptionStoreImpl(db).also { it.prepare() }
    return LockerClient(rpc, Stream(rpc, auth, session, subscriptions, Version()), LockerStoreImpl(db).also { it.prepare() }, keys, codecs, broadcastCodecs = broadcastCodecs)
}

class ReviewConnectorTests {
    @Test fun `committed ratchet adopts the new key even when agent fails`() = runBlocking {
        val old = Secp256r1KeyPair.generate()
        var adopted: Secp256r1KeyPair? = null
        var submitted: PostLockerChangeRequest? = null
        val source = object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = adopted ?: old
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) { adopted = newKeyPair }
        }
        val rpc = ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true).toByteArray()
            "PostLockerChange" -> { submitted = PostLockerChangeRequest.fromByteArray(bytes); PostLockerChangeResponse(result = PostLockerChangeResponse.Result.OK, version = 1, agentFailed = true).toByteArray() }
            else -> error(method.methodName)
        } }
        val client = reviewClient(rpc, source)
        try {
            runCatching { client.updateLocker(RoomId(byteArrayOf(1)), LockerId(byteArrayOf(2)), ratchet = true) { byteArrayOf(3) } }
            assertNotNull(adopted, "Source committed, so its new authority must be adopted before reporting agent status")
            assertContentEquals(submitted!!.ratchet!!.newPublicKey!!.rawValue, adopted.publicKey.encode())
            assertEquals(1, client.getAllKnownLockers().size)
        } finally { client.stop() }
    }
    @Test fun `ratchet journal survives client replacement and replays the exact request`() = runBlocking {
        val database = ConnectorStorage.inMemory()
        val old = Secp256r1KeyPair.generate()
        var requestBytes: ByteArray? = null
        var adopted: Secp256r1KeyPair? = null
        var calls = 0
        val rpc = ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true).toByteArray()
            "PostLockerChange" -> {
                calls++
                if (requestBytes == null) requestBytes = bytes else assertContentEquals(requestBytes, bytes)
                PostLockerChangeResponse(result = PostLockerChangeResponse.Result.OK, version = 1).toByteArray()
            }
            else -> error(method.methodName)
        } }
        fun source(crash: Boolean) = object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = adopted ?: old
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) {
                if (crash) throw IllegalStateException("app terminated before key adoption")
                adopted = newKeyPair
            }
        }
        val client = reviewClient(rpc, source(true), database)
        try { assertFailsWith<RatchetAdoptionPendingException> { client.updateLocker(RoomId(byteArrayOf(1)), LockerId(byteArrayOf(2)), ratchet = true) { byteArrayOf(3) } } } finally { client.stop() }
        assertEquals(1, LockerStoreImpl(database).pendingRatchets().size)
        val replacement = reviewClient(rpc, source(false), database)
        try {
            replacement.start()
            withTimeout(5_000) { while (LockerStoreImpl(database).pendingRatchets().isNotEmpty()) delay(10) }
            assertNotNull(adopted)
            assertContentEquals(PostLockerChangeRequest.fromByteArray(requestBytes!!).ratchet!!.newPublicKey!!.rawValue, adopted.publicKey.encode())
            assertEquals(2, calls)
            assertEquals(1, replacement.getAllKnownLockers().size)
        } finally { replacement.stop() }
    }

    @Test fun `null and zero keyspaces are rejected as duplicate batch identities`() = runBlocking {
        var calls = 0
        val rpc = ReviewRpc { _, _ -> calls++; error("duplicate batch must be rejected before I/O") }
        val client = reviewClient(rpc)
        try {
            val raw = byteArrayOf(2)
            assertFailsWith<IllegalArgumentException> { client.updateLockers(RoomId(byteArrayOf(1)), listOf(
                LockerClient.Change(LockerId(raw)) { byteArrayOf(3) },
                LockerClient.Change(LockerId(raw, LockerKeyspace(0))) { byteArrayOf(4) },
            )) }
            assertEquals(0, calls)
        } finally { client.stop() }
    }

    @Test fun `transport cancellation escapes a write without retry or wrapping`() = runBlocking {
        var calls = 0
        val cancellation = CancellationException("transport cancelled")
        val client = reviewClient(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true).toByteArray()
            "PostLockerChange" -> { calls++; throw cancellation }
            else -> error(method.methodName)
        } })
        try {
            val failure = assertFailsWith<CancellationException> { client.updateLocker(RoomId(byteArrayOf(1)), LockerId(byteArrayOf(2))) { byteArrayOf(3) } }
            assertSame(cancellation, failure)
            assertEquals(1, calls)
        } finally { client.stop() }
    }

    @Test fun `watch first cached emission contains the whole snapshot`() = runBlocking {
        val remote = mutableListOf<IdentifiedLocker>()
        val room = RoomId(byteArrayOf(1))
        val keyspace = LockerKeyspace(9)
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true).toByteArray()
            "PostLockerChange" -> {
                val request = PostLockerChangeRequest.fromByteArray(bytes)
                remote += IdentifiedLocker(request.lockerId, request.locker, 1)
                PostLockerChangeResponse(result = PostLockerChangeResponse.Result.OK, version = 1).toByteArray()
            }
            "GetAllLockers" -> GetAllLockersResponse(lockers = remote.toList()).toByteArray()
            else -> error(method.methodName)
        } })
        try {
            repeat(3) { index -> client.updateLocker(room, LockerId(byteArrayOf(index.toByte()), keyspace)) { byteArrayOf(3) } }
            assertEquals(3, withTimeout(5_000) { client.watchSnapshot(room, keyspace).first() }.size)
        } finally { client.stop() }
    }

    @Test fun `slow change subscriber cannot block unrelated room commits`() = runBlocking {
        val client = reviewClient(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true).toByteArray()
            "PostLockerChange" -> PostLockerChangeResponse(result = PostLockerChangeResponse.Result.OK, version = 1).toByteArray()
            else -> error(method.methodName)
        } })
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val collector = launch(start = CoroutineStart.UNDISPATCHED) { client.changes.collect { entered.complete(Unit); release.await() } }
        try {
            client.updateLocker(RoomId(byteArrayOf(0)), LockerId(byteArrayOf(2))) { byteArrayOf(3) }
            entered.await()
            withTimeout(2_000) { repeat(70) { index -> client.updateLocker(RoomId(byteArrayOf((index + 1).toByte())), LockerId(byteArrayOf(2))) { byteArrayOf(3) } } }
            assertEquals(71, client.getAllKnownLockers().size)
        } finally { release.complete(Unit); collector.cancelAndJoin(); client.stop() }
    }

    @Test fun `acceptance failure rolls back cache journal and ACK together`() = runBlocking {
        val database = ConnectorStorage.inMemory(); database.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), database)
        val lockers = LockerStoreImpl(database); lockers.prepare(); sessions.prepare()
        val event = Event(roomId = RoomId(byteArrayOf(1)), eventId = EventId(byteArrayOf(2)))
        assertFailsWith<IllegalStateException> {
            sessions.receive(event) {
                lockers.acceptLocker(StoredLocker(roomIdRawValue = byteArrayOf(1), lockerIdRawValue = byteArrayOf(2), lockerKeyspace = 0, version = 1))
                error("failure after cache save before ACK")
            }
        }
        assertTrue(lockers.getAllLockers().isEmpty())
        assertTrue(sessions.getPendingAcks().isEmpty())
    }

    @Test fun `events accepted without collectors replay from a persistent cursor`() = runBlocking {
        val database = ConnectorStorage.inMemory(); database.open()
        val store = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), database); store.prepare()
        repeat(70) { index -> store.receive(Event(roomId = RoomId(byteArrayOf(1)), eventId = EventId(byteArrayOf(index.toByte())))) { true } }
        val replacement = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), database)
        val replay = withTimeout(5_000) { replacement.eventsAfter(0).take(70).toList() }
        assertEquals(70, replay.size)
        assertEquals(70, replay.last().cursor)
        assertEquals(70, store.getPendingAcks().size)
    }

    @Test fun `starting a stream twice opens only one transport`() = runBlocking {
        val database = ConnectorStorage.inMemory(); database.open()
        val entered = CompletableDeferred<Unit>()
        val calls = java.util.concurrent.atomic.AtomicInteger()
        val parent = SupervisorJob()
        val stopped = CompletableDeferred<Unit>()
        val rpc = object : RpcClient {
            override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray) = error("not used")
            override suspend fun serverStreamingCall(method: RpcMethodSpecifier, block: suspend RpcServerStream.() -> Unit, readyCallback: () -> Unit) {
                calls.incrementAndGet(); entered.complete(Unit)
                try { awaitCancellation() } finally { stopped.complete(Unit) }
            }
        }
        val key = Secp256r1KeyPair.generate()
        val auth = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = key
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() {}
            override suspend fun revokeKeys() {}
        }
        val stream = Stream(rpc, auth, SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), database), SubscriptionStoreImpl(database), Version(), coroutineContext = Dispatchers.Default + parent)
        try {
            stream.start(); stream.start(); entered.await(); delay(200); assertEquals(1, calls.get())
            parent.cancelAndJoin(); assertTrue(stopped.isCompleted)
        } finally { stream.closeAndJoin(); parent.cancelAndJoin() }
    }

    @Test fun `awaitConnected reports a closed client instead of hanging`(): Unit = runBlocking {
        val key = Secp256r1KeyPair.generate()
        val auth = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = key
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() {}
            override suspend fun revokeKeys() {}
        }
        val client = LockersClient.create(ReviewRpc { _, _ -> error("not used") }, ConnectorStorage.inMemory(), KeyValueStore(InMemoryKeyValueStoreDelegate()), auth, Version())
        client.close()
        try {
            val failure = assertFailsWith<IllegalStateException> { withTimeout(500) { client.awaitConnected() } }
            assertFalse(failure is CancellationException, "Await should report closure without a timeout")
        }
        finally { client.closeAndJoin() }
    }

    @Test fun `transport failure clears readiness and session during backoff`() = runBlocking {
        val breakTransport = CompletableDeferred<Unit>()
        val rpc = object : RpcClient {
            override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray) = RpcResponse(CapabilitiesResponse().toByteArray(), emptyMap())
            override suspend fun serverStreamingCall(method: RpcMethodSpecifier, block: suspend RpcServerStream.() -> Unit, readyCallback: () -> Unit) {
                var first = true
                block(object : RpcServerStream {
                    override suspend fun receive(): ByteArray {
                        if (first) { first = false; return WatchSessionResponse(response = WatchSessionResponse.OneOfResponse.open(WatchSessionResponse.Open(result = WatchSessionResponse.Open.Result.OK, nextSequenceKey = ByteArray(32)))).toByteArray() }
                        breakTransport.await(); throw IllegalStateException("transport ended")
                    }
                    override suspend fun send(bytes: ByteArray) {}
                    override suspend fun closeOutbound() {}
                    override suspend fun closeInbound() {}
                })
                throw IllegalStateException("transport ended")
            }
        }
        val key = Secp256r1KeyPair.generate()
        val auth = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = key
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() {}
            override suspend fun revokeKeys() {}
        }
        val client = LockersClient.create(rpc, ConnectorStorage.inMemory(), KeyValueStore(InMemoryKeyValueStoreDelegate()), auth, Version())
        try {
            withTimeout(5_000) { client.awaitConnected() }
            assertNotNull(client.sessionId.value)
            breakTransport.complete(Unit)
            withTimeout(5_000) { client.connection.first { it is StreamConnectionState.Retrying } }
            assertFalse(client.isConnected.first())
            assertNull(client.sessionId.value)
        } finally { client.closeAndJoin() }
    }

    @Test fun `a silent session expires its receive deadline and reconnects`() = runBlocking {
        val opens = java.util.concurrent.atomic.AtomicInteger()
        val rpc = object : RpcClient {
            override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray) = error("not used")
            override suspend fun serverStreamingCall(method: RpcMethodSpecifier, block: suspend RpcServerStream.() -> Unit, readyCallback: () -> Unit) {
                opens.incrementAndGet()
                var first = true
                block(object : RpcServerStream {
                    override suspend fun receive(): ByteArray {
                        if (first) { first = false; return WatchSessionResponse(response = WatchSessionResponse.OneOfResponse.open(WatchSessionResponse.Open(result = WatchSessionResponse.Open.Result.OK, nextSequenceKey = ByteArray(32)))).toByteArray() }
                        awaitCancellation()
                    }
                    override suspend fun send(bytes: ByteArray) {}
                    override suspend fun closeOutbound() {}
                    override suspend fun closeInbound() {}
                })
            }
        }
        val key = Secp256r1KeyPair.generate()
        val auth = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = key
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() {}
            override suspend fun revokeKeys() {}
        }
        val database = ConnectorStorage.inMemory(); database.open()
        val stream = Stream(rpc, auth, SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), database), SubscriptionStoreImpl(database), Version(), heartbeatIntervalMillis = 20, heartbeatTimeoutMillis = 60)
        try {
            stream.start()
            withTimeout(1_500) { while (opens.get() < 2) delay(10) }
            assertTrue(opens.get() >= 2)
        } finally { stream.closeAndJoin() }
    }

}
