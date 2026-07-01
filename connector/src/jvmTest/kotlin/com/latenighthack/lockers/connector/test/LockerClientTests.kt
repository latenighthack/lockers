package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.test.server.runTestWithServer
import com.latenighthack.ktcrypto.Secp256r1KeyPair
import com.latenighthack.ktcrypto.generate
import com.latenighthack.ktstore.InMemoryKeyValueStoreDelegate
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.ktstore.KeyValueStore
import com.latenighthack.lockers.example.v1.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.connector.internal.LockerStoreImpl
import com.latenighthack.lockers.server.*
import io.ktor.server.application.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.random.Random
import kotlin.test.*

class LockerClientTests {

    private val DEFAULT_KEYSPACE = LockerKeyspace { value = 1L }
    private val GAME_KEYSPACE = LockerKeyspace { value = 30L }

    private data class ClientContext(
        val stream: Stream,
        val client: LockerClient,
        val typedClient: TypedLockerClient<ExampleLocker>,
        val rpcClient: RpcClient
    )

    private suspend fun CoroutineScope.createClient(rpcClient: RpcClient): ClientContext {
        val storeDelegate = InMemoryStoreDelegate()
        val keyValueStore = KeyValueStore(InMemoryKeyValueStoreDelegate())
        val sessionStore = SessionStoreImpl(keyValueStore, storeDelegate)
        val subscriptionStore = SubscriptionStoreImpl(storeDelegate)
        val lockerStore = LockerStoreImpl(storeDelegate)
        val sessionKeyPair = Secp256r1KeyPair.generate()
        val keySource = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = sessionKeyPair
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() {}
            override suspend fun revokeKeys() {}
        }

        subscriptionStore.prepare()
        sessionStore.prepare()
        lockerStore.prepare()
        storeDelegate.createStores()

        val stream = Stream(rpcClient, keySource, sessionStore, subscriptionStore, Version(0, 0, 1))
        val client = LockerClient(rpcClient, stream, lockerStore)
        val typedClient = TypedLockerClient(client, DEFAULT_KEYSPACE, ExampleLocker::toByteArray, ExampleLocker.Companion::fromByteArray)

        stream.start()
        client.start()

        stream.isConnected.filter { it }.first()

        return ClientContext(stream, client, typedClient, rpcClient)
    }

    private fun cleanup(vararg contexts: ClientContext) {
        for (ctx in contexts) {
            ctx.client.stop()
            ctx.stream.stop()
        }
    }

    private fun randomRoomId() = RoomId(Random.nextBytes(32))
    private fun randomLockerId(keyspace: LockerKeyspace = DEFAULT_KEYSPACE) = LockerId(Random.nextBytes(32), keyspace)

    // --- Single Client CRUD ---

    @Test(timeout = 5_000)
    fun `create and read locker`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "hello" } }

        val fetched = ctx.client.getLocker(roomId, lockerId.copy(keyspace = DEFAULT_KEYSPACE))
        assertNotNull(fetched)
        assertEquals("hello", ExampleLocker.fromByteArray(fetched.locker!!.open!!.encodedPayload).title)

        cleanup(ctx)
    }

    @Test(timeout = 5_000)
    fun `update existing locker`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "first" } }
        val result = ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "second" } }

        assertNotNull(result)
        assertEquals("second", result.title)

        cleanup(ctx)
    }

    @Test(timeout = 5_000)
    fun `delete locker`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "to-delete" } }
        ctx.typedClient.deleteLocker(roomId, lockerId.copy(keyspace = DEFAULT_KEYSPACE))

        val fetched = ctx.client.getLocker(roomId, lockerId.copy(keyspace = DEFAULT_KEYSPACE))
        assertTrue(fetched == null || fetched.locker == null || fetched.locker?.open?.encodedPayload?.isEmpty() != false)

        cleanup(ctx)
    }

    @Test(timeout = 5_000)
    fun `getAllLockers returns all in keyspace`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        ctx.typedClient.subscribeToRoom(roomId)

        repeat(3) { i ->
            ctx.typedClient.updateLocker(roomId, randomLockerId()) { it.copy { title = "locker-$i" } }
        }

        assertEquals(3, ctx.client.getAllLockers(roomId, DEFAULT_KEYSPACE).size)

        cleanup(ctx)
    }

    @Test(timeout = 5_000)
    fun `updateLocker returns updated value`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        val result = ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "change-test"; sequenceNumber = 42 } }
        assertNotNull(result)
        assertEquals("change-test", result.title)
        assertEquals(42, result.sequenceNumber)

        cleanup(ctx)
    }

    // --- Flows ---

    @Test(timeout = 5_000)
    fun `notification flow receives payload`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()

        val ready = CompletableDeferred<Unit>()
        val incomingPayload = CompletableDeferred<ExamplePayload>()
        val job = launch {
            ctx.typedClient.notifications
                .onStart { ready.complete(Unit) }
                .map { ExamplePayload.fromByteArray(it.payload) }
                .first()
                .let { incomingPayload.complete(it) }
        }

        ready.await()
        ctx.typedClient.subscribeToRoom(roomId)

        ctx.typedClient.updateLocker(roomId, lockerId, {
            push { title = "This is a title"; body = "and a body" }
            payload {
                rawValue = ExamplePayload {
                    data = "test".encodeToByteArray()
                }.toByteArray()
            }
        }) { it.copy { } }

        assertEquals("test", incomingPayload.await().data.decodeToString())
        job.cancel()

        cleanup(ctx)
    }

    @Test(timeout = 5_000)
    fun `watchAll accumulates updates`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()

        val ready = CompletableDeferred<Unit>()
        val watchResult = CompletableDeferred<Map<LockerId, ExampleLocker>>()
        val job = launch {
            ctx.typedClient.watchAll(roomId)
                .onStart { ready.complete(Unit) }
                .filter { it.size >= 2 }
                .first()
                .let { watchResult.complete(it) }
        }

        ready.await()

        ctx.typedClient.updateLocker(roomId, randomLockerId()) { it.copy { title = "a" } }
        ctx.typedClient.updateLocker(roomId, randomLockerId()) { it.copy { title = "b" } }

        val result = watchResult.await()
        assertEquals(2, result.size)
        assertTrue(result.values.map { it.title }.containsAll(listOf("a", "b")))
        job.cancel()

        cleanup(ctx)
    }

    @Test(timeout = 5_000)
    fun `watch single locker emits updates`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()

        val ready = CompletableDeferred<Unit>()
        val seenV2 = CompletableDeferred<String>()
        val job = launch {
            ctx.typedClient.watch(roomId, lockerId)
                .onStart { ready.complete(Unit) }
                .filter { it.value.title == "v2" }
                .first()
                .let { seenV2.complete(it.value.title) }
        }

        ready.await()

        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "v1" } }
        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "v2" } }

        assertEquals("v2", seenV2.await())
        job.cancel()

        cleanup(ctx)
    }

    @Test(timeout = 5_000)
    fun `multiple keyspaces filter correctly`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val gameClient = TypedLockerClient(ctx.client, GAME_KEYSPACE, ExampleLocker::toByteArray, ExampleLocker.Companion::fromByteArray)

        ctx.typedClient.subscribeToRoom(roomId)

        ctx.typedClient.updateLocker(roomId, randomLockerId(DEFAULT_KEYSPACE)) { it.copy { title = "chat-locker" } }
        gameClient.updateLocker(roomId, randomLockerId(GAME_KEYSPACE)) { it.copy { title = "game-locker" } }

        assertEquals(1, ctx.client.getAllLockers(roomId, DEFAULT_KEYSPACE).size)
        assertEquals(1, ctx.client.getAllLockers(roomId, GAME_KEYSPACE).size)

        cleanup(ctx)
    }

    // --- Multi-Client ---

    @Test(timeout = 5_000)
    fun `client 2 observes client 1 update`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx1 = createClient(server.rpcClient)
        val ctx2 = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()

        ctx1.typedClient.subscribeToRoom(roomId)
        ctx2.typedClient.subscribeToRoom(roomId)

        val ready = CompletableDeferred<Unit>()
        val seen = CompletableDeferred<ExampleLocker>()
        val job = launch {
            ctx2.typedClient.allUpdates
                .onStart { ready.complete(Unit) }
                .filter { it.roomId == roomId }
                .first()
                .let { seen.complete(it.value) }
        }

        ready.await()

        ctx1.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "from-client1" } }

        assertEquals("from-client1", seen.await().title)
        job.cancel()

        cleanup(ctx1, ctx2)
    }

    @Test(timeout = 5_000)
    fun `both clients update same locker sequentially`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx1 = createClient(server.rpcClient)
        val ctx2 = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()

        ctx1.typedClient.subscribeToRoom(roomId)
        ctx2.typedClient.subscribeToRoom(roomId)

        ctx1.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "client1-wrote" } }
        val result = ctx2.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "client2-wrote" } }

        assertNotNull(result)
        assertEquals("client2-wrote", result.title)

        cleanup(ctx1, ctx2)
    }

    @Test(timeout = 5_000)
    fun `concurrent updates both succeed`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx1 = createClient(server.rpcClient)
        val ctx2 = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()

        ctx1.typedClient.subscribeToRoom(roomId)
        ctx2.typedClient.subscribeToRoom(roomId)

        ctx1.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "seed"; sequenceNumber = 0 } }

        val r1 = async { ctx1.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "from-1" } } }
        val r2 = async { ctx2.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "from-2" } } }

        assertNotNull(r1.await())
        assertNotNull(r2.await())

        cleanup(ctx1, ctx2)
    }

    @Test(timeout = 5_000)
    fun `both clients see each other's updates`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx1 = createClient(server.rpcClient)
        val ctx2 = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId1 = randomLockerId()
        val lockerId2 = randomLockerId()

        ctx1.typedClient.subscribeToRoom(roomId)
        ctx2.typedClient.subscribeToRoom(roomId)

        val ctx1Sees2 = CompletableDeferred<Unit>()
        val ctx2Sees2 = CompletableDeferred<Unit>()
        val ready1 = CompletableDeferred<Unit>()
        val ready2 = CompletableDeferred<Unit>()

        val j1 = launch {
            val seen = mutableSetOf<String>()
            ctx1.typedClient.allUpdates
                .onStart { ready1.complete(Unit) }
                .filter { it.roomId == roomId }
                .collect {
                    seen.add(it.value.title)
                    if (seen.size >= 2) { ctx1Sees2.complete(Unit); return@collect }
                }
        }
        val j2 = launch {
            val seen = mutableSetOf<String>()
            ctx2.typedClient.allUpdates
                .onStart { ready2.complete(Unit) }
                .filter { it.roomId == roomId }
                .collect {
                    seen.add(it.value.title)
                    if (seen.size >= 2) { ctx2Sees2.complete(Unit); return@collect }
                }
        }

        ready1.await()
        ready2.await()

        ctx1.typedClient.updateLocker(roomId, lockerId1) { it.copy { title = "from-1" } }
        ctx2.typedClient.updateLocker(roomId, lockerId2) { it.copy { title = "from-2" } }

        ctx1Sees2.await()
        ctx2Sees2.await()
        j1.cancel()
        j2.cancel()

        cleanup(ctx1, ctx2)
    }

    // --- Edge Cases ---

    @Test(timeout = 5_000)
    fun `empty payload locker`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        val result = ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { } }
        assertNotNull(result)
        assertEquals("", result.title)
        assertEquals(0, result.sequenceNumber)

        cleanup(ctx)
    }

    @Test(timeout = 5_000)
    fun `multiple rooms isolate updates`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val room1 = randomRoomId()
        val room2 = randomRoomId()
        ctx.typedClient.subscribeToRoom(room1)
        ctx.typedClient.subscribeToRoom(room2)

        ctx.typedClient.updateLocker(room1, randomLockerId()) { it.copy { title = "room1-only" } }

        assertTrue(ctx.client.getAllLockers(room2, DEFAULT_KEYSPACE).isEmpty())
        assertEquals(1, ctx.client.getAllLockers(room1, DEFAULT_KEYSPACE).size)

        cleanup(ctx)
    }

    @Test(timeout = 5_000)
    fun `unsubscribe succeeds`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "before-unsub" } }
        assertNotNull(ctx.client.getLocker(roomId, lockerId.copy(keyspace = DEFAULT_KEYSPACE)))

        ctx.typedClient.unsubscribeFromRoom(roomId)

        cleanup(ctx)
    }
}
