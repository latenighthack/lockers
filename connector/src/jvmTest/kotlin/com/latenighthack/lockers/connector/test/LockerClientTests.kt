package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktbuf.test.server.runTestWithServer
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.InMemoryKeyValueStoreDelegate
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.ktstore.KeyValueStore
import com.latenighthack.lockers.example.v1.*
import com.latenighthack.lockers.common.RoomKeying
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
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
        val lockers: LockersClient,
        val client: LockerClient,
        val typedClient: TypedLockerClient<ExampleLocker>,
    )

    private suspend fun createClient(rpcClient: RpcClient, lockKeySource: LockKeySource? = null): ClientContext {
        val sessionKeyPair = Secp256r1KeyPair.generate()
        val keySource = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = sessionKeyPair
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() {}
            override suspend fun revokeKeys() {}
        }

        val lockers = LockersClient.create(
            rpcClient = rpcClient,
            storeDelegate = InMemoryStoreDelegate(),
            keyValueStore = KeyValueStore(InMemoryKeyValueStoreDelegate()),
            keySource = keySource,
            appVersion = Version(0, 0, 1),
            lockKeySource = lockKeySource,
        )
        lockers.awaitConnected()

        val typedClient = lockers.typed(DEFAULT_KEYSPACE, ExampleLocker::toByteArray, ExampleLocker.Companion::fromByteArray)

        return ClientContext(lockers, lockers.lockers, typedClient)
    }

    /** A per-locker in-memory key source; adopts rotated keys on ratchet. */
    private class MapLockKeySource : LockKeySource {
        private val keys = mutableMapOf<String, Secp256r1KeyPair>()

        private fun key(lockerId: LockerId) =
            (lockerId.keyspace?.value ?: 0L).toString() + ":" + lockerId.rawValue.joinToString(",")

        fun set(lockerId: LockerId, keyPair: Secp256r1KeyPair) {
            keys[key(lockerId)] = keyPair
        }

        override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId): Secp256r1KeyPair? = keys[key(lockerId)]

        override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) {
            keys[key(lockerId)] = newKeyPair
        }
    }

    private fun lockerScope(lockerId: LockerId) = LockScope(
        kind = LockScopeKind.LOCK_SCOPE_LOCKER,
        keyspace = lockerId.keyspace ?: DEFAULT_KEYSPACE,
        lockerRawValue = lockerId.rawValue,
    )

    private fun cleanup(vararg contexts: ClientContext) {
        for (ctx in contexts) {
            ctx.lockers.close()
        }
    }

    private fun randomRoomId() = RoomId(Random.nextBytes(32))
    private fun randomLockerId(keyspace: LockerKeyspace = DEFAULT_KEYSPACE) = LockerId(Random.nextBytes(32), keyspace)

    // --- Single Client CRUD ---

    @Test(timeout = 15_000)
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

    @Test(timeout = 15_000)
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

    @Test(timeout = 15_000)
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

    @Test(timeout = 15_000)
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

    @Test(timeout = 15_000)
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

    @Test(timeout = 15_000)
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

    @Test(timeout = 15_000)
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

    // Writes both versions before watching so the assertion doesn't race the watcher's
    // startup: `watch`'s own onStart (subscribe + getLocker) runs after any downstream
    // onStart, so a "ready" signal there fires before the collector has attached to the
    // replay-0 changes flow — a live write in that window is dropped and lost, which no
    // timeout can rescue. Here `watch` hydrates the current value deterministically.
    @Test(timeout = 10_000)
    fun `watch single locker emits updates`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()

        ctx.typedClient.subscribeToRoom(roomId)
        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "v1" } }
        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "v2" } }

        val seenV2 = ctx.typedClient
            .watch(roomId, lockerId)
            .mapNotNull { (it as? TypedLockerUpdate.Present)?.value }
            .filter { it.title == "v2" }
            .first()

        assertEquals("v2", seenV2.title)

        cleanup(ctx)
    }

    @Test(timeout = 15_000)
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

    @Test(timeout = 15_000)
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
                .mapNotNull { if (it is TypedLockerUpdate.Present) it.value else null }
                .first()
                .let { seen.complete(it) }
        }

        ready.await()

        ctx1.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "from-client1" } }

        assertEquals("from-client1", seen.await().title)
        job.cancel()

        cleanup(ctx1, ctx2)
    }

    @Test(timeout = 15_000)
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

    @Test(timeout = 15_000)
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

    @Test(timeout = 15_000)
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
                .mapNotNull { if (it is TypedLockerUpdate.Present) it.value else null }
                .collect {
                    seen.add(it.title)
                    if (seen.size >= 2) { ctx1Sees2.complete(Unit); return@collect }
                }
        }
        val j2 = launch {
            val seen = mutableSetOf<String>()
            ctx2.typedClient.allUpdates
                .onStart { ready2.complete(Unit) }
                .filter { it.roomId == roomId }
                .mapNotNull { if (it is TypedLockerUpdate.Present) it.value else null }
                .collect {
                    seen.add(it.title)
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

    @Test(timeout = 15_000)
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

    @Test(timeout = 15_000)
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

    @Test(timeout = 15_000)
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

    // --- Deletes ---

    @Test(timeout = 15_000)
    fun `local delete removes locker from watchAll`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        val ready = CompletableDeferred<Unit>()
        val emptied = CompletableDeferred<Unit>()
        val job = launch {
            var sawPresent = false
            ctx.typedClient.watchAll(roomId, includeHistory = false)
                .onStart { ready.complete(Unit) }
                .collect { map ->
                    if (map.isNotEmpty()) sawPresent = true
                    if (sawPresent && map.isEmpty()) { emptied.complete(Unit); return@collect }
                }
        }

        ready.await()
        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "doomed" } }
        ctx.typedClient.deleteLocker(roomId, lockerId)

        emptied.await()
        job.cancel()

        cleanup(ctx)
    }

    @Test(timeout = 15_000)
    fun `remote delete removes locker for other client`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx1 = createClient(server.rpcClient)
        val ctx2 = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()

        ctx1.typedClient.subscribeToRoom(roomId)
        ctx2.typedClient.subscribeToRoom(roomId)

        val ready = CompletableDeferred<Unit>()
        val emptied = CompletableDeferred<Unit>()
        val job = launch {
            var sawPresent = false
            ctx2.typedClient.watchAll(roomId, includeHistory = false)
                .onStart { ready.complete(Unit) }
                .collect { map ->
                    if (map.isNotEmpty()) sawPresent = true
                    if (sawPresent && map.isEmpty()) { emptied.complete(Unit); return@collect }
                }
        }

        ready.await()
        ctx1.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "doomed" } }
        ctx1.typedClient.deleteLocker(roomId, lockerId)

        emptied.await()
        job.cancel()

        cleanup(ctx1, ctx2)
    }

    @Test(timeout = 15_000)
    fun `notification delivered on body-less delete event`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "seed" } }

        val ready = CompletableDeferred<Unit>()
        val incoming = CompletableDeferred<ExamplePayload>()
        val job = launch {
            ctx.typedClient.notifications
                .onStart { ready.complete(Unit) }
                .map { ExamplePayload.fromByteArray(it.payload) }
                .first()
                .let { incoming.complete(it) }
        }
        ready.await()

        ctx.typedClient.deleteLocker(roomId, lockerId) {
            push { title = "gone"; body = "deleted" }
            payload { rawValue = ExamplePayload { data = "bye".encodeToByteArray() }.toByteArray() }
        }

        assertEquals("bye", incoming.await().data.decodeToString())
        job.cancel()

        cleanup(ctx)
    }

    // --- Equality ---

    @Test
    fun `locker update and notification equality is content based`() {
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        val payload = byteArrayOf(1, 2, 3)

        val a = LockerClient.LockerUpdate(roomId, lockerId, 1L, payload.copyOf())
        val b = LockerClient.LockerUpdate(roomId, lockerId, 1L, payload.copyOf())
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())

        val n1 = IncomingNotification(roomId, lockerId, payload.copyOf())
        val n2 = IncomingNotification(roomId, lockerId, payload.copyOf())
        assertEquals(n1, n2)
        assertEquals(n1.hashCode(), n2.hashCode())
    }

    // --- Cache-first reads / hydration ---

    @Test(timeout = 15_000)
    fun `getLocker serves repeated reads`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "cached" } }

        val first = ctx.client.getLocker(roomId, lockerId.copy(keyspace = DEFAULT_KEYSPACE))
        val second = ctx.client.getLocker(roomId, lockerId.copy(keyspace = DEFAULT_KEYSPACE))
        assertNotNull(first)
        assertNotNull(second)
        assertEquals("cached", ExampleLocker.fromByteArray(second.locker!!.open!!.encodedPayload).title)

        cleanup(ctx)
    }

    @Test(timeout = 15_000)
    fun `typed getLocker and getAllLockers decode values`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "typed" } }

        val one = ctx.typedClient.getLocker(roomId, lockerId)
        assertEquals("typed", one?.title)

        val all = ctx.typedClient.getAllLockers(roomId)
        assertEquals(1, all.size)
        assertEquals("typed", all.values.first().title)

        cleanup(ctx)
    }

    @Test(timeout = 15_000)
    fun `watch with includeHistory emits current value on start`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "existing" } }

        val hydrated = CompletableDeferred<String>()
        val job = launch {
            ctx.typedClient.watch(roomId, lockerId, includeHistory = true)
                .mapNotNull { if (it is TypedLockerUpdate.Present) it.value else null }
                .first()
                .let { hydrated.complete(it.title) }
        }

        assertEquals("existing", hydrated.await())
        job.cancel()

        cleanup(ctx)
    }

    @Test(timeout = 15_000)
    fun `watch without includeHistory skips current value`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "existing" } }

        val ready = CompletableDeferred<Unit>()
        val firstSeen = CompletableDeferred<String>()
        val job = launch {
            ctx.typedClient.watch(roomId, lockerId, includeHistory = false)
                .onStart { ready.complete(Unit) }
                .mapNotNull { if (it is TypedLockerUpdate.Present) it.value else null }
                .first()
                .let { firstSeen.complete(it.title) }
        }
        ready.await()

        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "new" } }

        assertEquals("new", firstSeen.await())
        job.cancel()

        cleanup(ctx)
    }

    // --- Keyspace guard ---

    @Test(timeout = 15_000)
    fun `mismatched keyspace is rejected`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ctx = createClient(server.rpcClient)
        val roomId = randomRoomId()
        val mismatched = randomLockerId(GAME_KEYSPACE)
        ctx.typedClient.subscribeToRoom(roomId)

        assertFailsWith<IllegalArgumentException> {
            ctx.typedClient.updateLocker(roomId, mismatched) { it.copy { title = "x" } }
        }
        assertFailsWith<IllegalArgumentException> {
            ctx.typedClient.deleteLocker(roomId, mismatched)
        }

        cleanup(ctx)
    }

    // --- Write failure handling (fault injection) ---

    @Test(timeout = 15_000)
    fun `updateLocker throws LockerWriteException when retries are exhausted`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val faulty = FaultInjectingRpcClient(server.rpcClient) { method, _ ->
            if (method.methodName == "PostLockerChange") throw FaultInjectingRpcClient.rpcError(Codes.UNAVAILABLE)
        }
        val ctx = createClient(faulty)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        assertFailsWith<LockerWriteException> {
            ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "x" } }
        }

        cleanup(ctx)
    }

    @Test(timeout = 15_000)
    fun `updateLocker rethrows non-retriable rpc error`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val faulty = FaultInjectingRpcClient(server.rpcClient) { method, _ ->
            if (method.methodName == "PostLockerChange") throw FaultInjectingRpcClient.rpcError(Codes.INVALID_ARGUMENT)
        }
        val ctx = createClient(faulty)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        assertFailsWith<RpcResponseException> {
            ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "x" } }
        }

        cleanup(ctx)
    }

    @Test(timeout = 15_000)
    fun `updateLocker recovers from transient errors within budget`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val faulty = FaultInjectingRpcClient(server.rpcClient) { method, attempt ->
            if (method.methodName == "PostLockerChange" && attempt <= 2) throw FaultInjectingRpcClient.rpcError(Codes.UNAVAILABLE)
        }
        val ctx = createClient(faulty)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        val result = ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "recovered" } }
        assertEquals("recovered", result?.title)

        cleanup(ctx)
    }

    @Test(timeout = 15_000)
    fun `deleteLocker throws LockerWriteException when retries are exhausted`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val faulty = FaultInjectingRpcClient(server.rpcClient) { method, _ ->
            if (method.methodName == "DeleteLocker") throw FaultInjectingRpcClient.rpcError(Codes.UNAVAILABLE)
        }
        val ctx = createClient(faulty)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ctx.typedClient.subscribeToRoom(roomId)

        ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "seed" } }

        assertFailsWith<LockerWriteException> {
            ctx.typedClient.deleteLocker(roomId, lockerId)
        }

        cleanup(ctx)
    }

    // --- Locked / signed lockers ---

    @Test(timeout = 15_000)
    fun `TOFU lock then signed write succeeds`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val keyPair = Secp256r1KeyPair.generate()
        val keys = MapLockKeySource()
        val ctx = createClient(server.rpcClient, keys)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        keys.set(lockerId, keyPair)
        ctx.typedClient.subscribeToRoom(roomId)

        val lockResponse = ctx.typedClient.lockLocker(roomId, lockerScope(lockerId), keyPair)
        assertTrue(lockResponse.result is LockLockerResponse.Result.OK)

        val result = ctx.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "locked-hello" } }
        assertEquals("locked-hello", result?.title)
        assertEquals("locked-hello", ctx.typedClient.getLocker(roomId, lockerId)?.title)

        cleanup(ctx)
    }

    @Test(timeout = 15_000)
    fun `unsigned write to locked locker is rejected`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val keyPair = Secp256r1KeyPair.generate()
        val ownerKeys = MapLockKeySource()
        val owner = createClient(server.rpcClient, ownerKeys)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ownerKeys.set(lockerId, keyPair)
        owner.typedClient.subscribeToRoom(roomId)
        assertTrue(owner.typedClient.lockLocker(roomId, lockerScope(lockerId), keyPair).result is LockLockerResponse.Result.OK)

        val stranger = createClient(server.rpcClient, MapLockKeySource())
        stranger.typedClient.subscribeToRoom(roomId)

        assertFailsWith<LockerWriteException> {
            stranger.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "intruder" } }
        }

        cleanup(owner, stranger)
    }

    @Test(timeout = 15_000)
    fun `wrong key write to locked locker is rejected`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val ownerKey = Secp256r1KeyPair.generate()
        val wrongKey = Secp256r1KeyPair.generate()
        val ownerKeys = MapLockKeySource()
        val owner = createClient(server.rpcClient, ownerKeys)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ownerKeys.set(lockerId, ownerKey)
        owner.typedClient.subscribeToRoom(roomId)
        assertTrue(owner.typedClient.lockLocker(roomId, lockerScope(lockerId), ownerKey).result is LockLockerResponse.Result.OK)

        val wrongKeys = MapLockKeySource().apply { set(lockerId, wrongKey) }
        val impostor = createClient(server.rpcClient, wrongKeys)
        impostor.typedClient.subscribeToRoom(roomId)

        assertFailsWith<LockerWriteException> {
            impostor.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "forged" } }
        }

        cleanup(owner, impostor)
    }

    @Test(timeout = 15_000)
    fun `signed write re-signs on version conflict`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val keyPair = Secp256r1KeyPair.generate()
        val roomId = randomRoomId()
        val lockerId = randomLockerId()

        val a = createClient(server.rpcClient, MapLockKeySource().apply { set(lockerId, keyPair) })
        val b = createClient(server.rpcClient, MapLockKeySource().apply { set(lockerId, keyPair) })
        a.typedClient.subscribeToRoom(roomId)
        b.typedClient.subscribeToRoom(roomId)
        assertTrue(a.typedClient.lockLocker(roomId, lockerScope(lockerId), keyPair).result is LockLockerResponse.Result.OK)

        a.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "from-a" } }
        // b has never seen this locker, so it writes against parentVersion 0 and must
        // re-fetch + re-sign against the current version before succeeding.
        val result = b.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "from-b" } }
        assertEquals("from-b", result?.title)

        cleanup(a, b)
    }

    @Test(timeout = 15_000)
    fun `ratchet rotates the signing key`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val oldKey = Secp256r1KeyPair.generate()
        val ownerKeys = MapLockKeySource()
        val owner = createClient(server.rpcClient, ownerKeys)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ownerKeys.set(lockerId, oldKey)
        owner.typedClient.subscribeToRoom(roomId)
        assertTrue(owner.typedClient.lockLocker(roomId, lockerScope(lockerId), oldKey).result is LockLockerResponse.Result.OK)

        owner.typedClient.updateLocker(roomId, lockerId, ratchet = true) { it.copy { title = "rotated" } }
        // The source has adopted the new key; a subsequent write signs with it.
        val result = owner.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "after-ratchet" } }
        assertEquals("after-ratchet", result?.title)

        // A client still holding the pre-ratchet key can no longer write.
        val stale = createClient(server.rpcClient, MapLockKeySource().apply { set(lockerId, oldKey) })
        stale.typedClient.subscribeToRoom(roomId)
        assertFailsWith<LockerWriteException> {
            stale.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "stale-key" } }
        }

        cleanup(owner, stale)
    }

    @Test(timeout = 15_000)
    fun `delegation chain room to keyspace to locker`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val roomKey = Secp256r1KeyPair.generate()
        val keyspaceKey = Secp256r1KeyPair.generate()
        val lockerKey = Secp256r1KeyPair.generate()
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        val ownerKeys = MapLockKeySource().apply { set(lockerId, lockerKey) }
        val owner = createClient(server.rpcClient, ownerKeys)
        owner.typedClient.subscribeToRoom(roomId)

        val roomScope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM)
        val keyspaceScope = LockScope(kind = LockScopeKind.LOCK_SCOPE_KEYSPACE, keyspace = DEFAULT_KEYSPACE)

        assertTrue(owner.typedClient.lockLocker(roomId, roomScope, roomKey).result is LockLockerResponse.Result.OK)
        assertTrue(owner.typedClient.lockLocker(roomId, keyspaceScope, keyspaceKey, parentKeyPair = roomKey).result is LockLockerResponse.Result.OK)
        assertTrue(owner.typedClient.lockLocker(roomId, lockerScope(lockerId), lockerKey, parentKeyPair = keyspaceKey).result is LockLockerResponse.Result.OK)

        val result = owner.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "delegated" } }
        assertEquals("delegated", result?.title)

        cleanup(owner)
    }

    @Test(timeout = 15_000)
    fun `public-keyed room requires room key to lock`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val roomKey = Secp256r1KeyPair.generate()
        val lockKey = Secp256r1KeyPair.generate()
        val roomId = RoomKeying.publicKeyed(roomKey.publicKey.encode())
        val lockerId = randomLockerId()
        val ownerKeys = MapLockKeySource().apply { set(lockerId, lockKey) }
        val owner = createClient(server.rpcClient, ownerKeys)
        owner.typedClient.subscribeToRoom(roomId)

        // TOFU without the room key's signature is rejected in a public-keyed room.
        val tofu = owner.typedClient.lockLocker(roomId, lockerScope(lockerId), lockKey)
        assertTrue(tofu.result is LockLockerResponse.Result.NOT_AUTHORIZED)

        // Signed by the room key, the lock is accepted.
        val granted = owner.typedClient.lockLocker(roomId, lockerScope(lockerId), lockKey, parentKeyPair = roomKey)
        assertTrue(granted.result is LockLockerResponse.Result.OK)

        val result = owner.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "public-keyed" } }
        assertEquals("public-keyed", result?.title)

        cleanup(owner)
    }

    @Test(timeout = 15_000)
    fun `unlock returns locker to open writes`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val keyPair = Secp256r1KeyPair.generate()
        val ownerKeys = MapLockKeySource()
        val owner = createClient(server.rpcClient, ownerKeys)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ownerKeys.set(lockerId, keyPair)
        owner.typedClient.subscribeToRoom(roomId)

        val lockResponse = owner.typedClient.lockLocker(roomId, lockerScope(lockerId), keyPair)
        assertTrue(lockResponse.result is LockLockerResponse.Result.OK)
        val lockVersion = lockResponse.lockState?.lockVersion ?: 0L
        owner.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "before-unlock" } }

        val unlockResponse = owner.typedClient.unlockLocker(roomId, lockerScope(lockerId), keyPair, lockVersion)
        assertTrue(unlockResponse.result is UnlockLockerResponse.Result.OK)

        // With the lock gone, a keyless client can write again.
        val stranger = createClient(server.rpcClient, MapLockKeySource())
        stranger.typedClient.subscribeToRoom(roomId)
        val result = stranger.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "after-unlock" } }
        assertEquals("after-unlock", result?.title)

        cleanup(owner, stranger)
    }

    @Test(timeout = 15_000)
    fun `signed delete succeeds with the lock key`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val keyPair = Secp256r1KeyPair.generate()
        val ownerKeys = MapLockKeySource()
        val owner = createClient(server.rpcClient, ownerKeys)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ownerKeys.set(lockerId, keyPair)
        owner.typedClient.subscribeToRoom(roomId)
        assertTrue(owner.typedClient.lockLocker(roomId, lockerScope(lockerId), keyPair).result is LockLockerResponse.Result.OK)

        owner.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "doomed" } }
        owner.typedClient.deleteLocker(roomId, lockerId)

        assertEquals(null, owner.typedClient.getLocker(roomId, lockerId))

        cleanup(owner)
    }

    @Test(timeout = 15_000)
    fun `unsigned delete of a locked locker is rejected`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val keyPair = Secp256r1KeyPair.generate()
        val ownerKeys = MapLockKeySource()
        val owner = createClient(server.rpcClient, ownerKeys)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ownerKeys.set(lockerId, keyPair)
        owner.typedClient.subscribeToRoom(roomId)
        assertTrue(owner.typedClient.lockLocker(roomId, lockerScope(lockerId), keyPair).result is LockLockerResponse.Result.OK)
        owner.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "keep" } }

        val stranger = createClient(server.rpcClient, MapLockKeySource())
        stranger.typedClient.subscribeToRoom(roomId)

        assertFailsWith<LockerWriteException> {
            stranger.typedClient.deleteLocker(roomId, lockerId)
        }

        cleanup(owner, stranger)
    }

    @Test(timeout = 15_000)
    fun `room-scope lock enforces signatures on its lockers`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val roomKey = Secp256r1KeyPair.generate()
        val ownerKeys = MapLockKeySource()
        val owner = createClient(server.rpcClient, ownerKeys)
        val roomId = randomRoomId()
        owner.typedClient.subscribeToRoom(roomId)
        assertTrue(
            owner.typedClient.lockLocker(roomId, LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), roomKey)
                .result is LockLockerResponse.Result.OK
        )

        // A locker with no lock of its own inherits the room-scope lock.
        val lockerId = randomLockerId()
        ownerKeys.set(lockerId, roomKey)
        val written = owner.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "room-signed" } }
        assertEquals("room-signed", written?.title)

        val stranger = createClient(server.rpcClient, MapLockKeySource())
        stranger.typedClient.subscribeToRoom(roomId)
        assertFailsWith<LockerWriteException> {
            stranger.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "no-key" } }
        }

        cleanup(owner, stranger)
    }

    @Test(timeout = 15_000)
    fun `keyspace-scope lock does not leak into other keyspaces`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val keyspaceKey = Secp256r1KeyPair.generate()
        val ownerKeys = MapLockKeySource()
        val owner = createClient(server.rpcClient, ownerKeys)
        val roomId = randomRoomId()
        val gameTyped = owner.lockers.typed(GAME_KEYSPACE, ExampleLocker::toByteArray, ExampleLocker.Companion::fromByteArray)
        owner.typedClient.subscribeToRoom(roomId)
        assertTrue(
            owner.typedClient
                .lockLocker(roomId, LockScope(kind = LockScopeKind.LOCK_SCOPE_KEYSPACE, keyspace = DEFAULT_KEYSPACE), keyspaceKey)
                .result is LockLockerResponse.Result.OK
        )

        // In-keyspace write must be signed by the keyspace key.
        val inKeyspace = randomLockerId()
        ownerKeys.set(inKeyspace, keyspaceKey)
        assertEquals("locked", owner.typedClient.updateLocker(roomId, inKeyspace) { it.copy { title = "locked" } }?.title)

        // A locker in a different keyspace is unaffected and stays open.
        val otherKeyspace = randomLockerId(GAME_KEYSPACE)
        assertEquals("open", gameTyped.updateLocker(roomId, otherKeyspace) { it.copy { title = "open" } }?.title)

        cleanup(owner)
    }

    @Test(timeout = 15_000)
    fun `signed locker surfaces as a present value not a tombstone`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val keyPair = Secp256r1KeyPair.generate()
        val ownerKeys = MapLockKeySource()
        val owner = createClient(server.rpcClient, ownerKeys)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ownerKeys.set(lockerId, keyPair)
        owner.typedClient.subscribeToRoom(roomId)
        assertTrue(owner.typedClient.lockLocker(roomId, lockerScope(lockerId), keyPair).result is LockLockerResponse.Result.OK)
        owner.typedClient.updateLocker(roomId, lockerId) { it.copy { title = "present-value" } }

        // A reader with no key fetches the sealed locker fresh (no subscribe, so its cache
        // stays empty) and must unwrap it to the plaintext value — a signed body must not
        // be mistaken for a delete tombstone.
        val reader = createClient(server.rpcClient)
        assertEquals("present-value", reader.typedClient.getLocker(roomId, lockerId)?.title)

        val update = reader.typedClient.watch(roomId, lockerId).first()
        assertTrue(update is TypedLockerUpdate.Present)
        assertEquals("present-value", update.value.title)

        cleanup(owner, reader)
    }

    @Test(timeout = 15_000)
    fun `unlock with the wrong key is rejected`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val keyPair = Secp256r1KeyPair.generate()
        val wrongKey = Secp256r1KeyPair.generate()
        val ownerKeys = MapLockKeySource()
        val owner = createClient(server.rpcClient, ownerKeys)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ownerKeys.set(lockerId, keyPair)
        owner.typedClient.subscribeToRoom(roomId)
        val lockResponse = owner.typedClient.lockLocker(roomId, lockerScope(lockerId), keyPair)
        assertTrue(lockResponse.result is LockLockerResponse.Result.OK)
        val lockVersion = lockResponse.lockState?.lockVersion ?: 0L

        val response = owner.typedClient.unlockLocker(roomId, lockerScope(lockerId), wrongKey, lockVersion)
        assertTrue(response.result is UnlockLockerResponse.Result.SIGNATURE_INVALID)

        cleanup(owner)
    }

    @Test(timeout = 15_000)
    fun `unlock with a stale version reports a conflict`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val keyPair = Secp256r1KeyPair.generate()
        val ownerKeys = MapLockKeySource()
        val owner = createClient(server.rpcClient, ownerKeys)
        val roomId = randomRoomId()
        val lockerId = randomLockerId()
        ownerKeys.set(lockerId, keyPair)
        owner.typedClient.subscribeToRoom(roomId)
        assertTrue(owner.typedClient.lockLocker(roomId, lockerScope(lockerId), keyPair).result is LockLockerResponse.Result.OK)

        val response = owner.typedClient.unlockLocker(roomId, lockerScope(lockerId), keyPair, parentLockVersion = 99L)
        assertTrue(response.result is UnlockLockerResponse.Result.UPDATE_LOCAL_VERSION)

        cleanup(owner)
    }

    @Test(timeout = 15_000)
    fun `fetch reports lock state for locked and open lockers`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val keyPair = Secp256r1KeyPair.generate()
        val ownerKeys = MapLockKeySource()
        val owner = createClient(server.rpcClient, ownerKeys)
        val roomId = randomRoomId()
        val lockedId = randomLockerId()
        val openId = randomLockerId()
        ownerKeys.set(lockedId, keyPair)
        owner.typedClient.subscribeToRoom(roomId)
        assertTrue(owner.typedClient.lockLocker(roomId, lockerScope(lockedId), keyPair).result is LockLockerResponse.Result.OK)
        owner.typedClient.updateLocker(roomId, lockedId) { it.copy { title = "locked" } }
        owner.typedClient.updateLocker(roomId, openId) { it.copy { title = "open" } }

        // A fresh reader fetches from the server (no subscribe, so its cache stays empty)
        // and therefore sees lock state, which the local cache does not retain.
        val reader = createClient(server.rpcClient)

        val lockedState = reader.client.getLocker(roomId, lockedId.copy(keyspace = DEFAULT_KEYSPACE))?.lockState
        assertNotNull(lockedState)
        assertTrue(lockedState.locked)
        assertTrue(lockedState.publicKey!!.rawValue.contentEquals(keyPair.publicKey.encode()))

        assertEquals(null, reader.client.getLocker(roomId, openId.copy(keyspace = DEFAULT_KEYSPACE))?.lockState)

        cleanup(owner, reader)
    }
}
