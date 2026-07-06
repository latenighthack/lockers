package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.bytes.toBase64String
import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.net.RpcMethodSpecifier
import com.latenighthack.ktbuf.net.RpcResponse
import com.latenighthack.ktbuf.net.RpcServerStream
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.common.v1.ShardRedirect
import com.latenighthack.lockers.connector.AuthenticationKeySource
import com.latenighthack.lockers.connector.LockerClient
import com.latenighthack.lockers.connector.RoutingRpcClient
import com.latenighthack.lockers.connector.Stream
import com.latenighthack.lockers.connector.SubscriptionStoreImpl
import com.latenighthack.lockers.connector.SessionStoreImpl
import com.latenighthack.lockers.connector.internal.LockerStoreImpl
import com.latenighthack.lockers.room.v1.PostLockerChangeRequest
import com.latenighthack.lockers.room.v1.PostLockerChangeResponse
import com.latenighthack.lockers.room.v1.fromByteArray
import com.latenighthack.lockers.room.v1.toByteArray
import com.latenighthack.ktcrypto.Secp256r1KeyPair
import com.latenighthack.ktcrypto.generate
import com.latenighthack.ktstore.KeyValueStore
import com.latenighthack.ktstore.InMemoryKeyValueStoreDelegate
import com.latenighthack.lockers.common.v1.Version
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * M4b: the smart client follows server-provided ownership redirects with NO client-side ring
 * logic. A room write to a non-owner ("seed") node is answered NOT_OWNER + ShardRedirect; the
 * routing client caches `routingKey(rid) -> owner_address` and the retry lands on the owner, which
 * returns OK. Mirrors the FaultInjectingRpcClient fake-transport pattern.
 */
class RoutingRpcClientTests {

    /** Records every method dispatched to it, and returns a canned encoded response per method. */
    private class FakeNode(
        val name: String,
        val hits: MutableList<Pair<String, Map<String, String>>>,
        private val respond: (RpcMethodSpecifier, ByteArray) -> ByteArray,
    ) : RpcClient {
        override suspend fun unaryCall(
            method: RpcMethodSpecifier,
            headers: Map<String, String>,
            request: ByteArray,
        ): RpcResponse {
            hits.add(method.methodName to method.additionalParameters)
            return RpcResponse(respond(method, request), emptyMap())
        }

        override suspend fun serverStreamingCall(
            method: RpcMethodSpecifier,
            block: suspend RpcServerStream.() -> Unit,
            readyCallback: () -> Unit,
        ) = error("streaming not used in this test")
    }

    private fun postRequest(roomId: RoomId, lockerId: LockerId) = PostLockerChangeRequest {
        this.roomId = roomId
        this.lockerId = lockerId
        parentVersion = 0
        locker { open { encodedPayload = byteArrayOf() } }
    }

    // --- RoutingRpcClient transport-level routing ---

    @Test
    fun `routes to seed until a redirect is recorded, then to the owner with the epoch stamped`() = runBlocking {
        val ridKey = "abc123"
        val seedHits = CopyOnWriteArrayList<Pair<String, Map<String, String>>>()
        val ownerHits = CopyOnWriteArrayList<Pair<String, Map<String, String>>>()

        val okBytes = PostLockerChangeResponse { result = PostLockerChangeResponse.Result.OK }.toByteArray()
        val seed = FakeNode("seed", seedHits) { _, _ -> okBytes }
        val owners = ConcurrentHashMap<String, FakeNode>()
        val routing = RoutingRpcClient(
            seed = seed,
            clientFactory = { address -> owners.getOrPut(address) { FakeNode(address, ownerHits) { _, _ -> okBytes } } },
        )

        fun method() = RpcMethodSpecifier(
            "com.latenighthack.lockers.room.v1", "Room", "PostLockerChange", mapOf("rid" to ridKey),
        )

        // No owner known yet: the call goes to the seed.
        routing.unaryCall(method(), emptyMap(), byteArrayOf())
        assertEquals(1, seedHits.size)
        assertTrue(ownerHits.isEmpty())

        // A recorded redirect re-targets subsequent calls for that key to the owner, and the
        // resolved epoch is stamped onto the call metadata alongside `rid`.
        routing.recordRedirect(ridKey, "owner-b:8080", epoch = 9)
        routing.unaryCall(method(), emptyMap(), byteArrayOf())

        assertEquals(1, seedHits.size, "no further calls to the seed after redirect")
        assertEquals(1, ownerHits.size)
        assertEquals("9", ownerHits.single().second["e"], "resolved epoch stamped on the owner call")
        assertTrue(owners.containsKey("http://owner-b:8080"), "owner client built from the bare host:port")

        // A different routing key with no redirect still goes to the seed.
        val other = RpcMethodSpecifier(
            "com.latenighthack.lockers.room.v1", "Room", "PostLockerChange", mapOf("rid" to "zzz"),
        )
        routing.unaryCall(other, emptyMap(), byteArrayOf())
        assertEquals(2, seedHits.size)
    }

    @Test
    fun `a lower-epoch redirect does not override a newer one`() = runBlocking {
        val hits = CopyOnWriteArrayList<Pair<String, Map<String, String>>>()
        val okBytes = PostLockerChangeResponse { result = PostLockerChangeResponse.Result.OK }.toByteArray()
        val owners = ConcurrentHashMap<String, FakeNode>()
        val routing = RoutingRpcClient(
            seed = FakeNode("seed", hits) { _, _ -> okBytes },
            clientFactory = { address -> owners.getOrPut(address) { FakeNode(address, hits) { _, _ -> okBytes } } },
        )
        routing.recordRedirect("k", "owner-new:1", epoch = 5)
        routing.recordRedirect("k", "owner-old:1", epoch = 2) // stale, ignored

        val method = RpcMethodSpecifier(
            "com.latenighthack.lockers.room.v1", "Room", "PostLockerChange", mapOf("rid" to "k"),
        )
        routing.unaryCall(method, emptyMap(), byteArrayOf())
        assertTrue(owners.containsKey("http://owner-new:1"))
        assertNull(owners["http://owner-old:1"], "the stale, lower-epoch owner was never targeted")
    }

    // --- End-to-end through LockerClient: NOT_OWNER -> retry -> OK ---

    @Test(timeout = 15_000)
    fun `updateLocker follows a NOT_OWNER redirect and succeeds on the owner`() = runBlocking {
        val roomId = RoomId(Random.nextBytes(32))
        val lockerId = LockerId(Random.nextBytes(32), LockerKeyspace { value = 1L })
        val ridKey = roomId.rawValue.toBase64String()

        val seedHits = CopyOnWriteArrayList<Pair<String, Map<String, String>>>()
        val ownerHits = CopyOnWriteArrayList<Pair<String, Map<String, String>>>()

        // Owner: accepts the write and echoes it back with an incremented version.
        val owner = FakeNode("owner", ownerHits) { _, req ->
            val request = PostLockerChangeRequest.fromByteArray(req)
            PostLockerChangeResponse {
                result = PostLockerChangeResponse.Result.OK
                version = 1L
                existingLocker = request.locker
            }.toByteArray()
        }

        // Seed: does not own the shard — always answers NOT_OWNER + a redirect to the owner.
        val seed = FakeNode("seed", seedHits) { _, _ ->
            PostLockerChangeResponse {
                result = PostLockerChangeResponse.Result.NOT_OWNER
                redirect = ShardRedirect {
                    ownerAddress = "owner-node:9000"
                    epoch = 3
                }
            }.toByteArray()
        }

        val routing = RoutingRpcClient(seed = seed, clientFactory = { owner })

        val lockerClient = lockerClientWith(routing)

        val result = lockerClient.updateLocker(roomId, lockerId) { byteArrayOf(4, 2) }

        // The write ultimately succeeded on the owner, not the seed.
        assertTrue(result != null)
        assertTrue(seedHits.isNotEmpty(), "the first attempt hit the non-owner seed")
        assertEquals(1, ownerHits.size, "the retry landed on the owner exactly once")
        // The retry carried the resolved epoch (3) that the redirect advertised.
        assertEquals("3", ownerHits.single().second["e"])
        assertEquals(ridKey, ownerHits.single().second["rid"])

        lockerClient.stop()
    }

    /** A [LockerClient] wired to [rpcClient] over in-memory stores; not started (writes need no stream). */
    private suspend fun lockerClientWith(rpcClient: RpcClient): LockerClient {
        val storeDelegate = InMemoryStoreDelegate()
        val keyValueStore = KeyValueStore(InMemoryKeyValueStoreDelegate())
        val lockerStore = LockerStoreImpl(storeDelegate).also { it.prepare() }
        val sessionStore = SessionStoreImpl(keyValueStore, storeDelegate).also { it.prepare() }
        val subscriptionStore = SubscriptionStoreImpl(storeDelegate).also { it.prepare() }
        storeDelegate.createStores()

        val kp = Secp256r1KeyPair.generate()
        val keySource = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = kp
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() {}
            override suspend fun revokeKeys() {}
        }
        val stream = Stream(rpcClient, keySource, sessionStore, subscriptionStore, Version(0, 0, 1))
        return LockerClient(rpcClient, stream, lockerStore)
    }
}
