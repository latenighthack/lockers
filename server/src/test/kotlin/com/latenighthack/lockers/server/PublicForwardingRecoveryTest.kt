package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktbuf.rpc.HttpRpcClient
import com.latenighthack.ktbuf.server.serveUnary
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.RoutingRpcClient
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.session.v1.SessionGatewayService
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.LockerSigning
import kotlin.test.*

class PublicForwardingRecoveryTest {
    private val room = RoomId(byteArrayOf(1))
    private fun request() = PostLockerChangesRequest(roomId = room, writeRequestId = ByteArray(16) { 7 }, changes = listOf(
        PostLockerChangeRequest(lockerId = LockerId(byteArrayOf(2)), locker = Locker { open { encodedPayload = byteArrayOf(3) } })))
    private class Ownership(var owner: RoomOwner, var refresh: (() -> RoomOwner)? = null) : RoomOwnership {
        override suspend fun resolve(keyspace: Long, roomId: RoomId) = owner
        override fun invalidate(roomId: RoomId) { refresh?.let { owner = it() } }
    }
    private class Fixture(ownership: RoomOwnership, token: String? = "secret") {
        val db = ServerStorage.inMemory()
        val service = RoomServiceImpl(SubscriptionStoreImpl(db), LockerStoreImpl(db), LockStoreImpl(db), object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
        }, ownership, LockerAgentRegistry.None, SimpleMeterRegistry(), LockersConfig.defaults().copy(peerToken = token, deliveryWorkerEnabled = false))
        suspend fun open() { db.open() }
        suspend fun close() { service.closeAndJoin(); db.close() }
    }
    private suspend fun listener(service: RoomServer, private: Boolean = false): Pair<io.ktor.server.engine.EmbeddedServer<*, *>, String> {
        val server = embeddedServer(CIO, host = "127.0.0.1", port = 0) { routing {
            for (method in RoomServer.Descriptor.methods) {
                @Suppress("UNCHECKED_CAST")
                serveUnary(service as Any, RoomServer.Descriptor, method as ServerMethodDescriptor<Any, Any, Any>) { context ->
                    if (private && !validPeerToken("secret", context.headers.entries.firstOrNull { it.key.equals(PEER_TOKEN_HEADER, true) }?.value.orEmpty()))
                        throw RpcResponseException(context.originalUrl, "POST", Codes.UNAUTHENTICATED, "Invalid peer credential")
                    context
                }
            }
        } }
        server.start(false)
        return server to "127.0.0.1:${server.engine.resolvedConnectors().single().port}"
    }
    private fun authenticated(address: String): RpcClient {
        val transport = HttpRpcClient(address)
        return object : RpcClient by transport {
            override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray) =
                transport.unaryCall(method, headers + (PEER_TOKEN_HEADER to "secret"), request)
        }
    }
    private suspend fun retryable(rpc: RoomService) {
        val failure = assertFailsWith<RpcResponseException> { rpc.postLockerChanges(request()) }
        assertEquals(Codes.UNAVAILABLE, failure.code)
    }
    @Test fun `public retries stay on seed after private peer failure`(): Unit = runBlocking {
        val peer = Fixture(LocalRoomOwnership()); peer.open()
        var fail = true
        var loseReply = true
        val unstable = object : RoomServer by peer.service {
            override suspend fun postLockerChanges(context: GrpcRequestContext, request: PostLockerChangesRequest): PostLockerChangesResponse {
                if (fail) throw RpcResponseException(context.originalUrl, "POST", Codes.UNAVAILABLE, "Temporary peer failure")
                val response = peer.service.postLockerChanges(context, request)
                if (loseReply) throw RpcResponseException(context.originalUrl, "POST", Codes.UNAVAILABLE, "Reply lost after commit")
                return response
            }
        }
        val (privateServer, address) = listener(unstable, true)
        val seed = Fixture(Ownership(RoomOwner.Remote(address, 1))); seed.open()
        val (publicServer, publicAddress) = listener(seed.service)
        val privateRoutes = mutableListOf<String>()
        val routing = RoutingRpcClient(HttpRpcClient(publicAddress), { target -> privateRoutes.add(target); HttpRpcClient(target) }, { it })
        val rpc = RoomServiceRpc(routing) { _, _ -> mapOf("rid" to "room") }
        try {
            // This is exactly the decode-layer behavior which formerly poisoned the public cache.
            val failure = runCatching { rpc.postLockerChanges(request()).also { response ->
                response.redirect?.let { routing.recordRedirect("room", it.ownerAddress.orEmpty(), it.epoch) }
            } }.exceptionOrNull()
            assertIs<RpcResponseException>(failure); assertEquals(Codes.UNAVAILABLE, failure.code)
            fail = false
            retryable(rpc) // The owner commits, but this reply is lost. Retry the same immutable ID.
            loseReply = false
            assertTrue(rpc.postLockerChanges(request()).result.isOk())
            assertTrue(privateRoutes.isEmpty(), "Public client must never dial the authenticated private endpoint")
            assertEquals(1L, LocalRoomServiceRpc(peer.service).getLocker(GetLockerRequest(room, LockerId(byteArrayOf(2)))).locker!!.version)
            assertEquals(Codes.UNAUTHENTICATED, assertFailsWith<RpcResponseException> { RoomServiceRpc(HttpRpcClient(address)).postLockerChanges(request()) }.code)
        } finally { publicServer.stop(0, 1000); privateServer.stop(0, 1000); seed.close(); peer.close() }
    }
    @Test fun `handoff invalidates seed ownership without relaying private redirect`(): Unit = runBlocking {
        val next = Fixture(LocalRoomOwnership()); next.open(); val (nextServer, nextAddress) = listener(next.service, true)
        val previous = Fixture(Ownership(RoomOwner.Remote(nextAddress, 2))); previous.open(); val (previousServer, previousAddress) = listener(previous.service, true)
        val placement = Ownership(RoomOwner.Remote(previousAddress, 1)) { RoomOwner.Remote(nextAddress, 2) }
        val seed = Fixture(placement); seed.open(); val (publicServer, publicAddress) = listener(seed.service)
        val privateRoutes = mutableListOf<String>()
        val rpc = RoomServiceRpc(RoutingRpcClient(HttpRpcClient(publicAddress), { privateRoutes.add(it); HttpRpcClient(it) }, { it })) { _, _ -> mapOf("rid" to "room") }
        try {
            retryable(rpc)
            assertTrue(rpc.postLockerChanges(request()).result.isOk())
            assertTrue(privateRoutes.isEmpty())
            // Authenticated stamped peer calls retain the handoff redirect and stop at one hop.
            val peerResponse = RoomServiceRpc(authenticated(previousAddress)) { _, _ -> mapOf("fwd" to "1") }.postLockerChanges(request())
            assertEquals(PostLockerChangesResponse.Result.NOT_OWNER, peerResponse.result)
            assertEquals(nextAddress, peerResponse.redirect!!.ownerAddress)
        } finally { publicServer.stop(0, 1000); previousServer.stop(0, 1000); nextServer.stop(0, 1000); seed.close(); previous.close(); next.close() }
    }
    @Test fun `forged public marker cannot suppress authenticated forwarding`(): Unit = runBlocking {
        val peer = Fixture(LocalRoomOwnership()); peer.open(); val (peerServer, peerAddress) = listener(peer.service, true)
        val seed = Fixture(Ownership(RoomOwner.Remote(peerAddress, 1))); seed.open(); val (server, address) = listener(seed.service)
        try {
            for (forgedToken in listOf("", "wrong")) {
                val transport = HttpRpcClient(address)
                val forged = object : RpcClient by transport {
                    override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray) =
                        transport.unaryCall(method, headers + ("X-Lockers-Peer-Token" to forgedToken), request)
                }
                val rpc = RoomServiceRpc(forged) { _, _ -> mapOf("fwd" to "1") }
                assertTrue(rpc.postLockerChanges(request()).result.isOk())
            }
        } finally { server.stop(0, 1000); peerServer.stop(0, 1000); seed.close(); peer.close() }
    }
    @Test fun `every write response rejects a private handoff on public HTTP`(): Unit = runBlocking {
        val peer = Fixture(LocalRoomOwnership()); peer.open()
        val redirect = ShardRedirect(ownerAddress = "private-next:123", epoch = 2)
        val moving = object : RoomServer by peer.service {
            override suspend fun postLockerChanges(context: GrpcRequestContext, request: PostLockerChangesRequest) = PostLockerChangesResponse(result = PostLockerChangesResponse.Result.NOT_OWNER, redirect = redirect)
            override suspend fun deleteLocker(context: GrpcRequestContext, request: DeleteLockerRequest) = DeleteLockerResponse(result = DeleteLockerResponse.Result.NOT_OWNER, redirect = redirect)
            override suspend fun lockLocker(context: GrpcRequestContext, request: LockLockerRequest) = LockLockerResponse(result = LockLockerResponse.Result.NOT_OWNER, redirect = redirect)
            override suspend fun unlockLocker(context: GrpcRequestContext, request: UnlockLockerRequest) = UnlockLockerResponse(result = UnlockLockerResponse.Result.NOT_OWNER, redirect = redirect)
        }
        val (peerServer, peerAddress) = listener(moving, true)
        val seed = Fixture(Ownership(RoomOwner.Remote(peerAddress, 1))); seed.open(); val (server, address) = listener(seed.service)
        try {
            val rpc = RoomServiceRpc(HttpRpcClient(address))
            val key = Secp256r1KeyPair.generate()
            val scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM)
            val calls: List<suspend () -> Unit> = listOf(
                { rpc.postLockerChanges(request()); Unit },
                { rpc.postLockerChange(request().changes.single().copy(roomId = room)); Unit },
                { rpc.deleteLocker(DeleteLockerRequest(roomId = room, lockerId = LockerId(byteArrayOf(2)))); Unit },
                { rpc.lockLocker(LockLockerRequest(roomId = room, grant = LockGrant(scope, publicKeyOf(key.publicKey.encode())))); Unit },
                { rpc.unlockLocker(UnlockLockerRequest(roomId = room, scope = scope, signature = Signature(signingVersion = 2,
                    signature = key.privateKey.sign(LockerSigning.unlockContextV2(room, scope, 1))))); Unit }
            )
            for (call in calls) assertEquals(Codes.UNAVAILABLE, assertFailsWith<RpcResponseException> { call() }.code)
        } finally { server.stop(0, 1000); peerServer.stop(0, 1000); seed.close(); peer.close() }
    }
    @Test fun `caller cancellation during forwarding remains cancellation`(): Unit = runBlocking {
        val peer = Fixture(LocalRoomOwnership()); peer.open()
        val slow = object : RoomServer by peer.service {
            override suspend fun postLockerChanges(context: GrpcRequestContext, request: PostLockerChangesRequest): PostLockerChangesResponse {
                delay(1000)
                return peer.service.postLockerChanges(context, request)
            }
        }
        val (peerServer, peerAddress) = listener(slow, true)
        val seed = Fixture(Ownership(RoomOwner.Remote(peerAddress, 1))); seed.open()
        try {
            val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), RoomServer.Descriptor, RoomServer.Descriptor.methods[0])
            assertFailsWith<TimeoutCancellationException> { withTimeout(50) { seed.service.postLockerChanges(context, request()) } }
        } finally { peerServer.stop(0, 1000); seed.close(); peer.close() }
    }
    @Test fun `public ownership fence loss is retryable while legacy stamped redirects remain compatible`(): Unit = runBlocking {
        var sourceExecuted = false
        val ownership = object : RoomOwnership {
            override suspend fun resolve(keyspace: Long, roomId: RoomId): RoomOwner = RoomOwner.Local()
            override suspend fun mutationFence(keyspace: Long, roomId: RoomId): RoomMutationFence = object : RoomMutationFence() {
                override suspend fun <T> guard(driver: com.latenighthack.ktstore.SqlDriver?, block: suspend () -> T): T {
                    block()
                    sourceExecuted = true
                    throw RoomOwnershipLost()
                }
            }
        }
        val seed = Fixture(ownership); seed.open(); val (server, address) = listener(seed.service)
        val legacy = Fixture(Ownership(RoomOwner.Remote("legacy:123", 9)), token = null); legacy.open()
        try {
            val rpc = RoomServiceRpc(HttpRpcClient(address))
            retryable(rpc)
            assertTrue(sourceExecuted, "Lose ownership after source mutation, before transaction commit")
            val absent = rpc.getLocker(GetLockerRequest(room, LockerId(byteArrayOf(2)))).locker!!
            assertNull(absent.locker)
            assertEquals(0L, absent.version)
            assertEquals(GetWriteOutcomeResponse.Result.NOT_FOUND, rpc.getWriteOutcome(GetWriteOutcomeRequest(roomId = room, writeRequestId = request().writeRequestId)).result)
            val context = GrpcRequestContext("", emptyMap(), mapOf("fwd" to "1"), emptyMap(), RoomServer.Descriptor, RoomServer.Descriptor.methods[0])
            val response = legacy.service.postLockerChanges(context, request())
            assertEquals(PostLockerChangesResponse.Result.NOT_OWNER, response.result)
            assertEquals("legacy:123", response.redirect!!.ownerAddress)
        } finally { server.stop(0, 1000); seed.close(); legacy.close() }
    }
}
