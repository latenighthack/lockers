package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktbuf.rpc.HttpRpcClient
import com.latenighthack.ktbuf.server.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.AuthenticationKeySource
import com.latenighthack.lockers.connector.RoutingRpcClient
import com.latenighthack.lockers.connector.Stream
import com.latenighthack.lockers.connector.StreamConnectionState
import com.latenighthack.lockers.server.cluster.BlueprintV
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.*
import com.latenighthack.lockers.sharding.NodeId
import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.test.*

class RingPublicSessionRecoveryTest {
    private class Node {
        lateinit var component: MonolithComponent
        val publicServer = embeddedServer(CIO, host = "127.0.0.1", port = 0) {
            install(WebSockets)
            routing { serveAll(object : SessionServer {
                override fun watchSession(context: GrpcRequestContext, request: Flow<WatchSessionRequest>): Flow<StreamControlEvent<WatchSessionResponse>> =
                    component.sessionServiceModule.server.watchSession(context, request)
                override suspend fun destroySession(context: GrpcRequestContext, request: DestroySessionRequest) =
                    component.sessionServiceModule.server.destroySession(context, request)
            }, SessionServer.Descriptor) }
        }
        val privateServer = embeddedServer(CIO, host = "127.0.0.1", port = 0) { routing {
            val gateway = object : SessionGatewayServer {
                override suspend fun postEvent(context: GrpcRequestContext, request: PostEventRequest) = component.sessionGatewayServiceModule.server.postEvent(context, request)
                override suspend fun postEvents(context: GrpcRequestContext, request: PostEventsRequest) = component.sessionGatewayServiceModule.server.postEvents(context, request)
            }
            for (method in SessionGatewayServer.Descriptor.methods) {
                @Suppress("UNCHECKED_CAST")
                serveUnary(gateway as Any, SessionGatewayServer.Descriptor, method as ServerMethodDescriptor<Any, Any, Any>) { context ->
                    val token = context.headers.entries.firstOrNull { it.key.equals(PEER_TOKEN_HEADER, true) }?.value.orEmpty()
                    if (!validPeerToken("secret", token)) throw RpcResponseException(context.originalUrl, "POST", Codes.UNAUTHENTICATED, "Invalid peer credential")
                    context
                }
            }
        } }
        lateinit var publicAddress: String
        lateinit var privateAddress: String
        suspend fun startListeners() {
            publicServer.start(false); privateServer.start(false)
            publicAddress = "http://127.0.0.1:${publicServer.engine.resolvedConnectors().single().port}"
            privateAddress = "127.0.0.1:${privateServer.engine.resolvedConnectors().single().port}"
        }
        suspend fun close() {
            if (::component.isInitialized) component.closeAndJoin()
            publicServer.stop(0, 1000); privateServer.stop(0, 1000)
        }
    }
    @Test fun `ring reconnect targets owner public websocket instead of private gateway`(): Unit = runBlocking {
        val a = Node(); val b = Node(); a.startListeners(); b.startListeners()
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val serverDb = ServerStorage.inMemory()
        var core: ServerCore? = null
        val wirings = mutableListOf<BlueprintV.Wiring>()
        val transports = java.util.concurrent.CopyOnWriteArrayList<HttpRpcClient>()
        var stream: Stream? = null
        val clientDb = com.latenighthack.lockers.connector.ConnectorStorage.inMemory()
        try {
            fun config(self: String) = LockersConfig.fromEnv { name -> mapOf(
                "LOCKERS_NODE_ID" to self, "LOCKERS_PEERS" to "a=${a.privateAddress},b=${b.privateAddress}",
                "LOCKERS_ADVERTISE_ADDR" to if (self == "a") a.privateAddress else b.privateAddress,
                "LOCKERS_PUBLIC_SESSION_ADDRS" to "a=${a.publicAddress},b=${b.publicAddress}",
                "LOCKERS_PEER_TOKEN" to "secret", "LOCKERS_ROOM_OWNERSHIP" to "ring",
                "LOCKERS_SHARD_COUNT_DEFAULT" to "4", "LOCKERS_SESSION_SHARD_COUNT" to "4",
                "LOCKERS_PUSH_WORKER_ENABLED" to "false", "LOCKERS_DELIVERY_WORKER_ENABLED" to "false"
            )[name] }
            val aWiring = BlueprintV.wire(config("a"), scope)!!.also { wirings.add(it) }
            val bWiring = BlueprintV.wire(config("b"), scope)!!.also { wirings.add(it) }
            val actualCore = ServerCore::class.create(config("a"), serverDb).also { core = it; it.overrideCoroutineContext = scope.coroutineContext; it.setup() }
            a.component = MonolithComponent(actualCore, cluster = aWiring.context)
            b.component = MonolithComponent(actualCore, cluster = bWiring.context)
            a.component.start(); b.component.start()
            val sid = (1..1000).map { SessionId(ByteArray(32) { index -> (it + index).toByte() }) }
                .first { aWiring.context.router.routeSession(it.rawValue).node == NodeId("b") }
            val key = Secp256r1KeyPair.generate(); val material = ByteArray(32) { 7 }
            actualCore.sessionStore.updateSession(ServerSession(sessionId = ServerSessionId(sid.rawValue), nextKeyMaterial = material,
                authorizedPublicKey = key.publicKey.encode()))
            val sessions = com.latenighthack.lockers.connector.SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), clientDb).also { it.prepare() }
            val subs = com.latenighthack.lockers.connector.SubscriptionStoreImpl(clientDb).also { it.prepare() }
            clientDb.open(); sessions.updateSessionId(sid); sessions.updateNextSequenceBytes(material)
            val auth = object : AuthenticationKeySource {
                override suspend fun getSessionKeyPair() = key
                override suspend fun hasSessionKeyPair() = true
                override suspend fun generateSessionKeyPair() {}
                override suspend fun revokeKeys() {}
            }
            val firstOwnerDial = CompletableDeferred<String>()
            val seed = HttpRpcClient(a.publicAddress).also { transports.add(it) }
            val routing = RoutingRpcClient(seed, { address -> firstOwnerDial.complete(address); HttpRpcClient(address).also { transports.add(it) } })
            val sdk = Stream(routing, auth, sessions, subs, Version(), coroutineContext = scope.coroutineContext).also { stream = it }
            sdk.start()
            assertEquals(b.publicAddress, withTimeout(5000) { firstOwnerDial.await() }, "A real SDK reconnect must dial the configured public owner endpoint")
            val connected = withTimeout(5000) { sdk.connection.filterIsInstance<StreamConnectionState.Connected>().first() }
            assertContentEquals(sid.rawValue, connected.sessionId.rawValue)
            assertEquals(NodeId("b"), aWiring.context.router.routeSession(sid.rawValue).node, "Ring authority remains on B")
            assertFalse(actualCore.sessionStore.getSessionById(ServerSessionId(sid.rawValue))!!.nextKeyMaterial.contentEquals(material))
            val privateClient = HttpRpcClient(b.privateAddress).also { transports.add(it) }
            assertEquals(Codes.UNAUTHENTICATED, assertFailsWith<RpcResponseException> { SessionGatewayServiceRpc(privateClient).postEvent(PostEventRequest()) }.code)
        } finally {
            stream?.closeAndJoin(); transports.forEach { it.closeAndJoin() }
            a.close(); b.close(); wirings.forEach { it.pool.close() }; core?.closeAndJoin()
            scope.cancel(); scope.coroutineContext[Job]!!.join(); clientDb.close(); serverDb.close()
        }
    }
}
