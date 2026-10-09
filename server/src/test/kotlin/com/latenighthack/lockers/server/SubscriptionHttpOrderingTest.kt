package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.rpc.HttpRpcClient
import com.latenighthack.ktbuf.server.serveUnary
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.connector.SubscriptionStoreImpl as ClientSubscriptionStoreImpl
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.SessionGatewayService
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlin.random.Random
import kotlin.test.*

class SubscriptionHttpOrderingTest {
    @Test fun `late old HTTP subscribe cannot undo a confirmed same-session unsubscribe`(): Unit = runBlocking { withContext(Dispatchers.Default) {
        val serverDb = ServerStorage.inMemory()
        val core = ServerCore::class.create(LockersConfig.defaults().copy(deliveryWorkerEnabled = false), serverDb)
        core.setup()
        val key = Secp256r1KeyPair.generate(); val sid = SessionId(byteArrayOf(1)); val room = RoomId(byteArrayOf(2))
        core.sessionStore.updateSession(ServerSession(sessionId = ServerSessionId(sid.rawValue), authorizedPublicKey = key.publicKey.encode(), nextKeyMaterial = byteArrayOf(3)))
        val module = RoomServiceModule::class.create(core, object : SessionGatewayDiscovery { override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null }, LocalRoomOwnership())
        val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>(); val applied = CompletableDeferred<SubscriptionResponse>()
        val delayed = object : RoomServer by module.server {
            override suspend fun subscription(context: GrpcRequestContext, request: SubscriptionRequest): SubscriptionResponse {
                if (request.kind is SubscriptionRequest.OneOfKind.subscribe) {
                    entered.complete(Unit)
                    release.await()
                    return module.server.subscription(context, request).also { applied.complete(it) }
                }
                return module.server.subscription(context, request)
            }
        }
        val listener = embeddedServer(CIO, host = "127.0.0.1", port = 0) { routing {
            for (method in RoomServer.Descriptor.methods) {
                @Suppress("UNCHECKED_CAST")
                serveUnary(delayed as Any, RoomServer.Descriptor, method as ServerMethodDescriptor<Any, Any, Any>)
            }
        } }
        listener.start(false)
        val rpc = HttpRpcClient("127.0.0.1:${listener.engine.resolvedConnectors().single().port}")
        val clientDb = ConnectorStorage.inMemory(); clientDb.open()
        val sessionStore = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), clientDb).also { it.prepare() }
        val subscriptions = ClientSubscriptionStoreImpl(clientDb).also { it.prepare() }
        val controller = SubscriptionController(rpc, subscriptions, sessionStore, MutableStateFlow<SessionId?>(sid), coroutineContext = currentCoroutineContext(), signRequest = { operation, session, bytes ->
            val issued = System.currentTimeMillis(); val nonce = Random.nextBytes(32)
            SessionProof(issued, nonce, Signature(signingVersion = 2, signature = key.privateKey.sign(SessionSigning.context(operation, session, SHA256.digest(bytes), issued, nonce))))
        })
        try {
            controller.subscribe(room); withTimeout(5_000) { entered.await() }
            controller.unsubscribe(room)
            withTimeout(5_000) { while (subscriptions.getSubscription(room) != null) delay(10) }
            assertTrue(core.subscriptionStore.getAllSubscriptions(ServerSessionId(sid.rawValue)).isEmpty(), "new HTTP unsubscribe completed")
            release.complete(Unit)
            assertTrue(withTimeout(5_000) { applied.await() }.result.isOk(), "old authenticated HTTP subscribe actually committed after unsubscribe")
            assertTrue(core.subscriptionStore.getAllSubscriptions(ServerSessionId(sid.rawValue)).isEmpty(), "late old subscribe restored the revoked subscription")
        } finally { release.complete(Unit); controller.closeAndJoin(); rpc.close(); listener.stop(0, 1_000); module.serverImpl.closeAndJoin(); core.closeAndJoin(); clientDb.close(); serverDb.close() }
    } }
}
