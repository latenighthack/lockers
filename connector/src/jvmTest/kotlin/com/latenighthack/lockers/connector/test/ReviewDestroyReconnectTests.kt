package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.server.*
import com.latenighthack.lockers.server.services.push.v1.providers.PushBackendKind
import com.latenighthack.lockers.session.v1.*
import kotlinx.coroutines.*
import kotlin.test.*

class ReviewDestroyReconnectTests {
    private fun auth(key: Secp256r1KeyPair) = object : AuthenticationKeySource {
        override suspend fun getSessionKeyPair() = key
        override suspend fun hasSessionKeyPair() = true
        override suspend fun generateSessionKeyPair() {}
        override suspend fun revokeKeys() {}
    }
    @Test fun revocationClosesReconnectBeforeADelayedServerReply() {
        lateinit var core: ServerCore
        runOwnedTestWithServer({ attachTestServicesWith {
            core = it
            it.overridePushProviders = listOf(RecordingPushProvider(PushBackendKind.FCM))
        } }) { server, _ -> withContext(Dispatchers.Default) {
            val accepted = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
            val delegate = server.ownedRpcClient
            val delayed = object : RpcClient by delegate {
                override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
                    val response = delegate.unaryCall(method, headers, request)
                    if (method.methodName == "DestroySession") { accepted.complete(Unit); release.await() }
                    return response
                }
            }
            val client = createOwnedTestClient(delayed, ConnectorStorage.inMemory(), KeyValueStore(InMemoryKeyValueStoreDelegate()), auth(Secp256r1KeyPair.generate()), Version())
            withTimeout(5000) { client.awaitConnected(); client.lockers.subscribeToRoom(RoomId(byteArrayOf(7))) }
            client.registerPush(PushRegistrations.fcm("token"))
            withTimeout(5000) { client.awaitPushRegistered(PushBackendType.FCM) }
            val destroying = async { client.destroySession() }
            try {
                withTimeout(5000) { accepted.await() }
                delay(2500) // actual transport close and both reconnect passes, with reply still held
                assertTrue(client.connection.value is StreamConnectionState.Closed, "Revocation must be terminal before its RPC returns")
                assertTrue(core.sessionStore.getAllSessions().isEmpty(), "Reconnect created surviving replacement authority")
            } finally { release.complete(Unit); destroying.await() }
            assertTrue(core.sessionStore.getAllSessions().isEmpty())
        } }
    }
    @Test fun cancelledRevocationStaysClosedAndExplicitRetryTargetsTheSameIdentity() {
        lateinit var core: ServerCore
        runOwnedTestWithServer({ attachTestServicesWith { core = it } }) { server, _ -> withContext(Dispatchers.Default) {
            val entered = CompletableDeferred<Unit>()
            val delegate = server.ownedRpcClient
            val requests = mutableListOf<DestroySessionRequest>()
            val held = object : RpcClient by delegate {
                override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
                    if (method.methodName == "DestroySession") {
                        requests.add(DestroySessionRequest.fromByteArray(request))
                        if (requests.size == 1) { entered.complete(Unit); awaitCancellation() }
                    }
                    return delegate.unaryCall(method, headers, request)
                }
            }
            val values = KeyValueStore(InMemoryKeyValueStoreDelegate())
            val client = createOwnedTestClient(held, ConnectorStorage.inMemory(), values, auth(Secp256r1KeyPair.generate()), Version())
            withTimeout(5000) { client.awaitConnected() }
            val original = requireNotNull(client.sessionId.value)
            val destroying = launch { client.destroySession() }
            withTimeout(5000) { entered.await() }
            destroying.cancelAndJoin()
            assertTrue(client.connection.value is StreamConnectionState.Closed)
            assertEquals(1, core.sessionStore.getAllSessions().size)
            client.destroySession()
            assertTrue(core.sessionStore.getAllSessions().isEmpty())
            assertEquals(2, requests.size)
            requests.forEach { assertContentEquals(original.rawValue, it.sessionId!!.rawValue) }
            assertFalse(requests[0].proof!!.nonce.contentEquals(requests[1].proof!!.nonce))
        } }
    }
}
