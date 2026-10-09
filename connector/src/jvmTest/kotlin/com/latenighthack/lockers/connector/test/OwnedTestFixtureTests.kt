package com.latenighthack.lockers.connector.test

import com.latenighthack.ktstore.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktbuf.net.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.server.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import kotlin.test.*

class OwnedTestFixtureTests {
    private class ExpectedFailure : Exception()
    @Test(timeout = 10_000) fun `fixture failure drains forgotten SDK clients and HTTP transports`() {
        var captured: LockersClient? = null; var transport: RpcClient? = null
        assertFailsWith<ExpectedFailure> {
            runOwnedTestWithServer({ attachTestServices() }) { server, _ ->
                val key = Secp256r1KeyPair.generate()
                val rpc = server.ownedRpcClient.also { transport = it }
                val client = createOwnedTestClient(rpc, ConnectorStorage.inMemory(), KeyValueStore(InMemoryKeyValueStoreDelegate()),
                    object : AuthenticationKeySource {
                        override suspend fun getSessionKeyPair() = key
                        override suspend fun hasSessionKeyPair() = true
                        override suspend fun generateSessionKeyPair() {}
                        override suspend fun revokeKeys() {}
                    }, Version()).also { captured = it }
                client.awaitConnected()
                throw ExpectedFailure()
            }
        }
        assertTrue(captured!!.connection.value is StreamConnectionState.Closed)
        runBlocking {
            val rejected = assertFailsWith<RpcResponseException> {
                transport!!.unaryCall(RpcMethodSpecifier("test", "Test", "Call"), emptyMap(), byteArrayOf())
            }
            assertEquals(com.latenighthack.ktbuf.proto.Codes.CANCELLED, rejected.code)
        }
    }
    @Test(timeout = 10_000) fun `normal fixture return drains forgotten SDK clients`() {
        var captured: LockersClient? = null; var collectorJoined = false
        runOwnedTestWithServer({ attachTestServices() }) { server, _ ->
            val key = Secp256r1KeyPair.generate()
            captured = createOwnedTestClient(server.ownedRpcClient, ConnectorStorage.inMemory(), KeyValueStore(InMemoryKeyValueStoreDelegate()),
                object : AuthenticationKeySource {
                    override suspend fun getSessionKeyPair() = key
                    override suspend fun hasSessionKeyPair() = true
                    override suspend fun generateSessionKeyPair() {}
                    override suspend fun revokeKeys() {}
                }, Version()).also { it.awaitConnected() }
            launch(start = CoroutineStart.UNDISPATCHED) {
                try { captured.lockers.notifications.first() } finally { collectorJoined = true }
            }
        }
        assertTrue(collectorJoined)
        assertTrue(captured!!.connection.value is StreamConnectionState.Closed)
    }
    @Test(timeout = 15_000) fun `authentication generation replaces the live session and its persisted proof`() {
        runOwnedTestWithServer({ attachTestServices() }) { server, _ ->
            var key = Secp256r1KeyPair.generate()
            val generation = kotlinx.coroutines.flow.MutableStateFlow(0L)
            val client = createOwnedTestClient(server.ownedRpcClient, ConnectorStorage.inMemory(),
                KeyValueStore(InMemoryKeyValueStoreDelegate()),
                object : AuthenticationKeySource, AuthenticationSessionGeneration {
                    override val sessionGeneration = generation
                    override suspend fun getSessionKeyPair() = key
                    override suspend fun hasSessionKeyPair() = true
                    override suspend fun generateSessionKeyPair() {}
                    override suspend fun revokeKeys() {}
                }, Version())
            withContext(Dispatchers.Default) { withTimeout(5_000) {
                client.awaitConnected()
                val oldSession = client.sessionId.value
                key = Secp256r1KeyPair.generate()
                generation.value += 1
                val newSession = client.sessionId.first { it != null && it != oldSession }
                assertNotEquals(oldSession, newSession)
                // This unary proof must authenticate against the replacement key, not the old session.
                client.lockers.subscribeToRoom(RoomId(byteArrayOf(8, 7, 6)))
            } }
        }
    }

}
