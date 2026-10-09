package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.attachTestServices
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.test.*

class ReviewSubscriptionEchoTests {
    @Test fun `successful subscription without the exact requested revision cannot confirm intent`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val store = SubscriptionStoreImpl(db).also { it.prepare() }; var calls = 0
        val controller = SubscriptionController(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(subscriptionRevisions = true).toByteArray()
            "Subscription" -> { calls++; SubscriptionResponse(currentRevision = 0).toByteArray() }
            else -> error(method.methodName)
        } }, store, sessions, MutableStateFlow(SessionId(byteArrayOf(1))))
        try {
            controller.subscribe(RoomId(byteArrayOf(2)))
            assertFailsWith<IllegalArgumentException> { withTimeout(2_000) { controller.awaitSubscription(RoomId(byteArrayOf(2))) } }
            assertEquals(1, calls); assertTrue(store.getAllSubscriptions().single().isPendingAdd)
        } finally { controller.closeAndJoin(); db.close() }
    }
    @Test fun `malformed successful HTTP snapshot cannot publish data or subscription confirmation`() = runOwnedTestWithServer({ attachTestServices() }) { server, _ -> withContext(Dispatchers.Default) {
        val room = RoomId(byteArrayOf(21)); val id = LockerId(byteArrayOf(22), LockerKeyspace(0))
        val writer = reviewClient(server.ownedRpcClient)
        val key = Secp256r1KeyPair.generate()
        val auth = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = key
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() {}
            override suspend fun revokeKeys() {}
        }
        var calls = 0
        val malformed = object : RpcClient by server.ownedRpcClient {
            override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
                val response = server.ownedRpcClient.unaryCall(method, headers, request)
                if (method.methodName == "SubscribeAndSnapshot") {
                    calls++; return RpcResponse(SubscribeAndSnapshotResponse.fromByteArray(response.data).copy(currentRevision = 0).toByteArray(), emptyMap())
                }
                return response
            }
        }
        val db = ConnectorStorage.inMemory(); var receiver: LockersClient? = null
        try {
            writer.updateLocker(room, id) { byteArrayOf(9) }
            val client = createOwnedTestClient(malformed, db, KeyValueStore(InMemoryKeyValueStoreDelegate()), auth, Version()).also { receiver = it }
            withTimeout(5_000) { client.awaitConnected() }
            assertFailsWith<IllegalArgumentException> { withTimeout(5_000) { client.lockers.subscribeToRoom(room) } }
            assertEquals(1, calls); assertTrue(client.getAllKnownLockers().isEmpty())
            assertTrue(SubscriptionStoreImpl(db).getAllSubscriptions().single().isPendingAdd)
        } finally { writer.closeAndJoin(); receiver?.closeAndJoin() }
    } }
}
