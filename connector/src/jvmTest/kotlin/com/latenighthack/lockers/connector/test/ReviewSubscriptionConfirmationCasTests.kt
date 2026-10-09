package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.attachTestServices
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.datetime.Clock
import kotlin.test.*

class ReviewSubscriptionConfirmationCasTests {
    @Test fun `late HTTP subscribe confirmation cannot recreate another controller removal`() = verifyLateConfirmation(true)
    @Test fun `late HTTP unsubscribe confirmation cannot delete another controller resubscription`() = verifyLateConfirmation(false)
    private fun verifyLateConfirmation(subscribe: Boolean) = runOwnedTestWithServer({ attachTestServices() }) { server, _ -> withContext(Dispatchers.Default) {
        val key = Secp256r1KeyPair.generate()
        val auth = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = key
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() {}
            override suspend fun revokeKeys() {}
        }
        val database = ConnectorStorage.inMemory(); val kv = KeyValueStore(InMemoryKeyValueStoreDelegate())
        val host = createOwnedTestClient(server.ownedRpcClient, database, kv, auth, Version())
        val sessions = SessionStoreImpl(kv, database).also { it.prepare() }
        val store = SubscriptionStoreImpl(database).also { it.prepare() }
        val held = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>(); val returned = CompletableDeferred<Unit>()
        val gate = object : RpcClient by server.ownedRpcClient {
            override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
                val response = server.ownedRpcClient.unaryCall(method, headers, request)
                if (method.methodName == "Subscription") {
                    val frame = SubscriptionRequest.fromByteArray(request)
                    if ((frame.kind is SubscriptionRequest.OneOfKind.subscribe) == subscribe) {
                        held.complete(Unit); withContext(NonCancellable) { release.await() }; returned.complete(Unit)
                    }
                }
                return response
            }
        }
        suspend fun proof(operation: String, sid: SessionId, unsigned: ByteArray): SessionProof {
            val issued = Clock.System.now().toEpochMilliseconds(); val nonce = kotlin.random.Random.nextBytes(32)
            return SessionProof(issued, nonce, Signature(publicKey = Secp256R1Key.PublicKey(key.publicKey.encode()), signingVersion = 2,
                signature = key.privateKey.sign(SessionSigning.context(operation, sid, SHA256.digest(unsigned), issued, nonce))))
        }
        var a: SubscriptionController? = null; var b: SubscriptionController? = null
        try {
            withTimeout(5_000) { host.awaitConnected() }
            val sid = host.sessionId.value!!; val room = RoomId(byteArrayOf(7))
            val first = SubscriptionController(gate, store, sessions, MutableStateFlow(sid), signRequest = ::proof).also { a = it }
            if (!subscribe) { first.subscribe(room); withTimeout(5_000) { first.awaitSubscription(room) } }
            if (subscribe) first.subscribe(room) else first.unsubscribe(room)
            withTimeout(5_000) { held.await() }
            val second = SubscriptionController(server.ownedRpcClient, store, sessions, MutableStateFlow(sid), signRequest = ::proof).also { b = it }
            if (subscribe) second.unsubscribe(room) else second.subscribe(room)
            if (subscribe) withTimeout(5_000) { while (store.getAllSubscriptions().isNotEmpty()) delay(10) }
            else withTimeout(5_000) { second.awaitSubscription(room) }
            val expected = store.intentRevision(room)!!
            release.complete(Unit); returned.await(); delay(100)
            if (subscribe) assertTrue(store.getAllSubscriptions().isEmpty(), "late subscribe ACK recreated a newer durable removal")
            else assertFalse(store.getAllSubscriptions().single().isPendingRemove, "late unsubscribe ACK deleted newer resubscription")
            assertEquals(expected.revision, store.intentRevision(room)!!.revision)
            assertEquals(expected.subscribed, store.intentRevision(room)!!.subscribed)
            withTimeout(2_000) { first.failures.first { room in it } }
            assertIs<SubscriptionRevisionConflictException>(first.failures.value[room])
        } finally { release.complete(Unit); a?.closeAndJoin(); b?.closeAndJoin(); host.closeAndJoin() }
    } }
}
