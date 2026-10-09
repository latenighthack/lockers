package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.session.v1.*
import com.latenighthack.lockers.push.v1.*
import kotlinx.coroutines.*
import kotlin.test.*

class ReviewSessionProofTests {
    private fun auth(key: Secp256r1KeyPair) = object : AuthenticationKeySource {
        override suspend fun getSessionKeyPair() = key
        override suspend fun hasSessionKeyPair() = true
        override suspend fun generateSessionKeyPair() {}
        override suspend fun revokeKeys() {}
    }
    @Test fun `public unary operations carry fresh parameter bound session proofs`() = runBlocking {
        val key = Secp256r1KeyPair.generate()
        val verified = mutableListOf<String>()
        val nonces = mutableListOf<ByteArray>()
        var registrations = 0
        suspend fun verify(operation: String, session: SessionId?, proof: SessionProof?, unsigned: ByteArray) {
            val actual = assertNotNull(proof)
            assertEquals(2, actual.signature!!.signingVersion)
            assertContentEquals(key.publicKey.encode(), actual.signature!!.publicKey!!.rawValue)
            assertTrue(key.publicKey.verify(SessionSigning.context(operation, session!!, SHA256.digest(unsigned), actual.issuedAtMs, actual.nonce), actual.signature!!.signature))
            assertFalse(key.publicKey.verify(SessionSigning.context(operation, session, SHA256.digest(unsigned + byteArrayOf(1)), actual.issuedAtMs, actual.nonce), actual.signature!!.signature))
            verified += operation
        }
        val rpc = object : RpcClient {
            override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
                val bytes = when (method.methodName) {
                    "Capabilities" -> CapabilitiesResponse(subscriptionRevisions = true).toByteArray()
                    "GetAllLockers" -> GetAllLockersResponse().toByteArray()
                    "Subscription" -> {
                        val decoded = SubscriptionRequest.fromByteArray(request)
                        verify(SessionSigning.SUBSCRIPTION, decoded.sessionId, decoded.proof, decoded.copy(proof = null).toByteArray())
                        SubscriptionResponse(currentRevision = decoded.intentRevision).toByteArray()
                    }
                    "RegisterSession" -> {
                        val decoded = RegisterSessionRequest.fromByteArray(request)
                        assertTrue(decoded.credentialRevision > 0)
                        verify(SessionSigning.REGISTER_PUSH, decoded.sessionId, decoded.proof, decoded.copy(proof = null).toByteArray())
                        nonces += decoded.proof!!.nonce
                        if (++registrations == 1) throw FaultInjectingRpcClient.rpcError(Codes.UNAVAILABLE)
                        RegisterSessionResponse().toByteArray()
                    }
                    "UnregisterSession" -> {
                        val decoded = UnregisterSessionRequest.fromByteArray(request)
                        assertEquals(2, decoded.credentialRevision)
                        verify(SessionSigning.UNREGISTER_PUSH, decoded.sessionId, decoded.proof, decoded.copy(proof = null).toByteArray())
                        UnregisterSessionResponse().toByteArray()
                    }
                    "DestroySession" -> {
                        val decoded = DestroySessionRequest.fromByteArray(request)
                        verify(SessionSigning.DESTROY, decoded.sessionId, decoded.proof, decoded.copy(proof = null).toByteArray())
                        DestroySessionResponse().toByteArray()
                    }
                    else -> error(method.methodName)
                }
                return RpcResponse(bytes, emptyMap())
            }
            override suspend fun serverStreamingCall(method: RpcMethodSpecifier, block: suspend RpcServerStream.() -> Unit, readyCallback: () -> Unit) {
                var first = true
                block(object : RpcServerStream {
                    override suspend fun receive(): ByteArray {
                        if (first) { first = false; return WatchSessionResponse(response = WatchSessionResponse.OneOfResponse.open(WatchSessionResponse.Open(result = WatchSessionResponse.Open.Result.OK, nextSequenceKey = ByteArray(32)))).toByteArray() }
                        awaitCancellation()
                    }
                    override suspend fun send(bytes: ByteArray) {}
                    override suspend fun closeOutbound() {}
                    override suspend fun closeInbound() {}
                })
            }
        }
        val client = LockersClient.create(rpc, ConnectorStorage.inMemory(), KeyValueStore(InMemoryKeyValueStoreDelegate()), auth(key), Version())
        try {
            withTimeout(5_000) { client.awaitConnected(); client.lockers.subscribeToRoom(RoomId(byteArrayOf(1))) }
            client.registerPush(PushRegistrations.fcm("token"))
            withTimeout(5_000) { client.awaitPushRegistered(PushBackendType.FCM) }
            assertEquals(2, nonces.size)
            assertFalse(nonces[0].contentEquals(nonces[1]))
            client.unregisterPush(PushBackendType.FCM)
            withTimeout(5_000) { client.awaitPushUnregistered(PushBackendType.FCM) }
            client.destroySession()
            assertTrue(verified.containsAll(listOf(SessionSigning.SUBSCRIPTION, SessionSigning.REGISTER_PUSH, SessionSigning.UNREGISTER_PUSH, SessionSigning.DESTROY)))
            assertTrue(client.connection.value is StreamConnectionState.Closed)
        } finally { client.closeAndJoin() }
    }
}
