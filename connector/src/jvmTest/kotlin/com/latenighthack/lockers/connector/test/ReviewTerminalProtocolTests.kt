package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.*
import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.session.v1.*
import kotlinx.coroutines.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.*

class ReviewTerminalProtocolTests {
    @Test fun `local output ceiling fails one write attempt before network submission`() = runBlocking {
        val posts = AtomicInteger(); var transforms = 0
        val client = reviewClient(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true).toByteArray()
            "PostLockerChange" -> { posts.incrementAndGet(); error("oversized local packet reached network") }
            else -> error(method.methodName)
        } })
        try {
            assertFailsWith<ProtobufOutputLimitException> { withTimeout(1_500) {
                client.updateLocker(RoomId(byteArrayOf(1)), LockerId(byteArrayOf(2))) {
                    transforms++
                    ProtobufOutputStream.encode(ProtobufOutputLimits(maxMessageBytes = 8)) { encode(ByteArray(9), 1) }
                }
            } }
            assertEquals(1, transforms); assertEquals(0, posts.get())
        } finally { client.closeAndJoin() }
    }

    @Test fun `permanent stream protocol failures fail promptly without reconnecting`() = runBlocking {
        for (failure in listOf(
            RpcResponseException("test", "BIDI", Codes.OUT_OF_RANGE, "packet ceiling"),
            RpcResponseException("test", "BIDI", Codes.INVALID_ARGUMENT, "malformed packet"),
            ProtobufOutputLimitException("local packet ceiling"),
            ProtobufLimitException("incoming packet ceiling"),
        )) {
            val attempts = AtomicInteger()
            val rpc = object : RpcClient {
                override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray) = RpcResponse(CapabilitiesResponse().toByteArray(), emptyMap())
                override suspend fun serverStreamingCall(method: RpcMethodSpecifier, block: suspend RpcServerStream.() -> Unit, readyCallback: () -> Unit) {
                    attempts.incrementAndGet(); throw failure
                }
            }
            val db = ConnectorStorage.inMemory(); val key = Secp256r1KeyPair.generate()
            val client = LockersClient.create(rpc, db, KeyValueStore(InMemoryKeyValueStoreDelegate()), auth(key), Version(), coroutineContext = Dispatchers.Default)
            try {
                val observed = assertFailsWith<StreamFailedException> { withTimeout(1_500) { client.awaitConnected() } }
                assertIs<StreamFatalError.ProtocolRejected>(observed.error)
                assertEquals(1, attempts.get())
            } finally { client.closeAndJoin(); db.close() }
        }
    }

    @Test fun `malformed stream protobuf is permanent while temporary RPC quota reconnects`() = runBlocking {
        for (malformed in listOf(true, false)) {
            val attempts = AtomicInteger()
            val rpc = object : RpcClient {
                override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray) = RpcResponse(CapabilitiesResponse().toByteArray(), emptyMap())
                override suspend fun serverStreamingCall(method: RpcMethodSpecifier, block: suspend RpcServerStream.() -> Unit, readyCallback: () -> Unit) {
                    val attempt = attempts.incrementAndGet()
                    if (!malformed && attempt == 1) throw RpcResponseException("test", "BIDI", Codes.RESOURCE_EXHAUSTED, "temporary quota")
                    var sent = false
                    block(object : RpcServerStream {
                        override suspend fun receive(): ByteArray {
                            if (sent) awaitCancellation()
                            sent = true
                            return if (malformed) byteArrayOf(0x0a, 0x7f) else WatchSessionResponse(
                                response = WatchSessionResponse.OneOfResponse.open(WatchSessionResponse.Open(nextSequenceKey = ByteArray(32)))).toByteArray()
                        }
                        override suspend fun send(bytes: ByteArray) {}
                        override suspend fun closeOutbound() {}
                        override suspend fun closeInbound() {}
                    })
                }
            }
            val db = ConnectorStorage.inMemory(); val key = Secp256r1KeyPair.generate()
            val client = LockersClient.create(rpc, db, KeyValueStore(InMemoryKeyValueStoreDelegate()), auth(key), Version(), coroutineContext = Dispatchers.Default)
            try {
                if (malformed) {
                    val observed = assertFailsWith<StreamFailedException> { withTimeout(1_500) { client.awaitConnected() } }
                    assertIs<StreamFatalError.ProtocolRejected>(observed.error)
                    assertEquals(1, attempts.get())
                } else { withTimeout(2_000) { client.awaitConnected() }; assertEquals(2, attempts.get()) }
            } finally { client.closeAndJoin(); db.close() }
        }
    }

    private fun auth(key: Secp256r1KeyPair) = object : AuthenticationKeySource {
        override suspend fun getSessionKeyPair() = key
        override suspend fun hasSessionKeyPair() = true
        override suspend fun generateSessionKeyPair() {}
        override suspend fun revokeKeys() {}
    }
}
