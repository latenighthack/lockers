package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.session.v1.*
import kotlinx.coroutines.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.*

class ReviewSessionAdmissionTests {
    @Test fun `malformed session open is terminal and temporary capacity rejection reconnects`(): Unit = runBlocking {
        for (first in listOf(WatchSessionResponse.Open.Result.INVALID_REQUEST, WatchSessionResponse.Open.Result.NAMESPACE_EXHAUSTED, WatchSessionResponse.Open.Result.RESOURCE_EXHAUSTED)) {
            val opens = AtomicInteger()
            val rpc = object : RpcClient {
                override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray) = RpcResponse(CapabilitiesResponse().toByteArray(), emptyMap())
                override suspend fun serverStreamingCall(method: RpcMethodSpecifier, block: suspend RpcServerStream.() -> Unit, readyCallback: () -> Unit) {
                    val attempt = opens.incrementAndGet(); var pending = true
                    block(object : RpcServerStream {
                        override suspend fun receive(): ByteArray {
                            if (!pending) awaitCancellation()
                            pending = false
                            return WatchSessionResponse(response = WatchSessionResponse.OneOfResponse.open(WatchSessionResponse.Open(
                                result = if (attempt == 1) first else WatchSessionResponse.Open.Result.OK, nextSequenceKey = ByteArray(32)))).toByteArray()
                        }
                        override suspend fun send(bytes: ByteArray) {}
                        override suspend fun closeOutbound() {}
                        override suspend fun closeInbound() {}
                    })
                }
            }
            val key = Secp256r1KeyPair.generate(); val db = ConnectorStorage.inMemory()
            val client = LockersClient.create(rpc, db, KeyValueStore(InMemoryKeyValueStoreDelegate()), object : AuthenticationKeySource {
                override suspend fun getSessionKeyPair() = key
                override suspend fun hasSessionKeyPair() = true
                override suspend fun generateSessionKeyPair() {}
                override suspend fun revokeKeys() {}
            }, Version(), coroutineContext = Dispatchers.Default + currentCoroutineContext()[Job]!!)
            try {
                if (first != WatchSessionResponse.Open.Result.RESOURCE_EXHAUSTED) {
                    val failure = assertFailsWith<StreamFailedException> { withTimeout(5_000) { client.awaitConnected() } }
                    assertSame(if (first == WatchSessionResponse.Open.Result.NAMESPACE_EXHAUSTED) StreamFatalError.NamespaceExhausted else StreamFatalError.InvalidRequest, failure.error); assertEquals(1, opens.get())
                } else { withTimeout(5_000) { client.awaitConnected() }; assertEquals(2, opens.get()) }
            } finally { client.closeAndJoin(); db.close() }
        }
    }
}
