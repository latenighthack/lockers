package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktbuf.test.server.runTestWithServer
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.RoomKeying
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.server.attachFastpathTestServices
import com.latenighthack.lockers.server.rpcClient
import io.ktor.server.application.Application
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.*

class FastpathClientTests {
    private suspend fun client(rpc: RpcClient, writeKey: Secp256r1KeyPair): LockersClient {
        val key = Secp256r1KeyPair.generate()
        return LockersClient.create(rpc, InMemoryStoreDelegate(), KeyValueStore(InMemoryKeyValueStoreDelegate()),
            object : AuthenticationKeySource {
                override suspend fun getSessionKeyPair() = key
                override suspend fun hasSessionKeyPair() = true
                override suspend fun generateSessionKeyPair() {}
                override suspend fun revokeKeys() {}
            }, Version(), object : LockKeySource { override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = writeKey })
            .also { it.awaitConnected() }
    }

    @Test fun signedBatchAndAmbiguousSingleWriteReplay() = runTestWithServer(Application::attachFastpathTestServices) { server, _ ->
        val counts = ConcurrentHashMap<String, AtomicInteger>()
        var loseReply = false
        val rpc = object : RpcClient by server.rpcClient {
            override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
                counts.computeIfAbsent(method.methodName) { AtomicInteger() }.incrementAndGet()
                val response = server.rpcClient.unaryCall(method, headers, request)
                if (method.methodName == "PostLockerChange" && loseReply) {
                    loseReply = false
                    throw RpcResponseException("test", "POST", Codes.UNAVAILABLE, "lost response after commit")
                }
                return response
            }
        }
        val key = Secp256r1KeyPair.generate()
        val client = client(rpc, key)
        try {
            val room = RoomKeying.publicKeyed(key.publicKey.encode())
            val ids = (1..2).map { LockerId(byteArrayOf(it.toByte()), LockerKeyspace(99)) }
            client.lockers.updateLockers(room, ids.map { id -> LockerClient.Change(id) { byteArrayOf(1) } }, key)
            assertEquals(1, counts["PostLockerChanges"]?.get())
            assertEquals(null, counts["LockLocker"]?.get())
            loseReply = true
            client.lockers.updateLocker(room, ids.first()) { old -> byteArrayOf((old.first() + 1).toByte()) }
            assertEquals(2, counts["PostLockerChange"]?.get())
            val body = client.lockers.getLocker(room, ids.first(), revalidate = false)!!.locker!!
            assertContentEquals(byteArrayOf(2), body.open!!.encodedPayload)
        } finally { client.close() }
    }
}
