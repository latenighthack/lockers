package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.connector.internal.*
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.*
import kotlin.test.*

internal class ReviewRpc(val response: suspend (RpcMethodSpecifier, ByteArray) -> ByteArray) : RpcClient {
    override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray) = RpcResponse(response(method, request), emptyMap())
    override suspend fun serverStreamingCall(method: RpcMethodSpecifier, block: suspend RpcServerStream.() -> Unit, readyCallback: () -> Unit) = error("not used")
}

internal suspend fun reviewClient(rpc: RpcClient, keys: LockKeySource? = null, db: Database = ConnectorStorage.inMemory()): LockerClient {
    db.open()
    val key = Secp256r1KeyPair.generate()
    val auth = object : AuthenticationKeySource {
        override suspend fun getSessionKeyPair() = key
        override suspend fun hasSessionKeyPair() = true
        override suspend fun generateSessionKeyPair() {}
        override suspend fun revokeKeys() {}
    }
    val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
    val subscriptions = SubscriptionStoreImpl(db).also { it.prepare() }
    return LockerClient(rpc, Stream(rpc, auth, session, subscriptions, Version()), LockerStoreImpl(db).also { it.prepare() }, keys)
}

class ReviewConnectorTests {
    @Test fun `committed ratchet adopts the new key even when agent fails`() = runBlocking {
        val old = Secp256r1KeyPair.generate()
        var adopted: Secp256r1KeyPair? = null
        var submitted: PostLockerChangeRequest? = null
        val source = object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = old
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) { adopted = newKeyPair }
        }
        val rpc = ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true).toByteArray()
            "PostLockerChange" -> { submitted = PostLockerChangeRequest.fromByteArray(bytes); PostLockerChangeResponse(result = PostLockerChangeResponse.Result.OK, version = 1, agentFailed = true).toByteArray() }
            else -> error(method.methodName)
        } }
        val client = reviewClient(rpc, source)
        try {
            runCatching { client.updateLocker(RoomId(byteArrayOf(1)), LockerId(byteArrayOf(2)), ratchet = true) { byteArrayOf(3) } }
            assertNotNull(adopted, "Source committed, so its new authority must be adopted before reporting agent status")
            assertContentEquals(submitted!!.ratchet!!.newPublicKey!!.rawValue, adopted!!.publicKey.encode())
            assertEquals(1, client.getAllKnownLockers().size)
        } finally { client.stop() }
    }
    @Test fun `ratchet journal survives client replacement and replays the exact request`() = runBlocking {
        val database = ConnectorStorage.inMemory()
        val old = Secp256r1KeyPair.generate()
        var requestBytes: ByteArray? = null
        var adopted: Secp256r1KeyPair? = null
        var calls = 0
        val rpc = ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true).toByteArray()
            "PostLockerChange" -> {
                calls++
                if (requestBytes == null) requestBytes = bytes else assertContentEquals(requestBytes, bytes)
                PostLockerChangeResponse(result = PostLockerChangeResponse.Result.OK, version = 1).toByteArray()
            }
            else -> error(method.methodName)
        } }
        fun source(crash: Boolean) = object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = old
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) {
                if (crash) throw IllegalStateException("app terminated before key adoption")
                adopted = newKeyPair
            }
        }
        val client = reviewClient(rpc, source(true), database)
        try { assertFailsWith<IllegalStateException> { client.updateLocker(RoomId(byteArrayOf(1)), LockerId(byteArrayOf(2)), ratchet = true) { byteArrayOf(3) } } } finally { client.stop() }
        assertEquals(1, LockerStoreImpl(database).pendingRatchets().size)
        val replacement = reviewClient(rpc, source(false), database)
        try {
            replacement.start()
            withTimeout(5_000) { while (LockerStoreImpl(database).pendingRatchets().isNotEmpty()) delay(10) }
            assertNotNull(adopted)
            assertContentEquals(PostLockerChangeRequest.fromByteArray(requestBytes!!).ratchet!!.newPublicKey!!.rawValue, adopted!!.publicKey.encode())
            assertEquals(2, calls)
            assertEquals(1, replacement.getAllKnownLockers().size)
        } finally { replacement.stop() }
    }

    @Test fun `null and zero keyspaces are rejected as duplicate batch identities`() = runBlocking {
        var calls = 0
        val rpc = ReviewRpc { _, _ -> calls++; error("duplicate batch must be rejected before I/O") }
        val client = reviewClient(rpc)
        try {
            val raw = byteArrayOf(2)
            assertFailsWith<IllegalArgumentException> { client.updateLockers(RoomId(byteArrayOf(1)), listOf(
                LockerClient.Change(LockerId(raw)) { byteArrayOf(3) },
                LockerClient.Change(LockerId(raw, LockerKeyspace(0))) { byteArrayOf(4) },
            )) }
            assertEquals(0, calls)
        } finally { client.stop() }
    }

}
