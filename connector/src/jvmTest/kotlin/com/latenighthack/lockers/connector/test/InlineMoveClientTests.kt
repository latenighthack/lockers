package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.net.RpcMethodSpecifier
import com.latenighthack.ktbuf.net.RpcResponse
import com.latenighthack.ktbuf.test.server.TestServer
import com.latenighthack.ktcrypto.Secp256r1KeyPair
import com.latenighthack.ktcrypto.encode
import com.latenighthack.ktcrypto.generate
import com.latenighthack.lockers.common.LockerEnvelope
import com.latenighthack.lockers.common.RoomKeying
import com.latenighthack.lockers.common.v1.Locker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.LockScope
import com.latenighthack.lockers.common.v1.LockScopeKind
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.connector.LockKeySource
import com.latenighthack.lockers.connector.LockerClient
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.agents.TransactionalLockerAgentRegistry
import com.latenighthack.lockers.server.attachTestServicesWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** A move against a transactional agent is one round trip that also brings back its frames. */
class InlineMoveClientTests {
    private val frameKeyspace = LockerKeyspace(30)

    /** Publishes the move's plaintext as an open frame locker, committed with the move. */
    private val agent = object : TransactionalLockerAgentRegistry {
        override suspend fun prepare(roomId: RoomId, lockerId: LockerId, locker: Locker) =
            TransactionalLockerAgentRegistry.Prepared(listOf(LockerAgentRegistry.LockerWrite(
                LockerId(lockerId.rawValue, frameKeyspace),
                Locker { open { encodedPayload = LockerEnvelope.payload(locker) } })))
    }

    private class Counting(private val inner: RpcClient) : RpcClient by inner {
        val calls = ConcurrentHashMap<String, AtomicInteger>()
        fun count(method: String) = calls[method]?.get() ?: 0
        override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>,
            request: ByteArray): RpcResponse {
            calls.computeIfAbsent(method.methodName) { AtomicInteger() }.incrementAndGet()
            return inner.unaryCall(method, headers, request)
        }
    }

    private fun keys(key: Secp256r1KeyPair) = object : LockKeySource {
        override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = key
    }

    private fun withAgentServer(block: suspend (TestServer) -> Unit) = runOwnedTestWithServer({
        attachTestServicesWith { core -> core.overrideAgentRegistry = agent }
    }) { server, _ -> withContext(Dispatchers.Default) { block(server) } }

    @Test(timeout = 30_000)
    fun `signed moves reuse known authority and return their frames`() = withAgentServer { server ->
        val key = Secp256r1KeyPair.generate()
        val room = RoomKeying.publicKeyed(key.publicKey.encode())
        val id = LockerId(byteArrayOf(1), LockerKeyspace(31))
        val rpc = Counting(server.ownedRpcClient)
        val client = reviewClient(rpc, keys(key))
        try {
            // Locking the room with the first write teaches the client the room's authority.
            client.updateLockers(room, listOf(LockerClient.Change(id) { byteArrayOf(0) }), key)
            rpc.calls.clear()
            for (move in 1..3) {
                client.updateLocker(room, id) { byteArrayOf(move.toByte()) }
                // Frames came back with the move; nothing else was fetched.
                val frameId = LockerId(id.rawValue, frameKeyspace)
                val frame = assertNotNull(client.getLocker(room, frameId, revalidate = false))
                assertContentEquals(byteArrayOf(move.toByte()), LockerEnvelope.payload(requireNotNull(frame.locker)))
            }
            assertEquals(3, rpc.count("PostLockerChange"))
            assertEquals(0, rpc.count("GetLocker"), "authority read before a move: ${rpc.calls}")
        } finally { client.closeAndJoin() }
    }

    @Test(timeout = 30_000)
    fun `a move signed with outdated authority is repaired and commits`() = withAgentServer { server ->
        val key = Secp256r1KeyPair.generate()
        val room = RoomKeying.publicKeyed(key.publicKey.encode())
        val id = LockerId(byteArrayOf(2), LockerKeyspace(31))
        val rpc = Counting(server.ownedRpcClient)
        val client = reviewClient(rpc, keys(key))
        val admin = reviewClient(server.ownedRpcClient)
        try {
            client.updateLockers(room, listOf(LockerClient.Change(id) { byteArrayOf(0) }), key)
            client.updateLocker(room, id) { byteArrayOf(1) }
            // Another client re-grants the room, so the client's remembered epoch is now stale.
            val root = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM)
            val epoch = assertNotNull(admin.getLockScope(room, root).scopeState).lockVersion
            assertTrue(admin.unlockLocker(room, root, key, epoch).result.isOk())
            assertTrue(admin.lockLocker(room, root, key, key).result.isOk())
            rpc.calls.clear()
            client.updateLocker(room, id) { byteArrayOf(2) }
            val stored = assertNotNull(client.getLocker(room, id, revalidate = false))
            assertContentEquals(byteArrayOf(2), LockerEnvelope.payload(requireNotNull(stored.locker)))
            // One rejected attempt and one re-signed attempt, at most one fresh authority read.
            assertEquals(2, rpc.count("PostLockerChange"), "${rpc.calls}")
            assertTrue(rpc.count("GetLocker") <= 1, "${rpc.calls}")
        } finally { client.closeAndJoin(); admin.closeAndJoin() }
    }
}
