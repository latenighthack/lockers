package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.LockerSigning
import com.latenighthack.lockers.common.RoomKeying
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.*
import kotlinx.coroutines.*
import kotlin.test.*

class ReviewConsumerSignedWritesTests {
    @Test(timeout = 20_000) fun `public typed writes inherit a room epoch for new nonzero keyspace lockers`() = verifyScope(LockScopeKind.LOCK_SCOPE_ROOM)
    @Test(timeout = 20_000) fun `public typed writes inherit a keyspace epoch for absent lockers`() = verifyScope(LockScopeKind.LOCK_SCOPE_KEYSPACE)
    private fun verifyScope(kind: LockScopeKind) = runOwnedTestWithServer({ attachTestServices() }) { server, _ -> withContext(Dispatchers.Default) {
        val key = Secp256r1KeyPair.generate()
        val room = RoomKeying.publicKeyed(key.publicKey.encode())
        val id = LockerId(Secp256r1KeyPair.generate().publicKey.encode(), LockerKeyspace(2))
        var epoch = 0L
        var writes = 0
        val checked = object : RpcClient by server.ownedRpcClient {
            override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
                if (method.methodName == "PostLockerChange") {
                    val change = PostLockerChangeRequest.fromByteArray(request)
                    val signature = requireNotNull(change.writeSignature)
                    val payload = requireNotNull(change.locker?.sealed?.payload)
                    assertEquals(2, signature.signingVersion)
                    assertEquals(1, epoch)
                    val inner = requireNotNull(payload.enclosure).innerPayload
                    assertContentEquals(SHA256.digest(inner), payload.checksum)
                    assertTrue(key.publicKey.verify(LockerSigning.writeContextV2(room, requireNotNull(change.lockerId), change.parentVersion, epoch,
                        SHA256.digest(inner), change.notification), signature.signature), "Signature no longer binds transmitted request")
                    writes++
                }
                val response = server.ownedRpcClient.unaryCall(method, headers, request)
                if (method.methodName == "GetLocker") epoch = GetLockerResponse.fromByteArray(response.data).locker?.lockState?.lockVersion ?: 0
                return response
            }
        }
        val client = reviewClient(checked, object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = key
        })
        try {
            val scope = LockScope(kind = kind, keyspace = if (kind == LockScopeKind.LOCK_SCOPE_KEYSPACE) id.keyspace else null)
            assertTrue(client.lockLocker(room, scope, key, key).result.isOk())
            if (kind == LockScopeKind.LOCK_SCOPE_ROOM) client.updateLocker(room, LockerId(byteArrayOf(1), LockerKeyspace(1))) { byteArrayOf(7) }
            val typed = TypedLockerClient(client, requireNotNull(id.keyspace), RoomId::toByteArray, RoomId.Companion::fromByteArray)
            val written = typed.updateLocker(room, id) { it.copy(rawValue = ByteArray(33) { 9 }) }
            assertContentEquals(ByteArray(33) { 9 }, written!!.rawValue)
            assertTrue(writes > 0)
        } finally { client.closeAndJoin() }
        Unit
    } }
}
