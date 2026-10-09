package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.LockerSigning
import com.latenighthack.lockers.common.RoomKeying
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.*
import kotlinx.coroutines.*
import kotlin.test.*

class ReviewAuthorityDriftTests {
    @Test(timeout = 20_000) fun `new room authority re-signs a definitively rejected frozen write`() = race(LockScopeKind.LOCK_SCOPE_ROOM)
    @Test(timeout = 20_000) fun `new keyspace authority re-signs a definitively rejected frozen write`() = race(LockScopeKind.LOCK_SCOPE_KEYSPACE)
    @Test(timeout = 20_000) fun `child authority counters do not compare against the ancestor counter`() = race(LockScopeKind.LOCK_SCOPE_KEYSPACE, withRoomHistory = true)
    @Test(timeout = 20_000) fun `different authority key remains a permanent rejection`() = race(LockScopeKind.LOCK_SCOPE_ROOM, differentKey = true)
    @Test(timeout = 20_000) fun `lost rejection reply never permits re-signing an ambiguous request`() = race(LockScopeKind.LOCK_SCOPE_ROOM, ambiguous = true)

    private fun race(kind: LockScopeKind, differentKey: Boolean = false, ambiguous: Boolean = false, withRoomHistory: Boolean = false) =
        runOwnedTestWithServer({ attachTestServices() }) { server, _ -> withContext(Dispatchers.Default) {
            val key = Secp256r1KeyPair.generate()
            val authorityKey = if (differentKey) Secp256r1KeyPair.generate() else key
            val room = RoomKeying.publicKeyed(key.publicKey.encode())
            val id = LockerId(Secp256r1KeyPair.generate().publicKey.encode(), LockerKeyspace(2))
            val discovered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
            var held = false; var transforms = 0; var encodes = 0
            val requests = java.util.concurrent.CopyOnWriteArrayList<PostLockerChangeRequest>()
            val checked = object : RpcClient by server.ownedRpcClient {
                override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
                    if (method.methodName == "PostLockerChange") requests += PostLockerChangeRequest.fromByteArray(request)
                    val response = server.ownedRpcClient.unaryCall(method, headers, request)
                    if (method.methodName == "GetLocker" && !held) {
                        held = true
                        val observed = GetLockerResponse.fromByteArray(response.data).locker!!.lockState
                        if (withRoomHistory) assertEquals(5L, observed!!.lockVersion) else assertNull(observed)
                        discovered.complete(Unit); release.await()
                    }
                    if (ambiguous && method.methodName == "PostLockerChange" && requests.size == 1) {
                        assertIs<PostLockerChangeResponse.Result.SIGNATURE_INVALID>(PostLockerChangeResponse.fromByteArray(response.data).result)
                        throw RpcResponseException("test", "POST", Codes.UNAVAILABLE, "Reply lost after authority rejection")
                    }
                    return response
                }
            }
            val codec = object : NotificationCodec {
                override suspend fun decode(context: NotificationContext, payload: ByteArray) = payload
                override suspend fun encode(context: NotificationContext, payload: ByteArray): ByteArray { encodes++; return byteArrayOf(8) + payload }
            }
            val client = reviewClient(checked, object : LockKeySource {
                override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = key
            }, codecs = NotificationCodecs.of(codec))
            val concurrent = reviewClient(server.ownedRpcClient)
            try {
                if (withRoomHistory) {
                    val root = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM)
                    repeat(3) { incarnation ->
                        val grant = concurrent.lockLocker(room, root, key, key)
                        assertTrue(grant.result.isOk())
                        val epoch = requireNotNull(grant.lockState).lockVersion
                        assertEquals(incarnation * 2L + 1, epoch)
                        if (incarnation < 2) assertTrue(concurrent.unlockLocker(room, root, key, epoch).result.isOk())
                    }
                }
                val outcome = async { runCatching { client.updateLocker(room, id, { payload { rawValue = byteArrayOf(9) } }) { transforms++; byteArrayOf(3) } } }
                withTimeout(5_000) { discovered.await() }
                val scope = LockScope(kind = kind, keyspace = if (kind == LockScopeKind.LOCK_SCOPE_KEYSPACE) id.keyspace else null)
                assertTrue(concurrent.lockLocker(room, scope, authorityKey, key).result.isOk())
                release.complete(Unit)
                val result = withTimeout(5_000) { outcome.await() }
                assertEquals(1, transforms); assertEquals(1, encodes)
                if (differentKey || ambiguous) {
                    assertIs<LockerWriteException>(result.exceptionOrNull())
                    assertEquals(if (ambiguous) 2 else 1, requests.size)
                    if (ambiguous) assertContentEquals(requests[0].toByteArray(), requests[1].toByteArray())
                    assertTrue(client.getAllKnownLockers().isEmpty())
                } else {
                    assertContentEquals(byteArrayOf(3), result.getOrThrow()!!.sealed!!.payload!!.enclosure!!.innerPayload)
                    assertEquals(2, requests.size)
                    val first = requests[0]; val repaired = requests[1]
                    assertFalse(first.writeRequestId.contentEquals(repaired.writeRequestId))
                    assertContentEquals(first.notification!!.toByteArray(), repaired.notification!!.toByteArray())
                    assertContentEquals(first.locker!!.sealed!!.payload!!.enclosure!!.innerPayload, repaired.locker!!.sealed!!.payload!!.enclosure!!.innerPayload)
                    assertEquals(first.parentVersion, repaired.parentVersion)
                    assertTrue(key.publicKey.verify(LockerSigning.writeContextV2(room, id, 0, 1, SHA256.digest(byteArrayOf(3)), repaired.notification), repaired.writeSignature!!.signature))
                }
            } finally { release.complete(Unit); client.closeAndJoin(); concurrent.closeAndJoin() }
            Unit
        } }

    @Test fun `authority-only drift retries have a finite independent budget`() = rejected(false)
    @Test fun `unchanged authority epoch invalid signature remains permanent`() = rejected(true)
    @Test fun `an unrelated reported keyspace cannot trigger authority repair`() = rejected(false, wrongScope = true)
    private fun rejected(unchanged: Boolean, wrongScope: Boolean = false) = runBlocking {
        val key = Secp256r1KeyPair.generate(); val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2), LockerKeyspace(2))
        var calls = 0; var transforms = 0; var encodes = 0
        val ids = mutableListOf<ByteArray>()
        suspend fun state(epoch: Long) = LockState(locked = true, scope = if (wrongScope) LockScope(kind = LockScopeKind.LOCK_SCOPE_KEYSPACE, keyspace = LockerKeyspace(99))
                else LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), lockVersion = epoch,
            publicKey = Secp256R1Key.PublicKey(key.publicKey.encode()))
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(authorityV2 = true, writeReceipts = true).toByteArray()
            "GetLocker" -> GetLockerResponse(locker = IdentifiedLocker(id, lockState = if (unchanged) state(1) else null)).toByteArray()
            "PostLockerChange" -> { ids += PostLockerChangeRequest.fromByteArray(bytes).writeRequestId; calls++
                PostLockerChangeResponse(result = PostLockerChangeResponse.Result.SIGNATURE_INVALID, lockState = state(if (unchanged) 1 else calls.toLong())).toByteArray() }
            else -> error(method.methodName)
        } }, object : LockKeySource { override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = key }, codecs = NotificationCodecs.of(object : NotificationCodec {
            override suspend fun decode(context: NotificationContext, payload: ByteArray) = payload
            override suspend fun encode(context: NotificationContext, payload: ByteArray): ByteArray { encodes++; return payload }
        }))
        try {
            assertFailsWith<LockerWriteException> { client.updateLocker(room, id, { payload { rawValue = byteArrayOf(9) } }) { transforms++; byteArrayOf(3) } }
            assertEquals(if (unchanged || wrongScope) 1 else 4, calls)
            assertEquals(calls, ids.map { it.toList() }.distinct().size)
            assertEquals(1, transforms); assertEquals(1, encodes)
        } finally { client.closeAndJoin() }
    }
}
