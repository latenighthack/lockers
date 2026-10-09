package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.connector.internal.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.*
import com.latenighthack.lockers.server.services.room.v1.WriteReceiptsDefinitionV1
import kotlinx.coroutines.*
import kotlin.test.*

class ReviewExpiredRatchetTests {
    @Test(timeout = 20_000)
    fun `lost committed HTTP reply recovers key after receipt removal without resurrecting old payload`() = runOwnedTestWithServer({
        serverDatabase = ServerStorage.inMemory()
        attachTestServicesWithDatabase(requireNotNull(serverDatabase))
    }) { server, _ -> withContext(Dispatchers.Default) {
        val room = RoomId(byteArrayOf(31)); val id = LockerId(byteArrayOf(32), LockerKeyspace(7))
        val scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER, keyspace = id.keyspace, lockerRawValue = id.rawValue)
        val old = Secp256r1KeyPair.generate(); var active = old
        val source = object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = active
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) { active = newKeyPair }
        }
        val committed = CompletableDeferred<PostLockerChangeResponse>()
        val gated = object : RpcClient by server.ownedRpcClient {
            override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
                val response = server.ownedRpcClient.unaryCall(method, headers, request)
                if (method.methodName == "PostLockerChange") {
                    committed.complete(PostLockerChangeResponse.fromByteArray(response.data))
                    awaitCancellation() // The source commits; the caller never receives its receipt.
                }
                return response
            }
        }
        val database = ConnectorStorage.inMemory()
        val first = reviewClient(gated, source, database)
        var replacement: LockerClient? = null
        var later: LockerClient? = null
        try {
            assertTrue(first.lockLocker(room, scope, old).result.isOk())
            val write = launch { first.updateLocker(room, id, ratchet = true) { byteArrayOf(3) } }
            val result = withTimeout(5_000) { committed.await() }
            assertTrue(result.result.isOk())
            write.cancelAndJoin(); first.closeAndJoin()
            val pending = LockerStoreImpl(database).pendingRatchets().single()
            val rotated = requireNotNull(Secp256r1KeyPair.fromPrivateKey(pending.privateKey))
            assertContentEquals(old.publicKey.encode(), active.publicKey.encode())
            val definition = WriteReceiptsDefinitionV1("delivery")
            assertEquals(1, requireNotNull(serverDatabase).deleteBatch(definition.storeName,
                definition.request.query(1, lower = pending.request.writeRequestId, upper = pending.request.writeRequestId)))
            // A later write makes replaying the original payload at the current version observably wrong.
            val laterClient = reviewClient(server.ownedRpcClient, object : LockKeySource {
                override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = rotated
            }).also { later = it }
            laterClient.updateLocker(room, id) { byteArrayOf(9) }
            val recovered = reviewClient(server.ownedRpcClient, source, database).also { replacement = it }
            recovered.start()
            withTimeout(5_000) { while (LockerStoreImpl(database).pendingRatchets().isNotEmpty()) delay(10) }
            assertContentEquals(rotated.publicKey.encode(), active.publicKey.encode(), "expired receipt discarded the only new private key")
            val cached = recovered.getAllKnownLockers().single()
            assertEquals(2L, cached.version)
            assertContentEquals(byteArrayOf(9), cached.payload)
            recovered.updateLocker(room, id) { byteArrayOf(10) }
        } finally { first.closeAndJoin(); replacement?.closeAndJoin(); later?.closeAndJoin(); database.close() }
    } }

    @Test fun `rejected ratchet preserves unresolved private intent without rerunning transform`() = runBlocking {
        val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2), LockerKeyspace(0))
        val old = Secp256r1KeyPair.generate(); val unrelated = Secp256r1KeyPair.generate()
        val state = LockState(locked = true, lockVersion = 9, publicKey = Secp256R1Key.PublicKey(unrelated.publicKey.encode()))
        var calls = 0; var transforms = 0
        val database = ConnectorStorage.inMemory()
        val client = reviewClient(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true, authorityV2 = true).toByteArray()
            "GetLocker" -> GetLockerResponse(locker = IdentifiedLocker(id, Locker(open = Locker.OpenLocker(byteArrayOf(9))), 8, state)).toByteArray()
            "GetLockScope" -> GetLockScopeResponse(scopeState = state).toByteArray()
            "PostLockerChange" -> { calls++
                if (calls == 1) throw RpcResponseException("test", "POST", Codes.UNAVAILABLE, "response lost")
                PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION, version = 8).toByteArray() }
            else -> error(method.methodName)
        } }, object : LockKeySource { override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = old }, database)
        try {
            val failure = assertFailsWith<RatchetRecoveryUnresolvedException> { client.updateLocker(room, id, ratchet = true) { transforms++; byteArrayOf(3) } }
            assertEquals(2, calls); assertEquals(1, transforms)
            val pending = LockerStoreImpl(database).pendingRatchets().single()
            assertContentEquals(pending.request.writeRequestId, failure.writeRequestId)
            failure.writeRequestId.fill(0)
            assertContentEquals(pending.request.writeRequestId, failure.writeRequestId)
            assertTrue(LockerStoreImpl(database).archivedRatchets().isEmpty())
            assertContentEquals(byteArrayOf(9), client.getAllKnownLockers().single().payload)
        } finally { client.closeAndJoin(); database.close() }
    }

    @Test fun `an overridden live ancestor recovers committed key and exposes immutable expired receipt metadata`() = runBlocking {
        val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2), LockerKeyspace(0))
        val ancestor = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM)
        val old = Secp256r1KeyPair.generate(); val child = Secp256r1KeyPair.generate(); var active = old
        var submitted: PostLockerChangeRequest? = null; var transforms = 0; var calls = 0
        val scopes = mutableListOf<LockScopeKind>()
        val database = ConnectorStorage.inMemory()
        suspend fun current() = LockState(locked = true, lockVersion = 8, publicKey = Secp256R1Key.PublicKey(child.publicKey.encode()))
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true, authorityV2 = true).toByteArray()
            "GetLocker" -> GetLockerResponse(locker = if (submitted == null) IdentifiedLocker(id, lockState = LockState(locked = true,
                scope = ancestor, lockVersion = 1, publicKey = Secp256R1Key.PublicKey(old.publicKey.encode())))
                else IdentifiedLocker(id, Locker(open = Locker.OpenLocker(byteArrayOf(9))), 9, current())).toByteArray()
            "GetLockScope" -> {
                val scope = requireNotNull(GetLockScopeRequest.fromByteArray(bytes).scope); scopes += scope.kind
                GetLockScopeResponse(scopeState = if (scope.kind == LockScopeKind.LOCK_SCOPE_ROOM)
                    LockState(locked = true, scope = ancestor, lockVersion = 2, publicKey = submitted!!.ratchet!!.newPublicKey) else current()).toByteArray()
            }
            "PostLockerChange" -> { calls++; submitted = PostLockerChangeRequest.fromByteArray(bytes)
                if (calls == 1) throw RpcResponseException("test", "POST", Codes.UNAVAILABLE, "response lost")
                PostLockerChangeResponse(result = PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION, version = 9).toByteArray() }
            else -> error(method.methodName)
        } }, object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = active
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) { active = newKeyPair }
        }, database)
        try {
            val failure = assertFailsWith<RatchetReceiptUnavailableException> { client.updateLocker(room, id, ratchet = true) { transforms++; byteArrayOf(3) } }
            assertEquals(2, calls); assertEquals(1, transforms)
            assertEquals(1L, failure.version); assertEquals(listOf(WriteSourceVersion(id, 1)), failure.sourceVersions)
            assertContentEquals(submitted!!.writeRequestId, failure.writeRequestId)
            assertContentEquals(submitted!!.ratchet!!.newPublicKey!!.rawValue, active.publicKey.encode())
            assertTrue(LockerStoreImpl(database).pendingRatchets().isEmpty())
            assertEquals(LockScopeKind.LOCK_SCOPE_ROOM, LockerStoreImpl(database).archivedRatchets().single().scope.kind)
            assertEquals(listOf(LockScopeKind.LOCK_SCOPE_ROOM, LockScopeKind.LOCK_SCOPE_KEYSPACE, LockScopeKind.LOCK_SCOPE_LOCKER), scopes)
            assertEquals(9L, client.getAllKnownLockers().single().version)
            assertContentEquals(byteArrayOf(9), client.getAllKnownLockers().single().payload)
        } finally { client.closeAndJoin(); database.close() }
    }

    private var serverDatabase: Database? = null
}
