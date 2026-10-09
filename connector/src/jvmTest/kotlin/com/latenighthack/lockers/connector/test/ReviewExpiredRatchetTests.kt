package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.common.LockerSigning
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
            withTimeout(5_000) { while (!active.publicKey.encode().contentEquals(rotated.publicKey.encode())) delay(10) }
            assertEquals(1, LockerStoreImpl(database).pendingRatchets().size, "authority proves key validity, never the original source write")
            assertContentEquals(rotated.publicKey.encode(), active.publicKey.encode(), "expired receipt discarded the only new private key")
            val cached = recovered.getAllKnownLockers().single()
            assertEquals(2L, cached.version)
            assertContentEquals(byteArrayOf(9), cached.payload)
            var rerun = 0
            val unknown = assertFailsWith<RatchetRecoveryUnresolvedException> { recovered.updateLocker(room, id) { rerun++; byteArrayOf(10) } }
            assertTrue(unknown.authorityRecovered); assertEquals(2L, unknown.observedVersion); assertEquals(0, rerun)
            assertFailsWith<IllegalArgumentException> { recovered.acknowledgeRatchetSourceUncertainty(room, id, ByteArray(32)) }
            recovered.acknowledgeRatchetSourceUncertainty(room, id, pending.request.writeRequestId)
            assertTrue(LockerStoreImpl(database).pendingRatchets().single().expectation!!.sourceUncertaintyAcknowledged)
            recovered.updateLocker(room, id) { assertContentEquals(byteArrayOf(9), it); byteArrayOf(10) }
            assertEquals(3L, recovered.getAllKnownLockers().single().version)
            recovered.closeAndJoin(); active = old
            val reopened = reviewClient(server.ownedRpcClient, source, database).also { replacement = it }
            reopened.start()
            withTimeout(5_000) { while (!active.publicKey.encode().contentEquals(rotated.publicKey.encode())) delay(10) }
            assertEquals(1, LockerStoreImpl(database).pendingRatchets().size)
            assertTrue(LockerStoreImpl(database).archivedRatchets().isEmpty())
            assertEquals(3L, reopened.getAllKnownLockers().single().version)
            assertContentEquals(byteArrayOf(10), reopened.getAllKnownLockers().single().payload)
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
            val failure = assertFailsWith<RatchetRecoveryUnresolvedException> { client.updateLocker(room, id, ratchet = true) { transforms++; byteArrayOf(3) } }
            assertEquals(2, calls); assertEquals(1, transforms)
            assertFalse((failure as Throwable) is LockerSourceCommittedException)
            assertContentEquals(submitted!!.writeRequestId, failure.writeRequestId)
            assertContentEquals(submitted!!.ratchet!!.newPublicKey!!.rawValue, active.publicKey.encode())
            assertEquals(1, LockerStoreImpl(database).pendingRatchets().size)
            assertTrue(LockerStoreImpl(database).archivedRatchets().isEmpty())
            assertEquals(1, scopes.size); assertEquals<LockScopeKind>(LockScopeKind.LOCK_SCOPE_ROOM, scopes.single())
            assertEquals(9L, client.getAllKnownLockers().single().version)
            assertContentEquals(byteArrayOf(9), client.getAllKnownLockers().single().payload)
        } finally { client.closeAndJoin(); database.close() }
    }

    @Test fun `proposed public key in a different scope cannot certify the original authority`() = runBlocking {
        val room = RoomId(byteArrayOf(41)); val id = LockerId(byteArrayOf(42), LockerKeyspace(0))
        val original = LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER, keyspace = id.keyspace, lockerRawValue = id.rawValue)
        val old = Secp256r1KeyPair.generate(); var active = old; var submitted: PostLockerChangeRequest? = null; var calls = 0
        val database = ConnectorStorage.inMemory()
        val oldState = LockState(locked = true, scope = original, lockVersion = 1, publicKey = Secp256R1Key.PublicKey(old.publicKey.encode()))
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true, authorityV2 = true).toByteArray()
            "GetLocker" -> GetLockerResponse(locker = IdentifiedLocker(id, lockState = oldState)).toByteArray()
            "GetLockScope" -> {
                val scope = requireNotNull(GetLockScopeRequest.fromByteArray(bytes).scope)
                GetLockScopeResponse(scopeState = if (scope.kind == LockScopeKind.LOCK_SCOPE_ROOM)
                    LockState(locked = true, scope = scope, lockVersion = 2, publicKey = submitted!!.ratchet!!.newPublicKey) else oldState).toByteArray()
            }
            "PostLockerChange" -> { submitted = PostLockerChangeRequest.fromByteArray(bytes); calls++
                if (calls == 1) throw RpcResponseException("test", "POST", Codes.UNAVAILABLE, "reply lost")
                PostLockerChangeResponse(result = PostLockerChangeResponse.Result.SIGNATURE_INVALID).toByteArray() }
            else -> error(method.methodName)
        } }, object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = active
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) { active = newKeyPair }
        }, database)
        try {
            assertFailsWith<RatchetRecoveryUnresolvedException> { client.updateLocker(room, id, ratchet = true) { byteArrayOf(3) } }
            assertContentEquals(old.publicKey.encode(), active.publicKey.encode())
            assertEquals(1, LockerStoreImpl(database).pendingRatchets().size)
            assertTrue(LockerStoreImpl(database).archivedRatchets().isEmpty())
        } finally { client.closeAndJoin(); database.close() }
    }

    @Test fun `a failing first recovery does not starve later intents or archives and eventually retries`() = runBlocking {
        val database = ConnectorStorage.inMemory(); database.open(); val store = LockerStoreImpl(database)
        val old = Secp256r1KeyPair.generate()
        val pairs = (1..3).associateWith { Secp256r1KeyPair.generate() }
        val active = java.util.concurrent.ConcurrentHashMap<Int, Secp256r1KeyPair>()
        suspend fun pending(n: Int): PendingRatchet = PendingRatchet(PostLockerChangeRequest(roomId = RoomId(byteArrayOf(n.toByte())),
            lockerId = LockerId(byteArrayOf(1), LockerKeyspace(0)), locker = Locker(open = Locker.OpenLocker(byteArrayOf(n.toByte()))),
            writeRequestId = ByteArray(32) { n.toByte() }, writeSignature = Signature(publicKey = Secp256R1Key.PublicKey(old.publicKey.encode())),
            ratchet = PostLockerChangeRequest.Ratchet(newPublicKey = Secp256R1Key.PublicKey(pairs.getValue(n).publicKey.encode()))), pairs.getValue(n).privateKey.encode())
        suspend fun state(n: Int) = LockState(locked = true, scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER,
            keyspace = LockerKeyspace(0), lockerRawValue = byteArrayOf(1)), lockVersion = 2, publicKey = Secp256R1Key.PublicKey(pairs.getValue(n).publicKey.encode()))
        store.saveRatchet(pending(1)); store.saveRatchet(pending(2)); store.archiveRatchet(ArchivedRatchet(pending(3), state(3), 1))
        val reachable = java.util.concurrent.atomic.AtomicBoolean(false)
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true, authorityV2 = true).toByteArray()
            "PostLockerChange" -> { val n = PostLockerChangeRequest.fromByteArray(bytes).roomId!!.rawValue[0].toInt()
                if (n == 1 && !reachable.get()) throw RpcResponseException("test", "POST", Codes.UNAVAILABLE, "temporarily unreachable")
                PostLockerChangeResponse(version = 1, lockState = state(n)).toByteArray() }
            "GetLockScope" -> GetLockScopeResponse(scopeState = state(GetLockScopeRequest.fromByteArray(bytes).roomId!!.rawValue[0].toInt())).toByteArray()
            else -> error(method.methodName)
        } }, object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = active[roomId.rawValue[0].toInt()] ?: old
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) { active[roomId.rawValue[0].toInt()] = newKeyPair }
        }, database)
        try {
            client.start()
            withTimeout(1_500) { while (active[2] == null || active[3] == null) delay(10) }
            assertEquals(1, store.pendingRatchets().size)
            withTimeout(1_500) { while (client.ratchetRecoveryFailures.value.isEmpty()) delay(10) }
            val failure = client.ratchetRecoveryFailures.value.single()
            assertContentEquals(ByteArray(32) { 1 }, failure.writeRequestId)
            failure.writeRequestId.fill(0); failure.roomId.rawValue.fill(0)
            assertContentEquals(ByteArray(32) { 1 }, failure.writeRequestId)
            assertContentEquals(byteArrayOf(1), failure.roomId.rawValue)
            assertEquals("RECOVERY_FAILED", failure.reason)
            reachable.set(true)
            withTimeout(5_000) { while (store.pendingRatchets().isNotEmpty()) delay(10) }
            assertContentEquals(pairs.getValue(1).publicKey.encode(), active.getValue(1).publicKey.encode())
            withTimeout(1_500) { while (client.ratchetRecoveryFailures.value.isNotEmpty()) delay(10) }
        } finally { client.closeAndJoin(); database.close() }
    }

    @Test(timeout = 20_000)
    fun `unrelated source write and direct public key grant cannot certify the lost ratchet source`() = runOwnedTestWithServer({
        attachTestServices()
    }) { server, _ -> withContext(Dispatchers.Default) {
        val room = RoomId(byteArrayOf(71)); val id = LockerId(byteArrayOf(72), LockerKeyspace(0))
        val root = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM)
        val scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER, keyspace = id.keyspace, lockerRawValue = id.rawValue)
        val old = Secp256r1KeyPair.generate(); var active = old
        val source = object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = active
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) { active = newKeyPair }
        }
        val proposed = CompletableDeferred<PostLockerChangeRequest>()
        val gated = object : RpcClient by server.ownedRpcClient {
            override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
                if (method.methodName == "PostLockerChange") {
                    val intent = PostLockerChangeRequest.fromByteArray(request)
                    if (intent.ratchet != null) { proposed.complete(intent); awaitCancellation() } // This source never reaches the server.
                }
                return server.ownedRpcClient.unaryCall(method, headers, request)
            }
        }
        val database = ConnectorStorage.inMemory()
        val first = reviewClient(gated, source, database)
        val other = reviewClient(server.ownedRpcClient, object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = old
        })
        var replacement: LockerClient? = null
        try {
            assertTrue(first.lockLocker(room, root, old).result.isOk())
            assertTrue(first.lockLocker(room, scope, old, old).result.isOk())
            first.updateLocker(room, id) { byteArrayOf(1) }
            val write = launch { first.updateLocker(room, id, ratchet = true) { byteArrayOf(3) } }
            val intent = withTimeout(5_000) { proposed.await() }; write.cancelAndJoin(); first.closeAndJoin()
            other.updateLocker(room, id) { byteArrayOf(9) }
            assertTrue(other.unlockLocker(room, scope, old, 1).result.isOk())
            val discovered = other.getLockScope(room, scope)
            val pub = intent.ratchet!!.newPublicKey!! // Only the public key is used by the unrelated authorized holder.
            val grant = LockLockerRequest(roomId = room, parentLockVersion = discovered.scopeState!!.lockVersion,
                grant = LockGrant(scope = scope, publicKey = pub, authorityVersion = discovered.parentState!!.lockVersion,
                    scopeVersion = discovered.scopeState!!.lockVersion, parentSignature = Signature(
                        publicKey = Secp256R1Key.PublicKey(old.publicKey.encode()), signingVersion = 2,
                        signature = old.privateKey.sign(LockerSigning.grantContextV2(room, scope, pub.rawValue,
                            discovered.parentState!!.lockVersion, discovered.scopeState!!.lockVersion)))))
            assertTrue(ShardedRoomServiceRpc(server.ownedRpcClient).lockLocker(grant).result.isOk())
            val recovered = reviewClient(server.ownedRpcClient, source, database).also { replacement = it }
            recovered.start()
            withTimeout(5_000) { while (recovered.ratchetRecoveryFailures.value.isEmpty()) delay(10) }
            assertContentEquals(old.publicKey.encode(), active.publicKey.encode(), "a different epoch cannot certify the expected transition")
            assertEquals(1, LockerStoreImpl(database).pendingRatchets().size, "an unrelated grant must not erase source uncertainty")
            assertTrue(LockerStoreImpl(database).archivedRatchets().isEmpty())
            var transforms = 0
            val unknown = assertFailsWith<RatchetRecoveryUnresolvedException> { recovered.updateLocker(room, id) { transforms++; byteArrayOf(10) } }
            assertFalse(unknown.authorityRecovered); assertEquals(2L, unknown.observedVersion); assertEquals(0, transforms)
            assertFalse((unknown as Throwable) is LockerSourceCommittedException)
            assertContentEquals(byteArrayOf(9), recovered.getAllKnownLockers().single().payload)
            assertFailsWith<RatchetRecoveryUnresolvedException> { recovered.acknowledgeRatchetSourceUncertainty(room, id, intent.writeRequestId) }
        } finally { first.closeAndJoin(); other.closeAndJoin(); replacement?.closeAndJoin(); database.close() }
    } }

    @Test(timeout = 20_000)
    fun `a different source write reaching the exact expected key epoch remains source unknown`() = runOwnedTestWithServer({
        attachTestServices()
    }) { server, _ -> withContext(Dispatchers.Default) {
        val room = RoomId(byteArrayOf(71)); val id = LockerId(byteArrayOf(72), LockerKeyspace(0))
        val root = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM)
        val scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER, keyspace = id.keyspace, lockerRawValue = id.rawValue)
        val old = Secp256r1KeyPair.generate(); var active = old
        val source = object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = active
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) { active = newKeyPair }
        }
        val proposed = CompletableDeferred<PostLockerChangeRequest>()
        val gated = object : RpcClient by server.ownedRpcClient {
            override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
                if (method.methodName == "PostLockerChange") {
                    val intent = PostLockerChangeRequest.fromByteArray(request)
                    if (intent.ratchet != null) { proposed.complete(intent); awaitCancellation() } // This source never reaches the server.
                }
                return server.ownedRpcClient.unaryCall(method, headers, request)
            }
        }
        val database = ConnectorStorage.inMemory()
        val first = reviewClient(gated, source, database)
        val other = reviewClient(server.ownedRpcClient, object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = old
        })
        var replacement: LockerClient? = null
        try {
            assertTrue(first.lockLocker(room, root, old).result.isOk())
            assertTrue(first.lockLocker(room, scope, old, old).result.isOk())
            first.updateLocker(room, id) { byteArrayOf(1) }
            val write = launch { first.updateLocker(room, id, ratchet = true) { byteArrayOf(3) } }
            val intent = withTimeout(5_000) { proposed.await() }; write.cancelAndJoin(); first.closeAndJoin()
            val pub = intent.ratchet!!.newPublicKey!! // No proposed private key is available to this old-key holder.
            val hash = SHA256.digest(byteArrayOf(9))
            val writeSignature = Signature(publicKey = Secp256R1Key.PublicKey(old.publicKey.encode()), signingVersion = 2,
                signature = old.privateKey.sign(LockerSigning.writeContextV2(room, id, 1, 1, hash, null)))
            val unrelated = PostLockerChangeRequest(roomId = room, lockerId = id, parentVersion = 1,
                writeRequestId = ByteArray(32) { 99 }, writeSignature = writeSignature, locker = Locker { sealed { payload {
                    checksum = hash; enclosure { signature = writeSignature; innerPayload = byteArrayOf(9) }
                } } }, ratchet = PostLockerChangeRequest.Ratchet(newPublicKey = pub, signature = Signature(
                    publicKey = Secp256R1Key.PublicKey(old.publicKey.encode()), signingVersion = 2,
                    signature = old.privateKey.sign(LockerSigning.ratchetContextV2(room, id, 1, 1, pub.rawValue, emptyList())))))
            val otherReceipt = ShardedRoomServiceRpc(server.ownedRpcClient).postLockerChange(unrelated)
            assertTrue(otherReceipt.result.isOk()); assertEquals(2L, otherReceipt.version); assertEquals(2L, otherReceipt.lockState!!.lockVersion)
            val originalPosts = java.util.concurrent.atomic.AtomicInteger()
            val recoveringRpc = object : RpcClient by server.ownedRpcClient {
                override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
                    if (method.methodName == "PostLockerChange" && PostLockerChangeRequest.fromByteArray(request).writeRequestId.contentEquals(intent.writeRequestId)) originalPosts.incrementAndGet()
                    return server.ownedRpcClient.unaryCall(method, headers, request)
                }
            }
            val recovered = reviewClient(recoveringRpc, source, database).also { replacement = it }
            recovered.start()
            withTimeout(5_000) { while (recovered.ratchetRecoveryFailures.value.isEmpty()) delay(10) }
            assertContentEquals(pub.rawValue, active.publicKey.encode(), "exact current authority may safely restore the private key")
            assertEquals(1, LockerStoreImpl(database).pendingRatchets().size, "an unrelated grant must not erase source uncertainty")
            assertTrue(LockerStoreImpl(database).archivedRatchets().isEmpty())
            var transforms = 0
            val unknown = assertFailsWith<RatchetRecoveryUnresolvedException> { recovered.updateLocker(room, id) { transforms++; byteArrayOf(10) } }
            assertTrue(unknown.authorityRecovered); assertEquals(2L, unknown.observedVersion); assertEquals(0, transforms)
            assertFalse((unknown as Throwable) is LockerSourceCommittedException)
            assertContentEquals(byteArrayOf(9), recovered.getAllKnownLockers().single().payload)
            recovered.acknowledgeRatchetSourceUncertainty(room, id, intent.writeRequestId)
            val before = originalPosts.get()
            recovered.updateLocker(room, id) { assertContentEquals(byteArrayOf(9), it); byteArrayOf(10) }
            recovered.closeAndJoin(); active = old
            val reopened = reviewClient(recoveringRpc, source, database).also { replacement = it }; reopened.start()
            withTimeout(5_000) { while (!active.publicKey.encode().contentEquals(pub.rawValue)) delay(10) }
            assertEquals(before, originalPosts.get(), "ACKed source must never replay, including after volatile provider reset")
            assertEquals(1, LockerStoreImpl(database).pendingRatchets().size)
            assertTrue(LockerStoreImpl(database).archivedRatchets().isEmpty())
            assertContentEquals(byteArrayOf(10), reopened.getAllKnownLockers().single().payload)
        } finally { first.closeAndJoin(); other.closeAndJoin(); replacement?.closeAndJoin(); database.close() }
    } }

    private var serverDatabase: Database? = null
}
