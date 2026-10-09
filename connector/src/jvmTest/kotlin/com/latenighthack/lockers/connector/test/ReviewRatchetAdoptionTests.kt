package com.latenighthack.lockers.connector.test

import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.connector.internal.*
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.*
import kotlin.test.*

class ReviewRatchetAdoptionTests {
    @Test fun `legacy archive restores an active ancestor key after authority protocol upgrade`() = runBlocking {
        val db = ConnectorStorage.inMemory()
        db.open()
        val old = Secp256r1KeyPair.generate()
        val rotated = Secp256r1KeyPair.generate()
        val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2), LockerKeyspace(0))
        val request = PostLockerChangeRequest(roomId = room, lockerId = id,
            writeSignature = Signature(publicKey = Secp256R1Key.PublicKey(old.publicKey.encode())),
            ratchet = PostLockerChangeRequest.Ratchet(newPublicKey = Secp256R1Key.PublicKey(rotated.publicKey.encode())))
        val legacy = LockState(locked = true, publicKey = request.ratchet!!.newPublicKey)
        val store = LockerStoreImpl(db)
        store.archiveRatchet(ArchivedRatchet(PendingRatchet(request, rotated.privateKey.encode()), legacy, 5))
        val adopted = CompletableDeferred<Unit>()
        var active = old
        var exactQueries = 0
        val rpc = ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(authorityV2 = true).toByteArray()
            "GetLockScope" -> { exactQueries++; GetLockScopeResponse(scopeState = LockState()).toByteArray() }
            "GetLocker" -> GetLockerResponse(locker = IdentifiedLocker(id, version = 5,
                lockState = legacy.copy(scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), lockVersion = 2))).toByteArray()
            else -> error(method.methodName)
        } }
        val client = reviewClient(rpc, object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = active
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) {
                active = newKeyPair; adopted.complete(Unit)
            }
        }, db)
        try {
            client.start()
            withTimeout(2_000) { while (!adopted.isCompleted && store.archivedRatchets().isNotEmpty()) delay(10) }
            assertTrue(adopted.isCompleted, "Still-active ancestor key must be restored")
            assertContentEquals(rotated.publicKey.encode(), active.publicKey.encode())
            assertEquals(1, store.archivedRatchets().size)
            assertEquals(0, exactQueries)
        } finally { client.closeAndJoin(); db.close() }
    }

    @Test fun `archive without previous signing identity cannot replace an unrelated provider key`() = runBlocking {
        val db = ConnectorStorage.inMemory()
        db.open()
        val rotated = Secp256r1KeyPair.generate()
        val unrelated = Secp256r1KeyPair.generate()
        val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2), LockerKeyspace(0))
        val scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM)
        val state = LockState(locked = true, scope = scope, lockVersion = 2,
            publicKey = Secp256R1Key.PublicKey(rotated.publicKey.encode()))
        val request = PostLockerChangeRequest(roomId = room, lockerId = id,
            ratchet = PostLockerChangeRequest.Ratchet(newPublicKey = state.publicKey))
        LockerStoreImpl(db).archiveRatchet(ArchivedRatchet(PendingRatchet(request, rotated.privateKey.encode()), state, 5))
        var active = unrelated
        var callbacks = 0
        val rpc = ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(authorityV2 = true, writeReceipts = true).toByteArray()
            "GetLocker" -> GetLockerResponse(locker = IdentifiedLocker(id, version = 5, lockState = state)).toByteArray()
            "GetLockScope" -> GetLockScopeResponse(scopeState = state).toByteArray()
            else -> error("Unexpected new source: ${method.methodName}")
        } }
        val client = reviewClient(rpc, object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = active
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) {
                callbacks++; active = newKeyPair
            }
        }, db)
        try {
            val failure = runCatching { client.updateLocker(room, id) { byteArrayOf(9) } }.exceptionOrNull()
            assertEquals(0, callbacks)
            assertIs<RatchetAdoptionPendingException>(failure)
            assertContentEquals(unrelated.publicKey.encode(), active.publicKey.encode())
            assertEquals(1, LockerStoreImpl(db).archivedRatchets().size)
        } finally { client.closeAndJoin(); db.close() }
    }

    @Test fun `shared scope adoption failure identifies the original committed source`() = runBlocking {
        val db = ConnectorStorage.inMemory()
        db.open()
        val old = Secp256r1KeyPair.generate()
        val rotated = Secp256r1KeyPair.generate()
        val room = RoomId(byteArrayOf(1))
        val original = LockerId(byteArrayOf(2), LockerKeyspace(0))
        val target = LockerId(byteArrayOf(3), LockerKeyspace(0))
        val scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM)
        val state = LockState(locked = true, scope = scope, lockVersion = 2,
            publicKey = Secp256R1Key.PublicKey(rotated.publicKey.encode()))
        val request = PostLockerChangeRequest(roomId = room, lockerId = original,
            parentVersion = 4, writeRequestId = ByteArray(32) { 7 },
            writeSignature = Signature(publicKey = Secp256R1Key.PublicKey(old.publicKey.encode())),
            ratchet = PostLockerChangeRequest.Ratchet(newPublicKey = state.publicKey))
        LockerStoreImpl(db).archiveRatchet(ArchivedRatchet(PendingRatchet(request, rotated.privateKey.encode()), state, 5))
        var posts = 0
        val rpc = ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(authorityV2 = true, writeReceipts = true).toByteArray()
            "GetLocker" -> GetLockerResponse(locker = IdentifiedLocker(target, version = 17, lockState = state)).toByteArray()
            "GetLockScope" -> GetLockScopeResponse(scopeState = state).toByteArray()
            "PostLockerChange" -> { posts++; error("No new source should commit before key adoption") }
            else -> error(method.methodName)
        } }
        val client = reviewClient(rpc, object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = old
        }, db)
        var transforms = 0
        try {
            val failure = assertFailsWith<RatchetAdoptionPendingException> {
                client.updateLocker(room, target) { transforms++; byteArrayOf(9) }
            }
            assertContentEquals(request.writeRequestId, failure.writeRequestId)
            assertEquals(original, failure.sourceVersions.single().lockerId)
            assertEquals(5, failure.sourceVersions.single().version)
            assertEquals(0, transforms)
            assertEquals(0, posts)
        } finally { client.closeAndJoin(); db.close() }
    }

    @Test fun `default no-op adoption retains the committed private key for a replacement source`() = runBlocking {
        val db = ConnectorStorage.inMemory()
        val old = Secp256r1KeyPair.generate()
        var submitted: PostLockerChangeRequest? = null
        val rpc = ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true).toByteArray()
            "PostLockerChange" -> { submitted = PostLockerChangeRequest.fromByteArray(bytes); PostLockerChangeResponse(version = 1).toByteArray() }
            "GetLocker" -> GetLockerResponse(locker = IdentifiedLocker(submitted!!.lockerId, version = 1,
                lockState = LockState(locked = true, publicKey = submitted.ratchet!!.newPublicKey))).toByteArray()
            else -> error(method.methodName)
        } }
        val client = reviewClient(rpc, object : LockKeySource { override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = old }, db)
        try {
            assertFailsWith<LockerWriteException> { client.updateLocker(RoomId(byteArrayOf(1)), LockerId(byteArrayOf(2)), ratchet = true) { byteArrayOf(3) } }
            assertEquals(1, LockerStoreImpl(db).pendingRatchets().size)
        } finally { client.closeAndJoin() }
        var adopted: Secp256r1KeyPair = old
        val replacement = reviewClient(rpc, object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = adopted
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) { adopted = newKeyPair }
        }, db)
        try {
            replacement.start()
            withTimeout(2_000) { while (LockerStoreImpl(db).pendingRatchets().isNotEmpty()) delay(10) }
            assertContentEquals(submitted!!.ratchet!!.newPublicKey!!.rawValue, adopted.publicKey.encode())
        } finally { replacement.closeAndJoin() }
    }
    @Test fun `a volatile source reset restores its committed key from the archive`() = runBlocking {
        val db = ConnectorStorage.inMemory()
        val old = Secp256r1KeyPair.generate()
        var active = old
        var submitted: PostLockerChangeRequest? = null
        val scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER, keyspace = LockerKeyspace(0), lockerRawValue = byteArrayOf(2))
        fun authority() = LockState(locked = true, scope = scope, lockVersion = 2,
            publicKey = submitted!!.ratchet!!.newPublicKey)
        val rpc = ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true).toByteArray()
            "PostLockerChange" -> { submitted = PostLockerChangeRequest.fromByteArray(bytes); PostLockerChangeResponse(version = 1, lockState = authority()).toByteArray() }
            "GetLocker" -> GetLockerResponse(locker = IdentifiedLocker(submitted!!.lockerId, version = 1, lockState = authority())).toByteArray()
            else -> error(method.methodName)
        } }
        val source = object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = active
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) { active = newKeyPair }
        }
        val client = reviewClient(rpc, source, db)
        try { client.updateLocker(RoomId(byteArrayOf(1)), LockerId(byteArrayOf(2)), ratchet = true) { byteArrayOf(3) } }
        finally { client.closeAndJoin() }
        assertTrue(LockerStoreImpl(db).pendingRatchets().isEmpty())
        assertEquals(1, LockerStoreImpl(db).archivedRatchets().size)
        active = old // Simulate a source which failed to persist its callback's update.
        val replacement = reviewClient(rpc, source, db)
        try {
            replacement.start()
            withTimeout(2_000) { while (active === old) delay(10) }
            assertContentEquals(submitted!!.ratchet!!.newPublicKey!!.rawValue, active.publicKey.encode())
        } finally { replacement.closeAndJoin() }
    }

    @Test fun `obsolete archived authority never overwrites a newer source key`() = runBlocking {
        val db = ConnectorStorage.inMemory()
        db.open()
        val old = Secp256r1KeyPair.generate()
        val archived = Secp256r1KeyPair.generate()
        val newer = Secp256r1KeyPair.generate()
        val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2), LockerKeyspace(0))
        val scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER, keyspace = LockerKeyspace(0), lockerRawValue = id.rawValue)
        val request = PostLockerChangeRequest(roomId = room, lockerId = id,
            writeSignature = Signature(publicKey = Secp256R1Key.PublicKey(old.publicKey.encode())),
            ratchet = PostLockerChangeRequest.Ratchet(newPublicKey = Secp256R1Key.PublicKey(archived.publicKey.encode())))
        LockerStoreImpl(db).archiveRatchet(ArchivedRatchet(PendingRatchet(request, archived.privateKey.encode()),
            LockState(locked = true, scope = scope, lockVersion = 2, publicKey = Secp256R1Key.PublicKey(archived.publicKey.encode())), 1))
        var callbacks = 0
        val rpc = ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(authorityV2 = true).toByteArray()
            "GetLockScope" -> GetLockScopeResponse(scopeState = LockState(locked = true, scope = scope, lockVersion = 3, publicKey = Secp256R1Key.PublicKey(newer.publicKey.encode()))).toByteArray()
            else -> error(method.methodName)
        } }
        val client = reviewClient(rpc, object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = newer
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) { callbacks++ }
        }, db)
        try {
            client.start()
            withTimeout(2_000) { while (LockerStoreImpl(db).archivedRatchets().isNotEmpty()) delay(10) }
            assertEquals(0, callbacks)
        } finally { client.closeAndJoin() }
    }
}
