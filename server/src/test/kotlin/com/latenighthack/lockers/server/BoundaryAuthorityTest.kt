package com.latenighthack.lockers.server

import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.LockerSigning
import com.latenighthack.lockers.common.RoomKeying
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.PostLockerChangeRequest
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.storage.v1.ServerRoomId
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class BoundaryAuthorityTest {
    @Test fun malformedPublicKeysAndUnknownScopesNeverBecomePersistedAuthority() = runTest {
        for (scope in listOf(LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), LockScope(kind = LockScopeKind.UNKNOWN_(99)))) {
            for (key in listOf(byteArrayOf(), byteArrayOf(1), ByteArray(33))) {
                val db = ServerStorage.inMemory(); db.open()
                try {
                    val store = LockStoreImpl(db); val verifier = LockVerifier(store)
                    val room = RoomId(byteArrayOf(4))
                    assertTrue(verifier.applyLock(room, LockGrant(scope = scope, publicKey = publicKeyOf(key)), 0) is LockVerifier.LockOutcome.NotAuthorized)
                    assertTrue(store.getAllLocksInRoom(ServerRoomId(room.rawValue)).isEmpty())
                } finally { db.close() }
            }
        }
    }
    @Test fun unknownScopeRejectsEvenAValidPublicKey() = runTest {
        val db = ServerStorage.inMemory(); db.open()
        try {
            val store = LockStoreImpl(db); val verifier = LockVerifier(store)
            val key = Secp256r1KeyPair.generate()
            assertTrue(verifier.applyLock(RoomId(byteArrayOf(4)), LockGrant(
                LockScope(kind = LockScopeKind.UNKNOWN_(99)), publicKeyOf(key.publicKey.encode())), 0) is LockVerifier.LockOutcome.NotAuthorized)
            assertTrue(store.getAllLocksInRoom(ServerRoomId(byteArrayOf(4))).isEmpty())
        } finally { db.close() }
    }
    @Test fun malformedPublicKeyedRoomCannotFallBackToUnsignedEstablishment() = runTest {
        val db = ServerStorage.inMemory(); db.open()
        try {
            val verifier = LockVerifier(LockStoreImpl(db))
            val room = RoomKeying.publicKeyed(ByteArray(33))
            val key = Secp256r1KeyPair.generate()
            assertTrue(verifier.applyLock(room, LockGrant(LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), publicKeyOf(key.publicKey.encode())), 0) is LockVerifier.LockOutcome.NotAuthorized)
        } finally { db.close() }
    }
    @Test fun correctlySignedRatchetStillRejectsAnUndecodableReplacementKey() = runTest {
        val db = ServerStorage.inMemory(); db.open()
        try {
            val verifier = LockVerifier(LockStoreImpl(db))
            val room = RoomId(byteArrayOf(4)); val id = LockerId(byteArrayOf(5))
            val key = Secp256r1KeyPair.generate()
            verifier.applyLock(room, LockGrant(LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), publicKeyOf(key.publicKey.encode())), 0)
            val lock = verifier.resolveEffective(room, 0, id.rawValue)!!
            val replacement = byteArrayOf(1)
            val signature = Signature(signingVersion = 2, signature = key.privateKey.sign(
                LockerSigning.ratchetContextV2(room, id, 0, 1, replacement, emptyList())))
            assertTrue(verifier.applyRatchet(lock, room, id, 0, PostLockerChangeRequest.Ratchet(
                newPublicKey = publicKeyOf(replacement), signature = signature)) is LockVerifier.RatchetOutcome.Invalid)
            assertContentEquals(key.publicKey.encode(), verifier.stateOf(verifier.resolveEffective(room, 0, id.rawValue)!!).publicKey!!.rawValue)
        } finally { db.close() }
    }
}
