package com.latenighthack.lockers.server

import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.common.RoomKeying
import com.latenighthack.lockers.common.LockerSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.services.room.v1.LockStoreImpl
import com.latenighthack.lockers.server.services.room.v1.LockVerifier
import com.latenighthack.lockers.server.services.room.v1.publicKeyOf
import com.latenighthack.lockers.server.storage.v1.ServerLock
import com.latenighthack.lockers.server.storage.v1.ServerLockerId
import com.latenighthack.lockers.server.storage.v1.ServerRoomId
import kotlinx.coroutines.test.runTest
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Unit coverage for the pure server-side lock logic (resolution + establishment
 * authorization), independent of the streaming stack the integration tests use.
 */
class LockVerifierTest {
    @Test
    fun `V2 delegation and unlock proofs cannot cross a target incarnation`() = runTest {
        val (_, verifier) = newVerifier()
        val room = RoomId(Random.nextBytes(16))
        val owner = Secp256r1KeyPair.generate()
        val childKey = Secp256r1KeyPair.generate()
        val root = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM)
        val child = LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER, lockerRawValue = byteArrayOf(4))
        assertTrue(verifier.applyLock(room, LockGrant(root, publicKeyOf(owner.publicKey.encode())), 0) is LockVerifier.LockOutcome.Ok)
        val grant = LockGrant(child, publicKeyOf(childKey.publicKey.encode()), Signature(signingVersion = 2,
            signature = owner.privateKey.sign(LockerSigning.grantContextV2(room, child, childKey.publicKey.encode(), 1, 0))),
            authorityVersion = 1, scopeVersion = 0)
        assertTrue(verifier.applyLock(room, grant, 0) is LockVerifier.LockOutcome.Ok)
        val unlock = Signature(signingVersion = 2, signature = childKey.privateKey.sign(LockerSigning.unlockContextV2(room, child, 1)))
        assertTrue(verifier.applyUnlock(room, child, unlock, 1) is LockVerifier.UnlockOutcome.Ok)
        assertTrue(verifier.applyLock(room, grant.copy(scopeVersion = 2), 2) is LockVerifier.LockOutcome.NotAuthorized)
        val replacement = grant.copy(scopeVersion = 2, parentSignature = Signature(signingVersion = 2,
            signature = owner.privateKey.sign(LockerSigning.grantContextV2(room, child, childKey.publicKey.encode(), 1, 2))))
        val state = (verifier.applyLock(room, replacement, 2) as LockVerifier.LockOutcome.Ok).state
        assertEquals(3, state.lockVersion)
        assertTrue(verifier.applyUnlock(room, child, unlock, 3) is LockVerifier.UnlockOutcome.SignatureInvalid)
        val current = Signature(signingVersion = 2, signature = childKey.privateKey.sign(LockerSigning.unlockContextV2(room, child, 3)))
        assertTrue(verifier.applyUnlock(room, child, current, 3) is LockVerifier.UnlockOutcome.Ok)
    }

    @Test
    fun `V2 content proofs bind notification metadata`() = runTest {
        val (_, verifier) = newVerifier()
        val room = RoomId(Random.nextBytes(16)); val id = LockerId(byteArrayOf(1))
        val key = Secp256r1KeyPair.generate()
        verifier.applyLock(room, LockGrant(LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), publicKeyOf(key.publicKey.encode())), 0)
        val lock = verifier.resolveEffective(room, 0, id.rawValue)!!
        val hash = verifier.contentHash(byteArrayOf(7))
        val notification = Notification(push = Push(title = "authentic"), payload = Payload(byteArrayOf(4)))
        val signature = Signature(signingVersion = 2, signature = key.privateKey.sign(LockerSigning.writeContextV2(room, id, 0, 1, hash, notification)))
        assertEquals(LockVerifier.WriteVerdict.OK, verifier.verifyWrite(lock, room, id, 0, hash, signature, notification))
        assertEquals(LockVerifier.WriteVerdict.INVALID, verifier.verifyWrite(lock, room, id, 0, hash, signature, notification.copy(push = Push(title = "modified"))))
    }

    @Test
    fun `unsigned descendants cannot replace room or keyspace authority`() = runTest {
        val owner = Secp256r1KeyPair.generate()
        val attacker = Secp256r1KeyPair.generate()
        val keyspaceScope = LockScope(kind = LockScopeKind.LOCK_SCOPE_KEYSPACE, keyspace = LockerKeyspace(5))
        val lockerScope = LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER, keyspace = LockerKeyspace(5), lockerRawValue = byteArrayOf(9))
        for ((parent, child) in listOf(
            LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM) to keyspaceScope,
            LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM) to lockerScope,
            keyspaceScope to lockerScope,
        )) {
            val (_, verifier) = newVerifier()
            val room = RoomId(Random.nextBytes(16))
            assertTrue(verifier.applyLock(room, LockGrant(parent, publicKeyOf(owner.publicKey.encode())), 0) is LockVerifier.LockOutcome.Ok)
            assertTrue(verifier.applyLock(room, LockGrant(child, publicKeyOf(attacker.publicKey.encode())), 0) is LockVerifier.LockOutcome.NotAuthorized)
            assertContentEquals(owner.publicKey.encode(), verifier.stateOf(verifier.resolveEffective(room, 5, byteArrayOf(9))!!).publicKey!!.rawValue)
            val signed = LockGrant(child, publicKeyOf(attacker.publicKey.encode()), Signature(
                signature = owner.privateKey.sign(LockerSigning.grantContext(room, child, attacker.publicKey.encode())),
            ))
            assertTrue(verifier.applyLock(room, signed, 0) is LockVerifier.LockOutcome.Ok)
        }
    }

    private suspend fun newVerifier(): Pair<LockStoreImpl, LockVerifier> {
        val delegate = com.latenighthack.lockers.server.ServerStorage.inMemory()
        val store = LockStoreImpl(delegate)
        store.prepare()
        delegate.open()
        return store to LockVerifier(store)
    }

    private fun stateBytes(scope: LockScope, publicKey: ByteArray) = LockState(
        locked = true,
        scope = scope,
        publicKey = Secp256R1Key.PublicKey(rawValue = publicKey),
        lockVersion = 1L,
    ).toByteArray()

    @Test
    fun `resolveEffective prefers most specific scope`() = runTest {
        val (store, verifier) = newVerifier()
        val roomRaw = Random.nextBytes(16)
        val roomId = RoomId(roomRaw)
        val keyspace = 5L
        val lockerRaw = Random.nextBytes(16)
        val roomKey = byteArrayOf(1)
        val keyspaceKey = byteArrayOf(2)
        val lockerKey = byteArrayOf(3)

        suspend fun effectiveKey() = verifier.resolveEffective(roomId, keyspace, lockerRaw)
            ?.let { verifier.stateOf(it).publicKey!!.rawValue }

        store.saveLock(
            ServerLock(
                roomId = ServerRoomId(roomRaw),
                scopeKind = LockVerifier.SCOPE_ROOM,
                keyspace = 0L,
                lockerId = ServerLockerId(ByteArray(0)),
                lockState = stateBytes(LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), roomKey),
                version = 1L,
            )
        )
        assertContentEquals(roomKey, effectiveKey())

        store.saveLock(
            ServerLock(
                roomId = ServerRoomId(roomRaw),
                scopeKind = LockVerifier.SCOPE_KEYSPACE,
                keyspace = keyspace,
                lockerId = ServerLockerId(ByteArray(0)),
                lockState = stateBytes(
                    LockScope(kind = LockScopeKind.LOCK_SCOPE_KEYSPACE, keyspace = LockerKeyspace(keyspace)),
                    keyspaceKey,
                ),
                version = 1L,
            )
        )
        assertContentEquals(keyspaceKey, effectiveKey())

        store.saveLock(
            ServerLock(
                roomId = ServerRoomId(roomRaw),
                scopeKind = LockVerifier.SCOPE_LOCKER,
                keyspace = keyspace,
                lockerId = ServerLockerId(lockerRaw),
                lockState = stateBytes(
                    LockScope(
                        kind = LockScopeKind.LOCK_SCOPE_LOCKER,
                        keyspace = LockerKeyspace(keyspace),
                        lockerRawValue = lockerRaw,
                    ),
                    lockerKey,
                ),
                version = 1L,
            )
        )
        assertContentEquals(lockerKey, effectiveKey())
    }

    @Test
    fun `applyLock TOFU succeeds in a non-public-keyed room`() = runTest {
        val (_, verifier) = newVerifier()
        val roomId = RoomId(Random.nextBytes(16))
        val key = Secp256r1KeyPair.generate()
        val grant = LockGrant(
            scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM),
            publicKey = Secp256R1Key.PublicKey(rawValue = key.publicKey.encode()),
        )

        assertTrue(verifier.applyLock(roomId, grant, 0L) is LockVerifier.LockOutcome.Ok)
        assertTrue(verifier.roomHasLocks(roomId))
    }

    @Test
    fun `applyLock TOFU is rejected in a public-keyed room`() = runTest {
        val (_, verifier) = newVerifier()
        val roomKey = Secp256r1KeyPair.generate()
        val roomId = RoomKeying.publicKeyed(roomKey.publicKey.encode())
        val lockKey = Secp256r1KeyPair.generate()
        val grant = LockGrant(
            scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM),
            publicKey = Secp256R1Key.PublicKey(rawValue = lockKey.publicKey.encode()),
        )

        assertTrue(verifier.applyLock(roomId, grant, 0L) is LockVerifier.LockOutcome.NotAuthorized)
    }

    @Test
    fun `verifyWrite requires a signature when locked`() = runTest {
        val (_, verifier) = newVerifier()
        val roomId = RoomId(Random.nextBytes(16))
        val key = Secp256r1KeyPair.generate()
        verifier.applyLock(
            roomId,
            LockGrant(
                scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM),
                publicKey = Secp256R1Key.PublicKey(rawValue = key.publicKey.encode()),
            ),
            0L,
        )
        val lock = verifier.resolveEffective(roomId, 1L, Random.nextBytes(8))!!

        val verdict = verifier.verifyWrite(
            lock,
            roomId,
            LockerId(Random.nextBytes(8), LockerKeyspace(1L)),
            0L,
            ByteArray(32),
            null,
        )
        assertEquals(LockVerifier.WriteVerdict.REQUIRED, verdict)
    }
}
