package com.latenighthack.lockers.server

import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.lockers.common.RoomKeying
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.services.room.v1.LockStoreImpl
import com.latenighthack.lockers.server.services.room.v1.LockVerifier
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
    private suspend fun newVerifier(): Pair<LockStoreImpl, LockVerifier> {
        val delegate = InMemoryStoreDelegate()
        val store = LockStoreImpl(delegate)
        store.prepare()
        delegate.createStores()
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
