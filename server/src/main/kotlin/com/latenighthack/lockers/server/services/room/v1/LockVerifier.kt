package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.LockerSigning
import com.latenighthack.lockers.common.RoomKeying
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.PostLockerChangeRequest
import com.latenighthack.lockers.server.storage.v1.*

/**
 * Resolves and enforces per-scope locks: effective-lock resolution, write-signature
 * verification, delegation-chain / TOFU / public-keyed-room authorization at lock
 * establishment, unlock, and key ratcheting. All persistence goes through [LockStore];
 * all crypto through ktcrypto. Callers must invoke these from the per-room shard so
 * the read-verify-write sequence is serialized.
 */
class LockVerifier(private val lockStore: LockStore) {
    companion object {
        const val SCOPE_LOCKER = 0L
        const val SCOPE_KEYSPACE = 1L
        const val SCOPE_ROOM = 2L
        private val EMPTY_LOCKER = ServerLockerId(ByteArray(0))
    }

    enum class WriteVerdict { OK, REQUIRED, INVALID }

    sealed class LockOutcome {
        data class Ok(val state: LockState) : LockOutcome()
        data class Stale(val state: LockState) : LockOutcome()
        object NotAuthorized : LockOutcome()
    }

    sealed class UnlockOutcome {
        object Ok : UnlockOutcome()
        object Stale : UnlockOutcome()
        object SignatureInvalid : UnlockOutcome()
    }

    sealed class RatchetOutcome {
        data class Ok(val state: LockState) : RatchetOutcome()
        object Invalid : RatchetOutcome()
    }

    /** Cheap gate for the common open case: skip resolution entirely when a room has no locks. */
    suspend fun roomHasLocks(roomId: RoomId): Boolean =
        lockStore.getAllLocksInRoom(ServerRoomId(roomId.rawValue)).isNotEmpty()

    /** The most-specific lock covering a write to (keyspace, lockerRaw), or null if open. */
    suspend fun resolveEffective(roomId: RoomId, keyspace: Long, lockerRaw: ByteArray): ServerLock? {
        val sr = ServerRoomId(roomId.rawValue)
        lockStore.getLock(sr, SCOPE_LOCKER, keyspace, ServerLockerId(lockerRaw))?.let { return it }
        lockStore.getLock(sr, SCOPE_KEYSPACE, keyspace, EMPTY_LOCKER)?.let { return it }
        lockStore.getLock(sr, SCOPE_ROOM, 0L, EMPTY_LOCKER)?.let { return it }
        return null
    }

    fun stateOf(lock: ServerLock): LockState = LockState.fromByteArray(lock.lockState)

    suspend fun verifyWrite(
        lock: ServerLock,
        roomId: RoomId,
        lockerId: LockerId,
        parentVersion: Long,
        contentHash: ByteArray,
        signature: Signature?,
    ): WriteVerdict {
        val pub = stateOf(lock).publicKey?.rawValue ?: return WriteVerdict.INVALID
        val sig = signature?.signature
        if (sig == null || sig.isEmpty()) return WriteVerdict.REQUIRED
        val ctx = LockerSigning.writeContext(roomId, lockerId, parentVersion, contentHash)
        return if (verifySig(pub, ctx, sig)) WriteVerdict.OK else WriteVerdict.INVALID
    }

    suspend fun contentHash(innerPayload: ByteArray): ByteArray = SHA256.digest(innerPayload)

    suspend fun applyLock(roomId: RoomId, grant: LockGrant, parentLockVersion: Long): LockOutcome {
        val scope = grant.scope ?: return LockOutcome.NotAuthorized
        val grantKey = grant.publicKey ?: return LockOutcome.NotAuthorized
        val scopeKind = scope.kind.value.toLong()
        val keyspace = if (scopeKind == SCOPE_ROOM) 0L else (scope.keyspace?.value ?: 0L)
        val lockerId = if (scopeKind == SCOPE_LOCKER) ServerLockerId(scope.lockerRawValue) else EMPTY_LOCKER
        val sr = ServerRoomId(roomId.rawValue)

        val existing = lockStore.getLock(sr, scopeKind, keyspace, lockerId)
        if (existing != null) {
            // Establishment only; rotate an existing lock via a ratchet on a write.
            return LockOutcome.Stale(stateOf(existing))
        }

        val roomKey = publicKeyedRoom(roomId)
        val parentLock = resolveParent(roomId, scopeKind, keyspace)
        val parentState = parentLock?.let { stateOf(it) }
        val parentKey = parentState?.publicKey?.rawValue
        val ctx = LockerSigning.grantContext(roomId, scope, grantKey.rawValue)
        val sig = grant.parentSignature?.signature

        val chain: List<LockGrant>
        if (sig != null && sig.isNotEmpty()) {
            when {
                parentKey != null && verifySig(parentKey, ctx, sig) -> chain = parentState.chain + grant
                roomKey != null && verifyWith(roomKey, ctx, sig) -> chain = listOf(grant)
                else -> return LockOutcome.NotAuthorized
            }
        } else {
            // TOFU root: only when the room is not public-keyed.
            if (roomKey != null) return LockOutcome.NotAuthorized
            chain = listOf(grant)
        }

        val newVersion = 1L
        val state = LockState(
            locked = true,
            scope = scope,
            publicKey = grantKey,
            chain = chain,
            ratchetKeys = emptyList(),
            lockVersion = newVersion,
        )
        lockStore.saveLock(
            ServerLock(
                roomId = sr,
                scopeKind = scopeKind,
                keyspace = keyspace,
                lockerId = lockerId,
                lockState = state.toByteArray(),
                version = newVersion,
            )
        )
        return LockOutcome.Ok(state)
    }

    suspend fun applyUnlock(
        roomId: RoomId,
        scope: LockScope,
        signature: Signature?,
        parentLockVersion: Long,
    ): UnlockOutcome {
        val scopeKind = scope.kind.value.toLong()
        val keyspace = if (scopeKind == SCOPE_ROOM) 0L else (scope.keyspace?.value ?: 0L)
        val lockerId = if (scopeKind == SCOPE_LOCKER) ServerLockerId(scope.lockerRawValue) else EMPTY_LOCKER
        val sr = ServerRoomId(roomId.rawValue)

        val existing = lockStore.getLock(sr, scopeKind, keyspace, lockerId) ?: return UnlockOutcome.Ok
        if (existing.version != parentLockVersion) return UnlockOutcome.Stale

        val pub = stateOf(existing).publicKey?.rawValue ?: return UnlockOutcome.SignatureInvalid
        val sig = signature?.signature
        val ctx = LockerSigning.unlockContext(roomId, scope)
        if (sig == null || sig.isEmpty() || !verifySig(pub, ctx, sig)) return UnlockOutcome.SignatureInvalid

        lockStore.deleteLock(sr, scopeKind, keyspace, lockerId)
        return UnlockOutcome.Ok
    }

    suspend fun applyRatchet(
        lock: ServerLock,
        roomId: RoomId,
        lockerId: LockerId,
        parentVersion: Long,
        ratchet: PostLockerChangeRequest.Ratchet,
    ): RatchetOutcome {
        val state = stateOf(lock)
        val oldKey = state.publicKey?.rawValue ?: return RatchetOutcome.Invalid
        val newKey = ratchet.newPublicKey ?: return RatchetOutcome.Invalid
        val sig = ratchet.signature?.signature
        if (sig == null || sig.isEmpty()) return RatchetOutcome.Invalid

        val ctx = LockerSigning.ratchetContext(roomId, lockerId, parentVersion, newKey.rawValue)
        if (!verifySig(oldKey, ctx, sig)) return RatchetOutcome.Invalid

        val newState = state.copy(
            publicKey = newKey,
            ratchetKeys = ratchet.newSharedKeys,
            lockVersion = state.lockVersion + 1,
        )
        lockStore.saveLock(lock.copy(lockState = newState.toByteArray(), version = lock.version + 1))
        return RatchetOutcome.Ok(newState)
    }

    private suspend fun resolveParent(roomId: RoomId, scopeKind: Long, keyspace: Long): ServerLock? {
        val sr = ServerRoomId(roomId.rawValue)
        return when (scopeKind) {
            SCOPE_LOCKER ->
                lockStore.getLock(sr, SCOPE_KEYSPACE, keyspace, EMPTY_LOCKER)
                    ?: lockStore.getLock(sr, SCOPE_ROOM, 0L, EMPTY_LOCKER)
            SCOPE_KEYSPACE -> lockStore.getLock(sr, SCOPE_ROOM, 0L, EMPTY_LOCKER)
            else -> null
        }
    }

    /**
     * A room is "public-keyed" when its id carries the [RoomKeying] marker and the
     * embedded bytes decode as a valid secp256r1 public key. Detection is gated on the
     * explicit marker first, then confirmed by decoding — never a bare decode guess.
     */
    private suspend fun publicKeyedRoom(roomId: RoomId): Secp256r1PublicKey? {
        val keyBytes = RoomKeying.authorityKey(roomId) ?: return null
        return try {
            Secp256r1PublicKey.decode(keyBytes)
        } catch (e: Exception) {
            null
        }
    }

    private suspend fun verifySig(publicKeyBytes: ByteArray, message: ByteArray, signature: ByteArray): Boolean =
        try {
            Secp256r1PublicKey.decode(publicKeyBytes).verify(message, signature)
        } catch (e: Exception) {
            false
        }

    private suspend fun verifyWith(key: Secp256r1PublicKey, message: ByteArray, signature: ByteArray): Boolean =
        try {
            key.verify(message, signature)
        } catch (e: Exception) {
            false
        }
}

/** Build a proto public key from raw compressed bytes. */
fun publicKeyOf(rawValue: ByteArray): Secp256R1Key.PublicKey = Secp256R1Key.PublicKey(rawValue = rawValue)
