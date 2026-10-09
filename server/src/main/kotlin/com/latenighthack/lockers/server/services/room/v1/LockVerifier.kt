package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.LockerSigning
import com.latenighthack.lockers.server.ProtocolValidation
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
        lockStore.getAllLocksInRoom(ServerRoomId(roomId.rawValue)).any { stateOf(it).locked }

    suspend fun scopeState(roomId: RoomId, scope: LockScope): LockState? = lockStore.getLock(
        ServerRoomId(roomId.rawValue), scope.kind.value.toLong(),
        if (scope.kind.value.toLong() == SCOPE_ROOM) 0 else scope.keyspace?.value ?: 0,
        if (scope.kind.value.toLong() == SCOPE_LOCKER) ServerLockerId(scope.lockerRawValue) else EMPTY_LOCKER,
    )?.let(::stateOf)

    suspend fun parentState(roomId: RoomId, scope: LockScope): LockState? =
        resolveParent(roomId, scope.kind.value.toLong(), scope.keyspace?.value ?: 0)?.let(::stateOf)

    /** The most-specific lock covering a write to (keyspace, lockerRaw), or null if open. */
    suspend fun resolveEffective(roomId: RoomId, keyspace: Long, lockerRaw: ByteArray): ServerLock? {
        val sr = ServerRoomId(roomId.rawValue)
        active(lockStore.getLock(sr, SCOPE_LOCKER, keyspace, ServerLockerId(lockerRaw)))?.let { return it }
        active(lockStore.getLock(sr, SCOPE_KEYSPACE, keyspace, EMPTY_LOCKER))?.let { return it }
        active(lockStore.getLock(sr, SCOPE_ROOM, 0L, EMPTY_LOCKER))?.let { return it }
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
        notification: Notification? = null,
    ): WriteVerdict {
        val pub = stateOf(lock).publicKey?.rawValue ?: return WriteVerdict.INVALID
        val sig = signature?.signature
        if (sig == null || sig.isEmpty()) return WriteVerdict.REQUIRED
        val ctx = when (signature.signingVersion) {
            0, 1 -> {
                if (lock.version != 1L || notification != null) return WriteVerdict.INVALID
                LockerSigning.writeContext(roomId, lockerId, parentVersion, contentHash)
            }
            2 -> LockerSigning.writeContextV2(roomId, lockerId, parentVersion, lock.version, contentHash, notification)
            else -> return WriteVerdict.INVALID
        }
        return if (verifySig(pub, ctx, sig)) WriteVerdict.OK else WriteVerdict.INVALID
    }

    suspend fun contentHash(innerPayload: ByteArray): ByteArray = SHA256.digest(innerPayload)

    suspend fun applyLock(roomId: RoomId, grant: LockGrant, parentLockVersion: Long): LockOutcome =
        lockStore.atomic(ServerRoomId(roomId.rawValue)) { applyLockUnchecked(roomId, grant, parentLockVersion) }

    private suspend fun applyLockUnchecked(roomId: RoomId, grant: LockGrant, parentLockVersion: Long): LockOutcome {
        if (!ProtocolValidation.room(roomId) || !ProtocolValidation.scope(grant.scope) ||
            !ProtocolValidation.publicKey(grant.publicKey?.rawValue) || parentLockVersion < 0 ||
            grant.authorityVersion < 0 || grant.scopeVersion < 0) return LockOutcome.NotAuthorized
        val scope = grant.scope ?: return LockOutcome.NotAuthorized
        val grantKey = grant.publicKey ?: return LockOutcome.NotAuthorized
        val scopeKind = scope.kind.value.toLong()
        val keyspace = if (scopeKind == SCOPE_ROOM) 0L else (scope.keyspace?.value ?: 0L)
        val lockerId = if (scopeKind == SCOPE_LOCKER) ServerLockerId(scope.lockerRawValue) else EMPTY_LOCKER
        val sr = ServerRoomId(roomId.rawValue)

        val existing = lockStore.getLock(sr, scopeKind, keyspace, lockerId)
        if (existing != null && stateOf(existing).locked) {
            // Establishment only; rotate an existing lock via a ratchet on a write.
            return LockOutcome.Stale(stateOf(existing))
        }
        val scopeVersion = existing?.version ?: 0L
        if (parentLockVersion != scopeVersion || scopeVersion == Long.MAX_VALUE) {
            return LockOutcome.Stale(existing?.let(::stateOf) ?: LockState(scope = scope))
        }

        val roomKey = publicKeyedRoom(roomId)
        val parentLock = resolveParent(roomId, scopeKind, keyspace)
        val parentState = parentLock?.let { stateOf(it) }
        val parentKey = parentState?.publicKey?.rawValue
        val sig = grant.parentSignature?.signature
        val authorityVersion = parentState?.lockVersion ?: 0L
        val ctx = if (grant.parentSignature?.signingVersion == 2) {
            if (grant.authorityVersion != authorityVersion || grant.scopeVersion != scopeVersion) return LockOutcome.NotAuthorized
            LockerSigning.grantContextV2(roomId, scope, grantKey.rawValue, authorityVersion, scopeVersion)
        } else {
            if (sig != null && sig.isNotEmpty() && (scopeVersion != 0L || authorityVersion > 1L ||
                grant.authorityVersion != 0L || grant.scopeVersion != 0L || grant.parentSignature?.signingVersion !in listOf(0, 1))) return LockOutcome.NotAuthorized
            LockerSigning.grantContext(roomId, scope, grantKey.rawValue)
        }

        val chain: List<LockGrant>
        if (sig != null && sig.isNotEmpty()) {
            when {
                parentKey != null -> {
                    if (!verifySig(parentKey, ctx, sig)) return LockOutcome.NotAuthorized
                    chain = parentState.chain + grant
                }
                roomKey != null && verifyWith(roomKey, ctx, sig) -> chain = listOf(grant)
                else -> return LockOutcome.NotAuthorized
            }
        } else {
            // Unsigned establishment is possible only in an unclaimed hierarchy.
            if (roomKey != null || parentLock != null) return LockOutcome.NotAuthorized
            chain = listOf(grant)
        }

        val newVersion = scopeVersion + 1L
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

    suspend fun applyUnlock(roomId: RoomId, scope: LockScope, signature: Signature?, parentLockVersion: Long): UnlockOutcome =
        lockStore.atomic(ServerRoomId(roomId.rawValue)) { applyUnlockUnchecked(roomId, scope, signature, parentLockVersion) }

    private suspend fun applyUnlockUnchecked(roomId: RoomId, scope: LockScope, signature: Signature?, parentLockVersion: Long): UnlockOutcome {
        if (!ProtocolValidation.room(roomId) || !ProtocolValidation.scope(scope) || parentLockVersion < 0)
            return UnlockOutcome.SignatureInvalid
        val scopeKind = scope.kind.value.toLong()
        val keyspace = if (scopeKind == SCOPE_ROOM) 0L else (scope.keyspace?.value ?: 0L)
        val lockerId = if (scopeKind == SCOPE_LOCKER) ServerLockerId(scope.lockerRawValue) else EMPTY_LOCKER
        val sr = ServerRoomId(roomId.rawValue)

        val existing = lockStore.getLock(sr, scopeKind, keyspace, lockerId) ?: return UnlockOutcome.Ok
        if (existing.version != parentLockVersion) return UnlockOutcome.Stale
        if (!stateOf(existing).locked) return UnlockOutcome.Ok
        if (existing.version == Long.MAX_VALUE) return UnlockOutcome.Stale

        val pub = stateOf(existing).publicKey?.rawValue ?: return UnlockOutcome.SignatureInvalid
        val sig = signature?.signature
        val ctx = when (signature?.signingVersion) {
            0, 1 -> {
                if (existing.version != 1L) return UnlockOutcome.SignatureInvalid
                LockerSigning.unlockContext(roomId, scope)
            }
            2 -> LockerSigning.unlockContextV2(roomId, scope, parentLockVersion)
            else -> return UnlockOutcome.SignatureInvalid
        }
        if (sig == null || sig.isEmpty() || !verifySig(pub, ctx, sig)) return UnlockOutcome.SignatureInvalid

        retire(existing)
        // Revoking a parent also revokes delegated descendants. Keep every incarnation.
        for (child in lockStore.getAllLocksInRoom(sr)) {
            val descendant = child.scopeKind < scopeKind && (scopeKind == SCOPE_ROOM || child.keyspace == keyspace)
            if (descendant && stateOf(child).locked) retire(child)
        }
        return UnlockOutcome.Ok
    }

    suspend fun applyRatchet(lock: ServerLock, roomId: RoomId, lockerId: LockerId, parentVersion: Long, ratchet: PostLockerChangeRequest.Ratchet): RatchetOutcome =
        lockStore.atomic(ServerRoomId(roomId.rawValue)) {
            val current = lockStore.getLock(ServerRoomId(roomId.rawValue), lock.scopeKind, lock.keyspace, requireNotNull(lock.lockerId))
            if (current == null || current.version != lock.version || !current.lockState.contentEquals(lock.lockState)) RatchetOutcome.Invalid
            else applyRatchetUnchecked(current, roomId, lockerId, parentVersion, ratchet)
        }

    private suspend fun applyRatchetUnchecked(lock: ServerLock, roomId: RoomId, lockerId: LockerId, parentVersion: Long, ratchet: PostLockerChangeRequest.Ratchet): RatchetOutcome {
        if (!ProtocolValidation.room(roomId) || !ProtocolValidation.locker(lockerId) || parentVersion < 0 ||
            !ProtocolValidation.publicKey(ratchet.newPublicKey?.rawValue) ||
            !ProtocolValidation.sharedKeys(ratchet.newSharedKeys)) return RatchetOutcome.Invalid
        val state = stateOf(lock)
        val oldKey = state.publicKey?.rawValue ?: return RatchetOutcome.Invalid
        val newKey = ratchet.newPublicKey ?: return RatchetOutcome.Invalid
        val sig = ratchet.signature?.signature
        if (sig == null || sig.isEmpty()) return RatchetOutcome.Invalid

        if (lock.version == Long.MAX_VALUE) return RatchetOutcome.Invalid
        val ctx = when (ratchet.signature?.signingVersion) {
            0, 1 -> {
                if (lock.version != 1L || ratchet.newSharedKeys.isNotEmpty()) return RatchetOutcome.Invalid
                LockerSigning.ratchetContext(roomId, lockerId, parentVersion, newKey.rawValue)
            }
            2 -> LockerSigning.ratchetContextV2(roomId, lockerId, parentVersion, lock.version, newKey.rawValue, ratchet.newSharedKeys)
            else -> return RatchetOutcome.Invalid
        }
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
                active(lockStore.getLock(sr, SCOPE_KEYSPACE, keyspace, EMPTY_LOCKER))
                    ?: active(lockStore.getLock(sr, SCOPE_ROOM, 0L, EMPTY_LOCKER))
            SCOPE_KEYSPACE -> active(lockStore.getLock(sr, SCOPE_ROOM, 0L, EMPTY_LOCKER))
            else -> null
        }
    }

    private fun active(lock: ServerLock?): ServerLock? = lock?.takeIf { stateOf(it).locked }

    private suspend fun retire(lock: ServerLock) {
        check(lock.version < Long.MAX_VALUE) { "Authority incarnation exhausted" }
        val version = lock.version + 1L
        lockStore.saveLock(lock.copy(version = version, lockState = LockState(scope = stateOf(lock).scope, lockVersion = version).toByteArray()))
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
        } catch (cancelled: kotlinx.coroutines.CancellationException) {
            throw cancelled
        } catch (expectedCryptographicRejection: Exception) {
            null
        }
    }

    private suspend fun verifySig(publicKeyBytes: ByteArray, message: ByteArray, signature: ByteArray): Boolean =
        try {
            Secp256r1PublicKey.decode(publicKeyBytes).verify(message, signature)
        } catch (cancelled: kotlinx.coroutines.CancellationException) {
            throw cancelled
        } catch (expectedCryptographicRejection: Exception) {
            false
        }

    private suspend fun verifyWith(key: Secp256r1PublicKey, message: ByteArray, signature: ByteArray): Boolean =
        try {
            key.verify(message, signature)
        } catch (cancelled: kotlinx.coroutines.CancellationException) {
            throw cancelled
        } catch (expectedCryptographicRejection: Exception) {
            false
        }
}

/** Build a proto public key from raw compressed bytes. */
fun publicKeyOf(rawValue: ByteArray): Secp256R1Key.PublicKey = Secp256R1Key.PublicKey(rawValue = rawValue)
