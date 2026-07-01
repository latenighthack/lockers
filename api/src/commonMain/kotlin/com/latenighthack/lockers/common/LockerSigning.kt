package com.latenighthack.lockers.common

import com.latenighthack.lockers.common.v1.LockScope
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.common.v1.Secp256R1Key

/**
 * Canonical, domain-separated byte encodings that are signed/verified for locked
 * lockers. This is the single source of truth for the exact bytes both the client
 * (which signs) and the server (which verifies) run through the signature — proto
 * wire encoding is not canonical, so we build the pre-image explicitly here.
 *
 * Every element is length-prefixed (4-byte big-endian) and every scalar is a
 * fixed 8-byte big-endian long, so no two distinct inputs can collide on the same
 * byte string.
 */
object LockerSigning {
    const val DOMAIN_WRITE = "lockers/v1/write"
    const val DOMAIN_RATCHET = "lockers/v1/ratchet"
    const val DOMAIN_GRANT = "lockers/v1/grant"
    const val DOMAIN_UNLOCK = "lockers/v1/unlock"

    /** Signed by the locker's current key for a content write (or delete, with an empty hash). */
    fun writeContext(roomId: RoomId, lockerId: LockerId, version: Long, contentHash: ByteArray): ByteArray =
        Writer(DOMAIN_WRITE)
            .bytes(roomId.rawValue)
            .bytes(lockerId.rawValue)
            .long(lockerId.keyspace?.value ?: 0L)
            .long(version)
            .bytes(contentHash)
            .finish()

    /** Signed by the OLD key when rotating to [newPublicKey] alongside a write at [version]. */
    fun ratchetContext(roomId: RoomId, lockerId: LockerId, version: Long, newPublicKey: ByteArray): ByteArray =
        Writer(DOMAIN_RATCHET)
            .bytes(roomId.rawValue)
            .bytes(lockerId.rawValue)
            .long(lockerId.keyspace?.value ?: 0L)
            .long(version)
            .bytes(newPublicKey)
            .finish()

    /** Signed by the parent (or room) key to delegate write authority to [childPublicKey] at [scope]. */
    fun grantContext(roomId: RoomId, scope: LockScope, childPublicKey: ByteArray): ByteArray =
        Writer(DOMAIN_GRANT)
            .bytes(roomId.rawValue)
            .scope(scope)
            .bytes(childPublicKey)
            .finish()

    /** Signed by the current lock key to remove the lock at [scope]. */
    fun unlockContext(roomId: RoomId, scope: LockScope): ByteArray =
        Writer(DOMAIN_UNLOCK)
            .bytes(roomId.rawValue)
            .scope(scope)
            .finish()

    fun publicKeyBytes(key: Secp256R1Key.PublicKey?): ByteArray = key?.rawValue ?: ByteArray(0)

    private class Writer(domain: String) {
        private val out = ArrayList<Byte>(64)

        init {
            bytes(domain.encodeToByteArray())
        }

        fun bytes(value: ByteArray): Writer {
            long(value.size.toLong())
            for (b in value) out.add(b)
            return this
        }

        fun long(value: Long): Writer {
            var v = value
            val buf = ByteArray(8)
            for (i in 7 downTo 0) {
                buf[i] = (v and 0xFF).toByte()
                v = v ushr 8
            }
            for (b in buf) out.add(b)
            return this
        }

        fun scope(scope: LockScope): Writer {
            long(scope.kind.value.toLong())
            long(scope.keyspace?.value ?: 0L)
            bytes(scope.lockerRawValue)
            return this
        }

        fun finish(): ByteArray = out.toByteArray()
    }
}
