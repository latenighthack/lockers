package com.latenighthack.lockers.server

import com.latenighthack.ktcrypto.Secp256r1PublicKey
import com.latenighthack.ktcrypto.decode
import com.latenighthack.ktcrypto.encode
import com.latenighthack.lockers.common.RoomKeying
import com.latenighthack.lockers.common.LockerEnvelope
import com.latenighthack.lockers.common.v1.*
import kotlinx.coroutines.CancellationException

/** Shared trust-boundary validation; opaque identifiers retain their existing variable length. */
object ProtocolValidation {
    const val MAX_ID_BYTES = 128
    const val MAX_RECIPIENTS = 1024
    const val MAX_NOTIFICATION_BYTES = 64 * 1024
    const val MAX_ENVELOPE_BYTES = 8 * 1024 * 1024

    fun identity(raw: ByteArray?): Boolean = raw != null && raw.size in 1..MAX_ID_BYTES
    fun locker(id: LockerId?): Boolean = identity(id?.rawValue)
    fun scope(scope: LockScope?): Boolean = scope != null && when (scope.kind.value) {
        0 -> identity(scope.lockerRawValue)
        1 -> scope.lockerRawValue.isEmpty()
        2 -> scope.lockerRawValue.isEmpty() && (scope.keyspace?.value ?: 0L) == 0L
        else -> false
    }
    suspend fun signature(value: Signature?, required: Boolean = false): Boolean {
        if (value == null) return !required
        return value.toByteArray().size <= 256 && value.signingVersion in 0..2 &&
            (value.signature.isEmpty() && !required || value.signature.size == 64) &&
            (value.publicKey == null || publicKey(value.publicKey?.rawValue))
    }
    suspend fun grant(value: LockGrant?): Boolean = value != null && scope(value.scope) &&
        value.toByteArray().size <= 1024 && publicKey(value.publicKey?.rawValue) &&
        signature(value.parentSignature) && value.authorityVersion >= 0 && value.scopeVersion >= 0

    suspend fun change(value: com.latenighthack.lockers.room.v1.PostLockerChangeRequest, expectedRoom: RoomId): Boolean {
        val body = value.locker ?: return false
        if (value.roomId?.let { !it.rawValue.contentEquals(expectedRoom.rawValue) } == true) return false
        if (!locker(value.lockerId) || value.parentVersion < 0 || !LockerEnvelope.isSupported(body) ||
            !notification(value.notification) || !signature(value.writeSignature)) return false
        val payload = body.sealed?.payload
        if (payload != null && (payload.checksum.isNotEmpty() && payload.checksum.size != 32 ||
                !signature(payload.enclosure?.signature))) return false
        val ratchet = value.ratchet
        return ratchet == null || publicKey(ratchet.newPublicKey?.rawValue) &&
            sharedKeys(ratchet.newSharedKeys) && signature(ratchet.signature, required = true)
    }
    suspend fun room(room: RoomId?): Boolean {
        if (!identity(room?.rawValue)) return false
        val authority = RoomKeying.authorityKey(requireNotNull(room)) ?: return true
        return publicKey(authority)
    }
    suspend fun publicKey(raw: ByteArray?): Boolean {
        if (raw == null || raw.size != 33 || raw[0] !in byteArrayOf(2, 3)) return false
        return try { Secp256r1PublicKey.decode(raw).encode().contentEquals(raw) }
        catch (cancelled: CancellationException) { throw cancelled }
        catch (_: Exception) { false }
    }
    fun notification(value: Notification?): Boolean = value == null ||
        (value.toByteArray().size <= MAX_NOTIFICATION_BYTES &&
            (value.push?.title?.encodeToByteArray()?.size ?: 0) <= 512 &&
            (value.push?.body?.encodeToByteArray()?.size ?: 0) <= 4096)
    fun sharedKeys(keys: List<SharedKey>): Boolean = keys.size <= 1024 && keys.all {
        identity(it.keyId?.rawValue) && it.encryptedKey.size in 1..4096
    }
    suspend fun event(value: Event?): Boolean {
        if (value == null || !identity(value.eventId?.rawValue) || value.roomId == null || value.roomSequence < 0 ||
            value.toByteArray().size > MAX_ENVELOPE_BYTES - 16 * 1024 || !notification(value.notification)) return false
        val roomId = requireNotNull(value.roomId)
        if (roomId.rawValue.isNotEmpty() && !room(roomId)) return false
        val identified = value.locker ?: return true
        return locker(identified.lockerId) && identified.version >= 0 &&
            (identified.locker?.let { LockerEnvelope.isSupported(it) } ?: true)
    }
}
