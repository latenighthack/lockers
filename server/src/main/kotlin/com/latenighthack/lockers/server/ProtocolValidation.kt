package com.latenighthack.lockers.server

import com.latenighthack.ktcrypto.Secp256r1PublicKey
import com.latenighthack.ktcrypto.decode
import com.latenighthack.ktcrypto.encode
import com.latenighthack.lockers.common.RoomKeying
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
}
