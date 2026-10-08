package com.latenighthack.lockers.common

import com.latenighthack.lockers.common.v1.Locker

/** The supported wire representations are open bytes and a clear signed enclosure. */
object LockerEnvelope {
    fun isSupported(locker: Locker): Boolean {
        if ((locker.open == null) == (locker.sealed == null)) return false
        if (locker.open != null) return true
        val payload = locker.sealed?.payload ?: return false
        return payload.enclosure != null && payload.sealedPayload.isEmpty() &&
            payload.publicKey == null && payload.sharedKeys.isEmpty()
    }

    fun payload(locker: Locker): ByteArray {
        require(isSupported(locker)) { "Unsupported or ambiguous locker envelope" }
        return locker.open?.encodedPayload ?: requireNotNull(locker.sealed?.payload?.enclosure).innerPayload
    }
}
