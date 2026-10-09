package com.latenighthack.lockers.common

import com.latenighthack.lockers.common.v1.SessionId

/** V2 unary proof: parameters are encoded with the proof field absent. */
object SessionSigning {
    const val SUBSCRIPTION = "subscription"
    const val SNAPSHOT = "subscribe-and-snapshot"
    const val REGISTER_PUSH = "register-push"
    const val UNREGISTER_PUSH = "unregister-push"
    const val DESTROY = "destroy-session"

    fun context(operation: String, sessionId: SessionId, requestDigest: ByteArray,
        issuedAtMs: Long, nonce: ByteArray): ByteArray {
        val bytes = mutableListOf<Byte>()
        fun long(value: Long) { for (shift in 56 downTo 0 step 8) bytes.add((value ushr shift).toByte()) }
        fun field(value: ByteArray) { long(value.size.toLong()); bytes.addAll(value.toList()) }
        field("lockers/v2/session/$operation".encodeToByteArray())
        field(sessionId.rawValue); field(requestDigest); long(issuedAtMs); field(nonce)
        return bytes.toByteArray()
    }
}
