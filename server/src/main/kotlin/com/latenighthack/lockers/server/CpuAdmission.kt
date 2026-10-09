package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.lockers.common.v1.*

/** Shared per-node work budget. One unit covers a key/signature operation or64KiB of input. */
class CpuAdmission(private val limits: ServerResourceLimits, private val nanoTime: () -> Long = System::nanoTime) {
    private var tokens = limits.globalCpuBurst.toDouble()
    private var last = nanoTime()
    @Synchronized fun tryAcquire(encodedBytes: Int, cryptoOperations: Int): Boolean {
        require(encodedBytes in 0..ProtocolValidation.MAX_ENVELOPE_BYTES && cryptoOperations in 1..1024)
        val cost = cryptoOperations + (encodedBytes.toLong() + BYTE_UNIT - 1) / BYTE_UNIT
        val now = nanoTime()
        tokens = minOf(limits.globalCpuBurst.toDouble(), tokens + (now - last).coerceAtLeast(0) / 1_000_000_000.0 * limits.globalCpuUnitsPerSecond)
        last = now
        if (tokens < cost) return false
        tokens -= cost
        return true
    }
    fun fits(encodedBytes: Int, cryptoOperations: Int): Boolean {
        require(encodedBytes in 0..ProtocolValidation.MAX_ENVELOPE_BYTES && cryptoOperations in 1..1024)
        return cryptoOperations + (encodedBytes.toLong() + BYTE_UNIT - 1) / BYTE_UNIT <= limits.globalCpuBurst
    }
    fun require(encodedBytes: Int, cryptoOperations: Int) {
        if (!fits(encodedBytes, cryptoOperations)) throw RpcResponseException("", "RPC", Codes.OUT_OF_RANGE, "Request exceeds configured verification work capacity")
        if (!tryAcquire(encodedBytes, cryptoOperations)) throw RpcResponseException("", "RPC", Codes.RESOURCE_EXHAUSTED, "Global verification work admission exhausted")
    }
    companion object {
        private const val BYTE_UNIT = 64L * 1024
        fun keyShape(raw: ByteArray?): Boolean = raw != null && raw.size == 33 && raw[0] in byteArrayOf(2, 3)
        fun signatureShape(value: Signature?, required: Boolean = false): Boolean = value == null && !required || value != null &&
            value.toByteArray().size <= 256 && value.signingVersion in 0..2 &&
            (value.signature.size == 64 || value.signature.isEmpty() && !required) &&
            (value.publicKey == null || keyShape(value.publicKey?.rawValue))
    }
}
