package com.latenighthack.lockers.connector.internal

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.room.v1.*

/** Contains private key material: the embedding application's database must be trusted/encrypted. */
data class PendingRatchet(val request: PostLockerChangeRequest, val privateKey: ByteArray)

internal fun encodeFrames(vararg frames: ByteArray): ByteArray {
    val output = ByteArray(frames.sumOf { 4 + it.size })
    var offset = 0
    frames.forEach { frame ->
        repeat(4) { output[offset++] = (frame.size ushr (24 - 8 * it)).toByte() }
        frame.copyInto(output, offset); offset += frame.size
    }
    return output
}
internal fun decodeFrames(bytes: ByteArray): List<ByteArray> {
    val result = mutableListOf<ByteArray>()
    var offset = 0
    while (offset < bytes.size) {
        require(bytes.size - offset >= 4) { "Truncated connector journal" }
        var size = 0
        repeat(4) { size = (size shl 8) or (bytes[offset++].toInt() and 255) }
        require(size >= 0 && size <= bytes.size - offset) { "Invalid connector journal frame" }
        result += bytes.copyOfRange(offset, offset + size); offset += size
    }
    return result
}
private fun encodePending(value: PendingRatchet) = encodeFrames(value.request.toByteArray(), value.privateKey)
private fun decodePending(bytes: ByteArray): PendingRatchet {
    val fields = decodeFrames(bytes)
    require(fields.size == 2)
    return PendingRatchet(PostLockerChangeRequest.fromByteArray(fields[0]), fields[1])
}
private val PendingRatchet.requestId: ByteArray get() = request.writeRequestId

object RatchetJournalDefinitionV1 : StoreDefinition<PendingRatchet>(
    StoreName("pending_ratchets"), "PendingRatchet-frames-v1", ::decodePending, ::encodePending,
) {
    val requestId = bytesIndex(IndexName("request_id"), PendingRatchet::requestId, "writeRequestId-v1").also { primaryKey(it) }
}
internal class RatchetJournal(database: Database) : Store<PendingRatchet>(database, RatchetJournalDefinitionV1) {
    suspend fun pending(): List<PendingRatchet> { prepare(); return getAll() }
    suspend fun put(value: PendingRatchet) { prepare(); save(value) }
    suspend fun remove(request: PostLockerChangeRequest) { prepare(); delete(RatchetJournalDefinitionV1.requestId.eq(request.writeRequestId)) }
}
