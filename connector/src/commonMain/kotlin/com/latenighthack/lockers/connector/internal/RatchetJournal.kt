package com.latenighthack.lockers.connector.internal

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.common.v1.*
import kotlinx.coroutines.flow.*

/** Contains private key material: the embedding application's database must be trusted/encrypted. */
data class PendingRatchet(val request: PostLockerChangeRequest, val privateKey: ByteArray, val expectation: RatchetExpectation? = null)

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
/** Original authority only; acknowledgment accepts source uncertainty, never certifies a commit. */
data class RatchetExpectation(val requestId: ByteArray, val previous: LockState, val sourceUncertaintyAcknowledged: Boolean = false)
private fun encodeExpectation(value: RatchetExpectation) = encodeFrames(value.requestId, value.previous.toByteArray(), byteArrayOf(if (value.sourceUncertaintyAcknowledged) 1 else 0))
private fun decodeExpectation(bytes: ByteArray): RatchetExpectation {
    val fields = decodeFrames(bytes); require(fields.size == 3 && fields[2].size == 1 && fields[2][0] in 0..1)
    return RatchetExpectation(fields[0], LockState.fromByteArray(fields[1]), fields[2][0] == 1.toByte())
}
/** Additive metadata: the frozen private PendingRatchet V1 codec remains unchanged. */
object RatchetExpectationDefinitionV2 : StoreDefinition<RatchetExpectation>(StoreName("ratchet_expectations_v2"),
    "RatchetExpectation-frames-v2", ::decodeExpectation, ::encodeExpectation) {
    val requestId = bytesIndex(IndexName("request_id"), RatchetExpectation::requestId, "writeRequestId-v2").also { primaryKey(it) }
}
private class Expectations(database: Database) : Store<RatchetExpectation>(database, RatchetExpectationDefinitionV2) {
    suspend fun find(id: ByteArray) = get(RatchetExpectationDefinitionV2.requestId.eq(id))
    suspend fun put(value: RatchetExpectation) = save(value)
    suspend fun remove(id: ByteArray) = delete(RatchetExpectationDefinitionV2.requestId.eq(id))
}
internal class RatchetJournal(private val database: Database) : Store<PendingRatchet>(database, RatchetJournalDefinitionV1) {
    private val expectations = Expectations(database)
    private suspend fun prepareBoth() { prepare(); expectations.prepare() }
    private suspend fun attach(value: PendingRatchet): PendingRatchet = decodePending(encodePending(value)).copy(
        expectation = expectations.find(value.requestId)?.let { decodeExpectation(encodeExpectation(it)) })
    fun entries(): Flow<PendingRatchet> = flow {
        prepareBoth(); var after: LocalContinuation? = null
        do {
            val page = database.query(RatchetJournalDefinitionV1.storeName, IndexedQuery(RatchetJournalDefinitionV1.requestId.key, 1, after = after))
            page.records.forEach { emit(attach(if (it is PendingRatchet) it else decodePending(it as ByteArray))) }
            after = page.continuation
        } while (after != null)
    }
    suspend fun pending(): List<PendingRatchet> = entries().toList()
    suspend fun put(value: PendingRatchet) {
        prepareBoth(); database.transaction("connector-ratchet") {
            val existing = get(RatchetJournalDefinitionV1.requestId.eq(value.requestId))
            if (existing == null && database.count(RatchetJournalDefinitionV1.storeName, IndexedQuery(RatchetJournalDefinitionV1.requestId.key, 1)) >= 1_024)
                throw com.latenighthack.lockers.connector.ConnectorRetentionExceededException("Pending ratchet admission limit exceeded")
            if (existing != null) {
                require(encodePending(existing).contentEquals(encodePending(value))) { "Ratchet intent identity reused" }
                return@transaction // Original expectation and ACK are immutable to repeat saves.
            }
            save(decodePending(encodePending(value)))
            value.expectation?.let {
                require(it.requestId.contentEquals(value.requestId) && it.previous.scope != null && it.previous.locked && it.previous.lockVersion in 1 until Long.MAX_VALUE)
                expectations.put(decodeExpectation(encodeExpectation(it)))
            }
        }
    }
    suspend fun acknowledge(request: PostLockerChangeRequest) {
        prepareBoth(); database.transaction("connector-ratchet") {
            val pending = requireNotNull(get(RatchetJournalDefinitionV1.requestId.eq(request.writeRequestId))) { "Unknown ratchet intent" }
            require(pending.request.toByteArray().contentEquals(request.toByteArray())) { "Ratchet intent identity mismatch" }
            val expected = requireNotNull(expectations.find(request.writeRequestId)) { "Original authority unavailable" }
            expectations.put(expected.copy(sourceUncertaintyAcknowledged = true))
        }
    }
    suspend fun remove(request: PostLockerChangeRequest) {
        prepareBoth(); database.transaction("connector-ratchet") {
            delete(RatchetJournalDefinitionV1.requestId.eq(request.writeRequestId))
            expectations.remove(request.writeRequestId)
        }
    }
}
