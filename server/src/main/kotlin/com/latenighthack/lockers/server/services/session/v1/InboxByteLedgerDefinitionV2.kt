package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*
import java.nio.ByteBuffer

/** Cursor is the last charged inbox primary key; ACK can decrement during bounded backfill. */
data class InboxByteLedger(val scope: ByteArray, val bytes: Long, val cursor: ByteArray = byteArrayOf(), val complete: Boolean = false)
object InboxByteLedgerDefinitionV2 : StoreDefinition<InboxByteLedger>(StoreName("inbox_byte_ledger_v2"), "scope-bytes-backfill-cursor-v2",
    { raw ->
        val buffer = ByteBuffer.wrap(raw); val scopeSize = buffer.short.toInt() and 65535
        require(scopeSize in 1..129 && raw.size >= scopeSize + 13)
        val scope = ByteArray(scopeSize).also { buffer.get(it) }; val bytes = buffer.long
        val complete = buffer.get().toInt(); require(bytes >= 0 && complete in 0..1)
        val cursorSize = buffer.short.toInt() and 65535
        require(cursorSize <= 265 && buffer.remaining() == cursorSize)
        InboxByteLedger(scope, bytes, ByteArray(cursorSize).also { buffer.get(it) }, complete == 1)
    },
    { row ->
        require(row.scope.size in 1..129 && row.cursor.size <= 265 && row.bytes >= 0)
        ByteBuffer.allocate(row.scope.size + row.cursor.size + 13).putShort(row.scope.size.toShort()).put(row.scope)
            .putLong(row.bytes).put((if (row.complete) 1 else 0).toByte()).putShort(row.cursor.size.toShort()).put(row.cursor).array()
    }) {
    val scope = bytesIndex(IndexName("scope"), InboxByteLedger::scope, "global-or-raw-session-v2").also { primaryKey(it) }
}

/** Exact wire byte length without re-encoding a potentially large payload for every recipient. */
internal fun inboxRowBytes(row: com.latenighthack.lockers.server.storage.v1.ServerSessionEvent,
    payloadBytes: Int = row.encodedPayload.size, lockerBytes: Int = row.encodedLocker.size): Long {
    fun varint(value: Long): Int { var v = value; var bytes = 1; while (v ushr 7 != 0L) { bytes++; v = v ushr 7 }; return bytes }
    fun field(size: Int): Long = 1L + varint(size.toLong()) + size
    return (row.unknownFields?.size ?: 0).toLong() +
        (row.sessionId?.let { field(it.toByteArray().size) } ?: 0) +
        (row.roomId?.let { field(it.toByteArray().size) } ?: 0) +
        (row.eventId?.let { field(it.toByteArray().size) } ?: 0) +
        (if (payloadBytes == 0) 0 else field(payloadBytes)) + (if (lockerBytes == 0) 0 else field(lockerBytes)) +
        (if (row.roomSequence == 0L) 0 else 1 + varint(row.roomSequence))
}
