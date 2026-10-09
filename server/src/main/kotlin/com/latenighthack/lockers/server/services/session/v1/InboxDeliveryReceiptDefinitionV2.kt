package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.*
import java.nio.ByteBuffer

internal const val INBOX_RECEIPT_RETENTION_MILLIS = 31L * 24 * 60 * 60 * 1000
internal class EventIdentityReuseException : IllegalArgumentException("Event identity reused with different bytes")
data class InboxDeliveryReceipt(val session: ByteArray, val event: ByteArray, val digest: ByteArray, val expiresAt: Long)
private val InboxDeliveryReceipt.receiptKey: ByteArray get() = inboxReceiptKey(session, event)
internal fun inboxReceiptKey(session: ByteArray, event: ByteArray): ByteArray =
    ByteBuffer.allocate(16 + session.size + event.size).putLong(session.size.toLong()).put(session)
        .putLong(event.size.toLong()).put(event).array()

object InboxDeliveryReceiptDefinitionV2 : StoreDefinition<InboxDeliveryReceipt>(
    StoreName("inbox_delivery_receipts_v2"), "length-framed-ids-sha256-expiry-v2",
    { raw ->
        val buffer = ByteBuffer.wrap(raw)
        val sidSize = buffer.get().toInt() and 255
        require(sidSize in 1..128 && raw.size >= sidSize + 42)
        val sid = ByteArray(sidSize).also { buffer.get(it) }
        val eventSize = buffer.get().toInt() and 255
        require(eventSize in 1..128 && raw.size == sidSize + eventSize + 42)
        val event = ByteArray(eventSize).also { buffer.get(it) }
        val digest = ByteArray(32).also { buffer.get(it) }
        InboxDeliveryReceipt(sid, event, digest, buffer.long)
    },
    { row ->
        require(row.session.size in 1..128 && row.event.size in 1..128 && row.digest.size == 32)
        ByteBuffer.allocate(row.session.size + row.event.size + 42).put(row.session.size.toByte()).put(row.session)
            .put(row.event.size.toByte()).put(row.event).put(row.digest).putLong(row.expiresAt).array()
    },
) {
    val identity = bytesIndex(IndexName("identity"), InboxDeliveryReceipt::receiptKey, "length-framed-ids-v2").also { primaryKey(it) }
    val session = bytesIndex(IndexName("session"), InboxDeliveryReceipt::session, "opaque-id-v2")
    val expires = mappedIndex(IndexName("expiresAt"), InboxDeliveryReceipt::expiresAt, object : StorageCodec<Long, ByteArray> {
        override fun encode(value: Long) = OrderedKeyEncoding.long(value)
        override fun key(name: IndexName) = StoreKey.SerializedKey(name.value)
    }, "ordered-long-v2")
}
