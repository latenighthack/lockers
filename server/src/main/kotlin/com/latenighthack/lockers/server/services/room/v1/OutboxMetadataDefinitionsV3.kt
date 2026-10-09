package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.IndexName
import com.latenighthack.ktstore.OrderedKeyEncoding
import com.latenighthack.ktstore.StoreDefinition
import com.latenighthack.ktstore.StoreName
import com.latenighthack.lockers.server.storage.v2.ServerOutboxEntry
import com.latenighthack.lockers.server.storage.v2.ServerOutboxHead
import com.latenighthack.lockers.server.storage.v2.fromByteArray
import com.latenighthack.lockers.server.storage.v2.toByteArray

private val ServerOutboxEntry.roomOrderV3: ByteArray get() = outboxRoomPrefix(roomId) +
    OrderedKeyEncoding.long(sequence)
private val ServerOutboxEntry.activeOrderV3: ByteArray? get() = if (parkedReason.isEmpty() &&
    resolvedAt == 0L
) {
    roomOrderV3
} else {
    null
}
private val ServerOutboxEntry.pendingOrderV3: ByteArray? get() = if (resolvedAt == 0L) roomOrderV3 else null
private val ServerOutboxEntry.parkedRoomOrderV3: ByteArray? get() = if (parkedReason.isNotEmpty() &&
    resolvedAt == 0L
) {
    roomOrderV3
} else {
    null
}
private val ServerOutboxEntry.parkedOrderV3: ByteArray? get() = if (parkedReason.isNotEmpty() &&
    resolvedAt == 0L
) {
    eventId
} else {
    null
}
private val ServerOutboxEntry.completedOrderV3: ByteArray? get() = resolvedAt.takeIf {
    it > 0L
}?.let(OrderedKeyEncoding::long)

private val ServerOutboxEntry.pendingAge: ByteArray? get() =
    if (resolvedAt == 0L &&
        parkedReason.isEmpty()
    ) {
        OrderedKeyEncoding.long(enqueuedAt)
    } else {
        null
    }

class OutboxEntriesDefinitionV3(prefix: String) :
    StoreDefinition<ServerOutboxEntry>(
        StoreName("${prefix}_outbox_entries_v2"),
        "ServerOutboxEntry-protobuf-v2",
        ServerOutboxEntry.Companion::fromByteArray,
        ServerOutboxEntry::toByteArray,
    ) {
    val id = bytesIndex(IndexName("event_id_raw"), ServerOutboxEntry::eventId, "raw-bytes-v2").also { primaryKey(it) }
    val room = bytesIndex(IndexName("room_order"), ServerOutboxEntry::roomOrderV3, "length-room-ordered-sequence-v2")
    val active = nullableBytesIndex(
        IndexName("active_room_order"),
        ServerOutboxEntry::activeOrderV3,
        "length-room-ordered-sequence-v2",
    )
    val pending = nullableBytesIndex(
        IndexName("pending_room_order"),
        ServerOutboxEntry::pendingOrderV3,
        "length-room-ordered-sequence-v2",
    )
    val parkedRoom =
        nullableBytesIndex(
            IndexName("parked_room_order"),
            ServerOutboxEntry::parkedRoomOrderV3,
            "length-room-ordered-sequence-v2",
        )
    val parked = nullableBytesIndex(IndexName("parked_event"), ServerOutboxEntry::parkedOrderV3, "raw-bytes-v2")
    val age = nullableBytesIndex(IndexName("pending_age"), ServerOutboxEntry::pendingAge, "ordered-time-v3")
    val recipients = integerIndex(IndexName("pending_recipients"), ServerOutboxEntry::recipients)
    val completed = nullableBytesIndex(
        IndexName("completed_time"),
        ServerOutboxEntry::completedOrderV3,
        "ordered-time-v2",
    )
}

private val ServerOutboxHead.dueOrderV3: ByteArray get() = OrderedKeyEncoding.long(dueAt)

class OutboxHeadsDefinitionV3(prefix: String) :
    StoreDefinition<ServerOutboxHead>(
        StoreName("${prefix}_outbox_heads_v2"),
        "ServerOutboxHead-protobuf-v2",
        ServerOutboxHead.Companion::fromByteArray,
        ServerOutboxHead::toByteArray,
    ) {
    val room = bytesIndex(IndexName("room_raw"), ServerOutboxHead::roomId, "raw-bytes-v2").also { primaryKey(it) }
    val due = bytesIndex(IndexName("due_time"), ServerOutboxHead::dueOrderV3, "ordered-time-v2")
}
