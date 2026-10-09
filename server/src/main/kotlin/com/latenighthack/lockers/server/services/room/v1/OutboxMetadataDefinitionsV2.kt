package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v2.*
import java.nio.ByteBuffer

internal fun outboxRoomPrefix(room: ByteArray) = ByteBuffer.allocate(4 + room.size).putInt(room.size).put(room).array()
private val ServerOutboxEntry.roomOrder: ByteArray get() = outboxRoomPrefix(roomId) + OrderedKeyEncoding.long(sequence)
private val ServerOutboxEntry.activeOrder: ByteArray? get() = if (parkedReason.isEmpty() && resolvedAt == 0L) roomOrder else null
private val ServerOutboxEntry.pendingOrder: ByteArray? get() = if (resolvedAt == 0L) roomOrder else null
private val ServerOutboxEntry.parkedRoomOrder: ByteArray? get() = if (parkedReason.isNotEmpty() && resolvedAt == 0L) roomOrder else null
private val ServerOutboxEntry.parkedOrder: ByteArray? get() = if (parkedReason.isNotEmpty() && resolvedAt == 0L) eventId else null
private val ServerOutboxEntry.completedOrder: ByteArray? get() = resolvedAt.takeIf { it > 0L }?.let(OrderedKeyEncoding::long)

class OutboxEntriesDefinitionV2(prefix: String) : StoreDefinition<ServerOutboxEntry>(
    StoreName("${prefix}_outbox_entries_v2"), "ServerOutboxEntry-protobuf-v2", ServerOutboxEntry.Companion::fromByteArray, ServerOutboxEntry::toByteArray,
) {
    val id = bytesIndex(IndexName("event_id_raw"), ServerOutboxEntry::eventId, "raw-bytes-v2").also { primaryKey(it) }
    val room = bytesIndex(IndexName("room_order"), ServerOutboxEntry::roomOrder, "length-room-ordered-sequence-v2")
    val active = nullableBytesIndex(IndexName("active_room_order"), ServerOutboxEntry::activeOrder, "length-room-ordered-sequence-v2")
    val pending = nullableBytesIndex(IndexName("pending_room_order"), ServerOutboxEntry::pendingOrder, "length-room-ordered-sequence-v2")
    val parkedRoom = nullableBytesIndex(IndexName("parked_room_order"), ServerOutboxEntry::parkedRoomOrder, "length-room-ordered-sequence-v2")
    val parked = nullableBytesIndex(IndexName("parked_event"), ServerOutboxEntry::parkedOrder, "raw-bytes-v2")
    val completed = nullableBytesIndex(IndexName("completed_time"), ServerOutboxEntry::completedOrder, "ordered-time-v2")
}
private val ServerOutboxHead.dueOrder: ByteArray get() = OrderedKeyEncoding.long(dueAt)
class OutboxHeadsDefinitionV2(prefix: String) : StoreDefinition<ServerOutboxHead>(
    StoreName("${prefix}_outbox_heads_v2"), "ServerOutboxHead-protobuf-v2", ServerOutboxHead.Companion::fromByteArray, ServerOutboxHead::toByteArray,
) {
    val room = bytesIndex(IndexName("room_raw"), ServerOutboxHead::roomId, "raw-bytes-v2").also { primaryKey(it) }
    val due = bytesIndex(IndexName("due_time"), ServerOutboxHead::dueOrder, "ordered-time-v2")
}

data class OutboxPolicy(
    val retryWindowMs: Long = 30L * 24 * 60 * 60 * 1000,
    val completedRetentionMs: Long = 31L * 24 * 60 * 60 * 1000,
    val retainedPerRoom: Int = 4096,
    val retainedGlobal: Int = 1_000_000,
) {
    init { require(retryWindowMs > 0 && completedRetentionMs > retryWindowMs && retainedPerRoom > 0 && retainedGlobal >= retainedPerRoom) }
}
