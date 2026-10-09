package com.latenighthack.lockers.server.services.room.v1
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.storage.v1.*

private val ServerDeliveryIntent.idStorage: ByteArray get() = requireNotNull(eventId).toByteArray()
private val ServerDeliveryIntent.roomStorage: ByteArray get() = requireNotNull(roomId).toByteArray()

class DeliveryOutboxStoreDefinitionV1(prefix: String) : StoreDefinition<ServerDeliveryIntent>(
    StoreName("${prefix}_outbox"), "ServerDeliveryIntent-protobuf-v1", ServerDeliveryIntent.Companion::fromByteArray, ServerDeliveryIntent::toByteArray,
) {
    val id = bytesIndex(IndexName("eventIdtoByteArray"), ServerDeliveryIntent::idStorage, "toByteArray-v1").also { primaryKey(it) }
    val room = bytesIndex(IndexName("roomIdtoByteArray"), ServerDeliveryIntent::roomStorage, "toByteArray-v1")
}


private val ServerWriteReceipt.roomStorage: ByteArray get() = requireNotNull(roomId).toByteArray()

class WriteReceiptsDefinitionV1(prefix: String) : StoreDefinition<ServerWriteReceipt>(
    StoreName("${prefix}_write_receipts"), "ServerWriteReceipt-protobuf-v1", ServerWriteReceipt.Companion::fromByteArray, ServerWriteReceipt::toByteArray,
) {
    val room = bytesIndex(IndexName("roomIdtoByteArray"), ServerWriteReceipt::roomStorage, "toByteArray-v1")
    val request = bytesIndex(IndexName("requestId"), ServerWriteReceipt::requestId, "raw-bytes-v1")
    val key = compositeIndex(IndexName("composite_roomIdtoByteArray_requestId"), room, request).also { primaryKey(it) }
}


private val ServerRoomSequence.roomStorage: ByteArray get() = requireNotNull(roomId).toByteArray()

class RoomSequencesDefinitionV1(prefix: String) : StoreDefinition<ServerRoomSequence>(
    StoreName("${prefix}_room_sequences"), "ServerRoomSequence-protobuf-v1", ServerRoomSequence.Companion::fromByteArray, ServerRoomSequence::toByteArray,
) {
    val room = bytesIndex(IndexName("roomIdtoByteArray"), ServerRoomSequence::roomStorage, "toByteArray-v1").also { primaryKey(it) }
}
