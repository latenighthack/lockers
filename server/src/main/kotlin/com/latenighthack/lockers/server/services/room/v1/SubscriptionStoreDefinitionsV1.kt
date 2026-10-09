package com.latenighthack.lockers.server.services.room.v1
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

private val ServerSubscription.sessionIdKeyStorage: ByteArray get() = requireNotNull(sessionId).toByteArray()
private val ServerSubscription.roomIdKeyStorage: ByteArray get() = requireNotNull(roomId).toByteArray()

object SubscriptionStoreImplDefinitionV1 : StoreDefinition<ServerSubscription>(
    StoreName("subscriptions"), "ServerSubscription-protobuf-v1", ServerSubscription.Companion::fromByteArray, ServerSubscription::toByteArray,
) {
    val sessionIdKey = bytesIndex(IndexName("sessionIdtoByteArray"), ServerSubscription::sessionIdKeyStorage, "toByteArray-v1")
    val roomIdKey = bytesIndex(IndexName("roomIdtoByteArray"), ServerSubscription::roomIdKeyStorage, "toByteArray-v1")
    val sessionIdAndRoomIdKey = compositeIndex(IndexName("composite_sessionIdtoByteArray_roomIdtoByteArray"), sessionIdKey, roomIdKey).also { primaryKey(it) }
}
