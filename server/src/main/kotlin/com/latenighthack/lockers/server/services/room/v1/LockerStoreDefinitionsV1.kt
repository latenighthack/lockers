package com.latenighthack.lockers.server.services.room.v1
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

private val ServerLocker.roomIdKeyStorage: ByteArray get() = requireNotNull(roomId).toByteArray()
private val ServerLocker.lockerIdKeyStorage: ByteArray get() = requireNotNull(lockerId).toByteArray()

object LockerStoreImplDefinitionV1 : StoreDefinition<ServerLocker>(
    StoreName("lockers"), "ServerLocker-protobuf-v1", ServerLocker.Companion::fromByteArray, ServerLocker::toByteArray,
) {
    val roomIdKey = bytesIndex(IndexName("roomIdtoByteArray"), ServerLocker::roomIdKeyStorage, "toByteArray-v1")
    val lockerIdKey = bytesIndex(IndexName("lockerIdtoByteArray"), ServerLocker::lockerIdKeyStorage, "toByteArray-v1")
    val keyspaceKey = longIndex(IndexName("keyspace"), ServerLocker::keyspace)
    val roomIdAndKeyspace = compositeIndex(IndexName("composite_roomIdtoByteArray_keyspace"), roomIdKey, keyspaceKey)
    val roomIdAndKeyspaceAndLockerIdKey = compositeIndex(IndexName("composite_roomIdtoByteArray_keyspace_lockerIdtoByteArray"), roomIdKey, keyspaceKey, lockerIdKey).also { primaryKey(it) }
}
