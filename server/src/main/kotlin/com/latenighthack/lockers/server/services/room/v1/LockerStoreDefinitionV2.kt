package com.latenighthack.lockers.server.services.room.v1
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

internal fun lockerKeyspaceKey(value: Long): ByteArray {
    val sortable = value xor Long.MIN_VALUE
    return ByteArray(8) { (sortable ushr (56 - it * 8)).toByte() }
}
private val ServerLocker.roomIdKeyStorageV2: ByteArray get() = requireNotNull(roomId).toByteArray()
private val ServerLocker.lockerIdKeyStorageV2: ByteArray get() = requireNotNull(lockerId).toByteArray()

object LockerStoreDefinitionV2 : StoreDefinition<ServerLocker>(
    StoreName("lockers"), "ServerLocker-protobuf-v1", ServerLocker.Companion::fromByteArray, ServerLocker::toByteArray,
) {
    val roomIdKey = bytesIndex(IndexName("roomIdtoByteArray"), ServerLocker::roomIdKeyStorageV2, "toByteArray-v1")
    val lockerIdKey = bytesIndex(IndexName("lockerIdtoByteArray"), ServerLocker::lockerIdKeyStorageV2, "toByteArray-v1")
    val keyspaceKey = mappedIndex(IndexName("keyspace"), ServerLocker::keyspace, object : StorageCodec<Long, ByteArray> {
        override fun key(name: IndexName) = StoreKey.SerializedKey(name.value)
        override fun encode(value: Long) = lockerKeyspaceKey(value)
    }, "sortable-long-v2")
    val roomIdAndKeyspace = compositeIndex(IndexName("composite_roomIdtoByteArray_keyspace"), roomIdKey, keyspaceKey)
    val roomIdAndKeyspaceAndLockerIdKey = compositeIndex(IndexName("composite_roomIdtoByteArray_keyspace_lockerIdtoByteArray"), roomIdKey, keyspaceKey, lockerIdKey).also { primaryKey(it) }
}
