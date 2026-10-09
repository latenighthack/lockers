package com.latenighthack.lockers.server.services.room.v1
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

private val ServerLock.roomIdKeyStorage: ByteArray get() = requireNotNull(roomId).toByteArray()
private val ServerLock.lockerIdKeyStorage: ByteArray get() = requireNotNull(lockerId).toByteArray()

object LockStoreImplDefinitionV1 : StoreDefinition<ServerLock>(
    StoreName("locks"), "ServerLock-protobuf-v1", ServerLock.Companion::fromByteArray, ServerLock::toByteArray,
) {
    val roomIdKey = bytesIndex(IndexName("roomIdtoByteArray"), ServerLock::roomIdKeyStorage, "toByteArray-v1")
    val scopeKindKey = longIndex(IndexName("scopeKind"), ServerLock::scopeKind)
    val keyspaceKey = longIndex(IndexName("keyspace"), ServerLock::keyspace)
    val lockerIdKey = bytesIndex(IndexName("lockerIdtoByteArray"), ServerLock::lockerIdKeyStorage, "toByteArray-v1")
    val primary = compositeIndex(IndexName("composite_roomIdtoByteArray_scopeKind_keyspace_lockerIdtoByteArray"), roomIdKey, scopeKindKey, keyspaceKey, lockerIdKey).also { primaryKey(it) }
}
