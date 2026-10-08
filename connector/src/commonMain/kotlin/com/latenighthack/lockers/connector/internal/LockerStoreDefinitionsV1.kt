package com.latenighthack.lockers.connector.internal
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.connector.byteArrayIdentity
import com.latenighthack.lockers.connector.storage.v1.StoredLocker
import com.latenighthack.lockers.connector.storage.v1.fromByteArray
import com.latenighthack.lockers.connector.storage.v1.toByteArray

private val StoredLocker.roomIdKeyStorage: ByteArray get() = byteArrayIdentity(requireNotNull(roomIdRawValue))
private val StoredLocker.lockerIdKeyStorage: ByteArray get() = byteArrayIdentity(requireNotNull(lockerIdRawValue))

object LockerStoreImplDefinitionV1 : StoreDefinition<StoredLocker>(
    StoreName("lockers"), "StoredLocker-protobuf-v1", StoredLocker.Companion::fromByteArray, StoredLocker::toByteArray,
) {
    val roomIdKey = bytesIndex(IndexName("roomIdRawValuebyteArrayIdentity"), StoredLocker::roomIdKeyStorage, "byteArrayIdentity-v1")
    val lockerIdKey = bytesIndex(IndexName("lockerIdRawValuebyteArrayIdentity"), StoredLocker::lockerIdKeyStorage, "byteArrayIdentity-v1")
    val lockerKeyspaceKey = longIndex(IndexName("lockerKeyspace"), StoredLocker::lockerKeyspace)
    val roomIdLockerKeyspaceKey = compositeIndex(IndexName("composite_roomIdRawValuebyteArrayIdentity_lockerKeyspace"), roomIdKey, lockerKeyspaceKey)
    val roomIdLockerIdLockerKeyspaceKey = compositeIndex(IndexName("composite_roomIdRawValuebyteArrayIdentity_lockerIdRawValuebyteArrayIdentity_lockerKeyspace"), roomIdKey, lockerIdKey, lockerKeyspaceKey).also { primaryKey(it) }
}
