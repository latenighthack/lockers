package com.latenighthack.lockers.connector
import com.latenighthack.ktstore.*
import com.diamondedge.logging.KmLog
import com.diamondedge.logging.logging
import com.latenighthack.ktbuf.bytes.toBase64String
import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.rpc.repeatWithBackoff
import com.latenighthack.ktcrypto.Secp256r1KeyPair
import com.latenighthack.ktcrypto.encode
import com.latenighthack.lockers.session.v1.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.random.Random
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.internal.ShardedRoomServiceRpc
import com.latenighthack.lockers.connector.internal.ShardedSessionServiceRpc
import com.latenighthack.lockers.connector.storage.v1.*
import com.latenighthack.lockers.room.v1.*

private val StoredAck.roomIdKeyStorage: ByteArray get() = byteArrayIdentity(requireNotNull(roomIdRawValue))
private val StoredAck.eventIdKeyStorage: ByteArray get() = byteArrayIdentity(requireNotNull(eventIdRawValue))

object SessionStoreImplDefinitionV1 : StoreDefinition<StoredAck>(
    StoreName("acks"), "StoredAck-protobuf-v1", StoredAck::fromByteArray, StoredAck::toByteArray,
) {
    val roomIdKey = bytesIndex(IndexName("roomIdRawValuebyteArrayIdentity"), StoredAck::roomIdKeyStorage, "byteArrayIdentity-v1")
    val eventIdKey = bytesIndex(IndexName("eventIdRawValuebyteArrayIdentity"), StoredAck::eventIdKeyStorage, "byteArrayIdentity-v1")
    val roomIdEventIdKey = compositeIndex(IndexName("composite_roomIdRawValuebyteArrayIdentity_eventIdRawValuebyteArrayIdentity"), roomIdKey, eventIdKey).also { primaryKey(it) }
}


private val StoredSubscription.roomIdKeyStorage: ByteArray get() = byteArrayIdentity(requireNotNull(roomIdRawValue))

object SubscriptionStoreImplDefinitionV1 : StoreDefinition<StoredSubscription>(
    StoreName("subscriptions"), "StoredSubscription-protobuf-v1", StoredSubscription.Companion::fromByteArray, StoredSubscription::toByteArray,
) {
    val roomIdKey = bytesIndex(IndexName("roomIdRawValuebyteArrayIdentity"), StoredSubscription::roomIdKeyStorage, "byteArrayIdentity-v1").also { primaryKey(it) }
}
