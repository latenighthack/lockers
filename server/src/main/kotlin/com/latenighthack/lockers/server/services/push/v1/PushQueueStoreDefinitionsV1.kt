package com.latenighthack.lockers.server.services.push.v1
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

private val ServerPush.pushIdKeyStorage: ByteArray get() = requireNotNull(pushId).toByteArray()

object PushQueueStoreImplDefinitionV1 : StoreDefinition<ServerPush>(
    StoreName("push"), "ServerPush-protobuf-v1", ServerPush.Companion::fromByteArray, ServerPush::toByteArray,
) {
    val pushIdKey = bytesIndex(IndexName("pushIdtoByteArray"), ServerPush::pushIdKeyStorage, "toByteArray-v1").also { primaryKey(it) }
}
