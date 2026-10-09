package com.latenighthack.lockers.server.services.push.v1
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

private val ServerDeadLetter.pushIdKeyStorage: ByteArray get() = requireNotNull(pushId).toByteArray()

object PushDeadLetterStoreImplDefinitionV1 : StoreDefinition<ServerDeadLetter>(
    StoreName("push_deadletter"), "ServerDeadLetter-protobuf-v1", ServerDeadLetter.Companion::fromByteArray, ServerDeadLetter::toByteArray,
) {
    val pushIdKey = bytesIndex(IndexName("pushIdtoByteArray"), ServerDeadLetter::pushIdKeyStorage, "toByteArray-v1").also { primaryKey(it) }
}
