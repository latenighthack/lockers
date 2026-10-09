package com.latenighthack.lockers.server.services.push.v1
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

private val ServerPushInfo.sessionIdKeyStorage: ByteArray get() = requireNotNull(sessionId).toByteArray()

object PushSessionStoreImplDefinitionV1 : StoreDefinition<ServerPushInfo>(
    StoreName("push_session"), "ServerPushInfo-protobuf-v1", ServerPushInfo.Companion::fromByteArray, ServerPushInfo::toByteArray,
) {
    val sessionIdKey = bytesIndex(IndexName("sessionIdtoByteArray"), ServerPushInfo::sessionIdKeyStorage, "toByteArray-v1").also { primaryKey(it) }
}
