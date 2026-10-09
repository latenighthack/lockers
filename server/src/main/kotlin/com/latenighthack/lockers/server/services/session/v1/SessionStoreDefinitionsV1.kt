package com.latenighthack.lockers.server.services.session.v1
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

private val ServerSession.sessionIdKeyStorage: ByteArray get() = requireNotNull(sessionId).toByteArray()

object SessionStoreImplDefinitionV1 : StoreDefinition<ServerSession>(
    StoreName("sessions"), "ServerSession-protobuf-v1", ServerSession.Companion::fromByteArray, ServerSession::toByteArray,
) {
    val sessionIdKey = bytesIndex(IndexName("sessionIdtoByteArray"), ServerSession::sessionIdKeyStorage, "toByteArray-v1").also { primaryKey(it) }
}
