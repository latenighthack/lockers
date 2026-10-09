package com.latenighthack.lockers.server.services.session.v1
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

private val ServerSessionEvent.sessionIdKeyStorage: ByteArray get() = requireNotNull(sessionId).toByteArray()
private val ServerSessionEvent.eventIdKeyStorage: ByteArray get() = requireNotNull(eventId).toByteArray()

object SessionInboxStoreImplDefinitionV1 : StoreDefinition<ServerSessionEvent>(
    StoreName("inbox"), "ServerSessionEvent-protobuf-v1", ServerSessionEvent.Companion::fromByteArray, ServerSessionEvent::toByteArray,
) {
    val sessionIdKey = bytesIndex(IndexName("sessionIdtoByteArray"), ServerSessionEvent::sessionIdKeyStorage, "toByteArray-v1")
    val eventIdKey = bytesIndex(IndexName("eventIdtoByteArray"), ServerSessionEvent::eventIdKeyStorage, "toByteArray-v1")
    val sessionIdEventIdKey = compositeIndex(IndexName("composite_sessionIdtoByteArray_eventIdtoByteArray"), sessionIdKey, eventIdKey).also { primaryKey(it) }
}
