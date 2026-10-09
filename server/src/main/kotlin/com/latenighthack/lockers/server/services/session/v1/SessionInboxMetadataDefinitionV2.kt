package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

private val ServerSessionEvent.metadataSessionKey: ByteArray get() = requireNotNull(sessionId).toByteArray()
private val ServerSessionEvent.metadataEventKey: ByteArray get() = requireNotNull(eventId).toByteArray()

/** Additive sidecar: encodedPayload contains the entire Event protobuf, including unknown fields. */
object SessionInboxMetadataDefinitionV2 : StoreDefinition<ServerSessionEvent>(
    StoreName("inbox_metadata_v2"), "ServerSessionEvent-full-Event-protobuf-v2",
    ServerSessionEvent.Companion::fromByteArray, ServerSessionEvent::toByteArray,
) {
    val sessionIdKey = bytesIndex(IndexName("sessionId"), ServerSessionEvent::metadataSessionKey, "protobuf-session-v1")
    val eventIdKey = bytesIndex(IndexName("eventId"), ServerSessionEvent::metadataEventKey, "protobuf-event-v1")
    val primary = compositeIndex(IndexName("session_event"), sessionIdKey, eventIdKey).also { primaryKey(it) }
}
