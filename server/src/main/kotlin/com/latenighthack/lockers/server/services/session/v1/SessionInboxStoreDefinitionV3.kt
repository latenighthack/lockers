package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.IndexName
import com.latenighthack.ktstore.StoreDefinition
import com.latenighthack.ktstore.StoreName
import com.latenighthack.lockers.server.storage.v1.ServerSessionEvent
import com.latenighthack.lockers.server.storage.v1.fromByteArray
import com.latenighthack.lockers.server.storage.v1.toByteArray

private val ServerSessionEvent.replayKey: ByteArray get() = inboxReplayKey(this)
private val ServerSessionEvent.sessionStorage: ByteArray get() = requireNotNull(sessionId).toByteArray()
private val ServerSessionEvent.eventStorage: ByteArray get() = requireNotNull(eventId).toByteArray()

/** V1 payload bytes are retained verbatim; only the current indexed layout changes. */
private val ServerSessionEvent.rawSessionStorage: ByteArray get() = requireNotNull(sessionId).rawValue

object SessionInboxStoreDefinitionV3 : StoreDefinition<ServerSessionEvent>(
    StoreName("inbox"),
    "ServerSessionEvent-protobuf-v1",
    ServerSessionEvent.Companion::fromByteArray,
    ServerSessionEvent::toByteArray,
) {
    val sessionIdKey = bytesIndex(
        IndexName("sessionIdtoByteArray"),
        ServerSessionEvent::sessionStorage,
        "toByteArray-v1",
    )
    val eventIdKey = bytesIndex(IndexName("eventIdtoByteArray"), ServerSessionEvent::eventStorage, "toByteArray-v1")
    val sessionIdEventIdKey = compositeIndex(
        IndexName("composite_sessionIdtoByteArray_eventIdtoByteArray"),
        sessionIdKey,
        eventIdKey,
    )
    val replay =
        bytesIndex(
            IndexName("replay"),
            ServerSessionEvent::replayKey,
            "length-session-sortable-sequence-event-v2",
        ).also {
            primaryKey(it)
        }
    val rawSession = bytesIndex(IndexName("sessionRaw"), ServerSessionEvent::rawSessionStorage, "raw-session-v3")
    val enqueued =
        mappedIndex(
            IndexName("enqueued"),
            ServerSessionEvent::enqueuedAt,
            com.latenighthack.lockers.server.tools.EpochMillisCodec,
            "ordered-time-v3",
        )
}
