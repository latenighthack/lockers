package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

internal fun inboxSessionPrefix(session: ByteArray): ByteArray = byteArrayOf(session.size.toByte()) + session
internal fun inboxReplayKey(row: ServerSessionEvent): ByteArray {
    val sequence = row.roomSequence xor Long.MIN_VALUE
    return inboxSessionPrefix(requireNotNull(row.sessionId).rawValue) + ByteArray(8) { (sequence ushr (56 - it * 8)).toByte() } + requireNotNull(row.eventId).rawValue
}
private val ServerSessionEvent.replayKey: ByteArray get() = inboxReplayKey(this)
private val ServerSessionEvent.sessionStorage: ByteArray get() = requireNotNull(sessionId).toByteArray()
private val ServerSessionEvent.eventStorage: ByteArray get() = requireNotNull(eventId).toByteArray()

/** V1 payload bytes are retained verbatim; only the current indexed layout changes. */
object SessionInboxStoreDefinitionV2 : StoreDefinition<ServerSessionEvent>(
    StoreName("inbox"), "ServerSessionEvent-protobuf-v1", ServerSessionEvent.Companion::fromByteArray, ServerSessionEvent::toByteArray,
) {
    val sessionIdKey = bytesIndex(IndexName("sessionIdtoByteArray"), ServerSessionEvent::sessionStorage, "toByteArray-v1")
    val eventIdKey = bytesIndex(IndexName("eventIdtoByteArray"), ServerSessionEvent::eventStorage, "toByteArray-v1")
    val sessionIdEventIdKey = compositeIndex(IndexName("composite_sessionIdtoByteArray_eventIdtoByteArray"), sessionIdKey, eventIdKey)
    val replay = bytesIndex(IndexName("replay"), ServerSessionEvent::replayKey, "length-session-sortable-sequence-event-v2").also { primaryKey(it) }
}
