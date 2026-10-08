package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.common.v1.*

internal fun ServerSessionEvent.legacyClientEvent() = Event {
    eventId { rawValue = this@legacyClientEvent.eventId!!.rawValue }
    roomId { rawValue = this@legacyClientEvent.roomId!!.rawValue }
    roomSequence = this@legacyClientEvent.roomSequence
    if (encodedLocker.isNotEmpty()) locker = IdentifiedLocker.fromByteArray(encodedLocker)
    if (encodedPayload.isNotEmpty()) notification { payload { rawValue = encodedPayload } }
}

interface SessionInboxStore {
    suspend fun saveClientEvents(events: List<Pair<ServerSessionEvent, Event>>) {
        require(events.all { it.second.notification?.push == null }) { "Inbox extension must persist full notification metadata" }
        saveEvents(events.map { it.first })
    }
    suspend fun clientEvent(row: ServerSessionEvent): Event = row.legacyClientEvent()
    suspend fun getAllClientEvents(sessionId: ServerSessionId): List<Event> =
        getAllEvents(sessionId).sortedBy { it.roomSequence }.map { clientEvent(it) }
    suspend fun deleteAllEvents(sessionId: ServerSessionId) {
        deleteEvents(getAllEvents(sessionId).mapNotNull { it.eventId }, sessionId)
    }
    suspend fun saveEvents(events: List<ServerSessionEvent>) { events.forEach { saveEvent(it) } }
    suspend fun deleteEvents(eventIds: List<ServerEventId>, sessionId: ServerSessionId) { eventIds.forEach { deleteEvent(it, sessionId) } }
    suspend fun saveEvent(event: ServerSessionEvent)

    suspend fun getAllEvents(sessionId: ServerSessionId): List<ServerSessionEvent>

    suspend fun deleteEvent(eventId: ServerEventId, sessionId: ServerSessionId)
}

class SessionInboxStoreImpl(private val database: Database): SessionInboxStore, Store<ServerSessionEvent>(database, SessionInboxStoreImplDefinitionV1) {
    private val sessionIdKey = SessionInboxStoreImplDefinitionV1.sessionIdKey
    private val eventIdKey = SessionInboxStoreImplDefinitionV1.eventIdKey
    private val sessionIdEventIdKey = SessionInboxStoreImplDefinitionV1.sessionIdEventIdKey

    private class MetadataStore(database: Database) : Store<ServerSessionEvent>(database, SessionInboxMetadataDefinitionV2) {
        suspend fun saveRows(rows: List<ServerSessionEvent>) = saveAll(rows)
        suspend fun find(relation: StoreRelation) = get(relation)
        suspend fun remove(relation: StoreRelation) = delete(relation)
        suspend fun removeMany(relations: List<StoreRelation>) = deleteMany(relations)
    }
    private val metadata = MetadataStore(database)
    private fun metadataRelation(eventId: ServerEventId, sessionId: ServerSessionId) =
        SessionInboxMetadataDefinitionV2.primary.eq(listOf(
            BoundStoreKey.SerializedKey(SessionInboxMetadataDefinitionV2.sessionIdKey.name.value, sessionId.toByteArray()),
            BoundStoreKey.SerializedKey(SessionInboxMetadataDefinitionV2.eventIdKey.name.value, eventId.toByteArray()),
        ))
    private fun rowRelation(eventId: ServerEventId, sessionId: ServerSessionId) = sessionIdEventIdKey.eq(listOf(
        BoundStoreKey.SerializedKey(sessionIdKey.name.value, sessionId.toByteArray()),
        BoundStoreKey.SerializedKey(eventIdKey.name.value, eventId.toByteArray()),
    ))

    override suspend fun saveClientEvents(events: List<Pair<ServerSessionEvent, Event>>) = database.transaction("inbox-metadata") {
        saveAll(events.map { it.first })
        metadata.saveRows(events.map { (row, event) -> row.copy(encodedPayload = event.toByteArray(), encodedLocker = byteArrayOf()) })
    }
    override suspend fun clientEvent(row: ServerSessionEvent): Event =
        metadata.find(metadataRelation(requireNotNull(row.eventId), requireNotNull(row.sessionId)))
            ?.let { Event.fromByteArray(it.encodedPayload) } ?: row.legacyClientEvent()

    override suspend fun getAllClientEvents(sessionId: ServerSessionId): List<Event> = database.transaction("inbox-metadata") {
        getAllEvents(sessionId).sortedBy { it.roomSequence }.map { clientEvent(it) }
    }
    override suspend fun saveEvent(event: ServerSessionEvent) = saveEvents(listOf(event))
    override suspend fun saveEvents(events: List<ServerSessionEvent>) = database.transaction("inbox-metadata") {
        saveAll(events)
        events.forEach { metadata.remove(metadataRelation(requireNotNull(it.eventId), requireNotNull(it.sessionId))) }
    }
    override suspend fun deleteEvents(eventIds: List<ServerEventId>, sessionId: ServerSessionId) = database.transaction("inbox-metadata") {
        deleteMany(eventIds.map { rowRelation(it, sessionId) })
        metadata.removeMany(eventIds.map { metadataRelation(it, sessionId) })
    }
    override suspend fun deleteAllEvents(sessionId: ServerSessionId) = database.transaction("inbox-metadata") {
        delete(sessionIdKey.eq(sessionId.toByteArray()))
        metadata.remove(SessionInboxMetadataDefinitionV2.sessionIdKey.eq(sessionId.toByteArray()))
    }
    override suspend fun getAllEvents(sessionId: ServerSessionId) = getAll(sessionIdKey.eq(sessionId.toByteArray()))
    override suspend fun deleteEvent(eventId: ServerEventId, sessionId: ServerSessionId) = deleteEvents(listOf(eventId), sessionId)
}
