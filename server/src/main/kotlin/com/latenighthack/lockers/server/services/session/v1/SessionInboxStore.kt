package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

interface SessionInboxStore {
    suspend fun saveEvents(events: List<ServerSessionEvent>) { events.forEach { saveEvent(it) } }
    suspend fun deleteEvents(eventIds: List<ServerEventId>, sessionId: ServerSessionId) { eventIds.forEach { deleteEvent(it, sessionId) } }
    suspend fun saveEvent(event: ServerSessionEvent)

    suspend fun getAllEvents(sessionId: ServerSessionId): List<ServerSessionEvent>

    suspend fun deleteEvent(eventId: ServerEventId, sessionId: ServerSessionId)
}

class SessionInboxStoreImpl(delegate: Database): SessionInboxStore, Store<ServerSessionEvent>(delegate, SessionInboxStoreImplDefinitionV1) {
    private val sessionIdKey = SessionInboxStoreImplDefinitionV1.sessionIdKey
    private val eventIdKey = SessionInboxStoreImplDefinitionV1.eventIdKey
    private val sessionIdEventIdKey = SessionInboxStoreImplDefinitionV1.sessionIdEventIdKey

    override suspend fun saveEvent(event: ServerSessionEvent) = save(event)
    override suspend fun saveEvents(events: List<ServerSessionEvent>) = saveAll(events)
    override suspend fun deleteEvents(eventIds: List<ServerEventId>, sessionId: ServerSessionId) = deleteMany(eventIds.map {
        sessionIdEventIdKey.eq(listOf(
            BoundStoreKey.SerializedKey(sessionIdKey.name.value, sessionId.toByteArray()),
            BoundStoreKey.SerializedKey(eventIdKey.name.value, it.toByteArray())
        ))
    })

    override suspend fun getAllEvents(sessionId: ServerSessionId) = getAll(sessionIdKey.eq(sessionId.toByteArray()))

    override suspend fun deleteEvent(eventId: ServerEventId, sessionId: ServerSessionId) = delete(sessionIdEventIdKey.eq(
        listOf(
            BoundStoreKey.SerializedKey(sessionIdKey.name.value, sessionId.toByteArray()),
            BoundStoreKey.SerializedKey(eventIdKey.name.value, eventId.toByteArray()),
        )
    ))
}
