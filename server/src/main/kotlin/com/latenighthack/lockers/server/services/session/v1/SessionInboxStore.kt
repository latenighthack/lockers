package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.BoundStoreKey
import com.latenighthack.ktstore.Store
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.server.storage.v1.*

interface SessionInboxStore {
    suspend fun saveEvents(events: List<ServerSessionEvent>) { events.forEach { saveEvent(it) } }
    suspend fun deleteEvents(eventIds: List<ServerEventId>, sessionId: ServerSessionId) { eventIds.forEach { deleteEvent(it, sessionId) } }
    suspend fun saveEvent(event: ServerSessionEvent)

    suspend fun getAllEvents(sessionId: ServerSessionId): List<ServerSessionEvent>

    suspend fun deleteEvent(eventId: ServerEventId, sessionId: ServerSessionId)
}

class SessionInboxStoreImpl(delegate: StoreDelegate): SessionInboxStore, Store<ServerSessionEvent>(
    delegate,
    "inbox",
    ServerSessionEvent::toByteArray,
    ServerSessionEvent.Companion::fromByteArray
) {
    private val sessionIdKey = serializedIndex(ServerSessionEvent::sessionId, ServerSessionId::toByteArray)
    private val eventIdKey = serializedIndex(ServerSessionEvent::eventId, ServerEventId::toByteArray)
    private val sessionIdEventIdKey = compositeIndex(sessionIdKey, eventIdKey).also { primaryKey(it) }

    override suspend fun saveEvent(event: ServerSessionEvent) = save(event)
    override suspend fun saveEvents(events: List<ServerSessionEvent>) = saveAll(events)
    override suspend fun deleteEvents(eventIds: List<ServerEventId>, sessionId: ServerSessionId) = deleteMany(eventIds.map {
        sessionIdEventIdKey.eq(listOf(
            BoundStoreKey.SerializedKey(sessionIdKey.name, sessionId.toByteArray()),
            BoundStoreKey.SerializedKey(eventIdKey.name, it.toByteArray())
        ))
    })

    override suspend fun getAllEvents(sessionId: ServerSessionId) = getAll(sessionIdKey.eq(sessionId.toByteArray()))

    override suspend fun deleteEvent(eventId: ServerEventId, sessionId: ServerSessionId) = delete(sessionIdEventIdKey.eq(
        listOf(
            BoundStoreKey.SerializedKey(sessionIdKey.name, sessionId.toByteArray()),
            BoundStoreKey.SerializedKey(eventIdKey.name, eventId.toByteArray()),
        )
    ))
}
