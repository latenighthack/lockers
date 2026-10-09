package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.*
import kotlin.test.*

class ResourceAdmissionTest {
    @Test fun identityAdmissionIsAtomicAndRevocationsConsumePermanentBudget() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val store = SessionStoreImpl(db)
        val limits = ServerResourceLimits(maxSessions = 3, maxReservedSessionIds = 3)
        try {
            val results = coroutineScope { (1..10).map { id -> async {
                store.admitIfAbsent(ServerSession(ServerSessionId(byteArrayOf(id.toByte()))), limits)
            } }.awaitAll() }
            assertEquals(3, results.count { it == SessionAdmission.CREATED })
            assertEquals(7, results.count { it == SessionAdmission.EXHAUSTED })
            val sid = store.getAllSessions().first().sessionId!!
            store.destroySession(sid)
            assertEquals(SessionAdmission.EXISTS, store.admitIfAbsent(ServerSession(sid), limits))
            assertEquals(SessionAdmission.EXHAUSTED, store.admitIfAbsent(ServerSession(ServerSessionId(byteArrayOf(20))), limits))
            assertEquals(2, store.getAllSessions().size)
        } finally { db.close() }
    }
    @Test fun inboxQuotaIsGlobalPerSessionAndIdempotentAndRejectedWritesRollbackMetadata() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val inbox = SessionInboxStoreImpl(db, ServerResourceLimits(maxInboxEvents = 2, maxInboxEventsPerSession = 1))
        fun pair(sid: Byte, id: Byte): Pair<ServerSessionEvent, Event> {
            val row = ServerSessionEvent(sessionId = ServerSessionId(byteArrayOf(sid)), eventId = ServerEventId(byteArrayOf(id)), roomId = ServerRoomId(byteArrayOf(9)))
            return row to Event(eventId = EventId(byteArrayOf(id)), roomId = RoomId(byteArrayOf(9)), notification = Notification { push { title = "Bound" } })
        }
        try {
            val first = pair(1, 1); inbox.saveClientEvents(listOf(first)); inbox.saveClientEvents(listOf(first))
            assertFailsWith<ResourceLimitException> { inbox.saveClientEvents(listOf(pair(1, 2))) }
            inbox.saveClientEvents(listOf(pair(2, 1)))
            assertFailsWith<ResourceLimitException> { inbox.saveClientEvents(listOf(pair(3, 1))) }
            assertEquals(1, inbox.getAllEvents(first.first.sessionId!!).size)
            assertTrue(inbox.getAllEvents(ServerSessionId(byteArrayOf(3))).isEmpty())
            inbox.deleteAllEvents(first.first.sessionId!!)
            inbox.saveClientEvents(listOf(pair(3, 1)))
            assertEquals("Bound", inbox.getAllClientEvents(ServerSessionId(byteArrayOf(3))).single().notification?.push?.title)
        } finally { db.close() }
    }
}
