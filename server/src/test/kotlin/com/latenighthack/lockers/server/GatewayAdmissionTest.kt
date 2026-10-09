package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.push.v1.PushGatewayService
import com.latenighthack.lockers.server.services.push.v1.PushGatewayDiscovery
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import kotlin.test.*

class GatewayAdmissionTest {
    private val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionGatewayServer.Descriptor, SessionGatewayServer.Descriptor.methods[0])
    @Test fun completeGatewayBatchExpansionIsRejectedBeforeAnyGroupPersistsAndRawSidAliasesDoNotFanoutTwice() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(db); val inbox = SessionInboxStoreImpl(db)
        val service = SessionServiceImpl(sessions, inbox, SimpleMeterRegistry(), object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults())
        try {
            val sid = SessionId(byteArrayOf(1)); sessions.updateSession(ServerSession(ServerSessionId(sid.rawValue)))
            val event = Event(eventId = EventId(byteArrayOf(1)), roomId = RoomId(byteArrayOf(9)),
                locker = IdentifiedLocker(lockerId = LockerId(byteArrayOf(1)),
                    locker = Locker { open { encodedPayload = ByteArray(1024 * 1024) } }))
            val groups = (0..1).map { group -> PostEventRequest((1..20).map { SessionId(byteArrayOf((it + group * 20).toByte())) },
                event.copy(eventId = EventId(byteArrayOf((group + 1).toByte())))) }
            val failure = assertFailsWith<RpcResponseException> { service.postEvents(context, PostEventsRequest(groups)) }
            assertEquals(com.latenighthack.ktbuf.proto.Codes.OUT_OF_RANGE, failure.code)
            assertTrue(inbox.getAllEvents(ServerSessionId(sid.rawValue)).isEmpty())
            val alias = sid.copy(unknownFields = byteArrayOf(0x78, 0x01))
            assertTrue(service.postEvent(context, PostEventRequest(listOf(sid, alias), event)).result.isOk())
            assertEquals(1, inbox.getAllEvents(ServerSessionId(sid.rawValue)).size)
        } finally { service.close(); db.close() }
    }
    @Test fun rejectedUnknownAndMalformedRecipientsNeverPersistAndRevokedDeliveryIsAcceptedDiscard() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val limits = ServerResourceLimits(maxInboxEvents = 2, maxInboxEventsPerSession = 1)
        val sessions = SessionStoreImpl(db); val inbox = SessionInboxStoreImpl(db, limits)
        val service = SessionServiceImpl(sessions, inbox, SimpleMeterRegistry(), object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults().copy(resourceLimits = limits))
        val sid = SessionId(byteArrayOf(1)); val unknown = SessionId(byteArrayOf(2))
        fun event(id: Byte, oversized: Boolean = false) = Event(eventId = EventId(byteArrayOf(id)), roomId = RoomId(byteArrayOf(9)),
            notification = Notification { push { body = if (oversized) "x".repeat(4097) else "Body" } })
        try {
            sessions.updateSession(ServerSession(ServerSessionId(sid.rawValue)))
            assertEquals(PostEventResponse.Result.UNKNOWN_SESSION, service.postEvent(context, PostEventRequest(listOf(sid, unknown), event(1))).result)
            assertTrue(inbox.getAllEvents(ServerSessionId(sid.rawValue)).isEmpty())
            assertEquals(PostEventResponse.Result.INVALID, service.postEvent(context, PostEventRequest(listOf(sid), event(1, true))).result)
            assertTrue(service.postEvent(context, PostEventRequest(listOf(sid), event(1))).result.isOk())
            assertEquals(PostEventResponse.Result.RESOURCE_EXHAUSTED, service.postEvent(context, PostEventRequest(listOf(sid), event(2))).result)
            sessions.destroySession(ServerSessionId(sid.rawValue))
            assertTrue(service.postEvent(context, PostEventRequest(listOf(sid), event(2))).result.isOk())
            assertTrue(inbox.getAllEvents(ServerSessionId(sid.rawValue)).isEmpty())
            // Deliver and destroy race under the same authority owner; no late insert can escape.
            val raced = SessionId(byteArrayOf(3)); sessions.updateSession(ServerSession(ServerSessionId(raced.rawValue)))
            coroutineScope {
                val delivery = async { service.postEvent(context, PostEventRequest(listOf(raced), event(3))) }
                val destroy = async { sessions.destroySession(ServerSessionId(raced.rawValue)) }
                delivery.await(); destroy.await()
            }
            assertTrue(inbox.getAllEvents(ServerSessionId(raced.rawValue)).isEmpty())
        } finally { service.close(); db.close() }
    }
}
