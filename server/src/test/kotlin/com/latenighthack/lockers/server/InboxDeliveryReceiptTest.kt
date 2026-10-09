package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.push.v1.*
import com.latenighthack.lockers.server.services.push.v1.PushGatewayDiscovery
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import com.latenighthack.ktcrypto.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.*

class InboxDeliveryReceiptTest {
    private val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionGatewayServer.Descriptor, SessionGatewayServer.Descriptor.methods[0])
    @Test fun lostGatewayReplyAfterAckNeverRefillsInboxOrRepeatsPushAndChangedBytesReject() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(db); val inbox = SessionInboxStoreImpl(db); val pushes = AtomicInteger()
        val service = SessionServiceImpl(sessions, inbox, SimpleMeterRegistry(), object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId) = object : PushGatewayService {
                override suspend fun sendPush(request: SendPushRequest): SendPushResponse { pushes.incrementAndGet(); return SendPushResponse(result = SendPushResponse.Result.OK) }
            }
        }, LocalSessionOwnership(), LockersConfig.defaults())
        val sid = SessionId(byteArrayOf(1)); val event = Event(roomId = RoomId(byteArrayOf(9)), eventId = EventId(byteArrayOf(7)),
            notification = Notification { push { title = "Immutable" }; payload { rawValue = byteArrayOf(8) } })
        val request = PostEventRequest(listOf(sid), event)
        try {
            sessions.updateSession(ServerSession(ServerSessionId(sid.rawValue)))
            assertTrue(service.postEvent(context, request).result.isOk())
            withTimeout(5000) { while (pushes.get() < 1) delay(1) }
            inbox.deleteEvent(ServerEventId(event.eventId!!.rawValue), ServerSessionId(sid.rawValue))
            assertTrue(service.postEvent(context, request).result.isOk())
            assertTrue(inbox.getAllEvents(ServerSessionId(sid.rawValue)).isEmpty())
            assertEquals(1, pushes.get())
            val changed = request.copy(event = event.copy(notification = Notification { payload { rawValue = byteArrayOf(9) } }))
            assertEquals(PostEventResponse.Result.INVALID, service.postEvent(context, changed).result)
            assertTrue(inbox.getAllEvents(ServerSessionId(sid.rawValue)).isEmpty())
        } finally { service.close(); db.close() }
    }
    @Test fun retryAtNewGatewayWakesPendingInboxWithoutRepeatingDelivery() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(db); val inbox = SessionInboxStoreImpl(db)
        val discovery = object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }
        val remoteRegistry = object : SessionRegistry {
            override fun attach(sessionId: ServerSessionId) {}
            override fun detach(sessionId: ServerSessionId) {}
            override suspend fun remoteSessions(sessionIds: List<SessionId>) = sessionIds.toSet()
        }
        val old = SessionServiceImpl(sessions, inbox, SimpleMeterRegistry(), discovery, LocalSessionOwnership(), LockersConfig.defaults(), sessionRegistry = remoteRegistry)
        val current = SessionServiceImpl(sessions, inbox, SimpleMeterRegistry(), discovery, LocalSessionOwnership(), LockersConfig.defaults())
        val sid = SessionId(byteArrayOf(3)); val pair = Secp256r1KeyPair.generate(); val nonce = ByteArray(32) { 4 }
        sessions.updateSession(ServerSession(ServerSessionId(sid.rawValue), nonce, pair.publicKey.encode()))
        val responses = Channel<WatchSessionResponse>(Channel.UNLIMITED)
        val signed = pair.privateKey.sign(nonce)
        val watcher = launch {
            current.watchSession(context, flow {
                emit(WatchSessionRequest { request.open { sessionId = sid; sequenceKeySignature { signature = signed } } })
                awaitCancellation()
            }).collect { if (it is StreamControlEvent.Message) responses.send(it.message) }
        }
        try {
            withTimeout(5000) { assertTrue(responses.receive().response!!.getOpen()!!.queuedEvents.isEmpty()) }
            val event = Event(roomId = RoomId(byteArrayOf(9)), eventId = EventId(byteArrayOf(11)), notification = Notification { payload { rawValue = byteArrayOf(5) } })
            val request = PostEventRequest(listOf(sid), event)
            assertEquals(PostEventResponse.Result.RETRY_ROUTING, old.postEvent(context, request).result)
            assertTrue(current.postEvent(context, request).result.isOk())
            withTimeout(5000) { assertEquals(event, responses.receive().response!!.getEvents()!!.event.single()) }
            assertTrue(current.postEvent(context, request).result.isOk())
            assertNull(withTimeoutOrNull(100) { responses.receive() })
            inbox.deleteEvent(ServerEventId(event.eventId!!.rawValue), ServerSessionId(sid.rawValue))
            assertTrue(current.postEvent(context, request).result.isOk())
            assertNull(withTimeoutOrNull(100) { responses.receive() })
        } finally { watcher.cancelAndJoin(); old.close(); current.close(); db.close() }
    }

}
