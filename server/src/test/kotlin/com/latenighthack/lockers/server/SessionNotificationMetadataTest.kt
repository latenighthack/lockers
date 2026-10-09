package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.push.v1.PushGatewayService
import com.latenighthack.lockers.server.services.push.v1.PushGatewayDiscovery
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.test.*

class SessionNotificationMetadataTest {
    private val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionServer.Descriptor, SessionServer.Descriptor.methods[0])
    @Test fun queuedAndLiveNotificationsKeepAuthenticatedMetadata() = runBlocking {
        val db = ServerStorage.inMemory()
        val sessions = SessionStoreImpl(db).also { it.prepare() }
        val inbox = SessionInboxStoreImpl(db).also { it.prepare() }
        db.open()
        val service = SessionServiceImpl(sessions, inbox, SimpleMeterRegistry(), object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults())
        val sid = SessionId(byteArrayOf(7)); val pair = Secp256r1KeyPair.generate(); val nonce = ByteArray(32) { 4 }
        sessions.updateSession(ServerSession(ServerSessionId(sid.rawValue), nonce, pair.publicKey.encode()))
        fun notification(title: String?, body: String?) = Notification {
            push { title?.let { this.title = it }; body?.let { this.body = it } }
            // Model an authenticated codec: the payload binds the actual serialized push context.
            payload { rawValue = (title.orEmpty() + "\u0000" + body.orEmpty()).encodeToByteArray() }
        }
        fun event(id: Byte, n: Notification) = Event(roomId = RoomId(byteArrayOf(9)), eventId = EventId(byteArrayOf(id)), notification = n)
        val queued = event(1, notification("Title", "Body"))
        val live = event(2, notification(null, "Partial"))
        try {
            service.postEvents(context, PostEventsRequest(listOf(PostEventRequest(listOf(sid), queued))))
            val responses = Channel<WatchSessionResponse>(Channel.UNLIMITED)
            val signed = pair.privateKey.sign(nonce)
            val watcher = launch {
                service.watchSession(context, flow {
                    emit(WatchSessionRequest { request.open { sessionId = sid; sequenceKeySignature { signature = signed } } })
                    awaitCancellation()
                }).collect { if (it is StreamControlEvent.Message) responses.send(it.message) }
            }
            withTimeout(5000) {
                val open = responses.receive().response!!.getOpen()!!
                assertEquals(WatchSessionResponse.Open.Result.OK, open.result)
                assertEquals(queued.notification, open.queuedEvents.single().notification)
                service.postEvent(context, PostEventRequest(listOf(sid), live))
                val delivered = responses.receive().response!!.getEvents()!!.event.single()
                assertEquals(live.notification, delivered.notification)
                assertEquals(live.notification!!.payload!!.rawValue.toList(),
                    (delivered.notification!!.push!!.title + "\u0000" + delivered.notification!!.push!!.body).encodeToByteArray().toList())
            }
            watcher.cancelAndJoin()
        } finally { service.close(); db.close() }
    }
}
