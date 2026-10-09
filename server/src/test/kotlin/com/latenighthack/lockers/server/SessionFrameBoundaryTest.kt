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
import kotlinx.coroutines.flow.*
import kotlin.test.*

class SessionFrameBoundaryTest {
    @Test fun oversizedFirstFrameDoesNotReserveASession(): Unit = runBlocking {
        fixture { sessions, _, service, context ->
            val public = Secp256r1KeyPair.generate().publicKey.encode()
            val frame = WatchSessionRequest { request.create { sessionId = sid; publicKey { rawValue = public } } }
                .copy(unknownFields = ByteArray(9000) { when (it % 3) { 0 -> 0x98.toByte(); 1 -> 6; else -> 1 } })
            val response = withTimeout(1000) { service.watchSession(context, flow { emit(frame); awaitCancellation() }).first { it is StreamControlEvent.Message } } as StreamControlEvent.Message
            assertEquals(WatchSessionResponse.Open.Result.INVALID_REQUEST, response.message.response!!.getOpen()!!.result)
            assertNull(sessions.getSessionById(ServerSessionId(sid.rawValue)))
        }
    }
    @Test fun oversizedAckIsRejectedBeforeErasingInboxData(): Unit = runBlocking {
        fixture { sessions, inbox, service, context ->
            val key = Secp256r1KeyPair.generate()
            val challenge = byteArrayOf(1, 2, 3)
            sessions.updateSession(ServerSession(ServerSessionId(sid.rawValue), challenge, key.publicKey.encode()))
            val event = Event(roomId = room, eventId = EventId(byteArrayOf(3)))
            inbox.saveEvent(ServerSessionEvent(ServerSessionId(sid.rawValue), ServerRoomId(room.rawValue), ServerEventId(byteArrayOf(3)), event.toByteArray()))
            val signedChallenge = key.privateKey.sign(challenge)
            val open = WatchSessionRequest { request.open { sessionId = sid; sequenceKeySignature { signature = signedChallenge } } }
            val ack = WatchSessionRequest { request.ack { acks = List(257) { EventAck(roomId = room, eventId = event.eventId) } } }
            val failure = assertFailsWith<RpcResponseException> {
                withTimeout(1000) { service.watchSession(context, flow { emit(open); emit(ack); awaitCancellation() }).first {
                    it is StreamControlEvent.Message && it.message.response?.getAck() != null
                } }
            }
            assertEquals(com.latenighthack.ktbuf.proto.Codes.INVALID_ARGUMENT, failure.code)
            assertEquals(1, inbox.getAllEvents(ServerSessionId(sid.rawValue)).size)
        }
    }
    private val sid = SessionId(byteArrayOf(1))
    private val room = RoomId(byteArrayOf(2))
    private suspend fun fixture(block: suspend (SessionStoreImpl, SessionInboxStoreImpl, SessionServiceImpl, GrpcRequestContext) -> Unit) {
        val db = ServerStorage.inMemory()
        val sessions = SessionStoreImpl(db).also { it.prepare() }
        val inbox = SessionInboxStoreImpl(db).also { it.prepare() }
        db.open()
        val service = SessionServiceImpl(sessions, inbox, SimpleMeterRegistry(), object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults())
        val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionServer.Descriptor, SessionServer.Descriptor.methods[0])
        try { block(sessions, inbox, service, context) }
        finally { service.closeAndJoin(); db.close() }
    }
}
