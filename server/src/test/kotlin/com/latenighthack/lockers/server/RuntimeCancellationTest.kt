package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.push.v1.*
import com.latenighthack.lockers.server.services.push.v1.*
import com.latenighthack.lockers.server.services.push.v1.providers.WebPushEndpointPolicy
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.test.*

class RuntimeCancellationTest {
    @Test fun `push registration propagates cancellation`(): Unit = runBlocking {
        val db = ServerStorage.inMemory()
        val queues = PushQueueStoreImpl(db).also { it.prepare() }
        val dead = PushDeadLetterStoreImpl(db).also { it.prepare() }
        db.open()
        val sessions = object : PushSessionStore {
            override suspend fun savePushInfo(pushInfo: ServerPushInfo) {}
            override suspend fun getPushInfo(sessionId: ServerSessionId): ServerPushInfo? = null
            override suspend fun deletePushInfo(sessionId: ServerSessionId) {}
            override suspend fun applyCredential(sessionId: ServerSessionId, backend: Int, encoded: ByteArray?, revision: Long): Boolean = throw CancellationException("cancelled storage")
        }
        val service = PushServiceImpl(sessions, queues, dead, SimpleMeterRegistry(), emptyList())
        try {
            assertFailsWith<CancellationException> {
                LocalPushServiceRpc(service).registerSession(RegisterSessionRequest {
                    sessionId = SessionId(byteArrayOf(1)); registration { backend.apns { deviceToken = byteArrayOf(1) } }
                })
            }
        } finally { service.stop() }
    }

    @Test fun `session creation propagates cancellation`(): Unit = runBlocking {
        val db = ServerStorage.inMemory()
        val inbox = SessionInboxStoreImpl(db).also { it.prepare() }
        db.open()
        val sessions = object : SessionStore {
            override suspend fun getSessionById(sessionId: ServerSessionId): ServerSession? = null
            override suspend fun getAllSessions(): List<ServerSession> = emptyList()
            override suspend fun updateSession(session: ServerSession) {}
            override suspend fun isRevoked(sessionId: ServerSessionId) = false
            override suspend fun destroySession(sessionId: ServerSessionId) {}
            override suspend fun admitIfAbsent(session: ServerSession, limits: ServerResourceLimits): SessionAdmission = throw CancellationException("cancelled storage")
        }
        val service = SessionServiceImpl(sessions, inbox, SimpleMeterRegistry(), object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults())
        val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionServer.Descriptor, SessionServer.Descriptor.methods[0])
        try {
            val public = Secp256r1KeyPair.generate().publicKey.encode()
            val cancelled = assertFailsWith<CancellationException> {
                withTimeout(2000) { service.watchSession(context, flowOf(WatchSessionRequest { request.create { sessionId = SessionId(byteArrayOf(2)); publicKey { rawValue = public } } })).first() }
            }
            assertEquals("cancelled storage", cancelled.message)
        } finally { service.close() }
    }

    @Test fun `endpoint DNS policy propagates cancellation`() {
        assertFailsWith<CancellationException> {
            WebPushEndpointPolicy(setOf("fcm.googleapis.com")) { throw CancellationException("cancelled DNS") }
                .rejection("https://fcm.googleapis.com/send/token")
        }
    }
}
