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

class SessionBoundaryValidationTest {
    private val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionServer.Descriptor, SessionServer.Descriptor.methods[0])
    private suspend fun firstOpen(service: SessionServiceImpl, request: WatchSessionRequest) = withTimeout(5000) {
        (service.watchSession(context, flow { emit(request); awaitCancellation() }).first { it is StreamControlEvent.Message }
            as StreamControlEvent.Message).message.response!!.getOpen()!!
    }
    @Test fun malformedSessionKeyAndIdentityAreRejectedBeforePersistence() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(db)
        val service = SessionServiceImpl(sessions, SessionInboxStoreImpl(db), SimpleMeterRegistry(), object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults())
        try {
            for (key in listOf(byteArrayOf(), ByteArray(33), byteArrayOf(3) + ByteArray(32) { -1 })) {
                val sid = SessionId(byteArrayOf(1))
                val result = firstOpen(service, WatchSessionRequest { request.create { sessionId = sid; publicKey { rawValue = key } } })
                assertEquals(WatchSessionResponse.Open.Result.INVALID_PUBLIC_KEY, result.result)
                assertNull(sessions.getSessionById(ServerSessionId(sid.rawValue)))
            }
            val key = Secp256r1KeyPair.generate().publicKey.encode()
            for (raw in listOf(byteArrayOf(), ByteArray(129))) {
                val result = firstOpen(service, WatchSessionRequest { request.create { sessionId = SessionId(raw); publicKey { rawValue = key } } })
                assertEquals(WatchSessionResponse.Open.Result.INVALID_SESSION_ID, result.result)
            }
        } finally { service.close(); db.close() }
    }
    @Test fun corruptStoredSessionKeyProducesAnExplicitOpenFailure() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(db); val sid = SessionId(byteArrayOf(7))
        val service = SessionServiceImpl(sessions, SessionInboxStoreImpl(db), SimpleMeterRegistry(), object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults())
        try {
            sessions.updateSession(ServerSession(ServerSessionId(sid.rawValue), ByteArray(32), byteArrayOf()))
            val result = firstOpen(service, WatchSessionRequest { request.open { sessionId = sid; sequenceKeySignature { signature = byteArrayOf(1) } } })
            assertEquals(WatchSessionResponse.Open.Result.INVALID_PUBLIC_KEY, result.result)
        } finally { service.close(); db.close() }
    }
}
