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

class SessionCreationFailureTest {
    @Test fun transientCreationFailureDoesNotReportAnIdentityCollision(): Unit = runBlocking {
        val db = ServerStorage.inMemory().also { it.open() }
        val actual = SessionStoreImpl(db)
        val sessions = object : SessionStore by actual {
            override suspend fun admitIfAbsent(session: ServerSession, limits: ServerResourceLimits): SessionAdmission =
                throw java.io.IOException("temporarily unavailable")
        }
        val service = SessionServiceImpl(sessions, SessionInboxStoreImpl(db), SimpleMeterRegistry(), object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults())
        val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionServer.Descriptor, SessionServer.Descriptor.methods[0])
        try {
            val key = Secp256r1KeyPair.generate().publicKey.encode()
            val response = withTimeout(2000) {
                service.watchSession(context, flow { emit(WatchSessionRequest { request.create { sessionId = SessionId(byteArrayOf(1)); publicKey { rawValue = key } } }); awaitCancellation() })
                    .filterIsInstance<StreamControlEvent.Message<WatchSessionResponse>>().first().message.response!!.getOpen()!!
            }
            assertEquals(WatchSessionResponse.Open.Result.UNKNOWN_ERROR, response.result)
            assertTrue(actual.getAllSessions().isEmpty())
        } finally { service.closeAndJoin(); db.close() }
    }
}
