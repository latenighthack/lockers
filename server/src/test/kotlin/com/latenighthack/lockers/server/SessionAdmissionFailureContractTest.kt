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

class SessionAdmissionFailureContractTest {
    @Test fun activeCapacityRecoversButReservedNamespaceReportsPermanentFailure() = runBlocking {
        val db = ServerStorage.inMemory(); db.open(); val sessions = SessionStoreImpl(db)
        val service = SessionServiceImpl(sessions, SessionInboxStoreImpl(db), SimpleMeterRegistry(), object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults().copy(resourceLimits = ServerResourceLimits(maxSessions = 1, maxReservedSessionIds = 2)))
        val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionServer.Descriptor, SessionServer.Descriptor.methods[0])
        val public = Secp256r1KeyPair.generate().publicKey.encode()
        suspend fun create(n: Byte) = withTimeout(5000) {
            (service.watchSession(context, flow {
                emit(WatchSessionRequest { request.create { sessionId = SessionId(byteArrayOf(n)); publicKey { rawValue = public } } })
                awaitCancellation()
            }).first { it is StreamControlEvent.Message } as StreamControlEvent.Message).message.response!!.getOpen()!!.result.value
        }
        try {
            assertEquals(0, create(1)); assertEquals(9, create(2))
            sessions.destroySession(ServerSessionId(byteArrayOf(1)))
            assertEquals(0, create(2))
            sessions.destroySession(ServerSessionId(byteArrayOf(2)))
            assertEquals(12, create(3))
            assertTrue(sessions.getAllSessions().isEmpty())
        } finally { service.close(); db.close() }
    }
}
