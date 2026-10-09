package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.session.v1.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SessionHeartbeatTest {
    @Test fun `ping emits pong after initial open snapshot`(): Unit = runBlocking {
        val core = ServerCore::class.create(LockersConfig.defaults(), ServerStorage.inMemory())
        core.setup()
        val component = MonolithComponent(core)
        val key = Secp256r1KeyPair.generate().publicKey.encode()
        val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionServer.Descriptor, SessionServer.Descriptor.methods[0])
        try {
            val responses = withTimeout(1000) {
                component.sessionServiceModule.serverImpl.watchSession(context, flow {
                    emit(WatchSessionRequest { request.create { sessionId { rawValue = byteArrayOf(11) }; publicKey { rawValue = key } } })
                    emit(WatchSessionRequest { request.ping {} })
                    awaitCancellation()
                }).filterIsInstance<StreamControlEvent.Message<WatchSessionResponse>>().map { it.message }.take(2).toList()
            }
            assertNotNull(responses.first().response?.getOpen())
            assertNotNull(responses.last().response?.getPong())
        } finally { component.stop() }
    }
}
