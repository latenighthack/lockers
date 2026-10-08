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

class SessionAttachFailureTest {
    @Test fun `failed routing attachment releases stream registration and permits reconnect`() = runBlocking {
        val database = ServerStorage.inMemory()
        val sessions = SessionStoreImpl(database).also { it.prepare() }
        val inbox = SessionInboxStoreImpl(database).also { it.prepare() }
        database.open()
        val meters = SimpleMeterRegistry()
        var attachCount = 0
        val registry = object : SessionRegistry {
            override suspend fun attachBeforeSnapshot(sessionId: ServerSessionId) { if (++attachCount == 1) error("registry unavailable") }
            override fun attach(sessionId: ServerSessionId) {}
            override fun detach(sessionId: ServerSessionId) {}
        }
        val service = SessionServiceImpl(sessions, inbox, meters, object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults(), registry)
        val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionServer.Descriptor, SessionServer.Descriptor.methods[0])
        val pair = Secp256r1KeyPair.generate()
        val public = pair.publicKey.encode()
        val sid = SessionId(byteArrayOf(1))
        suspend fun collect(request: WatchSessionRequest) = withTimeout(1000) {
            service.watchSession(context, flow { emit(request); awaitCancellation() }).first { it is StreamControlEvent.Message }
        }
        try {
            assertFailsWith<IllegalStateException> { collect(WatchSessionRequest { request.create { sessionId = sid; publicKey { rawValue = public } } }) }
            assertEquals(0.0, meters.get("lockers.session.streams.active").gauge().value())
            val challenge = sessions.getSessionById(ServerSessionId(sid.rawValue))!!.nextKeyMaterial
            val signed = pair.privateKey.sign(challenge)
            val response = collect(WatchSessionRequest { request.open { sessionId = sid; sequenceKeySignature { signature = signed } } }) as StreamControlEvent.Message
            assertIs<WatchSessionResponse.Open.Result.OK>(response.message.response!!.getOpen()!!.result)
            assertEquals(0.0, meters.get("lockers.session.streams.active").gauge().value())
        } finally { service.close() }
    }
    @Test fun `shared store revocation closes a stream on another service instance`() = runBlocking {
        val database = ServerStorage.inMemory()
        val sessions = SessionStoreImpl(database).also { it.prepare() }
        val inbox = SessionInboxStoreImpl(database).also { it.prepare() }
        database.open()
        val service = SessionServiceImpl(sessions, inbox, SimpleMeterRegistry(), object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults())
        val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionServer.Descriptor, SessionServer.Descriptor.methods[0])
        val sid = SessionId(byteArrayOf(2))
        val public = Secp256r1KeyPair.generate().publicKey.encode()
        val opened = CompletableDeferred<Unit>()
        try {
            val closed = async {
                withTimeout(2500) {
                    service.watchSession(context, flow {
                        emit(WatchSessionRequest { request.create { sessionId = sid; publicKey { rawValue = public } } })
                        awaitCancellation()
                    }).onEach { if (it is StreamControlEvent.Message) opened.complete(Unit) }
                        .first { it is StreamControlEvent.Close }
                }
            }
            opened.await()
            // A distinct instance using the shared database destroys it without touching local maps.
            SessionStoreImpl(database).destroySession(ServerSessionId(sid.rawValue))
            closed.await()
            Unit
        } finally { service.close() }
    }

}
