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
    @Test fun `ending a watch collection joins upstream producer cleanup`(): Unit = runBlocking {
        val database = ServerStorage.inMemory()
        val sessions = SessionStoreImpl(database).also { it.prepare() }
        val inbox = SessionInboxStoreImpl(database).also { it.prepare() }
        database.open()
        val service = SessionServiceImpl(sessions, inbox, SimpleMeterRegistry(), object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults())
        val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionServer.Descriptor, SessionServer.Descriptor.methods[0])
        val public = Secp256r1KeyPair.generate().publicKey.encode()
        val cleaning = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val collection = async {
            service.watchSession(context, flow {
                try {
                    emit(WatchSessionRequest { request.create { sessionId = SessionId(byteArrayOf(5)); publicKey { rawValue = public } } })
                    awaitCancellation()
                } finally { withContext(NonCancellable) { cleaning.complete(Unit); release.await() } }
            }).first { it is StreamControlEvent.Message }
        }
        try {
            withTimeout(1000) { cleaning.await() }
            delay(25)
            assertFalse(collection.isCompleted, "The collector must join the producer's cleanup")
            release.complete(Unit)
            withTimeout(1000) { collection.await() }
        } finally { release.complete(Unit); collection.cancelAndJoin(); service.closeAndJoin(); database.close() }
    }
    @Test fun `failed routing attachment releases stream registration and permits reconnect`() = runBlocking {
        val database = ServerStorage.inMemory()
        val sessions = SessionStoreImpl(database).also { it.prepare() }
        val inbox = SessionInboxStoreImpl(database).also { it.prepare() }
        database.open()
        val meters = SimpleMeterRegistry()
        val uncaught = java.util.concurrent.CopyOnWriteArrayList<Throwable>()
        var attachCount = 0
        val registry = object : SessionRegistry {
            override suspend fun attachBeforeSnapshot(sessionId: ServerSessionId) { if (++attachCount == 1) error("registry unavailable") }
            override fun attach(sessionId: ServerSessionId) {}
            override fun detach(sessionId: ServerSessionId) {}
        }
        val service = SessionServiceImpl(sessions, inbox, meters, object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults(), registry,
            coroutineContext = CoroutineExceptionHandler { _, failure -> uncaught.add(failure) })
        val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionServer.Descriptor, SessionServer.Descriptor.methods[0])
        val pair = Secp256r1KeyPair.generate()
        val public = pair.publicKey.encode()
        val sid = SessionId(byteArrayOf(1))
        suspend fun collect(request: WatchSessionRequest) = withTimeout(1000) {
            service.watchSession(context, flow { emit(request); awaitCancellation() }).first { it is StreamControlEvent.Message }
        }
        try {
            assertFailsWith<IllegalStateException> { collect(WatchSessionRequest { request.create { sessionId = sid; publicKey { rawValue = public } } }) }
            assertTrue(uncaught.isEmpty(), "Stream failure must reach its collector without escaping to the host exception handler")
            assertEquals(0.0, meters.get("lockers.session.streams.active").gauge().value())
            val challenge = sessions.getSessionById(ServerSessionId(sid.rawValue))!!.nextKeyMaterial
            val signed = pair.privateKey.sign(challenge)
            val response = collect(WatchSessionRequest { request.open { sessionId = sid; sequenceKeySignature { signature = signed } } }) as StreamControlEvent.Message
            assertIs<WatchSessionResponse.Open.Result.OK>(response.message.response!!.getOpen()!!.result)
            assertEquals(0.0, meters.get("lockers.session.streams.active").gauge().value())
        } finally { service.closeAndJoin(); database.close() }
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
                        .toList().also { frames -> assertTrue(frames.last() is StreamControlEvent.Close) }
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
