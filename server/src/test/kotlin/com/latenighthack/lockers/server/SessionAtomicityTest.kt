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
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals

class SessionAtomicityTest {
    private val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionServer.Descriptor, SessionServer.Descriptor.methods[0])
    private suspend fun firstOpen(service: SessionServiceImpl, request: WatchSessionRequest): WatchSessionResponse.Open =
        (service.watchSession(context, flow { emit(request); awaitCancellation() }).first { it is StreamControlEvent.Message } as StreamControlEvent.Message).message.response!!.getOpen()!!

    private suspend fun race(seed: ServerSession?, requests: List<WatchSessionRequest>): List<WatchSessionResponse.Open> = coroutineScope {
        val database = ServerStorage.inMemory()
        val actual = SessionStoreImpl(database).also { it.prepare() }
        val inbox = SessionInboxStoreImpl(database).also { it.prepare() }
        database.open()
        seed?.let { actual.updateSession(it) }
        val reads = AtomicInteger()
        val gate = CompletableDeferred<Unit>()
        val rendezvous = object : SessionStore by actual {
            override suspend fun getSessionById(sessionId: ServerSessionId): ServerSession? {
                val row = actual.getSessionById(sessionId)
                if (reads.incrementAndGet() == 2) gate.complete(Unit)
                gate.await()
                return row
            }
        }
        val services = requests.map { SessionServiceImpl(rendezvous, inbox, SimpleMeterRegistry(), object : PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults()) }
        try { withTimeout(5000) { requests.mapIndexed { index, request -> async { firstOpen(services[index], request) } }.awaitAll() } }
        finally { services.forEach { it.close() } }
    }

    @Test fun `concurrent creates accept exactly one session key`() = runBlocking {
        val sid = SessionId(byteArrayOf(7))
        val keys = listOf(Secp256r1KeyPair.generate(), Secp256r1KeyPair.generate())
        val requests = keys.map { pair -> val public = pair.publicKey.encode(); WatchSessionRequest { request.create { sessionId = sid; publicKey { rawValue = public } } } }
        val results = race(null, requests)
        assertEquals(1, results.count { it.result is WatchSessionResponse.Open.Result.OK })
        assertEquals(1, results.count { it.result is WatchSessionResponse.Open.Result.SESSION_EXISTS })
    }

    @Test fun `concurrent opens consume a sequence challenge exactly once`() = runBlocking {
        val key = Secp256r1KeyPair.generate()
        val challenge = ByteArray(32) { 9 }
        val sid = SessionId(byteArrayOf(8))
        val seed = ServerSession(sessionId = ServerSessionId(sid.rawValue), authorizedPublicKey = key.publicKey.encode(), nextKeyMaterial = challenge)
        val signed = key.privateKey.sign(challenge)
        val request = WatchSessionRequest { this.request.open { sessionId = sid; sequenceKeySignature { signature = signed } } }
        val results = race(seed, listOf(request, request))
        assertEquals(1, results.count { it.result is WatchSessionResponse.Open.Result.OK })
        assertEquals(1, results.count { it.result is WatchSessionResponse.Open.Result.INVALID_SEQUENCE })
    }
}
