package com.latenighthack.lockers.server

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.doesNotContain
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isEmpty
import assertk.assertions.isGreaterThanOrEqualTo
import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.common.v1.Push
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.common.v1.toByteArray
import com.latenighthack.lockers.push.v1.*
import com.latenighthack.lockers.server.services.push.v1.PushDeadLetterStoreImpl
import com.latenighthack.lockers.server.services.push.v1.PushDispatchConfig
import com.latenighthack.lockers.server.services.push.v1.PushQueueStoreImpl
import com.latenighthack.lockers.server.services.push.v1.PushRetryPolicy
import com.latenighthack.lockers.server.services.push.v1.PushServiceImpl
import com.latenighthack.lockers.server.services.push.v1.PushSessionStoreImpl
import com.latenighthack.lockers.server.services.push.v1.providers.PushBackendKind
import com.latenighthack.lockers.server.services.push.v1.providers.PushProvider
import com.latenighthack.lockers.server.storage.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.milliseconds

class PushServiceTest {

    private val fastRetries = PushRetryPolicy(maxAttempts = 3, baseDelay = 1.milliseconds, maxDelay = 5.milliseconds)

    private class Harness(
        val impl: PushServiceImpl,
        val sessionStore: PushSessionStoreImpl,
        val deadLetterStore: PushDeadLetterStoreImpl,
        val registry: SimpleMeterRegistry,
        val client: LocalPushServiceRpc,
        val gateway: LocalPushGatewayServiceRpc,
        val admin: LocalPushAdminServiceRpc,
    )

    private suspend fun harness(
        providers: List<PushProvider>,
        dispatch: PushDispatchConfig = PushDispatchConfig.DEFAULT,
        delegate: StoreDelegate = InMemoryStoreDelegate(),
    ): Harness {
        val sessionStore = PushSessionStoreImpl(delegate)
        val queueStore = PushQueueStoreImpl(delegate)
        val deadLetterStore = PushDeadLetterStoreImpl(delegate)
        sessionStore.prepare()
        queueStore.prepare()
        deadLetterStore.prepare()
        delegate.createStores()
        val registry = SimpleMeterRegistry()
        val impl = PushServiceImpl(sessionStore, queueStore, deadLetterStore, registry, providers, dispatch)
        return Harness(
            impl,
            sessionStore,
            deadLetterStore,
            registry,
            LocalPushServiceRpc(impl),
            LocalPushGatewayServiceRpc(impl),
            LocalPushAdminServiceRpc(impl),
        )
    }

    private fun adminContext(headers: Map<String, String>) = GrpcRequestContext(
        "",
        headers,
        emptyMap(),
        emptyMap(),
        PushAdminServer.Descriptor,
        PushAdminServer.Descriptor.methods[0],
    )

    private suspend fun awaitDeadLetters(harness: Harness, count: Int) = withTimeout(5_000) {
        while (harness.deadLetterStore.getAllDeadLetters().size < count) {
            delay(20)
        }
    }

    private fun sessionId(vararg bytes: Byte) = SessionId(bytes)

    private fun apns(token: String) = PushRegistration { backend.apns { deviceToken = token.encodeToByteArray() } }
    private fun fcm(token: String) = PushRegistration { backend.fcm { registrationToken = token } }

    private suspend fun LocalPushServiceRpc.register(sessionId: SessionId, registration: PushRegistration) =
        registerSession(RegisterSessionRequest {
            this.sessionId = sessionId
            this.registration = registration
        })

    @Test
    fun `registerSession upserts per backend`() = runBlocking {
        val h = harness(listOf(RecordingPushProvider(PushBackendKind.APNS), RecordingPushProvider(PushBackendKind.FCM)))
        val sid = sessionId(1, 2, 3)

        h.client.register(sid, apns("t1"))
        h.client.register(sid, fcm("f1"))

        var info = h.sessionStore.getPushInfo(ServerSessionId(sid.rawValue))
        assertThat(info).isNotNull()
        assertThat(info!!.registrations.size).isEqualTo(2)

        // Re-registering the same backend replaces, not appends.
        h.client.register(sid, apns("t2"))
        info = h.sessionStore.getPushInfo(ServerSessionId(sid.rawValue))
        assertThat(info!!.registrations.size).isEqualTo(2)

        val apns = info.registrations.first { it.backend == PushBackendKind.APNS.protoValue }
        val decoded = PushRegistration.fromByteArray(apns.encodedRegistration)
        assertThat(decoded.backend?.getApns()?.deviceToken?.decodeToString()).isEqualTo("t2")
    }

    @Test
    fun `sendPush enqueues one per backend and dispatches to each provider`() = runBlocking {
        val apnsProvider = RecordingPushProvider(PushBackendKind.APNS)
        val fcmProvider = RecordingPushProvider(PushBackendKind.FCM)
        val h = harness(listOf(apnsProvider, fcmProvider))
        h.impl.start()
        val sid = sessionId(4, 5, 6)

        h.client.register(sid, apns("t1"))
        h.client.register(sid, fcm("f1"))

        h.gateway.sendPush(SendPushRequest {
            sessionId = sid
            push = Push { title = "hi"; body = "yo" }
        })

        withTimeout(5_000) {
            apnsProvider.awaitSends(1)
            fcmProvider.awaitSends(1)
        }
        assertThat(apnsProvider.sends.first().title).isEqualTo("hi")
        assertThat(fcmProvider.sends.first().body).isEqualTo("yo")
        h.impl.stop()
    }

    @Test
    fun `retries a transient failure then succeeds`() = runBlocking {
        val apnsProvider = RecordingPushProvider(PushBackendKind.APNS, failuresBeforeSuccess = 1)
        val h = harness(listOf(apnsProvider))
        h.impl.start()
        val sid = sessionId(7, 8, 9)

        h.client.register(sid, apns("t1"))
        h.gateway.sendPush(SendPushRequest {
            sessionId = sid
            push = Push { title = "retry" }
        })

        // One failed attempt (backoff ~1s) then an accepted one.
        withTimeout(10_000) { apnsProvider.awaitSends(2) }
        assertThat(apnsProvider.sends.size).isEqualTo(2)
        h.impl.stop()
    }

    @Test
    fun `invalid token drops the registration`() = runBlocking {
        val apnsProvider = RecordingPushProvider(PushBackendKind.APNS, tokenInvalid = true)
        val h = harness(listOf(apnsProvider))
        h.impl.start()
        val sid = sessionId(10, 11, 12)

        h.client.register(sid, apns("dead"))
        h.gateway.sendPush(SendPushRequest {
            sessionId = sid
            push = Push { title = "x" }
        })

        withTimeout(5_000) {
            apnsProvider.awaitSends(1)
            // Registration removal happens right after the rejected send.
            while (h.sessionStore.getPushInfo(ServerSessionId(sid.rawValue)) != null) {
                delay(20)
            }
        }
        assertThat(h.sessionStore.getPushInfo(ServerSessionId(sid.rawValue))).isEqualTo(null)
        h.impl.stop()
    }

    @Test
    fun `getPushConfig reports only configured backends`() = runBlocking {
        val h = harness(
            listOf(
                RecordingPushProvider(PushBackendKind.APNS, isConfigured = true),
                RecordingPushProvider(PushBackendKind.FCM, isConfigured = false),
            )
        )
        val response = h.client.getPushConfig(GetPushConfigRequest {})
        val supported = response.config!!.supported.map { it.value }
        assertThat(supported).contains(PushBackendKind.APNS.protoValue)
        assertThat(supported).doesNotContain(PushBackendKind.FCM.protoValue)
    }

    @Test
    fun `unregisterSession removes only the named backend`() = runBlocking {
        val h = harness(listOf(RecordingPushProvider(PushBackendKind.APNS), RecordingPushProvider(PushBackendKind.FCM)))
        val sid = sessionId(13, 14)

        h.client.register(sid, apns("t1"))
        h.client.register(sid, fcm("f1"))

        h.client.unregisterSession(UnregisterSessionRequest {
            sessionId = sid
            backend = PushBackend.fromInt(PushBackendKind.APNS.protoValue)
        })

        val info = h.sessionStore.getPushInfo(ServerSessionId(sid.rawValue))
        assertThat(info!!.registrations.map { it.backend }).containsExactlyInAnyOrder(PushBackendKind.FCM.protoValue)
    }

    // --- dead-letter queue ---

    @Test
    fun `exhausted retries move the push to the dead-letter queue`() = runBlocking {
        val apnsProvider = RecordingPushProvider(PushBackendKind.APNS, failuresBeforeSuccess = Int.MAX_VALUE)
        val h = harness(listOf(apnsProvider), dispatch = PushDispatchConfig(retryPolicy = fastRetries))
        h.impl.start()
        val sid = sessionId(20, 21)

        h.client.register(sid, apns("t1"))
        h.gateway.sendPush(SendPushRequest { sessionId = sid; push = Push { title = "x" } })

        awaitDeadLetters(h, 1)
        val stats = h.admin.getQueueStats(GetQueueStatsRequest {})
        assertThat(stats.queued).isEqualTo(0L)
        assertThat(stats.deadLettered).isEqualTo(1L)

        val dead = h.deadLetterStore.getAllDeadLetters().first()
        assertThat(dead.attempts).isEqualTo(fastRetries.maxAttempts)
        assertThat(h.registry.counter("lockers.push.deadlettered", "backend", "apns").count())
            .isGreaterThanOrEqualTo(1.0)
        h.impl.stop()
    }

    @Test
    fun `a permanent rejection dead-letters after a single attempt`() = runBlocking {
        val apnsProvider = RecordingPushProvider(PushBackendKind.APNS, permanentReject = true)
        val h = harness(listOf(apnsProvider), dispatch = PushDispatchConfig(retryPolicy = fastRetries))
        h.impl.start()
        val sid = sessionId(22, 23)

        h.client.register(sid, apns("t1"))
        h.gateway.sendPush(SendPushRequest { sessionId = sid; push = Push { title = "x" } })

        awaitDeadLetters(h, 1)
        assertThat(apnsProvider.sends.size).isEqualTo(1)
        assertThat(h.deadLetterStore.getAllDeadLetters().first().reason).isEqualTo("permanent")
        h.impl.stop()
    }

    // --- admin surface ---

    private fun seedDeadLetter(idByte: Byte, backend: PushBackendKind) = ServerDeadLetter {
        this.pushId = ServerPushId(byteArrayOf(idByte))
        this.sessionId = ServerSessionId(byteArrayOf(9))
        this.backend = backend.protoValue
        this.encodedPush = Push { title = "x" }.toByteArray()
        this.attempts = 3
        this.reason = "boom"
    }

    @Test
    fun `admin getQueueStats reports per-backend queue and dead-letter depths`() = runBlocking {
        // Not started: enqueued rows stay put so the counts are deterministic.
        val h = harness(listOf(RecordingPushProvider(PushBackendKind.APNS), RecordingPushProvider(PushBackendKind.FCM)))
        val sid = sessionId(24, 25)
        h.client.register(sid, apns("t1"))
        h.client.register(sid, fcm("f1"))
        h.gateway.sendPush(SendPushRequest { sessionId = sid; push = Push { title = "x" } })
        h.deadLetterStore.saveDeadLetter(seedDeadLetter(1, PushBackendKind.FCM))

        val stats = h.admin.getQueueStats(GetQueueStatsRequest {})
        assertThat(stats.queued).isEqualTo(2L)
        assertThat(stats.deadLettered).isEqualTo(1L)
        assertThat(stats.queuedByBackend.associate { it.backend.value to it.count })
            .isEqualTo(mapOf(PushBackendKind.APNS.protoValue to 1L, PushBackendKind.FCM.protoValue to 1L))
        assertThat(stats.deadLetteredByBackend.single().backend.value).isEqualTo(PushBackendKind.FCM.protoValue)
    }

    @Test
    fun `admin listDeadLetters returns entries`() = runBlocking {
        val h = harness(listOf(RecordingPushProvider(PushBackendKind.APNS)))
        h.deadLetterStore.saveDeadLetter(seedDeadLetter(1, PushBackendKind.APNS))

        val listed = h.admin.listDeadLetters(ListDeadLettersRequest { limit = 10 }).deadLetters
        assertThat(listed.size).isEqualTo(1)
        assertThat(listed.first().reason).isEqualTo("boom")
        assertThat(listed.first().backend.value).isEqualTo(PushBackendKind.APNS.protoValue)
    }

    @Test
    fun `admin retryDeadLetters moves dead letters back onto the queue`() = runBlocking {
        val h = harness(listOf(RecordingPushProvider(PushBackendKind.APNS)))
        h.deadLetterStore.saveDeadLetter(seedDeadLetter(1, PushBackendKind.APNS))

        val response = h.admin.retryDeadLetters(RetryDeadLettersRequest {})
        assertThat(response.retried).isEqualTo(1L)

        val stats = h.admin.getQueueStats(GetQueueStatsRequest {})
        assertThat(stats.deadLettered).isEqualTo(0L)
        assertThat(stats.queued).isEqualTo(1L)
    }

    @Test
    fun `admin purgeDeadLetters clears the dead-letter queue`() = runBlocking {
        val h = harness(listOf(RecordingPushProvider(PushBackendKind.APNS)))
        h.deadLetterStore.saveDeadLetter(seedDeadLetter(1, PushBackendKind.APNS))
        h.deadLetterStore.saveDeadLetter(seedDeadLetter(2, PushBackendKind.APNS))

        val response = h.admin.purgeDeadLetters(PurgeDeadLettersRequest {})
        assertThat(response.purged).isEqualTo(2L)
        assertThat(h.deadLetterStore.getAllDeadLetters()).isEmpty()
    }

    @Test
    fun `admin drainQueue re-feeds pending pushes`() = runBlocking {
        val h = harness(listOf(RecordingPushProvider(PushBackendKind.APNS)))
        val sid = sessionId(26, 27)
        h.client.register(sid, apns("t1"))
        h.gateway.sendPush(SendPushRequest { sessionId = sid; push = Push { title = "x" } })

        val response = h.admin.drainQueue(DrainQueueRequest {})
        assertThat(response.drained).isEqualTo(1L)
    }

    // --- correctness / safety ---

    @Test
    fun `send concurrency is bounded per backend`() = runBlocking {
        val gate = CompletableDeferred<Unit>()
        val apnsProvider = RecordingPushProvider(PushBackendKind.APNS).apply { this.gate = gate }
        val h = harness(listOf(apnsProvider), dispatch = PushDispatchConfig(sendConcurrencyPerBackend = 2))
        h.impl.start()
        val sid = sessionId(28, 29)
        h.client.register(sid, apns("t1"))

        repeat(5) { i ->
            h.gateway.sendPush(SendPushRequest { sessionId = sid; push = Push { title = "n$i" } })
        }

        // Only two sends may be in flight; the rest block on the semaphore.
        withTimeout(5_000) { apnsProvider.awaitSends(2) }
        delay(200)
        assertThat(apnsProvider.sends.size).isEqualTo(2)
        assertThat(apnsProvider.maxObservedInFlight).isEqualTo(2)

        gate.complete(Unit)
        withTimeout(5_000) { apnsProvider.awaitSends(5) }
        assertThat(apnsProvider.maxObservedInFlight).isEqualTo(2)
        h.impl.stop()
    }

    @Test
    fun `a disabled worker enqueues but does not drain`() = runBlocking {
        val apnsProvider = RecordingPushProvider(PushBackendKind.APNS)
        val h = harness(listOf(apnsProvider), dispatch = PushDispatchConfig(workerEnabled = false))
        h.impl.start()
        val sid = sessionId(34, 35)

        h.client.register(sid, apns("t1"))
        h.gateway.sendPush(SendPushRequest { sessionId = sid; push = Push { title = "x" } })

        delay(300)
        assertThat(apnsProvider.sends).isEmpty()
        assertThat(h.admin.getQueueStats(GetQueueStatsRequest {}).queued).isEqualTo(1L)
        h.impl.stop()
    }

    @Test
    fun `admin calls require the token when one is configured`() = runBlocking {
        val h = harness(listOf(RecordingPushProvider(PushBackendKind.APNS)), dispatch = PushDispatchConfig(adminToken = "secret"))

        assertFailsWith<SecurityException> {
            h.impl.getQueueStats(adminContext(emptyMap()), GetQueueStatsRequest {})
        }
        assertFailsWith<SecurityException> {
            h.impl.purgeDeadLetters(adminContext(mapOf("x-admin-token" to "wrong")), PurgeDeadLettersRequest {})
        }

        val stats = h.impl.getQueueStats(adminContext(mapOf("x-admin-token" to "secret")), GetQueueStatsRequest {})
        assertThat(stats.queued).isEqualTo(0L)
    }
}
