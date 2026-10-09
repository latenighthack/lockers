package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.push.v1.*
import com.latenighthack.lockers.server.services.push.v1.providers.*
import kotlinx.coroutines.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.*

class ServerLifecycleDrainTest {
    @Test fun `component shutdown joins provider cleanup before closing resources`(): Unit = runBlocking {
        val entered = CompletableDeferred<Unit>()
        val cleaning = CompletableDeferred<Unit>()
        val finishCleanup = CompletableDeferred<Unit>()
        val closed = AtomicBoolean(false)
        val provider = object : PushProvider {
            override val backend = PushBackendKind.APNS
            override val isConfigured = true
            override suspend fun send(registration: PushRegistration, push: Push): PushResult {
                entered.complete(Unit)
                try { awaitCancellation() }
                finally { withContext(NonCancellable) { cleaning.complete(Unit); finishCleanup.await() } }
            }
            override fun close() { closed.set(true) }
        }
        val core = ServerCore::class.create(LockersConfig.defaults(), ServerStorage.inMemory())
        core.overridePushProviders = listOf(provider)
        core.setup()
        val component = MonolithComponent(core)
        try {
            component.start()
            val sid = SessionId(byteArrayOf(1))
            LocalPushServiceRpc(component.pushServiceModule.serverImpl).registerSession(RegisterSessionRequest {
                sessionId = sid; registration { backend.apns { deviceToken = byteArrayOf(1) } }
            })
            LocalPushGatewayServiceRpc(component.pushServiceModule.serverImpl).sendPush(SendPushRequest { sessionId = sid; push = Push { title = "one" } })
            withTimeout(1000) { entered.await() }
            val shutdown = async(Dispatchers.Default) { component.stop() }
            withTimeout(1000) { cleaning.await() }
            delay(50)
            assertFalse(closed.get(), "provider closed before active send cleanup completed")
            assertFalse(shutdown.isCompleted, "shutdown returned before child cleanup completed")
            finishCleanup.complete(Unit)
            shutdown.await()
            assertTrue(closed.get())
        } finally { finishCleanup.complete(Unit); component.stop() }
    }

    @Test fun `startup failure rolls back extensions that started successfully`(): Unit = runBlocking {
        val stopped = AtomicBoolean(false)
        val first = object : ServerExtension { override fun stop() { stopped.set(true) } }
        val failing = object : ServerExtension { override suspend fun start() { error("extension startup failed") } }
        val core = ServerCore::class.create(LockersConfig.defaults(), ServerStorage.inMemory())
        core.overridePushProviders = emptyList()
        core.setup()
        val component = MonolithComponent(core, listOf(first, failing))
        try {
            assertFailsWith<IllegalStateException> { component.start() }
            assertTrue(stopped.get(), "earlier extensions must unwind when later startup fails")
        } finally { component.stop() }
    }
    @Test fun `embedding parent cancellation terminates provider work`(): Unit = runBlocking {
        val parent = SupervisorJob()
        val entered = CompletableDeferred<Unit>()
        val finished = CompletableDeferred<Unit>()
        val provider = object : PushProvider {
            override val backend = PushBackendKind.APNS
            override val isConfigured = true
            override suspend fun send(registration: PushRegistration, push: Push): PushResult {
                entered.complete(Unit)
                try { awaitCancellation() } finally { finished.complete(Unit) }
            }
        }
        val core = ServerCore::class.create(LockersConfig.defaults(), ServerStorage.inMemory())
        core.overrideCoroutineContext = parent + Dispatchers.Default
        core.overridePushProviders = listOf(provider)
        core.setup()
        val component = MonolithComponent(core)
        try {
            component.start()
            val sid = SessionId(byteArrayOf(3))
            LocalPushServiceRpc(component.pushServiceModule.serverImpl).registerSession(RegisterSessionRequest { sessionId = sid; registration { backend.apns { deviceToken = byteArrayOf(1) } } })
            LocalPushGatewayServiceRpc(component.pushServiceModule.serverImpl).sendPush(SendPushRequest { sessionId = sid; push = Push { title = "cancel me" } })
            withTimeout(1000) { entered.await() }
            parent.cancel()
            withTimeout(1000) { finished.await() }
            component.closeAndJoin()
            assertFailsWith<IllegalStateException> { component.start() }
        } finally { component.closeAndJoin(); parent.cancel() }
    }

    @Test fun `shard close waits for a suspended operation and rejects self close`(): Unit = runBlocking {
        val dispatcher = com.latenighthack.lockers.server.tools.ShardedDispatcher<Int>(1, "test-shard") { it }
        val entered = CompletableDeferred<Unit>()
        val proceed = CompletableDeferred<Unit>()
        try {
            val operation = async {
                dispatcher.runOnDispatcher(1) {
                    assertFailsWith<IllegalStateException> { dispatcher.close() }
                    entered.complete(Unit)
                    proceed.await()
                }
            }
            entered.await()
            val closing = async { dispatcher.closeAndJoin() }
            delay(50)
            assertFalse(closing.isCompleted)
            proceed.complete(Unit)
            operation.await(); closing.await()
            assertFailsWith<IllegalStateException> { dispatcher.runOnDispatcher(1) {} }
        } finally { proceed.complete(Unit); dispatcher.closeAndJoin() }
    }

    @Test fun `factory failure closes earlier results before component construction`(): Unit = runBlocking {
        val stopped = AtomicBoolean(false)
        val first = object : ServerExtensionFactory {
            override fun create(meterRegistry: io.micrometer.core.instrument.MeterRegistry): ServerExtension =
                object : ServerExtension { override fun stop() { stopped.set(true) } }
        }
        val second = object : ServerExtensionFactory {
            override fun create(meterRegistry: io.micrometer.core.instrument.MeterRegistry): ServerExtension = error("factory failed")
        }
        assertFailsWith<IllegalStateException> { ServerExtensions.create(listOf(first, second), io.micrometer.core.instrument.simple.SimpleMeterRegistry(), ServerStorage.inMemory()) }
        assertTrue(stopped.get())
    }

}
