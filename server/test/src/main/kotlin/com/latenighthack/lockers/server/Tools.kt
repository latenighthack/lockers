package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.server.serveAll
import io.ktor.server.routing.Routing
import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.rpc.HttpRpcClient
import com.latenighthack.ktbuf.test.server.TestServer
import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.common.v1.Push
import com.latenighthack.lockers.push.v1.PushRegistration
import com.latenighthack.lockers.server.services.push.v1.providers.PushBackendKind
import com.latenighthack.lockers.server.services.push.v1.providers.PushProvider
import com.latenighthack.lockers.server.services.push.v1.providers.PushResult
import io.ktor.server.application.Application
import io.ktor.server.routing.routing
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * Boots the real locker monolith over an embedded Ktor server backed by
 * in-memory stores — the same router production uses. Consumed by the connector
 * integration tests via `runTestWithServer(Application::attachTestServices)`.
 */
suspend fun Application.attachTestServices() = attachTestServicesWithConfig(LockersConfig.defaults())

suspend fun Application.attachFastpathTestServices() = attachTestServicesWithConfig(LockersConfig.defaults().copy(deliveryOutboxEnabled = true))

suspend fun Application.attachTestServicesWithConfig(config: LockersConfig) {
    attachOwnedTestServices(config) {}
}

/**
 * Like [attachTestServices] but lets a test mutate the [ServerCore] before setup
 * (e.g. swap in a [RecordingPushProvider]) and starts the monolith so the push
 * processor drains its queue. Returns the started [MonolithComponent].
 */
suspend fun Application.attachTestServicesWith(configureCore: (ServerCore) -> Unit): MonolithComponent {
    return attachOwnedTestServices(LockersConfig.defaults(), configureCore)
}

private suspend fun Application.attachOwnedTestServices(config: LockersConfig, configureCore: (ServerCore) -> Unit): MonolithComponent {
    val database = ServerStorage.inMemory()
    var core: ServerCore? = null
    var component: MonolithComponent? = null
    try {
        val createdCore = ServerCore::class.create(config, database)
        core = createdCore
        createdCore.overrideCoroutineContext = coroutineContext
        configureCore(createdCore)
        createdCore.setup()
        val createdComponent = MonolithComponent(createdCore)
        component = createdComponent
        createdComponent.start()
        ownTestComponent(createdComponent, database)
        routing { trustedTestMonolith(createdComponent) }
        return createdComponent
    } catch (failure: Throwable) {
        withContext(NonCancellable) {
            try { component?.closeAndJoin() ?: core?.closeAndJoin() }
            catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
            try { database.close() }
            catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
        }
        throw failure
    }
}

val TestServer.rpcClient: RpcClient get() = HttpRpcClient(serverUrl)

/**
 * A test [PushProvider] that records the pushes it is handed and can simulate
 * transient failures or a dead token. [awaitSends] lets a test wait for delivery
 * without polling.
 */
class RecordingPushProvider(
    override val backend: PushBackendKind,
    override val isConfigured: Boolean = true,
    private val failuresBeforeSuccess: Int = 0,
    private val tokenInvalid: Boolean = false,
    private val permanentReject: Boolean = false,
) : PushProvider {
    private val recorded = CopyOnWriteArrayList<Push>()
    private val attempts = AtomicInteger(0)
    private val inFlight = AtomicInteger(0)
    private val maxInFlight = AtomicInteger(0)
    private val signal = MutableSharedFlow<Int>(replay = 1, extraBufferCapacity = 64)

    val sends: List<Push> get() = recorded.toList()

    /** Highest number of concurrent [send] calls observed — used to assert concurrency caps. */
    val maxObservedInFlight: Int get() = maxInFlight.get()

    /** When set, [send] blocks on this until completed — lets a test hold sends in flight. */
    @Volatile
    var gate: CompletableDeferred<Unit>? = null

    override suspend fun send(registration: PushRegistration, push: Push): PushResult {
        recorded.add(push)
        val attempt = attempts.incrementAndGet()
        val current = inFlight.incrementAndGet()
        maxInFlight.getAndUpdate { maxOf(it, current) }
        signal.tryEmit(recorded.size)
        try {
            gate?.await()
            return when {
                tokenInvalid -> PushResult.Rejected("invalid", tokenInvalid = true)
                permanentReject -> PushResult.Rejected("permanent", tokenInvalid = false)
                attempt <= failuresBeforeSuccess -> PushResult.Retryable("transient")
                else -> PushResult.Accepted
            }
        } finally {
            inFlight.decrementAndGet()
        }
    }

    /** Suspends until at least [count] pushes have been handed to this provider. */
    suspend fun awaitSends(count: Int) {
        if (recorded.size >= count) return
        signal.first { it >= count }
    }
}

/** Explicitly trusted all-service fixture. Never install this test-artifact router on a public host. */
fun Routing.trustedTestMonolith(component: MonolithComponent) {
    for (service in component.allServices) serveAll(service.server as Any, service.descriptor)
    for (extension in component.extensions) extension.install(this)
}

/** The application owns this cleanup child independently of the service's child lifetime. */
private fun Application.ownTestComponent(component: MonolithComponent, database: Database) {
    launch(start = CoroutineStart.UNDISPATCHED) {
        try { awaitCancellation() }
        finally { withContext(NonCancellable) { try { component.closeAndJoin() } finally { database.close() } } }
    }
}
