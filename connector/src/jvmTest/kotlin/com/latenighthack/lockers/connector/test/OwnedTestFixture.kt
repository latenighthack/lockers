package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.rpc.HttpRpcClient
import com.latenighthack.ktbuf.test.server.TestServer
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.Version
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.observability.LockersTelemetry
import io.ktor.server.application.Application
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.coroutines.*

private class FixtureResources : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<FixtureResources>
    private val mutex = Mutex()
    private val cleanup = mutableListOf<suspend () -> Unit>()
    private val databases = mutableListOf<Database>()
    suspend fun own(client: LockersClient, database: Database) = mutex.withLock {
        cleanup += { client.closeAndJoin() }
        if (databases.none { it === database }) databases += database
    }
    suspend fun own(stream: Stream) = mutex.withLock { cleanup += { stream.closeAndJoin() } }
    suspend fun closeAndJoin() = withContext(NonCancellable) {
        val resources = mutex.withLock { (cleanup.asReversed().toList() to databases.toList()).also { cleanup.clear(); databases.clear() } }
        var failed: Throwable? = null
        suspend fun close(action: suspend () -> Unit) { try { action() } catch (failure: Throwable) { if (failed == null) failed = failure else failed!!.addSuppressed(failure) } }
        resources.first.forEach { close(it) }
        resources.second.forEach { db -> close { db.close() } }
        failed?.let { throw it }
    }
}

private val transports = java.util.concurrent.ConcurrentHashMap<TestServer, HttpRpcClient>()
/** One transport per fixture, owned by its host rather than allocated on each property access. */
internal val TestServer.ownedRpcClient: RpcClient get() = requireNotNull(transports[this]) { "Test transport requires an owned fixture" }

/** Failure-safe external lifecycle: clients, HTTP transport, then server application. */
internal fun <T> runOwnedTestWithServer(
    extensions: suspend Application.() -> T? = { null },
    runner: suspend CoroutineScope.(server: TestServer, context: T?) -> Unit,
) = runTest {
    val server = TestServer()
    val resources = FixtureResources()
    var context: T? = null
    var failure: Throwable? = null
    try {
        server.start { context = extensions() }
        transports[server] = HttpRpcClient(server.serverUrl)
        withContext(resources) {
            val runnerJob = currentCoroutineContext()[Job]!!
            var runnerFailure: Throwable? = null
            try { runner(server, context) }
            catch (error: Throwable) { runnerFailure = error; throw error }
            finally {
                withContext(NonCancellable) { runnerJob.children.toList().forEach { it.cancelAndJoin() } }
                try { resources.closeAndJoin() }
                catch (cleanup: Throwable) { if (runnerFailure == null) throw cleanup else runnerFailure.addSuppressed(cleanup) }
            }
        }
    } catch (error: Throwable) { failure = error; throw error }
    finally { withContext(NonCancellable) {
        var cleanupFailure: Throwable? = null
        suspend fun close(action: suspend () -> Unit) {
            try { action() } catch (cleanup: Throwable) {
                if (failure != null) failure.addSuppressed(cleanup)
                else if (cleanupFailure == null) cleanupFailure = cleanup else cleanupFailure!!.addSuppressed(cleanup)
            }
        }
        close { resources.closeAndJoin() }
        close { transports.remove(server)?.closeAndJoin() }
        close { server.stop() }
        cleanupFailure?.let { throw it }
    } }

}

internal suspend fun createOwnedTestClient(
    rpcClient: RpcClient, database: Database, keyValueStore: KeyValueStore,
    keySource: AuthenticationKeySource, appVersion: Version, lockKeySource: LockKeySource? = null,
    codecs: NotificationCodecs = NotificationCodecs.identity(), telemetry: LockersTelemetry = LockersTelemetry.NONE,
    coroutineContext: CoroutineContext = Dispatchers.Default,
    broadcastCodecs: BroadcastCodecs = BroadcastCodecs.identity(), retentionPolicy: ConnectorRetentionPolicy = ConnectorRetentionPolicy(),
): LockersClient = LockersClient.create(rpcClient, database, keyValueStore, keySource, appVersion, lockKeySource, codecs, telemetry,
    coroutineContext, broadcastCodecs, retentionPolicy).also { currentCoroutineContext()[FixtureResources]?.own(it, database) }

internal suspend fun createOwnedTestStream(
    rpcClient: RpcClient, keySource: AuthenticationKeySource, sessionStore: SessionStore,
    subscriptionStore: SubscriptionStore, appVersion: Version,
    telemetry: LockersTelemetry = LockersTelemetry.NONE, coroutineContext: CoroutineContext = Dispatchers.Default,
    heartbeatIntervalMillis: Long = Stream.PING_TIMEOUT, heartbeatTimeoutMillis: Long = 2 * Stream.PING_TIMEOUT,
): Stream = Stream(rpcClient, keySource, sessionStore, subscriptionStore, appVersion, telemetry, coroutineContext,
    heartbeatIntervalMillis, heartbeatTimeoutMillis).also { currentCoroutineContext()[FixtureResources]?.own(it) }
