package com.latenighthack.lockers.server

import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.ktstore.createStoreDelegate
import com.latenighthack.lockers.server.cluster.BlueprintV
import com.latenighthack.lockers.server.cluster.ShardMetrics
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.binder.system.UptimeMetrics
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import java.util.ServiceLoader

private const val JDBC_POSTGRES_PREFIX = "jdbc:postgresql:"

/**
 * Chooses the persistence backend. When [LockersConfig.databaseUrl] is set we
 * build a durable Postgres-backed delegate via ktstore's createStoreDelegate;
 * otherwise we fall back to in-memory storage for local development. In-memory
 * data does NOT survive a restart, so production must set LOCKERS_DB_URL.
 */
private fun storeDelegate(config: LockersConfig): StoreDelegate {
    val dbUrl = config.databaseUrl
    return if (dbUrl != null) {
        createStoreDelegate(dbUrl.removePrefix(JDBC_POSTGRES_PREFIX))
    } else {
        System.err.println(
            "WARNING: LOCKERS_DB_URL is not set — using in-memory storage. Data will " +
                "NOT survive a restart. Set LOCKERS_DB_URL to a Postgres JDBC URL " +
                "(jdbc:postgresql://host:5432/lockers?user=U&password=P) in production."
        )
        InMemoryStoreDelegate()
    }
}

/**
 * Binds the standard JVM/system runtime meters to [registry] so `/metrics` reports heap/GC/threads/
 * CPU/file-descriptors/uptime — the telemetry gap folded into this milestone (§7). [JvmGcMetrics] is
 * an [AutoCloseable] (it registers GC notification listeners); the process runs until exit so we let
 * the JVM reclaim it rather than tracking a close.
 */
private fun bindRuntimeMetrics(registry: MeterRegistry) {
    JvmMemoryMetrics().bindTo(registry)
    JvmGcMetrics().bindTo(registry)
    JvmThreadMetrics().bindTo(registry)
    ProcessorMetrics().bindTo(registry)
    FileDescriptorMetrics().bindTo(registry)
    UptimeMetrics().bindTo(registry)
}

/**
 * The live cluster wiring for one node, or a monolith sentinel. Owns the background scope that
 * drives the shard-map poller and the peer connection pool, and answers readiness. Building it is
 * the ONLY behavioral change vs. the monolith, and only when [LockersConfig.clusterEnabled].
 */
private class ClusterRuntime private constructor(
    private val scope: CoroutineScope,
    private val wiring: BlueprintV.Wiring?,
) {
    val context get() = wiring?.context

    /**
     * Ring-aware readiness. The monolith is always ready (no ring, no external coordination); a
     * cluster node delegates to its [BlueprintV.Wiring] (shard map loaded + DB reachable). Returns
     * null when ready, else a short reason string for the `/readyz` body.
     */
    suspend fun notReadyReason(): String? = wiring?.notReadyReason()

    /** Ordered drain: stop routing to peers (evict the pool) then cancel background pollers. */
    fun close() {
        wiring?.pool?.close()
        scope.cancel()
    }

    companion object {
        suspend fun start(config: LockersConfig, parent: CoroutineScope): ClusterRuntime {
            if (!config.clusterEnabled) return ClusterRuntime(parent, null)
            return ClusterRuntime(parent, BlueprintV.wire(config, parent))
        }
    }
}

fun main() {
    runBlocking {
        val config = LockersConfig.fromEnv()

        // Fail fast: an operator who declared the DB mandatory must not silently boot on the
        // ephemeral in-memory store (data loss on restart, no shared shard_map/advisory locks).
        if (config.requireDb && config.databaseUrl == null) {
            System.err.println(
                "FATAL: LOCKERS_REQUIRE_DB=true but LOCKERS_DB_URL is unset. Set a Postgres JDBC URL " +
                    "or clear LOCKERS_REQUIRE_DB."
            )
            kotlin.system.exitProcess(1)
        }

        val metricsRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        bindRuntimeMetrics(metricsRegistry)
        // Pre-register the sharding/reshard meters (§7) so `/metrics` is shape-stable across
        // deployment modes; the monolith owns every shard and never mutates them.
        ShardMetrics(metricsRegistry)

        val core = ServerCore::class.create(config, storeDelegate(config))
        core.overrideMeterRegistry = metricsRegistry
        core.setup()

        // Background scope for cluster pollers/pool — supervised so one failure doesn't kill the
        // process, and cancelled last on shutdown.
        val clusterScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val cluster = ClusterRuntime.start(config, clusterScope)

        // Optional add-ons contributed from the classpath (e.g. remote content).
        val extensions = ServiceLoader.load(ServerExtensionFactory::class.java)
            .map { it.create(metricsRegistry) }
        // When sharding is configured the cluster context swaps in Ring*Discovery; otherwise the
        // monolith wires in-process Local*Discovery exactly as before (byte-for-byte).
        val component = MonolithComponent(core, extensions, cluster.context)
        component.start()

        // Public port: client/peer traffic + probes + metrics.
        val server = embeddedServer(CIO, port = config.httpPort) {
            install(WebSockets)
            routing {
                // Liveness only: the process is up and the event loop is turning. Never gated on
                // the ring/DB so a not-ready node is not killed by the liveness probe.
                get("/healthz") { call.respondText("ok") }
                // Readiness: gate traffic until the shard map is loaded and the DB is reachable.
                get("/readyz") {
                    val reason = cluster.notReadyReason()
                    if (reason == null) {
                        call.respondText("ok")
                    } else {
                        call.respondText("not ready: $reason", status = HttpStatusCode.ServiceUnavailable)
                    }
                }
                get("/metrics") { call.respondText(metricsRegistry.scrape(), ContentType.Text.Plain) }
                monolithClient(component)
            }
        }

        // Internal admin port: management RPCs only. Bind this cluster-internal
        // (do not expose via the public Service/Ingress); set LOCKERS_ADMIN_TOKEN
        // for defense in depth.
        val adminServer = embeddedServer(CIO, port = config.adminPort) {
            install(WebSockets)
            routing {
                monolithAdmin(component)
            }
        }

        Runtime.getRuntime().addShutdownHook(
            Thread {
                runBlocking {
                    // Ordered drain: leave the ring first (peers stop routing to us), then stop
                    // services, then the listeners.
                    cluster.close()
                    component.stop()
                }
                adminServer.stop(gracePeriodMillis = 1_000, timeoutMillis = 5_000)
                server.stop(gracePeriodMillis = 1_000, timeoutMillis = 5_000)
            }
        )

        adminServer.start(wait = false)
        server.start(wait = true)
    }
}
