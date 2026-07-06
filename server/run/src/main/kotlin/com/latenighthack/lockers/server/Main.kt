package com.latenighthack.lockers.server

import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.ktstore.createStoreDelegate
import com.latenighthack.lockers.server.cluster.ShardMetrics
import io.ktor.http.ContentType
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

fun main() {
    runBlocking {
        val config = LockersConfig.fromEnv()
        val metricsRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        bindRuntimeMetrics(metricsRegistry)
        // Pre-register the sharding/reshard meters (§7) so `/metrics` is shape-stable across
        // deployment modes; the monolith owns every shard and never mutates them.
        ShardMetrics(metricsRegistry)

        val core = ServerCore::class.create(config, storeDelegate(config))
        core.overrideMeterRegistry = metricsRegistry
        core.setup()

        // Optional add-ons contributed from the classpath (e.g. remote content).
        val extensions = ServiceLoader.load(ServerExtensionFactory::class.java)
            .map { it.create(metricsRegistry) }
        val component = MonolithComponent(core, extensions)
        component.start()

        // Public port: client/peer traffic + probes + metrics.
        val server = embeddedServer(CIO, port = config.httpPort) {
            install(WebSockets)
            routing {
                get("/healthz") { call.respondText("ok") }
                get("/readyz") { call.respondText("ok") }
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
                runBlocking { component.stop() }
                adminServer.stop(gracePeriodMillis = 1_000, timeoutMillis = 5_000)
                server.stop(gracePeriodMillis = 1_000, timeoutMillis = 5_000)
            }
        )

        adminServer.start(wait = false)
        server.start(wait = true)
    }
}
