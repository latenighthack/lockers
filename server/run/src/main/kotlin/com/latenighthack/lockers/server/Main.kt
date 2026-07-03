package com.latenighthack.lockers.server

import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.ktstore.createStoreDelegate
import io.ktor.http.ContentType
import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
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

fun main() {
    runBlocking {
        val config = LockersConfig.fromEnv()
        val metricsRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

        val core = ServerCore::class.create(config, storeDelegate(config))
        core.overrideMeterRegistry = metricsRegistry
        core.setup()

        // Optional add-ons contributed from the classpath (e.g. remote content).
        val extensions = ServiceLoader.load(ServerExtensionFactory::class.java)
            .map { it.create(metricsRegistry) }
        val component = MonolithComponent(core, extensions)
        component.start()

        val server = embeddedServer(CIO, port = config.httpPort) {
            install(WebSockets)
            routing {
                get("/healthz") { call.respondText("ok") }
                get("/readyz") { call.respondText("ok") }
                get("/metrics") { call.respondText(metricsRegistry.scrape(), ContentType.Text.Plain) }
                monolith(component)
            }
        }

        Runtime.getRuntime().addShutdownHook(
            Thread {
                runBlocking { component.stop() }
                server.stop(gracePeriodMillis = 1_000, timeoutMillis = 5_000)
            }
        )

        server.start(wait = true)
    }
}
