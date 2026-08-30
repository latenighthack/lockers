package com.latenighthack.lockers.server

import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.ktstore.createStoreDelegate
import com.latenighthack.lockers.server.claim.ClaimContext
import com.latenighthack.lockers.server.claim.ClaimMetrics
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

private fun fatal(message: String): Nothing {
    System.err.println("FATAL: $message")
    kotlin.system.exitProcess(1)
}

/**
 * Validates `LOCKERS_ROOM_OWNERSHIP` and its mode-specific requirements at boot (config holds raw
 * strings only; all validation fails fast here). Returns the validated mode.
 */
private fun validateOwnershipMode(config: LockersConfig): String {
    val mode = config.roomOwnership
    when (mode) {
        "local" -> {
            if (config.clusterEnabled) {
                System.err.println(
                    "WARNING: LOCKERS_PEERS/LOCKERS_NODE_ID are set but LOCKERS_ROOM_OWNERSHIP=local — " +
                        "running as a single-node monolith. Multi-node operation requires " +
                        "LOCKERS_ROOM_OWNERSHIP=claim (or the deprecated 'ring')."
                )
            }
        }

        // Claim requirements are validated (single source of truth) in ClaimContext.fromConfig;
        // startCoordination surfaces its IllegalStateException as a fatal boot error.
        "claim" -> {}

        "ring" -> {
            System.err.println(
                "WARNING: LOCKERS_ROOM_OWNERSHIP=ring is DEPRECATED (see docs/design/claim-ownership.md). " +
                    "Each shard pins a dedicated Postgres connection; prefer 'claim'."
            )
            if (!config.clusterEnabled) {
                fatal("LOCKERS_ROOM_OWNERSHIP=ring requires LOCKERS_PEERS and LOCKERS_NODE_ID.")
            }
            if (config.sharding.shardCountDefault > config.ringMaxConnections) {
                fatal(
                    "LOCKERS_ROOM_OWNERSHIP=ring with LOCKERS_SHARD_COUNT_DEFAULT=" +
                        "${config.sharding.shardCountDefault} would pin more Postgres connections than " +
                        "LOCKERS_RING_MAX_CONNECTIONS=${config.ringMaxConnections} allows. Lower the shard " +
                        "count or (preferably) use LOCKERS_ROOM_OWNERSHIP=claim."
                )
            }
        }

        else -> fatal("LOCKERS_ROOM_OWNERSHIP='$mode' is not one of: local, ring, claim.")
    }
    return mode
}

/**
 * Fail fast: an operator who declared the DB mandatory must not silently boot on the ephemeral
 * in-memory store (data loss on restart, no shared claim/coordination tables).
 */
private fun validateDbRequirement(config: LockersConfig) {
    if (config.requireDb && config.databaseUrl == null) {
        fatal(
            "LOCKERS_REQUIRE_DB=true but LOCKERS_DB_URL is unset. Set a Postgres JDBC URL or " +
                "clear LOCKERS_REQUIRE_DB."
        )
    }
}

/**
 * The coordination wiring for the validated ownership mode: the (deprecated) ring's
 * [ClusterRuntime] — which wires only under an explicit `ring`; `LOCKERS_PEERS` alone no longer
 * enables it — and claim mode's [ClaimContext] (via [ClaimContext.fromConfig]).
 */
private suspend fun startCoordination(
    config: LockersConfig,
    ownershipMode: String,
    metricsRegistry: MeterRegistry,
    scope: CoroutineScope,
): Pair<ClusterRuntime, ClaimContext?> {
    val ringConfig =
        if (ownershipMode == "ring") config
        else config.copy(sharding = config.sharding.copy(peers = null))
    val cluster = ClusterRuntime.start(ringConfig, scope)
    val claim =
        if (ownershipMode == "claim") {
            try {
                ClaimContext.fromConfig(config, metricsRegistry)
            } catch (e: IllegalStateException) {
                fatal(e.message ?: "invalid claim configuration")
            }
        } else null
    return cluster to claim
}

fun main() {
    runBlocking {
        val config = LockersConfig.fromEnv()
        validateDbRequirement(config)
        val ownershipMode = validateOwnershipMode(config)

        val metricsRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        bindRuntimeMetrics(metricsRegistry)
        // Pre-register the sharding/reshard + claim meters so `/metrics` is shape-stable across
        // deployment modes; unused modes simply never mutate them.
        ShardMetrics(metricsRegistry)
        val claimMetrics = ClaimMetrics(metricsRegistry)

        val core = ServerCore::class.create(config, storeDelegate(config))
        core.overrideMeterRegistry = metricsRegistry
        core.setup()

        // Background scope for cluster pollers/pool — supervised so one failure doesn't kill the
        // process, and cancelled last on shutdown.
        val clusterScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val (cluster, claim) = startCoordination(config, ownershipMode, metricsRegistry, clusterScope)

        // Optional add-ons contributed from the classpath (e.g. remote content).
        val extensions = ServiceLoader.load(ServerExtensionFactory::class.java)
            .map { it.create(metricsRegistry) }
        // The ownership mode swaps in Registry*/Ring* discovery; the default monolith wires
        // in-process Local*Discovery exactly as before (byte-for-byte).
        val component = MonolithComponent(core, extensions, cluster.context, claim)
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
                        ?: claim?.let { runCatching { it.roomClaims.ping() }.exceptionOrNull() }
                            ?.let { "claim store unreachable: ${it.message}" }
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
                    // services (claim mode's stop releases its rows), then the coordination pool
                    // and the listeners.
                    cluster.close()
                    component.stop()
                    claim?.close()
                }
                adminServer.stop(gracePeriodMillis = 1_000, timeoutMillis = 5_000)
                server.stop(gracePeriodMillis = 1_000, timeoutMillis = 5_000)
            }
        )

        adminServer.start(wait = false)
        server.start(wait = true)
    }
}
