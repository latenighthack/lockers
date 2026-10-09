package com.latenighthack.lockers.server

import com.latenighthack.lockers.server.tools.GrpcRouteProvider
import io.ktor.server.routing.Routing
import io.micrometer.core.instrument.MeterRegistry

/**
 * An optional server component contributed from the classpath. When a jar on the
 * server's classpath provides a [ServerExtensionFactory] (registered via
 * [java.util.ServiceLoader]), the monolith mounts the extension's extra gRPC
 * [services] and raw HTTP routes ([install]) alongside the built-in locker
 * services. This is the seam by which an add-on — e.g. a remote-content
 * upload/hosting service — attaches to the locker server only when it is needed.
 */
interface ServerExtension {
    /** Extra gRPC services to serve, mounted next to the built-in locker services. */
    val services: List<GrpcRouteProvider<*>> get() = emptyList()

    /** Installs any raw (non-gRPC) HTTP routes this extension serves. */
    fun install(routing: Routing) {}

    /** Starts background work, if any. Mirrors the monolith's own start/stop. */
    suspend fun start() {}

    /** New extensions launch work in this owned scope; legacy start overrides remain supported. */
    suspend fun start(scope: kotlinx.coroutines.CoroutineScope) { start() }

    /** Releases resources for a clean shutdown. */
    fun stop() {}

    /** Suspends until extension-owned work and resources finish; legacy extensions retain stop. */
    suspend fun closeAndJoin() { stop() }
}

/**
 * Discovered via [java.util.ServiceLoader] at startup and asked to build a
 * [ServerExtension]. The factory reads its own configuration (environment
 * variables); the shared [MeterRegistry] is passed in so extensions publish
 * metrics on the same registry as the core services.
 */
interface ServerExtensionFactory {
    val storeDefinitions: List<com.latenighthack.ktstore.StoreDefinition<*>> get() = emptyList()
    fun create(meterRegistry: MeterRegistry, database: com.latenighthack.ktstore.Database): ServerExtension = create(meterRegistry)

    fun create(meterRegistry: MeterRegistry): ServerExtension
}

/** Unwinds earlier factory results if a later factory fails before a component can own them. */
object ServerExtensions {
    suspend fun create(factories: List<ServerExtensionFactory>, meters: MeterRegistry, database: com.latenighthack.ktstore.Database): List<ServerExtension> {
        val created = mutableListOf<ServerExtension>()
        try {
            factories.forEach { created.add(it.create(meters, database)) }
            return created
        } catch (failure: Throwable) {
            kotlinx.coroutines.withContext(kotlinx.coroutines.NonCancellable) {
                created.asReversed().forEach {
                    try { it.closeAndJoin() } catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
                }
            }
            throw failure
        }
    }
}
