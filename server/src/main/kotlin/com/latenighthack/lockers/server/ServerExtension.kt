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

    /** Releases resources for a clean shutdown. */
    fun stop() {}
}

/**
 * Discovered via [java.util.ServiceLoader] at startup and asked to build a
 * [ServerExtension]. The factory reads its own configuration (environment
 * variables); the shared [MeterRegistry] is passed in so extensions publish
 * metrics on the same registry as the core services.
 */
interface ServerExtensionFactory {
    fun create(meterRegistry: MeterRegistry): ServerExtension
}
