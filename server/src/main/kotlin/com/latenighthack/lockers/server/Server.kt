package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.server.serveAll
import io.ktor.server.routing.Routing

/**
 * Mounts an already-built monolith onto a Ktor router: for each pluggable
 * service, hand its server impl + descriptor to ktbuf's [serveAll], which
 * registers the gRPC routes (unary over HTTP, streaming over WebSockets).
 */
fun Routing.monolith(component: MonolithComponent) {
    for (service in component.allServices) {
        serveAll(service.server as Any, service.descriptor)
    }
}

/** Convenience overload that builds a fresh [MonolithComponent] and mounts it. */
fun Routing.monolith(serverCore: ServerCore) = monolith(MonolithComponent(serverCore))
