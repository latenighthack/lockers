package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.server.serveAll
import io.ktor.server.routing.Routing

/**
 * Mounts every service (public + admin) plus extension HTTP routes onto one
 * router. Used by the in-process test harness; production splits public and
 * admin across separate listeners ([monolithClient] / [monolithAdmin]).
 */
fun Routing.monolith(component: MonolithComponent) {
    serveServices(component.allServices)
    for (extension in component.extensions) {
        extension.install(this)
    }
}

/** Mounts the public client/peer services plus extension HTTP routes. */
fun Routing.monolithClient(component: MonolithComponent) {
    serveServices(component.clientServices)
    for (extension in component.extensions) {
        extension.install(this)
    }
}

/** Mounts the internal management services. Bind this router to an internal-only port. */
fun Routing.monolithAdmin(component: MonolithComponent) {
    serveServices(component.adminServices)
}

/**
 * Hands each service's impl + descriptor to ktbuf's [serveAll], which registers
 * the gRPC routes (unary over HTTP, streaming over WebSockets).
 */
private fun Routing.serveServices(services: List<com.latenighthack.lockers.server.tools.GrpcRouteProvider<*>>) {
    for (service in services) {
        serveAll(service.server as Any, service.descriptor)
    }
}

/** Convenience overload that builds a fresh [MonolithComponent] and mounts it. */
fun Routing.monolith(serverCore: ServerCore) = monolith(MonolithComponent(serverCore))
