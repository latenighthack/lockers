package com.latenighthack.kitkit.server

import com.latenighthack.ktbuf.server.serveAll
import io.ktor.server.routing.Routing

/**
 * Mounts the whole monolith onto a Ktor router: for each pluggable service,
 * hand its server impl + descriptor to ktbuf's [serveAll], which registers the
 * gRPC routes (unary over HTTP, streaming over WebSockets).
 */
fun Routing.monolith(serverCore: ServerCore) {
    val monolithComponent = MonolithComponent(serverCore)

    for (service in monolithComponent.allServices) {
        serveAll(service.server as Any, service.descriptor)
    }
}
