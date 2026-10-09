package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.server.serveAll
import com.latenighthack.ktbuf.server.serveUnary
import io.ktor.server.routing.Routing
import io.ktor.server.routing.application
import kotlinx.coroutines.launch
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

/** Public monolith routing; privileged peers and admin services require separate listeners. */
fun Routing.monolith(component: MonolithComponent) = monolithClient(component)

/** Mounts public client services plus extension HTTP routes. */
fun Routing.monolithClient(component: MonolithComponent) {
    serveServices(component.clientServices)
    for (extension in component.extensions) {
        extension.install(this)
    }
}

/** Management routes are disabled until an administrator credential is configured. */
fun Routing.monolithAdmin(component: MonolithComponent) {
    val token = component.adminToken?.takeIf { it.isNotBlank() } ?: return
    for (service in component.adminServices) {
        for (method in service.descriptor.methods) {
            require(!method.streamingIn && !method.streamingOut) { "Management methods must be unary" }
            @Suppress("UNCHECKED_CAST")
            val unary = method as com.latenighthack.ktbuf.net.ServerMethodDescriptor<Any, Any, Any>
            serveUnary(service.server as Any, service.descriptor, unary) { context ->
                val presented = context.headers.entries.firstOrNull { it.key.equals("x-admin-token", true) }?.value.orEmpty()
                if (!validPeerToken(token, presented)) throw com.latenighthack.ktbuf.net.RpcResponseException(
                    context.originalUrl, "POST", com.latenighthack.ktbuf.proto.Codes.UNAUTHENTICATED, "Invalid administrator credential")
                context
            }
        }
    }
}

/** Mount privileged peer RPCs on an internal listener protected by a cluster credential. */
fun Routing.monolithPeer(component: MonolithComponent, token: String) {
    require(token.isNotBlank()) { "Internal peer routes require LOCKERS_PEER_TOKEN" }
    for (service in component.peerServices) {
        for (method in service.descriptor.methods) {
            require(!method.streamingIn && !method.streamingOut) { "Peer methods must be unary" }
            @Suppress("UNCHECKED_CAST")
            val unary = method as com.latenighthack.ktbuf.net.ServerMethodDescriptor<Any, Any, Any>
            // ktbuf 1.1.10 serveAll discards the unary context processor; bind it explicitly.
            serveUnary(service.server as Any, service.descriptor, unary) { context ->
            val presented = context.headers.entries.firstOrNull { it.key.equals(PEER_TOKEN_HEADER, true) }?.value.orEmpty()
            if (!validPeerToken(token, presented)) throw com.latenighthack.ktbuf.net.RpcResponseException(
                context.originalUrl, "POST", com.latenighthack.ktbuf.proto.Codes.UNAUTHENTICATED, "Invalid peer credential")
                context
            }
        }
    }
}

internal fun validPeerToken(required: String, presented: String): Boolean =
    required.isNotBlank() && java.security.MessageDigest.isEqual(required.encodeToByteArray(), presented.encodeToByteArray())
const val PEER_TOKEN_HEADER = "x-lockers-peer-token"

/**
 * Hands each service's impl + descriptor to ktbuf's [serveAll], which registers
 * the gRPC routes (unary over HTTP, streaming over WebSockets).
 */
private fun Routing.serveServices(services: List<com.latenighthack.lockers.server.tools.GrpcRouteProvider<*>>) {
    for (service in services) {
        serveAll(service.server as Any, service.descriptor)
    }
}

/** Public convenience routing with component lifetime owned by the host application. */
fun Routing.monolith(serverCore: ServerCore) {
    val component = MonolithComponent(serverCore)
    application.launch(start = CoroutineStart.UNDISPATCHED) {
        try { component.start(); awaitCancellation() }
        finally { withContext(NonCancellable) { component.closeAndJoin() } }
    }
    monolithClient(component)
}
