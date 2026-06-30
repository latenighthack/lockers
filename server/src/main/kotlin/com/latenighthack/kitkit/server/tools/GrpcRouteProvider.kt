package com.latenighthack.kitkit.server.tools

import com.latenighthack.ktbuf.net.ServerDescriptor

/**
 * The pluggable unit of the monolith: a service contributes its ktbuf
 * [ServerDescriptor] (its RPC method table) and the [server] implementation
 * those methods dispatch to. The monolith router mounts every provider.
 */
interface GrpcRouteProvider<Server> {
    val descriptor: ServerDescriptor
    val server: Server
}
