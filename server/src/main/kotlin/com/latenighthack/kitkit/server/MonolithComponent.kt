package com.latenighthack.kitkit.server

import com.latenighthack.kitkit.server.services.greeter.v1.GreeterServiceModule
import com.latenighthack.kitkit.server.services.greeter.v1.create
import com.latenighthack.kitkit.server.tools.GrpcRouteProvider

/**
 * The monolith composition root. It instantiates every service module from the
 * shared [ServerCore] graph and exposes them as a flat list of mountable
 * providers. Splitting a service into its own deployable server later is just a
 * matter of building a different component that lists a subset (and swaps any
 * cross-service dependency from a local impl to a remote RPC stub).
 */
class MonolithComponent(serverCore: ServerCore) {
    val greeterServiceModule: GreeterServiceModule = GreeterServiceModule::class.create(serverCore)

    val allServices: List<GrpcRouteProvider<*>>
        get() = listOf(greeterServiceModule)
}
