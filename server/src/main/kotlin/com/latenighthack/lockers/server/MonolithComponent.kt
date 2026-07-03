package com.latenighthack.lockers.server

import com.latenighthack.lockers.server.services.push.v1.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.tools.GrpcRouteProvider

/**
 * The monolith composition root. It instantiates every locker service module
 * from the shared [ServerCore] graph, wiring cross-service dependencies to
 * in-process local discovery (room -> session-gateway, session -> push-gateway).
 * Splitting a service into its own deployable server later means building a
 * different component that lists a subset and swaps a local discovery for a
 * remote RPC stub.
 *
 * Optional [extensions] (discovered from the classpath — see [ServerExtension])
 * contribute their own gRPC services and HTTP routes on top of the built-ins.
 */
class MonolithComponent(
    serverCore: ServerCore,
    val extensions: List<ServerExtension> = emptyList(),
) {
    val pushServiceModule: PushServiceModule =
        PushServiceModule::class.create(serverCore)
    val pushGatewayServiceModule: PushGatewayServiceModule =
        PushGatewayServiceModule::class.create(serverCore, pushServiceModule)
    private val pushGatewayDiscovery: PushGatewayDiscovery =
        LocalPushGatewayDiscovery(pushGatewayServiceModule.server)

    val sessionServiceModule: SessionServiceModule =
        SessionServiceModule::class.create(serverCore, pushGatewayDiscovery)
    val sessionGatewayServiceModule: SessionGatewayServiceModule =
        SessionGatewayServiceModule::class.create(serverCore, sessionServiceModule)
    private val sessionGatewayDiscovery: SessionGatewayDiscovery =
        LocalSessionGatewayDiscovery(sessionGatewayServiceModule.server)

    val roomServiceModule: RoomServiceModule =
        RoomServiceModule::class.create(serverCore, sessionGatewayDiscovery)

    val allServices: List<GrpcRouteProvider<*>>
        get() = listOf(
            sessionServiceModule,
            sessionGatewayServiceModule,
            roomServiceModule,
            pushServiceModule,
            pushGatewayServiceModule,
        ) + extensions.flatMap { it.services }

    suspend fun start() {
        pushServiceModule.start()
        extensions.forEach { it.start() }
    }

    /** Releases background scopes and sharded thread pools for a clean shutdown. */
    fun stop() {
        pushServiceModule.stop()
        sessionServiceModule.serverImpl.close()
        roomServiceModule.serverImpl.close()
        extensions.forEach { it.stop() }
    }
}
