package com.latenighthack.lockers.server

import com.latenighthack.lockers.server.cluster.ClusterContext
import com.latenighthack.lockers.server.cluster.OwnerLifecycle
import com.latenighthack.lockers.server.cluster.RingPushGatewayDiscovery
import com.latenighthack.lockers.server.cluster.RingRoomOwnership
import com.latenighthack.lockers.server.cluster.RingSessionGatewayDiscovery
import com.latenighthack.lockers.server.services.push.v1.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.tools.GrpcRouteProvider
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking

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
    private val cluster: ClusterContext? = null,
) {
    val pushServiceModule: PushServiceModule =
        PushServiceModule::class.create(serverCore)
    val pushGatewayServiceModule: PushGatewayServiceModule =
        PushGatewayServiceModule::class.create(serverCore, pushServiceModule)
    val pushAdminServiceModule: PushAdminServiceModule =
        PushAdminServiceModule::class.create(serverCore, pushServiceModule)
    private val pushGatewayDiscovery: PushGatewayDiscovery =
        cluster?.let { RingPushGatewayDiscovery(it.router, pushGatewayServiceModule.server, it.pushGateways) }
            ?: LocalPushGatewayDiscovery(pushGatewayServiceModule.server)

    val sessionServiceModule: SessionServiceModule =
        SessionServiceModule::class.create(serverCore, pushGatewayDiscovery)
    val sessionGatewayServiceModule: SessionGatewayServiceModule =
        SessionGatewayServiceModule::class.create(serverCore, sessionServiceModule)
    private val sessionGatewayDiscovery: SessionGatewayDiscovery =
        cluster?.let { RingSessionGatewayDiscovery(it.router, sessionGatewayServiceModule.server, it.sessionGateways) }
            ?: LocalSessionGatewayDiscovery(sessionGatewayServiceModule.server)
    val broadcastAdminServiceModule: BroadcastAdminServiceModule =
        BroadcastAdminServiceModule::class.create(serverCore, sessionServiceModule)

    /**
     * M5 owner lifecycle for the room ring: present only in a clustered deployment that supplies an
     * `ownerCoordinator`. It holds a fenced lease per owned shard and, on a fenced handoff away from
     * this node, evicts the room caches (below) so the new owner rebuilds lazily from the store.
     * [RingRoomOwnership] consults it so a write only proceeds when this node is both route-local
     * and still holds a valid lease. Its map watch runs on [clusterScope], started in [start].
     */
    val ownerLifecycle: OwnerLifecycle? =
        cluster?.ownerCoordinator?.let { coordinator ->
            OwnerLifecycle(
                self = cluster.router.roomSelf,
                coordinator = coordinator,
                keyspaces = cluster.roomKeyspaces,
                onShardsDropped = { roomServiceModule.serverImpl.evictRoomCaches() },
                metrics = cluster.ownerMetrics,
            )
        }

    private val roomOwnership: RoomOwnership =
        cluster?.let { RingRoomOwnership(it.router, ownerLifecycle) } ?: LocalRoomOwnership()

    val roomServiceModule: RoomServiceModule =
        RoomServiceModule::class.create(serverCore, sessionGatewayDiscovery, roomOwnership)

    /** Scope for the owner lifecycle's shard-map watch; cancelled on [stop]. */
    private val clusterScope = CoroutineScope(SupervisorJob())

    /**
     * Client- and peer-facing services, mounted on the public port. The gateways
     * are internal cross-service RPCs but are called in-process here (via local
     * discovery); they remain in this set to preserve the future split-service
     * topology.
     */
    val clientServices: List<GrpcRouteProvider<*>>
        get() = listOf(
            sessionServiceModule,
            sessionGatewayServiceModule,
            roomServiceModule,
            pushServiceModule,
            pushGatewayServiceModule,
        ) + extensions.flatMap { it.services }

    /** Operator-facing management services, mounted on the internal admin port only. */
    val adminServices: List<GrpcRouteProvider<*>>
        get() = listOf(
            pushAdminServiceModule,
            broadcastAdminServiceModule,
        )

    /** Every service (public + admin) — used by the in-process test harness. */
    val allServices: List<GrpcRouteProvider<*>>
        get() = clientServices + adminServices

    suspend fun start() {
        pushServiceModule.start()
        // In a cluster, begin maintaining shard leases: acquire for owned shards and react to
        // every reassignment. Reconcile once synchronously against the current map so the node is
        // ready (owns its leases) before it starts serving; the watch keeps it in step thereafter.
        ownerLifecycle?.let { lifecycle ->
            cluster?.router?.let { router ->
                lifecycle.reconcile(router.roomMap())
                lifecycle.start(clusterScope, router.roomMapWatch())
            }
        }
        extensions.forEach { it.start() }
    }

    /**
     * Releases background scopes and sharded thread pools for a clean shutdown. In a cluster this
     * first drains shard leases (`releaseAll`) so peers stop being redirected here and a successor
     * can acquire at the next epoch, then cancels the lifecycle's map watch.
     */
    fun stop() {
        ownerLifecycle?.let { runBlocking { it.releaseAll() } }
        clusterScope.cancel()
        pushServiceModule.stop()
        sessionServiceModule.serverImpl.close()
        roomServiceModule.serverImpl.close()
        extensions.forEach { it.stop() }
    }
}
