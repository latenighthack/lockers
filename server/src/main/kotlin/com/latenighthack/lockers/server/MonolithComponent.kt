package com.latenighthack.lockers.server

import com.latenighthack.lockers.server.claim.ClaimContext
import com.latenighthack.lockers.server.claim.ClaimRenewalService
import com.latenighthack.lockers.server.claim.ClaimRoomOwnership
import com.latenighthack.lockers.server.claim.ClaimSessionRegistry
import com.latenighthack.lockers.server.claim.RegistryPushGatewayDiscovery
import com.latenighthack.lockers.server.claim.RegistrySessionGatewayDiscovery
import com.latenighthack.lockers.server.cluster.ClusterContext
import com.latenighthack.lockers.server.cluster.ClusterServiceModule
import com.latenighthack.lockers.server.cluster.OwnerLifecycle
import com.latenighthack.lockers.server.cluster.RingPushGatewayDiscovery
import com.latenighthack.lockers.server.cluster.RingRoomOwnership
import com.latenighthack.lockers.server.cluster.RingSessionGatewayDiscovery
import com.latenighthack.lockers.server.cluster.RingSessionOwnership
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
    private val claim: ClaimContext? = null,
) {
    init {
        require(cluster == null || claim == null) { "ring and claim ownership are mutually exclusive" }
    }

    /**
     * Scope for multi-node background work (the ring's shard-map watch, or claim mode's renewal
     * loop and async registry writes); cancelled on [stop]. Declared first: properties below
     * capture it during construction.
     */
    private val clusterScope = CoroutineScope(SupervisorJob())

    val pushServiceModule: PushServiceModule =
        PushServiceModule::class.create(serverCore)
    val pushGatewayServiceModule: PushGatewayServiceModule =
        PushGatewayServiceModule::class.create(serverCore, pushServiceModule)
    val pushAdminServiceModule: PushAdminServiceModule =
        PushAdminServiceModule::class.create(serverCore, pushServiceModule)
    private val pushGatewayDiscovery: PushGatewayDiscovery =
        claim?.let { RegistryPushGatewayDiscovery(pushGatewayServiceModule.server, it.sessionGateways, it.pool, it.nodeId, it.meters) }
            ?: cluster?.let { RingPushGatewayDiscovery(it.router, pushGatewayServiceModule.server, it.pushGateways) }
            ?: LocalPushGatewayDiscovery(pushGatewayServiceModule.server)

    // Claim mode keeps session ownership Local: a session lives wherever its WebSocket is, and a
    // reconnect legitimately moves it (the registry's unconditional upsert follows the socket).
    private val sessionOwnership: SessionOwnership =
        cluster?.let { RingSessionOwnership(it.router) } ?: LocalSessionOwnership()

    /** Claim-mode registry publishing live sessions to `session_gateway`; Noop otherwise. */
    private val sessionRegistry: SessionRegistry =
        claim?.let { ClaimSessionRegistry(it.sessionGateways, it.nodeId, it.advertiseAddr, it.ttlMs, clusterScope) }
            ?: SessionRegistry.Noop

    val sessionServiceModule: SessionServiceModule =
        SessionServiceModule::class.create(serverCore, pushGatewayDiscovery, sessionOwnership, sessionRegistry)
    val sessionGatewayServiceModule: SessionGatewayServiceModule =
        SessionGatewayServiceModule::class.create(serverCore, sessionServiceModule)
    private val sessionGatewayDiscovery: SessionGatewayDiscovery =
        claim?.let { RegistrySessionGatewayDiscovery(sessionGatewayServiceModule.server, it.sessionGateways, it.pool, it.nodeId, it.meters) }
            ?: cluster?.let { RingSessionGatewayDiscovery(it.router, sessionGatewayServiceModule.server, it.sessionGateways) }
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

    /** Claim-mode room ownership; kept as the concrete type so the renewal service can drive it. */
    private val claimRoomOwnership: ClaimRoomOwnership? =
        claim?.let { ClaimRoomOwnership(it.roomClaims, it.nodeId, it.advertiseAddr, it.ttlMs, it.renewMs, it.meters) }

    private val roomOwnership: RoomOwnership =
        claimRoomOwnership
            ?: cluster?.let { RingRoomOwnership(it.router, ownerLifecycle) }
            ?: LocalRoomOwnership()

    val roomServiceModule: RoomServiceModule =
        RoomServiceModule::class.create(serverCore, sessionGatewayDiscovery, roomOwnership)

    /**
     * Claim mode's heartbeat: batched TTL renewal for `room_claim`/`session_gateway`, demotion of
     * lost claims (with room-cache eviction, mirroring [ownerLifecycle]'s `onShardsDropped`), and
     * row release on drain. Started in [start] on [clusterScope], drained first in [stop].
     */
    val claimRenewal: ClaimRenewalService? =
        claim?.let { ctx ->
            ClaimRenewalService(
                roomClaims = ctx.roomClaims,
                ownership = claimRoomOwnership!!,
                nodeId = ctx.nodeId,
                ttlMs = ctx.ttlMs,
                renewIntervalMs = ctx.renewMs,
                meters = ctx.meters,
                onDemoted = { roomServiceModule.serverImpl.evictRoomCaches() },
                sessionRenewRound = { (sessionRegistry as ClaimSessionRegistry).renewRound() },
                sessionReleaseAll = { (sessionRegistry as ClaimSessionRegistry).releaseAll() },
            )
        }

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

    /**
     * The cluster topology service, present only when running as a cluster node. Read-only,
     * backed by the [ShardRouter]; the monolith has no ring, so it contributes nothing here.
     */
    private val clusterServiceModule: ClusterServiceModule? =
        cluster?.let { ClusterServiceModule(it.router) }

    /** Operator-facing management services, mounted on the internal admin port only. */
    val adminServices: List<GrpcRouteProvider<*>>
        get() = listOfNotNull(
            pushAdminServiceModule,
            broadcastAdminServiceModule,
            clusterServiceModule,
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
        claimRenewal?.start(clusterScope)
        extensions.forEach { it.start() }
    }

    /**
     * Releases background scopes and sharded thread pools for a clean shutdown. In a cluster this
     * first drains shard leases (`releaseAll`) so peers stop being redirected here and a successor
     * can acquire at the next epoch, then cancels the lifecycle's map watch.
     */
    fun stop() {
        ownerLifecycle?.let { runBlocking { it.releaseAll() } }
        // Claim drain mirrors the lease drain: delete this node's claim/registry rows so peers
        // stop redirecting here and successors claim without waiting out a TTL.
        claimRenewal?.let { runBlocking { it.stopAndRelease() } }
        clusterScope.cancel()
        pushServiceModule.stop()
        sessionServiceModule.serverImpl.close()
        roomServiceModule.serverImpl.close()
        extensions.forEach { it.stop() }
    }
}
