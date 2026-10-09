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
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import com.latenighthack.lockers.server.tools.ServiceLifecycle
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
    private val serverCore: ServerCore,
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
    private val telemetry = serverCore.telemetry
    private val clusterScope = CoroutineScope(serverCore.coroutineContext + SupervisorJob(serverCore.coroutineContext[Job]) + Dispatchers.IO)
    private val extensionScope = CoroutineScope(serverCore.coroutineContext + SupervisorJob(serverCore.coroutineContext[Job]) + Dispatchers.IO + ServiceLifecycle.context)
    private val closing = Mutex()
    private var started = false
    private var closed = false

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
                telemetry = telemetry,
            )
        }

    /**
     * Public client services. Privileged gateways are available only through local discovery
     * or the authenticated internal peer router.
     */
    val clientServices: List<GrpcRouteProvider<*>>
        get() = listOf(
            sessionServiceModule,
            roomServiceModule,
            pushServiceModule,
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

    /** Internal peer services; never mount these on the public listener. */
    val peerServices: List<GrpcRouteProvider<*>>
        get() = listOf(sessionGatewayServiceModule, pushGatewayServiceModule, roomServiceModule)

    /** Every service — used by the in-process test harness. */
    val allServices: List<GrpcRouteProvider<*>>
        get() = (clientServices + adminServices + peerServices).distinct()

    suspend fun start() {
        synchronized(this) { check(!started && !closed) { "Component already started or closed" }; started = true }
        try {
            roomServiceModule.serverImpl.start()
            sessionServiceModule.serverImpl.start()
            pushServiceModule.start()
            ownerLifecycle?.let { lifecycle ->
                cluster?.router?.let { router ->
                    lifecycle.reconcile(router.roomMap())
                    lifecycle.start(clusterScope, router.roomMapWatch())
                }
            }
            claimRenewal?.start(clusterScope)
            extensions.forEach { it.start(extensionScope) }
        } catch (failure: Throwable) {
            withContext(NonCancellable) {
                try { closeAndJoin() } catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
            }
            throw failure
        }
    }

    /** Drains service work before releasing ownership or closing providers and coordination. */
    suspend fun closeAndJoin() {
        ServiceLifecycle.requireExternalClose()
        closing.withLock {
            if (closed) return
            closed = true
            // Complete every cleanup even if one extension/provider reports a failure.
            var failed: Throwable? = null
            suspend fun cleanup(block: suspend () -> Unit) {
                try { block() } catch (failure: Throwable) {
                    if (failed == null) failed = failure else failed!!.addSuppressed(failure)
                }
            }
            withContext(NonCancellable) {
                cleanup { extensionScope.coroutineContext[Job]!!.cancelAndJoin() }
                extensions.asReversed().forEach { cleanup { it.closeAndJoin() } }
                cleanup { roomServiceModule.serverImpl.closeAndJoin() }
                cleanup { sessionServiceModule.serverImpl.closeAndJoin() }
                cleanup { pushServiceModule.serverImpl.stopAndJoin() }
                cleanup { ownerLifecycle?.stopAndRelease() }
                cleanup { claimRenewal?.stopAndRelease() }
                cleanup { clusterScope.coroutineContext[Job]!!.cancelAndJoin() }
                cleanup { serverCore.closeAndJoin() }
            }
            failed?.let { throw it }
        }
    }

    /** Compatibility adapter; external owners can use closeAndJoin without blocking a thread. */
    fun stop() = ServiceLifecycle.blockingClose(serverCore.coroutineContext) { closeAndJoin() }
}
