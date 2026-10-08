package com.latenighthack.lockers.server.cluster

import com.latenighthack.lockers.push.v1.PushGatewayService
import com.latenighthack.lockers.session.v1.SessionGatewayService
import com.latenighthack.lockers.sharding.Keyspace
import com.latenighthack.lockers.sharding.ShardRouter
import com.latenighthack.lockers.sharding.spi.OwnershipCoordinator

/**
 * Everything `MonolithComponent` needs to run as one node of a cluster: the [ShardRouter] (both
 * rings), the transports that reach peer session/push gateways, and — for M5 elastic reshard — the
 * [OwnershipCoordinator] and the set of [roomKeyspaces] whose shard leases this node maintains.
 * When absent, the monolith wires the in-process `Local*Discovery`/`LocalRoomOwnership` and needs no
 * sharding infrastructure at all.
 *
 * @param ownerCoordinator fencing coordinator for the room ring. When set, the component builds and
 *   runs an [OwnerLifecycle] that acquires a lease per owned shard and gates writes on it; when
 *   null (e.g. an early cluster wiring), routing alone gates writes (route-local only).
 * @param roomKeyspaces the authority dimension; must contain only keyspace 0. Every locker
 *   keyspace in a room shares this lease so room authority and derived writes stay atomic.
 * @param ownerMetrics sink for the sharding counters the lifecycle bumps.
 */
class ClusterContext(
    val router: ShardRouter,
    val sessionGateways: RemoteGateway<SessionGatewayService>,
    val pushGateways: RemoteGateway<PushGatewayService>,
    val ownerCoordinator: OwnershipCoordinator? = null,
    val roomKeyspaces: List<Keyspace> = listOf(Keyspace(0L)),
    val ownerMetrics: OwnerLifecycleMetrics = OwnerLifecycleMetrics.NONE,
) {
    init { require(roomKeyspaces == listOf(Keyspace(0))) { "Room authority is sharded as a whole under keyspace 0" } }
}
