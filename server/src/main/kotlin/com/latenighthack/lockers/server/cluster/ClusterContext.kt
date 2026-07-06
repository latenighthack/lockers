package com.latenighthack.lockers.server.cluster

import com.latenighthack.lockers.push.v1.PushGatewayService
import com.latenighthack.lockers.session.v1.SessionGatewayService
import com.latenighthack.lockers.sharding.ShardRouter

/**
 * Everything `MonolithComponent` needs to run as one node of a cluster: the [ShardRouter] (both
 * rings) and the transports that reach peer session/push gateways. When absent, the monolith wires
 * the in-process `Local*Discovery` and needs no sharding infrastructure at all.
 */
class ClusterContext(
    val router: ShardRouter,
    val sessionGateways: RemoteGateway<SessionGatewayService>,
    val pushGateways: RemoteGateway<PushGatewayService>,
)
