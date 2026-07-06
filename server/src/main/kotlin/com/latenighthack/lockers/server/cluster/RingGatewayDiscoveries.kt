package com.latenighthack.lockers.server.cluster

import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.push.v1.LocalPushGatewayServiceRpc
import com.latenighthack.lockers.push.v1.PushGatewayServer
import com.latenighthack.lockers.push.v1.PushGatewayService
import com.latenighthack.lockers.server.services.push.v1.PushGatewayDiscovery
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.session.v1.LocalSessionGatewayServiceRpc
import com.latenighthack.lockers.session.v1.SessionGatewayServer
import com.latenighthack.lockers.session.v1.SessionGatewayService
import com.latenighthack.lockers.sharding.ShardRouter

/**
 * Routes a session's gateway lookup over the **session ring**: if this node owns the session it
 * returns the in-process [LocalSessionGatewayServiceRpc]; otherwise it returns a stub that reaches
 * the owning WebSocket/gateway node. This is what turns the in-process fan-out
 * (`RoomServiceImpl` → `findServer(sessionId).postEvent(...)`) into cross-node delivery.
 */
class RingSessionGatewayDiscovery(
    private val router: ShardRouter,
    private val local: SessionGatewayServer,
    private val remote: RemoteGateway<SessionGatewayService>,
) : SessionGatewayDiscovery {
    override suspend fun findServer(sessionId: SessionId): SessionGatewayService? {
        val route = router.routeSession(sessionId.rawValue)
        return if (route.isLocal) {
            LocalSessionGatewayServiceRpc(local)
        } else {
            remote.connect(route.node, route.address)
        }
    }
}

/** The push equivalent — push is also keyed by `sessionId`, so it routes on the same session ring. */
class RingPushGatewayDiscovery(
    private val router: ShardRouter,
    private val local: PushGatewayServer,
    private val remote: RemoteGateway<PushGatewayService>,
) : PushGatewayDiscovery {
    override suspend fun findServer(sessionId: SessionId): PushGatewayService? {
        val route = router.routeSession(sessionId.rawValue)
        return if (route.isLocal) {
            LocalPushGatewayServiceRpc(local)
        } else {
            remote.connect(route.node, route.address)
        }
    }
}
