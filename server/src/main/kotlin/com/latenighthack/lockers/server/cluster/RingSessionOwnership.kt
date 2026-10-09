package com.latenighthack.lockers.server.cluster

import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.server.services.session.v1.SessionOwner
import com.latenighthack.lockers.server.services.session.v1.SessionOwnership
import com.latenighthack.lockers.sharding.ShardRouter
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes

/**
 * Ring-backed [SessionOwnership]: resolves the session ring for a `sessionId` and reports whether
 * this node hosts the session. When it doesn't, it carries the owning node's address + epoch so the
 * open can be answered `EPOCH_STALE` + `ShardRedirect`. Mirrors [RingRoomOwnership] on the session
 * axis (`ShardRouter.routeSession`).
 */
class RingSessionOwnership(private val router: ShardRouter, publicAddresses: Map<NodeId, String>?) : SessionOwnership {
    /** Trusted legacy embedding whose ring addresses already target public WebSocket listeners. */
    constructor(router: ShardRouter) : this(router, null)
    // Freeze operator data; a caller's mutable map must not change an admitted address book.
    private val publicAddresses = publicAddresses?.mapValues { PublicSessionAddresses.validate(it.value) }?.toMap().also { addresses ->
        require(addresses == null || addresses.keys.containsAll(router.sessionMap().nodes)) { "Public session endpoints must cover every session ring member" }
    }
    override suspend fun resolve(sessionId: SessionId): SessionOwner {
        val route = router.routeSession(sessionId.rawValue)
        return if (route.isLocal) {
            SessionOwner.Local
        } else {
            SessionOwner.Remote(
                address = publicAddresses?.let { addresses -> addresses[route.node]
                    ?: throw RpcResponseException("", "SESSION", Codes.UNAVAILABLE, "Public session endpoint unavailable")
                } ?: route.address?.hostPort().orEmpty(),
                epoch = route.epoch.value,
            )
        }
    }
}
