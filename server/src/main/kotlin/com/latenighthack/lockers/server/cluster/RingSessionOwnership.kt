package com.latenighthack.lockers.server.cluster

import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.server.services.session.v1.SessionOwner
import com.latenighthack.lockers.server.services.session.v1.SessionOwnership
import com.latenighthack.lockers.sharding.ShardRouter

/**
 * Ring-backed [SessionOwnership]: resolves the session ring for a `sessionId` and reports whether
 * this node hosts the session. When it doesn't, it carries the owning node's address + epoch so the
 * open can be answered `EPOCH_STALE` + `ShardRedirect`. Mirrors [RingRoomOwnership] on the session
 * axis (`ShardRouter.routeSession`).
 */
class RingSessionOwnership(private val router: ShardRouter) : SessionOwnership {
    override suspend fun resolve(sessionId: SessionId): SessionOwner {
        val route = router.routeSession(sessionId.rawValue)
        return if (route.isLocal) {
            SessionOwner.Local
        } else {
            SessionOwner.Remote(
                address = route.address?.hostPort() ?: "",
                epoch = route.epoch.value,
            )
        }
    }
}
