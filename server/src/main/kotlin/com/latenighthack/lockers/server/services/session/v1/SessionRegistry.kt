package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.lockers.server.storage.v1.ServerSessionId

/**
 * Seam for tracking where a session's WebSocket lives. [SessionServiceImpl] calls [attach] when a
 * `watchSession` stream opens and [detach] when the registered stream tears down; claim mode wires
 * a registry that publishes rows to the `session_gateway` table so peers can discover this node for
 * fan-out. The monolith and ring modes use [Noop] — zero behavior change.
 *
 * Implementations must not block the WebSocket path: publishing may be asynchronous, and a missed
 * publish only degrades delivery to the push-queue path until the next registry renew round.
 */
interface SessionRegistry {
    fun attach(sessionId: ServerSessionId)
    fun detach(sessionId: ServerSessionId)

    object Noop : SessionRegistry {
        override fun attach(sessionId: ServerSessionId) {}
        override fun detach(sessionId: ServerSessionId) {}
    }
}
