package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.lockers.server.storage.v1.ServerSessionId

/**
 * Seam for tracking where a session's WebSocket lives. [SessionServiceImpl] calls [attach] when a
 * `watchSession` stream opens and [detach] when the registered stream tears down; claim mode wires
 * a registry that publishes rows to the `session_gateway` table so peers can discover this node for
 * fan-out. The monolith and ring modes use [Noop] — zero behavior change.
 *
 * Claim gateways publish through attachBeforeSnapshot before opening the inbox snapshot. This
 * ordering plus remoteSessions detection lets late deliveries follow a migrated socket safely.
 */
interface SessionRegistry {
    suspend fun attachBeforeSnapshot(sessionId: ServerSessionId) { attach(sessionId) }
    suspend fun remoteSessions(sessionIds: List<com.latenighthack.lockers.common.v1.SessionId>): Set<com.latenighthack.lockers.common.v1.SessionId> = emptySet()
    fun attach(sessionId: ServerSessionId)
    fun detach(sessionId: ServerSessionId)

    object Noop : SessionRegistry {
        override fun attach(sessionId: ServerSessionId) {}
        override fun detach(sessionId: ServerSessionId) {}
    }
}
