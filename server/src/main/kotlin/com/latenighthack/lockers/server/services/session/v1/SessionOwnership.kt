package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.lockers.common.v1.SessionId

/**
 * Decides whether THIS node hosts a session's live WebSocket + inbox — i.e. owns the session's
 * shard on the session/gateway ring (keyed by `sessionId`). Mirrors `RoomOwnership` for the room
 * ring. The monolith uses [LocalSessionOwnership] (always local); a cluster injects a ring-backed
 * implementation so a session that opens against a non-owner is answered `EPOCH_STALE` + a redirect
 * to the owner, and the client's reconnect loop re-resolves against it.
 */
interface SessionOwnership {
    suspend fun resolve(sessionId: SessionId): SessionOwner
}

sealed interface SessionOwner {
    /** This node owns the session's shard; open the stream locally. */
    data object Local : SessionOwner

    /** Another node owns the shard; the client should reconnect toward [address]. */
    data class Remote(val address: String, val epoch: Long) : SessionOwner
}

/** Single-node / monolith default: every session is owned locally, so opens never redirect. */
class LocalSessionOwnership : SessionOwnership {
    override suspend fun resolve(sessionId: SessionId): SessionOwner = SessionOwner.Local
}
