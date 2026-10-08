package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

interface SessionStore {
    /** Shared authority transaction; implementations must serialize with authorization and destroy. */
    suspend fun <T> atomic(sessionId: ServerSessionId, block: suspend () -> T): T =
        throw UnsupportedOperationException("SessionStore requires atomic session authority transactions")

    suspend fun createIfAbsent(session: ServerSession): Boolean = atomic(requireNotNull(session.sessionId)) {
        if (getSessionById(requireNotNull(session.sessionId)) != null) false
        else { updateSession(session); true }
    }

    suspend fun rotateIfCurrent(expected: ServerSession, next: ServerSession): Boolean = atomic(requireNotNull(expected.sessionId)) {
        require(expected.sessionId == next.sessionId)
        val current = getSessionById(requireNotNull(expected.sessionId))
        if (current == null || !current.nextKeyMaterial.contentEquals(expected.nextKeyMaterial) ||
            !current.authorizedPublicKey.contentEquals(expected.authorizedPublicKey)) false
        else { updateSession(next); true }
    }

    suspend fun getSessionById(sessionId: ServerSessionId): ServerSession?

    suspend fun getAllSessions(): List<ServerSession>

    suspend fun updateSession(session: ServerSession)

    suspend fun destroySession(sessionId: ServerSessionId)
}

class SessionStoreImpl(private val database: Database) : SessionStore, Store<ServerSession>(database, SessionStoreImplDefinitionV1) {
    override suspend fun <T> atomic(sessionId: ServerSessionId, block: suspend () -> T): T =
        database.transaction("lockers.session-authority", block)

    private val sessionIdKey = SessionStoreImplDefinitionV1.sessionIdKey

    override suspend fun getSessionById(sessionId: ServerSessionId): ServerSession? = get(sessionIdKey.eq(sessionId.toByteArray()))

    override suspend fun getAllSessions(): List<ServerSession> = getAll()

    override suspend fun updateSession(session: ServerSession) = save(session)

    override suspend fun destroySession(sessionId: ServerSessionId) = delete(sessionIdKey.eq(sessionId.toByteArray()))
}
