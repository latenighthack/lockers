package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

interface SessionStore {
    suspend fun getSessionById(sessionId: ServerSessionId): ServerSession?

    suspend fun getAllSessions(): List<ServerSession>

    suspend fun updateSession(session: ServerSession)

    suspend fun destroySession(sessionId: ServerSessionId)
}

class SessionStoreImpl(delegate: Database) : SessionStore, Store<ServerSession>(delegate, SessionStoreImplDefinitionV1) {
    private val sessionIdKey = SessionStoreImplDefinitionV1.sessionIdKey

    override suspend fun getSessionById(sessionId: ServerSessionId): ServerSession? = get(sessionIdKey.eq(sessionId.toByteArray()))

    override suspend fun getAllSessions(): List<ServerSession> = getAll()

    override suspend fun updateSession(session: ServerSession) = save(session)

    override suspend fun destroySession(sessionId: ServerSessionId) = delete(sessionIdKey.eq(sessionId.toByteArray()))
}
