package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.server.ServerResourceLimits

enum class SessionAdmission { CREATED, EXISTS, EXHAUSTED }

interface SessionStore {
    suspend fun admitIfAbsent(session: ServerSession, limits: ServerResourceLimits): SessionAdmission =
        throw UnsupportedOperationException("SessionStore requires bounded identity admission")
    /** Shared authority transaction; implementations must serialize with authorization and destroy. */
    suspend fun <T> atomic(sessionId: ServerSessionId, block: suspend () -> T): T =
        throw UnsupportedOperationException("SessionStore requires atomic session authority transactions")

    suspend fun createIfAbsent(session: ServerSession): Boolean = atomic(requireNotNull(session.sessionId)) {
        if (isRevoked(requireNotNull(session.sessionId)) || getSessionById(requireNotNull(session.sessionId)) != null) false
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

    suspend fun isRevoked(sessionId: ServerSessionId): Boolean

    /** Atomically reserve the identity and erase its authority, subscriptions, inbox and push work. */
    suspend fun destroySession(sessionId: ServerSessionId)
}

class SessionStoreImpl(private val database: Database) : SessionStore, Store<ServerSession>(database, SessionStoreImplDefinitionV1) {
    override suspend fun <T> atomic(sessionId: ServerSessionId, block: suspend () -> T): T =
        database.transaction("lockers.session-authority", block)

    private class Revocations(database: Database) : Store<ServerSessionId>(database, RevokedSessionDefinitionV2) {
        suspend fun contains(id: ServerSessionId) = get(RevokedSessionDefinitionV2.sessionId.eq(id.toByteArray())) != null
        suspend fun reserve(id: ServerSessionId) = save(id)
    }
    private val revocations = Revocations(database)
    private val inbox = SessionInboxStoreImpl(database)
    private val pushInfo = com.latenighthack.lockers.server.services.push.v1.PushSessionStoreImpl(database)
    private val pushQueue = com.latenighthack.lockers.server.services.push.v1.PushQueueStoreImpl(database)

    override suspend fun admitIfAbsent(session: ServerSession, limits: ServerResourceLimits): SessionAdmission =
        atomic(requireNotNull(session.sessionId)) {
            val id = requireNotNull(session.sessionId)
            if (isRevoked(id) || getSessionById(id) != null) return@atomic SessionAdmission.EXISTS
            val active = database.count(SessionStoreImplDefinitionV1.storeName, SessionStoreImplDefinitionV1.sessionIdKey.query(1))
            val revoked = database.count(RevokedSessionDefinitionV2.storeName, RevokedSessionDefinitionV2.sessionId.query(1))
            if (active >= limits.maxSessions || active + revoked >= limits.maxReservedSessionIds) SessionAdmission.EXHAUSTED
            else if (createIfAbsent(session)) SessionAdmission.CREATED else SessionAdmission.EXISTS
        }

    private val sessionIdKey = SessionStoreImplDefinitionV1.sessionIdKey

    override suspend fun getSessionById(sessionId: ServerSessionId): ServerSession? = get(sessionIdKey.eq(sessionId.toByteArray()))

    override suspend fun getAllSessions(): List<ServerSession> = getAll()

    override suspend fun updateSession(session: ServerSession) = atomic(requireNotNull(session.sessionId)) {
        require(!isRevoked(requireNotNull(session.sessionId))) { "Session identity has been permanently revoked" }
        save(session)
    }

    override suspend fun isRevoked(sessionId: ServerSessionId) = revocations.contains(sessionId)

    override suspend fun destroySession(sessionId: ServerSessionId): Unit = atomic(sessionId) {
        revocations.reserve(sessionId)
        delete(sessionIdKey.eq(sessionId.toByteArray()))
        val subscriptions = com.latenighthack.lockers.server.services.room.v1.SubscriptionStoreImplDefinitionV1
        var more: Boolean
        do {
            more = database.deleteBatch(subscriptions.storeName, subscriptions.sessionIdKey.query(256,
                lower = sessionId.toByteArray(), upper = sessionId.toByteArray())) > 0
        } while (more)
        inbox.deleteAllEvents(sessionId)
        pushInfo.deletePushInfo(sessionId)
        pushQueue.clearForSession(sessionId)
    }
}
