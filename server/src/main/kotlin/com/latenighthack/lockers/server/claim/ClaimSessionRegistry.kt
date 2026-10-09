package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.server.services.session.v1.SessionRegistry
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/** Routes live sessions with a distinct identity for every attachment, including same-node opens. */
class ClaimSessionRegistry(
    private val store: SessionGatewayStore,
    private val nodeId: String,
    private val advertiseAddr: String,
    private val ttlMs: Long,
    private val scope: CoroutineScope,
) : SessionRegistry {
    private val logger = LoggerFactory.getLogger(ClaimSessionRegistry::class.java)
    private val attached = ConcurrentHashMap<SessionId, String>()
    private val writes = Mutex()

    override suspend fun attachBeforeSnapshot(sessionId: ServerSessionId) {
        val id = SessionId(sessionId.rawValue)
        val incarnation = UUID.randomUUID().toString()
        writes.withLock {
            attached[id] = incarnation
            try { store.upsert(id, nodeId, advertiseAddr, ttlMs, incarnation) }
            catch (failure: Throwable) { attached.remove(id, incarnation); throw failure }
        }
    }

    override suspend fun remoteSessions(sessionIds: List<SessionId>): Set<SessionId> =
        store.lookupMany(sessionIds).filterValues { it.nodeId != nodeId }.keys

    override fun attach(sessionId: ServerSessionId) {
        val id = SessionId(sessionId.rawValue)
        val incarnation = UUID.randomUUID().toString()
        attached[id] = incarnation
        scope.launch {
            try { writes.withLock {
                if (attached[id] == incarnation) store.upsert(id, nodeId, advertiseAddr, ttlMs, incarnation)
            } } catch (cancelled: CancellationException) { throw cancelled }
            catch (failure: Exception) { logger.warn("session_gateway upsert failed (renew round will retry)", failure) }
        }
    }

    override fun detach(sessionId: ServerSessionId) {
        val id = SessionId(sessionId.rawValue)
        val incarnation = attached.remove(id) ?: return
        scope.launch {
            try { writes.withLock { store.delete(id, nodeId, incarnation) } }
            catch (cancelled: CancellationException) { throw cancelled }
            catch (failure: Exception) { logger.warn("session_gateway delete failed (row will expire by TTL)", failure) }
        }
    }

    suspend fun renewRound() = writes.withLock {
        val renewed = store.renewAll(nodeId, ttlMs)
        val missing = attached.entries.filter { it.key !in renewed }.map { it.key to it.value }
        val rows = store.lookupMany(missing.map { it.first })
        for ((id, incarnation) in missing) {
            if (attached[id] != incarnation) continue
            val row = rows[id]
            if (row != null && (row.nodeId != nodeId || row.attachmentId != incarnation)) attached.remove(id, incarnation)
            else store.upsert(id, nodeId, advertiseAddr, ttlMs, incarnation)
        }
    }

    suspend fun releaseAll() = writes.withLock {
        attached.clear()
        store.releaseAll(nodeId)
    }
}
