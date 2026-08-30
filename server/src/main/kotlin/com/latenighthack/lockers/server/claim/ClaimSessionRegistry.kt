package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.server.services.session.v1.SessionRegistry
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * Claim-mode [SessionRegistry]: mirrors this node's live WebSocket sessions into the
 * `session_gateway` table so peers can discover the gateway for directed fan-out.
 *
 * The local set is the source of truth and is updated synchronously; row writes are launched on
 * [scope] so the WebSocket open/close path never blocks on the database. Failed upserts self-heal:
 * every renew round re-upserts any locally-attached session whose row the batched renew did not
 * confirm (and rows for detached sessions expire by TTL even if the async delete was lost).
 */
class ClaimSessionRegistry(
    private val store: SessionGatewayStore,
    private val nodeId: String,
    private val advertiseAddr: String,
    private val ttlMs: Long,
    private val scope: CoroutineScope,
) : SessionRegistry {
    private val logger = LoggerFactory.getLogger(ClaimSessionRegistry::class.java)
    private val attached = ConcurrentHashMap.newKeySet<SessionId>()

    override fun attach(sessionId: ServerSessionId) {
        val id = SessionId(rawValue = sessionId.rawValue)
        attached.add(id)
        scope.launch {
            runCatching { store.upsert(id, nodeId, advertiseAddr, ttlMs) }
                .onFailure { logger.warn("session_gateway upsert failed (renew round will retry)", it) }
        }
    }

    override fun detach(sessionId: ServerSessionId) {
        val id = SessionId(rawValue = sessionId.rawValue)
        attached.remove(id)
        scope.launch {
            runCatching { store.delete(id, nodeId) }
                .onFailure { logger.warn("session_gateway delete failed (row will expire by TTL)", it) }
        }
    }

    /** Batched renew + re-upsert of any local session the renew did not confirm. */
    suspend fun renewRound() {
        val renewed = store.renewAll(nodeId, ttlMs)
        for (id in attached) {
            if (id !in renewed) store.upsert(id, nodeId, advertiseAddr, ttlMs)
        }
    }

    /** Graceful drain: drop all of this node's rows so peers fail fast to the push-queue path. */
    suspend fun releaseAll() {
        attached.clear()
        store.releaseAll(nodeId)
    }
}
