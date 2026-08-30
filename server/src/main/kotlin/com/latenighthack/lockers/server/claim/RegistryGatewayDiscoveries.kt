package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.push.v1.LocalPushGatewayServiceRpc
import com.latenighthack.lockers.push.v1.PushGatewayServer
import com.latenighthack.lockers.push.v1.PushGatewayService
import com.latenighthack.lockers.push.v1.PushGatewayServiceRpc
import com.latenighthack.lockers.server.cluster.PeerConnectionPool
import com.latenighthack.lockers.server.services.push.v1.PushGatewayDiscovery
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.session.v1.LocalSessionGatewayServiceRpc
import com.latenighthack.lockers.session.v1.SessionGatewayServer
import com.latenighthack.lockers.session.v1.SessionGatewayService
import com.latenighthack.lockers.session.v1.SessionGatewayServiceRpc
import com.latenighthack.lockers.sharding.PeerAddress
import io.github.reactivecircus.cache4k.Cache
import kotlin.time.Duration.Companion.milliseconds

/** Parses a claim row's `node_addr` ("host:port", NOT NULL by schema) into a [PeerAddress]. */
internal fun parsePeerAddress(nodeAddr: String): PeerAddress {
    val host = nodeAddr.substringBeforeLast(':')
    val port = nodeAddr.substringAfterLast(':').toIntOrNull()
    require(host.isNotBlank() && port != null) { "malformed node_addr '$nodeAddr' (want host:port)" }
    return PeerAddress(host, port)
}

/**
 * Shared `session_gateway` lookup with a short-TTL cache: a session's socket rarely moves, but a
 * reconnect can move it to another node at any time, so rows are only trusted for [cacheTtlMs].
 * A null result means no live row — the session is offline and the caller falls back to the
 * push-queue path (the pre-existing null-discovery semantics).
 */
internal class SessionGatewayLookup(
    private val store: SessionGatewayStore,
    private val meters: ClaimMetrics,
    cacheTtlMs: Long,
) {
    private val cache = Cache.Builder<SessionId, SessionGatewayRow>()
        .expireAfterWrite(cacheTtlMs.milliseconds)
        .maximumCacheSize(CACHE_SIZE)
        .build()

    suspend fun find(sessionId: SessionId): SessionGatewayRow? {
        cache.get(sessionId)?.let { return it }
        val row = store.lookup(sessionId)
        if (row == null) {
            meters.registryMisses.increment()
        } else {
            cache.put(sessionId, row)
        }
        return row
    }

    companion object {
        private const val CACHE_SIZE = 10_000L
    }
}

/**
 * Claim-mode [SessionGatewayDiscovery]: resolves the node holding a session's WebSocket from the
 * `session_gateway` registry (replacing the session ring). A local row short-circuits to the
 * in-process gateway; a remote row dials the peer over the existing east-west transport.
 */
class RegistrySessionGatewayDiscovery(
    private val local: SessionGatewayServer,
    store: SessionGatewayStore,
    private val pool: PeerConnectionPool,
    private val selfNodeId: String,
    meters: ClaimMetrics,
    cacheTtlMs: Long = DEFAULT_CACHE_TTL_MS,
) : SessionGatewayDiscovery {
    private val lookup = SessionGatewayLookup(store, meters, cacheTtlMs)

    override suspend fun findServer(sessionId: SessionId): SessionGatewayService? {
        val row = lookup.find(sessionId) ?: return null
        return if (row.nodeId == selfNodeId) {
            LocalSessionGatewayServiceRpc(local)
        } else {
            SessionGatewayServiceRpc(pool.clientFor(parsePeerAddress(row.nodeAddr)))
        }
    }

    companion object {
        const val DEFAULT_CACHE_TTL_MS = 5_000L
    }
}

/** The push mirror — push delivery is also keyed by `sessionId`, over the same registry table. */
class RegistryPushGatewayDiscovery(
    private val local: PushGatewayServer,
    store: SessionGatewayStore,
    private val pool: PeerConnectionPool,
    private val selfNodeId: String,
    meters: ClaimMetrics,
    cacheTtlMs: Long = RegistrySessionGatewayDiscovery.DEFAULT_CACHE_TTL_MS,
) : PushGatewayDiscovery {
    private val lookup = SessionGatewayLookup(store, meters, cacheTtlMs)

    override suspend fun findServer(sessionId: SessionId): PushGatewayService? {
        val row = lookup.find(sessionId) ?: return LocalPushGatewayServiceRpc(local)
        return if (row.nodeId == selfNodeId) {
            LocalPushGatewayServiceRpc(local)
        } else {
            PushGatewayServiceRpc(pool.clientFor(parsePeerAddress(row.nodeAddr)))
        }
    }
}
