package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.server.cluster.PeerConnectionPool

/**
 * Everything claim-based multi-node mode needs, bundled for `MonolithComponent`. Deliberately not
 * a `ClusterContext`: claim mode has no ring, no shard map and no coordinator — just the two
 * claim/registry stores, this node's identity, and the east-west connection pool. Constructed in
 * `Main` (production, JDBC stores) or by the test harness (in-memory stores).
 */
class ClaimContext(
    val nodeId: String,
    val advertiseAddr: String,
    val roomClaims: RoomClaimStore,
    val sessionGateways: SessionGatewayStore,
    val pool: PeerConnectionPool,
    val ttlMs: Long,
    val renewMs: Long,
    val meters: ClaimMetrics,
)
