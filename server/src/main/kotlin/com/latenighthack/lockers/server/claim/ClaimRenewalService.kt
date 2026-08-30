package com.latenighthack.lockers.server.claim

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory

/**
 * The claim heartbeat: every [renewIntervalMs] it extends all of this node's `room_claim` rows in
 * one batched statement, demotes any room the store no longer confirms (expired and stolen while we
 * weren't looking), and runs the session-registry renew round. This loop is claim mode's entire
 * steady-state coordination cost — one statement per node per interval per table.
 *
 * Fencing lives here, not on a fast path: a claim can only be stolen after its TTL expired, and the
 * TTL only expires if this node failed to renew for a full TTL — i.e. it is partitioned from
 * Postgres, at which point it cannot write lockers either (same database). So on a renew-failure
 * streak longer than the TTL every claim is demoted locally ([ClaimRoomOwnership.demoteAll]); a
 * later successful renew never resurrects ownership — demoted rooms re-resolve like cold ones and
 * typically find the thief's valid row. Interleaved writes racing a steal remain covered by the
 * locker-version CAS, the data backstop.
 */
class ClaimRenewalService(
    private val roomClaims: RoomClaimStore,
    private val ownership: ClaimRoomOwnership,
    private val nodeId: String,
    private val ttlMs: Long,
    private val renewIntervalMs: Long,
    private val meters: ClaimMetrics,
    /** Invoked after any demotion so the component can evict room caches. */
    private val onDemoted: suspend () -> Unit = {},
    /** The session-registry renew round (claim mode wires `ClaimSessionRegistry::renewRound`). */
    private val sessionRenewRound: suspend () -> Unit = {},
    /** Session-registry drain, run alongside [RoomClaimStore.releaseAll] on stop. */
    private val sessionReleaseAll: suspend () -> Unit = {},
    private val clock: () -> Long = System::currentTimeMillis,
) {
    private val logger = LoggerFactory.getLogger(ClaimRenewalService::class.java)
    private var job: Job? = null

    fun start(scope: CoroutineScope) {
        check(job == null) { "ClaimRenewalService already started" }
        job = scope.launch {
            var lastSuccess = clock()
            var demotedForStreak = false
            while (isActive) {
                delay(renewIntervalMs)
                try {
                    renewOnce()
                    lastSuccess = clock()
                    demotedForStreak = false
                } catch (t: Exception) {
                    logger.warn("claim renew round failed node={}", nodeId, t)
                    if (!demotedForStreak && clock() - lastSuccess > ttlMs) {
                        // Partitioned from the store for a full TTL: our rows are now stealable, so
                        // stop acting as owner. Ownership is only re-earned through resolve().
                        val lostCount = ownership.ownedRooms().size
                        ownership.demoteAll()
                        if (lostCount > 0) {
                            meters.lost.increment(lostCount.toDouble())
                            onDemoted()
                        }
                        demotedForStreak = true
                    }
                }
            }
        }
    }

    private suspend fun renewOnce() {
        val startedAt = clock()
        val startNanos = System.nanoTime()
        val renewed = roomClaims.renewAll(nodeId, ttlMs)
        meters.recordRenew(System.nanoTime() - startNanos)
        // Diff only rooms claimed before the renew started: a room claimed while the UPDATE was in
        // flight is legitimately absent from RETURNING and must not be demoted.
        val lost = ownership.ownedRoomsClaimedBefore(startedAt) - renewed
        ownership.confirmRenewed(renewed)
        if (lost.isNotEmpty()) {
            for (roomId in lost) {
                logger.info("claim lost roomId={} node={}", roomId, nodeId)
                ownership.demote(roomId)
            }
            meters.lost.increment(lost.size.toDouble())
            onDemoted()
        }
        sessionRenewRound()
    }

    /** Stops the loop WITHOUT releasing rows — crash simulation in tests; prefer [stopAndRelease]. */
    fun stop() {
        job?.cancel()
        job = null
    }

    /** Graceful drain: stop renewing, delete this node's rows so successors claim with no TTL wait. */
    suspend fun stopAndRelease() {
        stop()
        ownership.demoteAll()
        runCatching { roomClaims.releaseAll(nodeId) }
            .onFailure { logger.warn("claim releaseAll failed on drain node={}", nodeId, it) }
        runCatching { sessionReleaseAll() }
            .onFailure { logger.warn("session registry release failed on drain node={}", nodeId, it) }
    }
}
