package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.server.services.room.v1.RoomOwner
import com.latenighthack.lockers.server.services.room.v1.RoomOwnership
import io.github.reactivecircus.cache4k.Cache
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.milliseconds

/**
 * Claim-backed [RoomOwnership]: a room is owned by whichever node holds its live `room_claim` row.
 * First contact wins — an unclaimed or expired room is claimed by the node that receives its first
 * write; no placement policy. `keyspace` is deliberately ignored: a room is claimed whole so its
 * server-side game agent has exactly one runner (the ring's (keyspace, room) split defeated that).
 *
 * Caching: owner entries are trusted for `min(ttl/3, renewInterval)` (the renew loop keeps the row
 * alive and [confirmRenewed] re-freshens them); non-owner redirect rows for [nonOwnerCacheTtlMs]
 * (≤2s) so takeovers propagate quickly. [RoomOwner.Remote.address] always comes from a claim row's
 * `node_addr`, which is NOT NULL and boot-validated — the ring's dead-end `""` redirect cannot
 * occur by construction.
 *
 * A store failure propagates out of [resolve]: if this node cannot reach the claim store it cannot
 * reach the locker store either (same substrate), so failing the write is both honest and safe.
 */
class ClaimRoomOwnership(
    private val store: RoomClaimStore,
    private val selfNodeId: String,
    private val advertiseAddr: String,
    private val ttlMs: Long,
    renewIntervalMs: Long,
    private val meters: ClaimMetrics,
    private val nonOwnerCacheTtlMs: Long = NON_OWNER_CACHE_TTL_MS,
    private val clock: () -> Long = System::currentTimeMillis,
) : RoomOwnership {
    private data class Owned(val epoch: Long, val freshUntil: Long, val claimedAt: Long)

    private val logger = LoggerFactory.getLogger(ClaimRoomOwnership::class.java)
    private val ownerFreshMs = minOf(ttlMs / OWNER_FRESH_TTL_DIVISOR, renewIntervalMs)
    private val owned = ConcurrentHashMap<RoomId, Owned>()
    private val redirects = Cache.Builder<RoomId, RoomClaimRow>()
        .expireAfterWrite(nonOwnerCacheTtlMs.milliseconds)
        .maximumCacheSize(REDIRECT_CACHE_SIZE)
        .build()

    override suspend fun resolve(keyspace: Long, roomId: RoomId): RoomOwner {
        val now = clock()
        owned[roomId]?.let { if (now < it.freshUntil) return RoomOwner.Local(it.epoch) }
        redirects.get(roomId)?.let {
            meters.redirects.increment()
            return RoomOwner.Remote(it.nodeAddr, it.epoch)
        }
        val row = store.claim(roomId, selfNodeId, advertiseAddr, ttlMs)
        return if (row.nodeId == selfNodeId) {
            val previous = owned.put(roomId, Owned(row.epoch, now + ownerFreshMs, claimedAt = now))
            if (previous == null) {
                meters.acquires.increment()
                if (row.epoch > 1) {
                    meters.steals.increment()
                    logger.info("claim steal roomId={} epoch={} node={}", roomId, row.epoch, selfNodeId)
                } else {
                    logger.info("claim acquire roomId={} epoch={} node={}", roomId, row.epoch, selfNodeId)
                }
                meters.onRoomsOwned(owned.size)
            }
            RoomOwner.Local(row.epoch)
        } else {
            demote(roomId)
            redirects.put(roomId, row)
            meters.redirects.increment()
            RoomOwner.Remote(row.nodeAddr, row.epoch)
        }
    }

    /** Snapshot of the rooms this node currently believes it owns. */
    fun ownedRooms(): Set<RoomId> = owned.keys.toSet()

    /**
     * Rooms owned as of [beforeMs] — the renew loop diffs against this rather than [ownedRooms] so
     * a room claimed *while* the batched renew was in flight is never demoted as "missing".
     */
    fun ownedRoomsClaimedBefore(beforeMs: Long): Set<RoomId> =
        owned.entries.filter { it.value.claimedAt < beforeMs }.map { it.key }.toSet()

    /** Re-freshens owner cache entries whose rows the renew round just extended. */
    fun confirmRenewed(rooms: Set<RoomId>) {
        val freshUntil = clock() + ownerFreshMs
        for (roomId in rooms) {
            owned.computeIfPresent(roomId) { _, entry -> entry.copy(freshUntil = freshUntil) }
        }
    }

    /** Drops local ownership of [roomId]; the next write re-resolves against the store. */
    fun demote(roomId: RoomId) {
        if (owned.remove(roomId) != null) {
            logger.info("claim demote roomId={} node={}", roomId, selfNodeId)
            meters.onRoomsOwned(owned.size)
        }
    }

    /** Drops all local ownership (partitioned from the store longer than a TTL). */
    fun demoteAll() {
        if (owned.isEmpty()) return
        logger.info("claim demote-all count={} node={}", owned.size, selfNodeId)
        owned.clear()
        meters.onRoomsOwned(0)
    }

    companion object {
        private const val NON_OWNER_CACHE_TTL_MS = 2_000L
        private const val OWNER_FRESH_TTL_DIVISOR = 3
        private const val REDIRECT_CACHE_SIZE = 10_000L
    }
}
