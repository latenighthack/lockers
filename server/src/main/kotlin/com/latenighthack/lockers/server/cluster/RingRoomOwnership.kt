package com.latenighthack.lockers.server.cluster

import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.server.services.room.v1.RoomOwner
import com.latenighthack.lockers.server.services.room.v1.RoomOwnership
import com.latenighthack.lockers.sharding.Keyspace
import com.latenighthack.lockers.sharding.ShardRouter

/**
 * Ring-backed [RoomOwnership]: resolves the room ring for `(keyspace, roomId)` and reports whether
 * this node owns the shard. When it doesn't, it carries the owning node's address + epoch so the
 * write can be answered with `NOT_OWNER` + `ShardRedirect`.
 *
 * When an [OwnerLifecycle] is supplied (M5), local ownership is additionally lease-checked: the
 * write only proceeds if this node routes-local **and** holds a valid, non-preempted [ShardLease]
 * for the room's shard. During a fenced handoff the map may briefly still say "local" on the old
 * owner after its lease was released/preempted; gating on the lease closes that window — the write
 * is redirected to the epoch's authoritative owner instead of racing the new one (the CAS on
 * locker version is the final backstop). With no lifecycle (or on the acquiring owner) the
 * route-local decision stands, so the monolith and single-owner cluster paths are unchanged.
 */
class RingRoomOwnership(
    private val router: ShardRouter,
    private val lifecycle: OwnerLifecycle? = null,
) : RoomOwnership {
    override suspend fun resolve(keyspace: Long, roomId: RoomId): RoomOwner {
        val ks = Keyspace(keyspace)
        val route = router.routeRoom(ks, roomId.rawValue)
        val holdsLease = lifecycle == null ||
            lifecycle.leaseFor(ks, router.roomMap().shard(ks, roomId.rawValue)) != null
        return if (route.isLocal && holdsLease) {
            RoomOwner.Local
        } else {
            RoomOwner.Remote(
                address = route.address?.let { "${it.host}:${it.port}" } ?: "",
                epoch = route.epoch.value,
            )
        }
    }
}
