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
 */
class RingRoomOwnership(private val router: ShardRouter) : RoomOwnership {
    override suspend fun resolve(keyspace: Long, roomId: RoomId): RoomOwner {
        val route = router.routeRoom(Keyspace(keyspace), roomId.rawValue)
        return if (route.isLocal) {
            RoomOwner.Local
        } else {
            RoomOwner.Remote(
                address = route.address?.let { "${it.host}:${it.port}" } ?: "",
                epoch = route.epoch.value,
            )
        }
    }
}
