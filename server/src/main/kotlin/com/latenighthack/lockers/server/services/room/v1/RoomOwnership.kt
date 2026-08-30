package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.lockers.common.v1.RoomId

/**
 * Decides whether THIS node coordinates writes for a room shard `(keyspace, roomId)`. The room
 * ring shards by `(keyspace, roomId)` so a busy keyspace within a room can be spread and scaled
 * independently. The monolith uses [LocalRoomOwnership] (always local); a cluster injects a
 * ring-backed implementation so non-owner writes are rejected with a redirect to the owner.
 */
interface RoomOwnership {
    suspend fun resolve(keyspace: Long, roomId: RoomId): RoomOwner
}

sealed interface RoomOwner {
    /**
     * This node owns the room; process the write locally. [epoch] is the ownership fencing token
     * (the claim epoch under claim-based ownership; 0 in the monolith and on the unfenced ring).
     */
    data class Local(val epoch: Long = 0) : RoomOwner

    /** Another node owns the shard; the client should retry against [address]. */
    data class Remote(val address: String, val epoch: Long) : RoomOwner
}

/** Single-node / monolith default: every room is owned locally, so writes never redirect. */
class LocalRoomOwnership : RoomOwnership {
    override suspend fun resolve(keyspace: Long, roomId: RoomId): RoomOwner = RoomOwner.Local()
}
