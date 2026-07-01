package com.latenighthack.lockers.server.agents

import com.latenighthack.lockers.common.v1.Locker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.RoomId

interface LockerAgentRegistry {
    data class LockerWrite(val lockerId: LockerId, val locker: Locker)

    suspend fun processPayload(roomId: RoomId, lockerId: LockerId, locker: Locker): List<LockerWrite>
}
