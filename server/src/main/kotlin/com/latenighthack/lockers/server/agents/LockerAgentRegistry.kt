package com.latenighthack.lockers.server.agents

import com.latenighthack.lockers.common.v1.Locker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.RoomId

interface LockerAgentRegistry {
    /** Safe default: derived writes require explicit installation of trusted server logic. */
    object None : IdempotentLockerAgentRegistry {
        override val agentVersion = "lockers.none/v1"
        override suspend fun processPayload(effectKey: ByteArray, roomId: RoomId, lockerId: LockerId, locker: Locker) = emptyList<LockerWrite>()
        override suspend fun processPayload(roomId: RoomId, lockerId: LockerId, locker: Locker) = emptyList<LockerWrite>()
    }

    data class LockerWrite(val lockerId: LockerId, val locker: Locker)

    suspend fun processPayload(roomId: RoomId, lockerId: LockerId, locker: Locker): List<LockerWrite>
}

/**
 * Explicit recovery contract. Repeated calls with the same effectKey and immutable inputs must
 * converge to the same external effect and derived writes, including after process termination.
 * Change agentVersion whenever that interpretation changes. The version is bounded to128 UTF8 bytes.
 */
interface IdempotentLockerAgentRegistry : LockerAgentRegistry {
    val agentVersion: String
    suspend fun processPayload(effectKey: ByteArray, roomId: RoomId, lockerId: LockerId, locker: Locker): List<LockerAgentRegistry.LockerWrite>
    override suspend fun processPayload(roomId: RoomId, lockerId: LockerId, locker: Locker): List<LockerAgentRegistry.LockerWrite> =
        throw UnsupportedOperationException("Durable agents require an immutable effect key")
}
