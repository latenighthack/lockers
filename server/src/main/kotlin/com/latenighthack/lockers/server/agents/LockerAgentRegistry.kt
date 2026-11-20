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

/**
 * Agents whose derived writes can share the source write's transaction.
 *
 * [prepare] must have no external effect: the room service may call it before the source write
 * is accepted, discard its result (a rejected or conflicting write, an aborted commit), or call it
 * again for the same input. Its state changes belong in [Prepared.commit], which runs inside the
 * room's write transaction on the server's shared database, so they commit or roll back with the
 * source write and the derived lockers. When the room service cannot apply a prepared result in
 * the same transaction, it uses the journaled [processPayload] path instead, which by default
 * prepares and commits immediately.
 */
interface TransactionalLockerAgentRegistry : LockerAgentRegistry {
    class Prepared(val writes: List<LockerAgentRegistry.LockerWrite>, val commit: suspend () -> Unit = {})

    suspend fun prepare(roomId: RoomId, lockerId: LockerId, locker: Locker): Prepared

    override suspend fun processPayload(roomId: RoomId, lockerId: LockerId,
        locker: Locker): List<LockerAgentRegistry.LockerWrite> =
        prepare(roomId, lockerId, locker).let { prepared -> prepared.commit(); prepared.writes }
}
