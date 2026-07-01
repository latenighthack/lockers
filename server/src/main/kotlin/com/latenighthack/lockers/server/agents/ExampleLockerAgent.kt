package com.latenighthack.lockers.server.agents

import com.latenighthack.lockers.common.v1.Locker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId

// Opaque keyspace values this demo agent reacts to / writes. The primitive
// assigns no meaning to keyspaces; these numbers are chosen by this example.
const val LOBBY_KEYSPACE = 31L
const val GAME_KEYSPACE = 30L

/**
 * Minimal illustrative [LockerAgentRegistry]. [RoomServiceImpl] invokes it on
 * every locker change; this example only acts on the LOBBY keyspace, deriving a
 * single GAME-keyspace locker that mirrors the payload (which the room service
 * then persists and broadcasts). Replace with real server-authoritative logic
 * (a game engine, validation, moderation, etc.) — the plumbing stays the same,
 * and keyspaces stay opaque to the core.
 */
class ExampleLockerAgent : LockerAgentRegistry {
    override suspend fun processPayload(
        roomId: RoomId,
        lockerId: LockerId,
        locker: Locker
    ): List<LockerAgentRegistry.LockerWrite> {
        if (lockerId.keyspace?.value != LOBBY_KEYSPACE) return emptyList()

        val derivedId = LockerId(
            rawValue = lockerId.rawValue,
            keyspace = LockerKeyspace { value = GAME_KEYSPACE }
        )
        val derived = Locker {
            open {
                encodedPayload = locker.open?.encodedPayload ?: ByteArray(0)
            }
        }
        return listOf(LockerAgentRegistry.LockerWrite(derivedId, derived))
    }
}
