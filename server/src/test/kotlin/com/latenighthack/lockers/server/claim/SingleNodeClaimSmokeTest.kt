package com.latenighthack.lockers.server.claim

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isTrue
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.lockers.common.v1.Locker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.room.v1.PostLockerChangeRequest
import com.latenighthack.lockers.room.v1.PostLockerChangeResponse
import kotlinx.coroutines.runBlocking
import kotlin.test.Test

/**
 * Design-doc test 7: a single node in claim mode behaves like the monolith — every claim resolves
 * local on first contact, so no `NOT_OWNER` is ever emitted and the rooms-owned gauge fills up.
 */
class SingleNodeClaimSmokeTest {
    @Test
    fun `single-node claim mode never redirects`() = runBlocking {
        val node = startClaimNode(
            nodeId = "solo",
            delegate = InMemoryStoreDelegate(),
            roomClaims = InMemoryRoomClaimStore(),
            sessionGateways = InMemorySessionGatewayStore(),
            ttlMs = 15_000,
            renewMs = 5_000,
        )
        try {
            repeat(5) { i ->
                val response = node.roomClient().postLockerChange(PostLockerChangeRequest {
                    roomId = RoomId("room-$i".encodeToByteArray())
                    lockerId = LockerId {
                        rawValue = byteArrayOf(1)
                        keyspace = LockerKeyspace { value = 1 }
                    }
                    locker = Locker { open { encodedPayload = byteArrayOf(1) } }
                })
                assertThat(response.result is PostLockerChangeResponse.Result.OK).isTrue()
            }
            assertThat(
                node.meterRegistry.get("lockers.claim.rooms.owned").gauge().value()
            ).isEqualTo(5.0)
            assertThat(node.meterRegistry.get("lockers.claim.acquires").counter().count()).isEqualTo(5.0)
        } finally {
            node.stopGracefully()
        }
    }
}
