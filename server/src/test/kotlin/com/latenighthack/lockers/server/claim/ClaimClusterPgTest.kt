package com.latenighthack.lockers.server.claim

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNotEmpty
import assertk.assertions.isTrue
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.lockers.common.v1.Locker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.room.v1.PostLockerChangeRequest
import com.latenighthack.lockers.room.v1.PostLockerChangeResponse
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

/**
 * Redirect + failover from the two-node suite re-run over the real Jdbc claim/registry stores
 * (gated — see [PgTestGate]): proves the production SQL and the harness compose. The full matrix
 * runs on the in-memory stores in [ClaimClusterTest]; the store semantics themselves are covered
 * by the contract suites.
 */
class ClaimClusterPgTest {
    private lateinit var pool: ClaimJdbcPool

    @BeforeTest
    fun setUp() {
        val url = PgTestGate.urlOrSkip()
        runBlocking {
            pool = ClaimJdbcPool(url)
            JdbcRoomClaimStore(pool).prepare()
            JdbcSessionGatewayStore(pool).prepare()
            pool.withConnection { conn ->
                conn.createStatement().use { it.execute("TRUNCATE room_claim, session_gateway") }
            }
        }
    }

    @AfterTest
    fun tearDown() {
        if (::pool.isInitialized) pool.close()
    }

    private fun room(name: String) = RoomId(name.encodeToByteArray())

    private fun post(roomName: String, lockerRaw: Byte = 1) = PostLockerChangeRequest {
        roomId = room(roomName)
        lockerId = LockerId {
            rawValue = byteArrayOf(lockerRaw)
            keyspace = LockerKeyspace { value = 1 }
        }
        locker = Locker { open { encodedPayload = byteArrayOf(1) } }
    }

    private suspend fun twoPgNodes(ttlMs: Long = 500, renewMs: Long = 100): Pair<ClaimNode, ClaimNode> {
        val delegate = InMemoryStoreDelegate()
        val roomClaims = JdbcRoomClaimStore(pool)
        val sessionGateways = JdbcSessionGatewayStore(pool)
        val node1 = startClaimNode("node1", delegate, roomClaims, sessionGateways, ttlMs, renewMs)
        val node2 = startClaimNode("node2", delegate, roomClaims, sessionGateways, ttlMs, renewMs)
        return node1 to node2
    }

    @Test
    fun `redirects carry the owner's address over the real store`() = runBlocking {
        val (node1, node2) = twoPgNodes()
        try {
            val first = node1.roomClient().postLockerChange(post("pg-r1"))
            assertThat(first.result is PostLockerChangeResponse.Result.OK).isTrue()

            val second = node2.roomClient().postLockerChange(post("pg-r1"))
            assertThat(second.result is PostLockerChangeResponse.Result.NOT_OWNER).isTrue()
            assertThat(second.redirect!!.ownerAddress).isNotEmpty()
            assertThat(second.redirect!!.ownerAddress).isEqualTo(node1.addr)
        } finally {
            node1.stopGracefully()
            node2.stopGracefully()
        }
    }

    @Test
    fun `a crashed owner's room fails over with a bumped epoch over the real store`() = runBlocking {
        val (node1, node2) = twoPgNodes()
        val roomClaims = JdbcRoomClaimStore(pool)
        try {
            assertThat(
                node1.roomClient().postLockerChange(post("pg-failover")).result
                    is PostLockerChangeResponse.Result.OK
            ).isTrue()
            node1.crash()

            // Fresh locker per attempt: the version CAS must not mask the ownership result.
            var attempt = 0
            withTimeout(10_000) {
                while (true) {
                    val request = post("pg-failover", lockerRaw = (100 + attempt).toByte())
                    attempt++
                    val response =
                        runCatching { node2.roomClient().postLockerChange(request) }.getOrNull()
                    if (response?.result is PostLockerChangeResponse.Result.OK) break
                    delay(50)
                }
            }
            val row = roomClaims.lookup(room("pg-failover"))!!
            assertThat(row.nodeId).isEqualTo("node2")
            assertThat(row.epoch).isEqualTo(2L)
        } finally {
            runCatching { node1.stopGracefully() }
            node2.stopGracefully()
        }
    }
}
