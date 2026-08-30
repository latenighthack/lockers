package com.latenighthack.lockers.server.claim

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNull
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.common.v1.SessionId
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test

/**
 * Semantic contract for [RoomClaimStore]: steal only after expiry, epoch bumps only on takeover,
 * owner-guarded release, exact renew sets. Run against [InMemoryRoomClaimStore] everywhere (keeps
 * the test double honest) and against [JdbcRoomClaimStore] on a real Postgres (the ON CONFLICT
 * semantics are the product). Expiry is exercised with short real TTLs — the store's own clock
 * (`now()` on Postgres) is the only clock authority, so tests never inject timestamps.
 */
abstract class RoomClaimStoreContract {
    protected abstract val store: RoomClaimStore

    private fun room(name: String) = RoomId(name.encodeToByteArray())

    @Test
    fun `fresh claim inserts epoch 1 and returns self`() = runBlocking {
        val row = store.claim(room("r1"), "n1", "addr1:1", ttlMs = 5_000)
        assertThat(row).isEqualTo(RoomClaimRow("n1", "addr1:1", 1))
    }

    @Test
    fun `claim while owner is valid does not steal`() = runBlocking {
        store.claim(room("r1"), "n1", "addr1:1", ttlMs = 5_000)
        val row = store.claim(room("r1"), "n2", "addr2:2", ttlMs = 5_000)
        assertThat(row).isEqualTo(RoomClaimRow("n1", "addr1:1", 1))
    }

    @Test
    fun `claim after expiry steals with a strictly higher epoch`() = runBlocking {
        store.claim(room("r1"), "n1", "addr1:1", ttlMs = SHORT_TTL_MS)
        delay(EXPIRY_WAIT_MS)
        val row = store.claim(room("r1"), "n2", "addr2:2", ttlMs = 5_000)
        assertThat(row).isEqualTo(RoomClaimRow("n2", "addr2:2", 2))
    }

    @Test
    fun `self reclaim extends expiry without bumping the epoch`() = runBlocking {
        store.claim(room("r1"), "n1", "addr1:1", ttlMs = SHORT_TTL_MS)
        delay(SHORT_TTL_MS / 2)
        val reclaimed = store.claim(room("r1"), "n1", "addr1:1", ttlMs = 5_000)
        assertThat(reclaimed).isEqualTo(RoomClaimRow("n1", "addr1:1", 1))
        // Past the original expiry: the extension must hold off a steal.
        delay(SHORT_TTL_MS)
        val other = store.claim(room("r1"), "n2", "addr2:2", ttlMs = 5_000)
        assertThat(other).isEqualTo(RoomClaimRow("n1", "addr1:1", 1))
    }

    @Test
    fun `renewAll returns exactly the still-owned set`() = runBlocking {
        store.claim(room("r1"), "n1", "addr1:1", ttlMs = 5_000)
        store.claim(room("r2"), "n1", "addr1:1", ttlMs = 5_000)
        store.claim(room("r3"), "n2", "addr2:2", ttlMs = 5_000)
        assertThat(store.renewAll("n1", ttlMs = 5_000)).isEqualTo(setOf(room("r1"), room("r2")))
        assertThat(store.renewAll("n3", ttlMs = 5_000)).isEqualTo(emptySet<RoomId>())
    }

    @Test
    fun `renew keeps a short-TTL claim alive across its original expiry`() = runBlocking {
        store.claim(room("r1"), "n1", "addr1:1", ttlMs = SHORT_TTL_MS)
        delay(SHORT_TTL_MS / 2)
        assertThat(store.renewAll("n1", ttlMs = 5_000)).isEqualTo(setOf(room("r1")))
        delay(SHORT_TTL_MS)
        assertThat(store.renewAll("n1", ttlMs = 5_000)).isEqualTo(setOf(room("r1")))
    }

    @Test
    fun `renew after a steal excludes the stolen room`() = runBlocking {
        store.claim(room("stolen"), "n1", "addr1:1", ttlMs = SHORT_TTL_MS)
        store.claim(room("kept"), "n1", "addr1:1", ttlMs = 30_000)
        delay(EXPIRY_WAIT_MS)
        store.claim(room("stolen"), "n2", "addr2:2", ttlMs = 5_000)
        assertThat(store.renewAll("n1", ttlMs = 5_000)).isEqualTo(setOf(room("kept")))
    }

    @Test
    fun `release deletes only the caller's own row`() = runBlocking {
        store.claim(room("r1"), "n1", "addr1:1", ttlMs = 5_000)
        store.release(room("r1"), "n2")
        assertThat(store.lookup(room("r1"))).isEqualTo(RoomClaimRow("n1", "addr1:1", 1))
        store.release(room("r1"), "n1")
        assertThat(store.lookup(room("r1"))).isNull()
    }

    @Test
    fun `release after a steal is a no-op`() = runBlocking {
        store.claim(room("r1"), "n1", "addr1:1", ttlMs = SHORT_TTL_MS)
        delay(EXPIRY_WAIT_MS)
        store.claim(room("r1"), "n2", "addr2:2", ttlMs = 5_000)
        store.release(room("r1"), "n1")
        assertThat(store.lookup(room("r1"))).isEqualTo(RoomClaimRow("n2", "addr2:2", 2))
    }

    @Test
    fun `releaseAll drops every row of the node and nothing else`() = runBlocking {
        store.claim(room("r1"), "n1", "addr1:1", ttlMs = 5_000)
        store.claim(room("r2"), "n1", "addr1:1", ttlMs = 5_000)
        store.claim(room("r3"), "n2", "addr2:2", ttlMs = 5_000)
        store.releaseAll("n1")
        assertThat(store.lookup(room("r1"))).isNull()
        assertThat(store.lookup(room("r2"))).isNull()
        assertThat(store.lookup(room("r3"))).isEqualTo(RoomClaimRow("n2", "addr2:2", 1))
    }

    @Test
    fun `concurrent claims on one expired room elect exactly one winner`() = runBlocking {
        val roomId = room("contested")
        store.claim(roomId, "n0", "addr0:0", ttlMs = SHORT_TTL_MS)
        delay(EXPIRY_WAIT_MS)
        val rows = (0 until 32).map { i ->
            async(Dispatchers.Default) {
                store.claim(roomId, "n${i % 2 + 1}", "addr${i % 2 + 1}:0", ttlMs = 30_000)
            }
        }.awaitAll()
        // Exactly one steal happened: every caller (winner via self-view, losers via redirect row)
        // sees the same owner at epoch 2, and the stored row agrees.
        val winner = rows.first()
        assertThat(rows.toSet()).isEqualTo(setOf(winner))
        assertThat(winner.epoch).isEqualTo(2L)
        assertThat(store.lookup(roomId)).isEqualTo(winner)
    }

    @Test
    fun `epochs are strictly monotonic across successive takeovers`() = runBlocking {
        val roomId = room("churny")
        var lastEpoch = 0L
        repeat(4) { i ->
            val row = store.claim(roomId, "n$i", "addr$i:0", ttlMs = SHORT_TTL_MS)
            assertThat(row.nodeId).isEqualTo("n$i")
            assertThat(row.epoch).isEqualTo(lastEpoch + 1)
            lastEpoch = row.epoch
            delay(EXPIRY_WAIT_MS)
        }
    }

    companion object {
        const val SHORT_TTL_MS = 200L
        const val EXPIRY_WAIT_MS = 450L
    }
}

/** Semantic contract for [SessionGatewayStore]: unconditional move, TTL-visible lookup. */
abstract class SessionGatewayStoreContract {
    protected abstract val store: SessionGatewayStore

    private fun session(name: String) = SessionId(name.encodeToByteArray())

    @Test
    fun `upsert registers and lookup resolves the live row`() = runBlocking {
        store.upsert(session("s1"), "n1", "addr1:1", ttlMs = 5_000)
        assertThat(store.lookup(session("s1"))).isEqualTo(SessionGatewayRow("n1", "addr1:1"))
    }

    @Test
    fun `upsert moves a session between nodes unconditionally`() = runBlocking {
        store.upsert(session("s1"), "n1", "addr1:1", ttlMs = 5_000)
        store.upsert(session("s1"), "n2", "addr2:2", ttlMs = 5_000)
        assertThat(store.lookup(session("s1"))).isEqualTo(SessionGatewayRow("n2", "addr2:2"))
    }

    @Test
    fun `expired rows are invisible to lookup`() = runBlocking {
        store.upsert(session("s1"), "n1", "addr1:1", ttlMs = RoomClaimStoreContract.SHORT_TTL_MS)
        delay(RoomClaimStoreContract.EXPIRY_WAIT_MS)
        assertThat(store.lookup(session("s1"))).isNull()
    }

    @Test
    fun `renewAll extends exactly the node's live rows`() = runBlocking {
        store.upsert(session("s1"), "n1", "addr1:1", ttlMs = RoomClaimStoreContract.SHORT_TTL_MS)
        store.upsert(session("s2"), "n2", "addr2:2", ttlMs = 5_000)
        assertThat(store.renewAll("n1", ttlMs = 5_000)).isEqualTo(setOf(session("s1")))
        delay(RoomClaimStoreContract.EXPIRY_WAIT_MS)
        assertThat(store.lookup(session("s1"))).isEqualTo(SessionGatewayRow("n1", "addr1:1"))
    }

    @Test
    fun `delete only removes the row when the node still holds it`() = runBlocking {
        store.upsert(session("s1"), "n1", "addr1:1", ttlMs = 5_000)
        store.delete(session("s1"), "n2")
        assertThat(store.lookup(session("s1"))).isEqualTo(SessionGatewayRow("n1", "addr1:1"))
        store.delete(session("s1"), "n1")
        assertThat(store.lookup(session("s1"))).isNull()
    }

    @Test
    fun `releaseAll drops the node's rows only`() = runBlocking {
        store.upsert(session("s1"), "n1", "addr1:1", ttlMs = 5_000)
        store.upsert(session("s2"), "n2", "addr2:2", ttlMs = 5_000)
        store.releaseAll("n1")
        assertThat(store.lookup(session("s1"))).isNull()
        assertThat(store.lookup(session("s2"))).isEqualTo(SessionGatewayRow("n2", "addr2:2"))
    }
}
