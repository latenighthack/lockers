package com.latenighthack.lockers.server.claim

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNotEmpty
import assertk.assertions.isTrue
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.server.services.room.v1.RoomOwner
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test

class ClaimRoomOwnershipTest {
    private val registry = SimpleMeterRegistry()
    private val meters = ClaimMetrics(registry)

    /** Store decorator that counts claim round trips so cache behavior is observable. */
    private class CountingStore(private val delegate: RoomClaimStore) : RoomClaimStore by delegate {
        val claims = AtomicInteger(0)
        override suspend fun claim(roomId: RoomId, nodeId: String, nodeAddr: String, ttlMs: Long): RoomClaimRow {
            claims.incrementAndGet()
            return delegate.claim(roomId, nodeId, nodeAddr, ttlMs)
        }
    }

    private fun room(name: String) = RoomId(name.encodeToByteArray())

    private fun ownership(
        store: RoomClaimStore,
        node: String = "n1",
        addr: String = "addr1:1",
        nonOwnerTtlMs: Long = 2_000,
    ) = ClaimRoomOwnership(
        store = store,
        selfNodeId = node,
        advertiseAddr = addr,
        ttlMs = 15_000,
        renewIntervalMs = 5_000,
        meters = meters,
        nonOwnerCacheTtlMs = nonOwnerTtlMs,
    )

    @Test
    fun `first resolve claims and returns Local with the claim epoch`() = runBlocking {
        val ownership = ownership(InMemoryRoomClaimStore())
        assertThat(ownership.resolve(0, room("r1"))).isEqualTo(RoomOwner.Local(1))
        assertThat(meters.acquires.count()).isEqualTo(1.0)
        assertThat(ownership.ownedRooms()).isEqualTo(setOf(room("r1")))
    }

    @Test
    fun `fresh owner cache hits skip the store`() = runBlocking {
        val store = CountingStore(InMemoryRoomClaimStore())
        val ownership = ownership(store)
        ownership.resolve(0, room("r1"))
        repeat(5) { assertThat(ownership.resolve(0, room("r1"))).isEqualTo(RoomOwner.Local(1)) }
        assertThat(store.claims.get()).isEqualTo(1)
    }

    @Test
    fun `keyspace does not affect ownership`() = runBlocking {
        val store = CountingStore(InMemoryRoomClaimStore())
        val ownership = ownership(store)
        assertThat(ownership.resolve(31, room("r1"))).isEqualTo(RoomOwner.Local(1))
        assertThat(ownership.resolve(30, room("r1"))).isEqualTo(RoomOwner.Local(1))
        assertThat(store.claims.get()).isEqualTo(1)
    }

    @Test
    fun `foreign-owned room resolves Remote with the owner's address`() = runBlocking {
        val store = InMemoryRoomClaimStore()
        store.claim(room("r1"), "other", "peer:9", ttlMs = 15_000)
        val ownership = ownership(store)
        assertThat(ownership.resolve(0, room("r1"))).isEqualTo(RoomOwner.Remote("peer:9", 1))
        assertThat(meters.redirects.count()).isEqualTo(1.0)
    }

    @Test
    fun `remote address is never empty across store states`() = runBlocking {
        val store = InMemoryRoomClaimStore()
        val mine = ownership(store, node = "n1", addr = "addr1:1")
        val theirs = ownership(store, node = "n2", addr = "addr2:2")
        val rooms = (0 until 50).map { room("r$it") }
        for ((i, roomId) in rooms.withIndex()) {
            val first = if (i % 2 == 0) mine else theirs
            first.resolve(0, roomId)
            val resolved = (if (i % 2 == 0) theirs else mine).resolve(0, roomId)
            when (resolved) {
                is RoomOwner.Local -> {}
                is RoomOwner.Remote -> assertThat(resolved.address).isNotEmpty()
            }
        }
    }

    @Test
    fun `non-owner cache entries expire so takeovers propagate`() = runBlocking {
        val store = CountingStore(InMemoryRoomClaimStore())
        store.claim(room("r1"), "other", "peer:9", ttlMs = 15_000)
        val ownership = ownership(store, nonOwnerTtlMs = 50)
        ownership.resolve(0, room("r1"))
        ownership.resolve(0, room("r1")) // cached redirect
        val cachedClaims = store.claims.get()
        delay(120)
        ownership.resolve(0, room("r1"))
        assertThat(store.claims.get()).isEqualTo(cachedClaims + 1)
    }

    @Test
    fun `steal of an expired foreign claim counts as steal and bumps epoch`() = runBlocking {
        val clock = longArrayOf(0)
        val store = InMemoryRoomClaimStore { clock[0] }
        store.claim(room("r1"), "other", "peer:9", ttlMs = 100)
        clock[0] = 500 // the foreign claim is expired now
        val ownership = ownership(store)
        assertThat(ownership.resolve(0, room("r1"))).isEqualTo(RoomOwner.Local(2))
        assertThat(meters.steals.count()).isEqualTo(1.0)
    }

    @Test
    fun `demote flips resolve back to the store and to Remote after a steal`() = runBlocking {
        val store = InMemoryRoomClaimStore()
        val ownership = ownership(store)
        ownership.resolve(0, room("r1"))
        // Simulate a steal behind our back, then the renew loop demoting us.
        store.releaseAll("n1")
        store.claim(room("r1"), "thief", "thief:9", ttlMs = 15_000)
        ownership.demote(room("r1"))
        assertThat(ownership.resolve(0, room("r1"))).isEqualTo(RoomOwner.Remote("thief:9", 1))
    }

    @Test
    fun `demoteAll clears the owned set and the gauge`() = runBlocking {
        val ownership = ownership(InMemoryRoomClaimStore())
        ownership.resolve(0, room("r1"))
        ownership.resolve(0, room("r2"))
        ownership.demoteAll()
        assertThat(ownership.ownedRooms()).isEqualTo(emptySet<RoomId>())
        assertThat(registry.get("lockers.claim.rooms.owned").gauge().value()).isEqualTo(0.0)
    }

    @Test
    fun `store failure propagates out of resolve`() = runBlocking {
        val failing = object : RoomClaimStore by InMemoryRoomClaimStore() {
            override suspend fun claim(roomId: RoomId, nodeId: String, nodeAddr: String, ttlMs: Long) =
                throw java.io.IOException("simulated partition")
        }
        val ownership = ownership(failing)
        val result = runCatching { ownership.resolve(0, room("r1")) }
        assertThat(result.isFailure).isTrue()
    }
}
