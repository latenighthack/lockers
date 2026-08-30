package com.latenighthack.lockers.server.claim

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isNull
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.server.services.room.v1.RoomOwner
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test

/** Store decorator whose failure can be toggled — the "partitioned from Postgres" lever. */
class ToggleableFailingRoomClaimStore(private val delegate: RoomClaimStore) : RoomClaimStore by delegate {
    val failing = AtomicBoolean(false)

    private fun gate() {
        if (failing.get()) throw IOException("simulated pg partition")
    }

    override suspend fun claim(roomId: RoomId, nodeId: String, nodeAddr: String, ttlMs: Long): RoomClaimRow {
        gate()
        return delegate.claim(roomId, nodeId, nodeAddr, ttlMs)
    }

    override suspend fun renewAll(nodeId: String, ttlMs: Long): Set<RoomId> {
        gate()
        return delegate.renewAll(nodeId, ttlMs)
    }

    override suspend fun release(roomId: RoomId, nodeId: String) {
        gate()
        delegate.release(roomId, nodeId)
    }

    override suspend fun releaseAll(nodeId: String) {
        gate()
        delegate.releaseAll(nodeId)
    }

    override suspend fun lookup(roomId: RoomId): RoomClaimRow? {
        gate()
        return delegate.lookup(roomId)
    }

    override suspend fun ping() {
        gate()
        delegate.ping()
    }
}

class ClaimRenewalServiceTest {
    private val registry = SimpleMeterRegistry()
    private val meters = ClaimMetrics(registry)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private fun room(name: String) = RoomId(name.encodeToByteArray())

    @AfterTest
    fun tearDown() {
        scope.cancel()
    }

    private fun harness(
        store: RoomClaimStore,
        ttlMs: Long = 400,
        renewMs: Long = 50,
        onDemoted: suspend () -> Unit = {},
    ): Pair<ClaimRoomOwnership, ClaimRenewalService> {
        val ownership = ClaimRoomOwnership(
            store = store,
            selfNodeId = "n1",
            advertiseAddr = "addr1:1",
            ttlMs = ttlMs,
            renewIntervalMs = renewMs,
            meters = meters,
        )
        val renewal = ClaimRenewalService(
            roomClaims = store,
            ownership = ownership,
            nodeId = "n1",
            ttlMs = ttlMs,
            renewIntervalMs = renewMs,
            meters = meters,
            onDemoted = onDemoted,
        )
        return ownership to renewal
    }

    private suspend fun awaitUntil(timeoutMs: Long = 5_000, condition: suspend () -> Boolean) {
        withTimeout(timeoutMs) {
            while (!condition()) delay(10)
        }
    }

    @Test
    fun `renewal keeps short-TTL claims owned indefinitely`() = runBlocking {
        val store = InMemoryRoomClaimStore()
        val (ownership, renewal) = harness(store, ttlMs = 200, renewMs = 40)
        ownership.resolve(0, room("r1"))
        renewal.start(scope)
        delay(600) // several TTLs
        assertThat(ownership.resolve(0, room("r1"))).isEqualTo(RoomOwner.Local(1))
        renewal.stopAndRelease()
    }

    @Test
    fun `renew shortfall demotes the lost room and fires the eviction hook`() = runBlocking {
        val store = InMemoryRoomClaimStore()
        val demotions = AtomicInteger(0)
        val (ownership, renewal) = harness(store, onDemoted = { demotions.incrementAndGet() })
        ownership.resolve(0, room("r1"))
        // Steal behind the owner's back (as if its claim expired during a pause).
        store.releaseAll("n1")
        store.claim(room("r1"), "thief", "thief:9", ttlMs = 15_000)
        renewal.start(scope)
        awaitUntil { ownership.ownedRooms().isEmpty() }
        assertThat(demotions.get()).isGreaterThan(0)
        assertThat(meters.lost.count()).isEqualTo(1.0)
        assertThat(ownership.resolve(0, room("r1"))).isEqualTo(RoomOwner.Remote("thief:9", 1))
        renewal.stopAndRelease()
    }

    @Test
    fun `failure streak longer than the TTL demotes everything`() = runBlocking {
        val store = ToggleableFailingRoomClaimStore(InMemoryRoomClaimStore())
        val demotions = AtomicInteger(0)
        val (ownership, renewal) = harness(store, ttlMs = 200, renewMs = 40, onDemoted = { demotions.incrementAndGet() })
        ownership.resolve(0, room("r1"))
        ownership.resolve(0, room("r2"))
        renewal.start(scope)
        store.failing.set(true)
        awaitUntil { ownership.ownedRooms().isEmpty() }
        assertThat(demotions.get()).isGreaterThan(0)
        renewal.stopAndRelease()
    }

    @Test
    fun `recovery after a partition does not resurrect ownership by itself`() = runBlocking {
        val store = ToggleableFailingRoomClaimStore(InMemoryRoomClaimStore())
        val (ownership, renewal) = harness(store, ttlMs = 200, renewMs = 40)
        ownership.resolve(0, room("r1"))
        renewal.start(scope)
        store.failing.set(true)
        awaitUntil { ownership.ownedRooms().isEmpty() }
        // While n1 was partitioned its row expired; another node steals it, then the link heals.
        delay(250) // > ttl: n1's unrenewed row is expired
        store.failing.set(false)
        store.claim(room("r1"), "n2", "addr2:2", ttlMs = 15_000)
        // n1's link is back, but it must redirect rather than assume it still owns the room.
        assertThat(ownership.ownedRooms()).isEqualTo(emptySet<RoomId>())
        assertThat(ownership.resolve(0, room("r1"))).isEqualTo(RoomOwner.Remote("addr2:2", 2))
        renewal.stopAndRelease()
    }

    @Test
    fun `stopAndRelease deletes the node's rows for zero-wait handoff`() = runBlocking {
        val store = InMemoryRoomClaimStore()
        val (ownership, renewal) = harness(store)
        ownership.resolve(0, room("r1"))
        renewal.start(scope)
        renewal.stopAndRelease()
        assertThat(store.lookup(room("r1"))).isNull()
        // A successor claims immediately at the next epoch-1 (fresh insert after delete).
        assertThat(store.claim(room("r1"), "n2", "addr2:2", ttlMs = 5_000))
            .isEqualTo(RoomClaimRow("n2", "addr2:2", 1))
    }

    @Test
    fun `a room claimed during the renew round is not demoted as missing`() = runBlocking {
        // Regression guard for the renew/claim race: claim a room *between* renewAll's snapshot
        // and the diff by making the store's renewAll slow.
        val inner = InMemoryRoomClaimStore()
        val claimDuringRenew = AtomicBoolean(false)
        lateinit var ownership: ClaimRoomOwnership
        val store = object : RoomClaimStore by inner {
            override suspend fun renewAll(nodeId: String, ttlMs: Long): Set<RoomId> {
                val renewed = inner.renewAll(nodeId, ttlMs)
                if (claimDuringRenew.compareAndSet(true, false)) {
                    ownership.resolve(0, room("late"))
                }
                delay(20)
                return renewed
            }
        }
        val (o, renewal) = harness(store, ttlMs = 5_000, renewMs = 30)
        ownership = o
        ownership.resolve(0, room("r1"))
        renewal.start(scope)
        claimDuringRenew.set(true)
        awaitUntil { ownership.ownedRooms().contains(room("late")) }
        delay(200) // several renew rounds; "late" must survive them all
        assertThat(ownership.ownedRooms().contains(room("late"))).isEqualTo(true)
        renewal.stopAndRelease()
    }
}
