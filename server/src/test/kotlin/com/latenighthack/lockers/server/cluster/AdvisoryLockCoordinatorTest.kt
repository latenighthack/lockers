package com.latenighthack.lockers.server.cluster

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isTrue
import com.latenighthack.lockers.sharding.Epoch
import com.latenighthack.lockers.sharding.Keyspace
import com.latenighthack.lockers.sharding.ShardId
import kotlinx.coroutines.test.runTest
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.Test

/**
 * Coverage for the advisory-lock coordinator's pure key derivation and its acquire/release
 * protocol, exercised over an in-memory [AdvisoryLockGateway] that models Postgres's "one holder
 * per key, released when the session closes" semantics — no database required.
 */
class AdvisoryLockCoordinatorTest {
    // --- advisoryKey: pure, deterministic, per-(keyspace,shard) ---------------------------------

    @Test
    fun advisoryKeyIsDeterministic() {
        val a = AdvisoryLockCoordinator.advisoryKey(Keyspace(1), ShardId(7))
        val b = AdvisoryLockCoordinator.advisoryKey(Keyspace(1), ShardId(7))
        assertThat(a).isEqualTo(b)
    }

    @Test
    fun advisoryKeyDistinguishesShardWithinKeyspace() {
        val s0 = AdvisoryLockCoordinator.advisoryKey(Keyspace(1), ShardId(0))
        val s1 = AdvisoryLockCoordinator.advisoryKey(Keyspace(1), ShardId(1))
        assertThat(s0 == s1).isFalse()
    }

    @Test
    fun advisoryKeyFoldsKeyspaceSoSameShardDiffersAcrossKeyspaces() {
        // Shard 3 of keyspace 1 and shard 3 of keyspace 2 must not collide onto one lock.
        val k1 = AdvisoryLockCoordinator.advisoryKey(Keyspace(1), ShardId(3))
        val k2 = AdvisoryLockCoordinator.advisoryKey(Keyspace(2), ShardId(3))
        assertThat(k1 == k2).isFalse()
    }

    @Test
    fun advisoryKeyGoldenVector() {
        // Pin the derivation so an accidental hash change (which would silently re-key every lock
        // and break mutual exclusion during a rolling deploy) is caught.
        assertThat(AdvisoryLockCoordinator.advisoryKey(Keyspace(0), ShardId(0)))
            .isEqualTo(-8637869204239850395L)
    }

    // --- coordinator protocol over a fake gateway -----------------------------------------------

    @Test
    fun grantsAtMostOneLeasePerShard() = runTest {
        val gateway = FakeAdvisoryLockGateway()
        val coordinator = AdvisoryLockCoordinator(gateway)

        val first = coordinator.acquire(Keyspace(1), ShardId(0), Epoch(1))
        assertThat(first).isNotNull()
        assertThat(first!!.isValid).isTrue()

        // A second acquire of the same shard is refused while the first holds the lock.
        assertThat(coordinator.acquire(Keyspace(1), ShardId(0), Epoch(1))).isNull()

        // A different shard is independently acquirable.
        assertThat(coordinator.acquire(Keyspace(1), ShardId(1), Epoch(1))).isNotNull()
    }

    @Test
    fun releaseMakesLeaseInvalidAndReacquirable() = runTest {
        val gateway = FakeAdvisoryLockGateway()
        val coordinator = AdvisoryLockCoordinator(gateway)

        val lease = coordinator.acquire(Keyspace(1), ShardId(0), Epoch(1))!!
        lease.release()
        assertThat(lease.isValid).isFalse()

        // Once released the same shard can be acquired again (new owner after handoff).
        assertThat(coordinator.acquire(Keyspace(1), ShardId(0), Epoch(2))).isNotNull()
    }

    @Test
    fun lostDbSessionInvalidatesLease() = runTest {
        val gateway = FakeAdvisoryLockGateway()
        val coordinator = AdvisoryLockCoordinator(gateway)

        val lease = coordinator.acquire(Keyspace(1), ShardId(0), Epoch(1))!!
        assertThat(lease.isValid).isTrue()

        // Simulate the DB connection dropping out from under us (partition / server restart).
        gateway.killSession(AdvisoryLockCoordinator.advisoryKey(Keyspace(1), ShardId(0)))
        assertThat(lease.isValid).isFalse()
    }

    @Test
    fun leaseCarriesKeyspaceShardEpochAndFencingToken() = runTest {
        val coordinator = AdvisoryLockCoordinator(FakeAdvisoryLockGateway())
        val lease = coordinator.acquire(Keyspace(5), ShardId(9), Epoch(42))!!
        assertThat(lease.keyspace).isEqualTo(Keyspace(5))
        assertThat(lease.shard).isEqualTo(ShardId(9))
        assertThat(lease.epoch).isEqualTo(Epoch(42))
        assertThat(lease.fencingToken).isEqualTo(AdvisoryLockCoordinator.advisoryKey(Keyspace(5), ShardId(9)))
    }
}

/** In-memory stand-in for Postgres advisory locks: one live session per key, killable. */
private class FakeAdvisoryLockGateway : AdvisoryLockGateway {
    private val held = ConcurrentHashMap<Long, FakeSession>()

    override suspend fun tryLock(key: Long): AdvisoryLockSession? {
        val session = FakeSession { held.remove(key) }
        return if (held.putIfAbsent(key, session) == null) session else null
    }

    /** Model a dropped DB session: the lock is gone and any lease over it becomes invalid. */
    fun killSession(key: Long) {
        held.remove(key)?.let { it.alive = false }
    }

    private class FakeSession(private val onClose: () -> Unit) : AdvisoryLockSession {
        @Volatile
        var alive = true

        override fun isAlive(): Boolean = alive
        override suspend fun close() {
            alive = false
            onClose()
        }
    }
}
