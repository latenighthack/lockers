package com.latenighthack.lockers.server

import com.latenighthack.lockers.server.cluster.OwnerLifecycle
import com.latenighthack.lockers.sharding.*
import com.latenighthack.lockers.sharding.inmem.InMemoryOwnershipCoordinator
import com.latenighthack.lockers.sharding.spi.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.test.*
import kotlin.test.*

@OptIn(ExperimentalCoroutinesApi::class)
class OwnerRecoveryTest {
    private val self = NodeId("a")
    private val keyspace = Keyspace(0)
    private val map = ShardMap(Epoch(1), ShardCounts(1), RingAssignment(setOf(self)))
    @Test fun `a denied acquire is retried for an unchanged desired map`() = runTest {
        val actual = InMemoryOwnershipCoordinator().coordinatorFor(self)
        var denied = true
        val coordinator = object : OwnershipCoordinator {
            override suspend fun acquire(keyspace: Keyspace, shard: ShardId, epoch: Epoch) =
                if (denied) null else actual.acquire(keyspace, shard, epoch)
        }
        val owner = OwnerLifecycle(self, coordinator, listOf(keyspace))
        owner.reconcile(map)
        assertEquals(0, owner.heldLeaseCount())
        denied = false
        owner.reconcile(map)
        assertEquals(1, owner.heldLeaseCount())
        owner.releaseAll()
    }

    @Test fun `desired ownership recovers without a new map emission`() = runTest {
        val actual = InMemoryOwnershipCoordinator().coordinatorFor(self)
        var denied = true
        val coordinator = object : OwnershipCoordinator {
            override suspend fun acquire(keyspace: Keyspace, shard: ShardId, epoch: Epoch) =
                if (denied) null else actual.acquire(keyspace, shard, epoch)
        }
        val owner = OwnerLifecycle(self, coordinator, listOf(keyspace))
        owner.start(backgroundScope, MutableStateFlow(map))
        runCurrent()
        assertEquals(0, owner.heldLeaseCount())
        denied = false
        advanceTimeBy(501)
        runCurrent()
        assertEquals(1, owner.heldLeaseCount())
        owner.releaseAll()
    }
}
