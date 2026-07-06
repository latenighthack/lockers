package com.latenighthack.lockers.sharding

import assertk.assertThat
import assertk.assertions.isFalse
import assertk.assertions.isGreaterThan
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isTrue
import com.latenighthack.lockers.sharding.inmem.InMemoryOwnershipCoordinator
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

class OwnershipCoordinatorTest {
    @Test
    fun `only one node holds a shard at a given epoch`() = runTest {
        val coord = InMemoryOwnershipCoordinator()
        val la = coord.coordinatorFor(NodeId("a")).acquire(Keyspace(1), ShardId(7), Epoch(0))
        val lb = coord.coordinatorFor(NodeId("b")).acquire(Keyspace(1), ShardId(7), Epoch(0))
        assertThat(la).isNotNull()
        assertThat(lb).isNull()
        assertThat(la!!.isValid).isTrue()
    }

    @Test
    fun `a higher epoch preempts and invalidates the older lease`() = runTest {
        val coord = InMemoryOwnershipCoordinator()
        val la = coord.coordinatorFor(NodeId("a")).acquire(Keyspace(1), ShardId(3), Epoch(0))!!
        assertThat(la.isValid).isTrue()

        val lb = coord.coordinatorFor(NodeId("b")).acquire(Keyspace(1), ShardId(3), Epoch(1))
        assertThat(lb).isNotNull()
        assertThat(la.isValid).isFalse() // fenced out by the newer epoch
        assertThat(lb!!.fencingToken).isGreaterThan(la.fencingToken)
    }

    @Test
    fun `release frees the shard for another node`() = runTest {
        val coord = InMemoryOwnershipCoordinator()
        val la = coord.coordinatorFor(NodeId("a")).acquire(Keyspace(1), ShardId(1), Epoch(0))!!
        la.release()
        assertThat(la.isValid).isFalse()

        val again = coord.coordinatorFor(NodeId("b")).acquire(Keyspace(1), ShardId(1), Epoch(0))
        assertThat(again).isNotNull()
    }
}
