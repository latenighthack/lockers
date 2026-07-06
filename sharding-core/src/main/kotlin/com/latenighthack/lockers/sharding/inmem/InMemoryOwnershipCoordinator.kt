package com.latenighthack.lockers.sharding.inmem

import com.latenighthack.lockers.sharding.Epoch
import com.latenighthack.lockers.sharding.Keyspace
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.ShardId
import com.latenighthack.lockers.sharding.spi.OwnershipCoordinator
import com.latenighthack.lockers.sharding.spi.ShardLease
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * A single shared coordinator for a simulated cluster: at most one live holder per
 * `(keyspace, shard)`. An [OwnershipCoordinator.acquire] at a strictly higher epoch preempts a
 * lower-epoch holder (invalidating its lease), modelling a fenced ownership handoff with a
 * monotonic fencing token. Deterministic and infra-free.
 */
class InMemoryOwnershipCoordinator {
    private data class Holder(val node: NodeId, val epoch: Epoch, val lease: InMemoryLease)

    private val holders = ConcurrentHashMap<Pair<Keyspace, ShardId>, Holder>()
    private val tokens = AtomicLong(0)

    /** A view scoped to one node; hand each simulated node its own coordinator. */
    fun coordinatorFor(node: NodeId): OwnershipCoordinator = NodeCoordinator(node)

    private inner class NodeCoordinator(private val node: NodeId) : OwnershipCoordinator {
        override suspend fun acquire(keyspace: Keyspace, shard: ShardId, epoch: Epoch): ShardLease? {
            val key = keyspace to shard
            while (true) {
                val existing = holders[key]
                when {
                    existing != null && existing.node == node && existing.epoch >= epoch ->
                        return existing.lease // re-entrant: already ours at >= epoch
                    existing != null && existing.epoch >= epoch ->
                        return null // held by someone else at >= epoch
                }
                val lease = InMemoryLease(keyspace, shard, epoch, tokens.incrementAndGet(), key)
                val holder = Holder(node, epoch, lease)
                val swapped = if (existing == null) {
                    holders.putIfAbsent(key, holder) == null
                } else {
                    holders.replace(key, existing, holder)
                }
                if (swapped) {
                    existing?.lease?.invalidate() // preempt the older-epoch holder after winning
                    return lease
                }
                // lost a race; retry
            }
        }
    }

    private inner class InMemoryLease(
        override val keyspace: Keyspace,
        override val shard: ShardId,
        override val epoch: Epoch,
        override val fencingToken: Long,
        private val key: Pair<Keyspace, ShardId>,
    ) : ShardLease {
        @Volatile
        private var valid = true

        override val isValid: Boolean get() = valid

        fun invalidate() {
            valid = false
        }

        override suspend fun release() {
            valid = false
            val cur = holders[key]
            if (cur != null && cur.lease.fencingToken == fencingToken) {
                holders.remove(key, cur)
            }
        }
    }
}
