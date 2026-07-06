package com.latenighthack.lockers.server.cluster

import com.latenighthack.lockers.sharding.Epoch
import com.latenighthack.lockers.sharding.Keyspace
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.ShardId
import com.latenighthack.lockers.sharding.ShardMap
import com.latenighthack.lockers.sharding.spi.OwnershipCoordinator
import com.latenighthack.lockers.sharding.spi.ShardLease
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * Per-node, per-shard owner lifecycle for one ring (the **room** ring). Watches the ring's
 * [ShardMap] stream; for every shard this node owns at the current epoch it holds a fenced
 * [ShardLease] from the [OwnershipCoordinator]. On an epoch/topology change it diffs the new map
 * against the last one via [ShardMap.movedShards]:
 *
 *  - **dropped** shards (were ours, now someone else's) are *quiesced* — its cache is evicted so
 *    the new owner rebuilds lazily from the shared store — and their lease is [ShardLease.release]d
 *    so the higher-epoch acquire on the new owner is unopposed;
 *  - **newly-owned** shards are acquired at the new epoch (a higher-epoch acquire preempts any
 *    stale holder, giving a fenced handoff with no overlap of valid tokens).
 *
 * The held-lease table also backs the write gate: [leaseFor] lets [RingRoomOwnership] confirm this
 * node both routes-local *and* still holds a valid lease before coordinating a write. This models
 * "reject-and-reconnect": ownership (not durable data) moves; the new owner rebuilds room→session
 * routing from `SubscriptionStore` on first access.
 *
 * Only wired in a clustered [ClusterContext]; the monolith owns every shard and skips it entirely.
 */
class OwnerLifecycle(
    private val self: NodeId,
    private val coordinator: OwnershipCoordinator,
    private val keyspaces: List<Keyspace>,
    private val onShardsDropped: suspend (Set<ShardId>) -> Unit = {},
    private val metrics: OwnerLifecycleMetrics = OwnerLifecycleMetrics.NONE,
) {
    private val logger = LoggerFactory.getLogger(OwnerLifecycle::class.java)

    /** Leases currently held by this node, keyed by (keyspace, shard). */
    private val leases = ConcurrentHashMap<Pair<Keyspace, ShardId>, ShardLease>()

    // Serialises reconcile() so overlapping map emissions can't interleave acquire/release.
    private val mutex = Mutex()

    @Volatile
    private var last: ShardMap? = null

    /** The valid lease this node holds for `(keyspace, shard)`, or null (none / revoked). */
    fun leaseFor(keyspace: Keyspace, shard: ShardId): ShardLease? =
        leases[keyspace to shard]?.takeIf { it.isValid }

    /** Number of shards this node currently owns a lease for (observability / tests). */
    fun heldLeaseCount(): Int = leases.values.count { it.isValid }

    /** Begins tracking the ring's [ShardMap] stream. Reconciles once for the initial map. */
    fun start(scope: CoroutineScope, watch: kotlinx.coroutines.flow.Flow<ShardMap>) {
        watch.onEach { reconcile(it) }.launchIn(scope)
    }

    /**
     * Brings the held-lease set in line with [map]: acquire newly-owned shards, release dropped
     * ones (after evicting their caches). Idempotent — re-emitting the same map is a no-op.
     */
    suspend fun reconcile(map: ShardMap) = mutex.withLock {
        val prev = last
        val prevEpoch = prev?.epoch
        val startNanos = System.nanoTime()
        var changed = false
        for (keyspace in keyspaces) {
            val ownedNow = ownedShards(map, keyspace)
            if (prev == null) {
                for (shard in ownedNow) { acquire(keyspace, shard, map.epoch, prevEpoch); changed = true }
                continue
            }
            // Shards whose owner changed between prev and map, within this keyspace.
            val moved = map.movedShards(prev, keyspace)
            val dropped = LinkedHashSet<ShardId>()
            val gained = LinkedHashSet<ShardId>()
            for (shard in moved) {
                val ownedBefore = leases.containsKey(keyspace to shard)
                val ownedAfter = shard in ownedNow
                if (ownedBefore && !ownedAfter) dropped.add(shard)
                if (!ownedBefore && ownedAfter) gained.add(shard)
            }
            if (dropped.isNotEmpty() || gained.isNotEmpty()) changed = true
            metrics.onHandoffDelta(dropped.size + gained.size)
            try {
                if (dropped.isNotEmpty()) {
                    // Quiesce: evict caches for dropped shards BEFORE releasing, so a redirected
                    // client that reconnects to the new owner never reads this node's stale routing.
                    runCatching { onShardsDropped(dropped) }
                        .onFailure { logger.warn("cache eviction for dropped shards failed", it) }
                    for (shard in dropped) release(keyspace, shard, prevEpoch, map.epoch)
                }
                for (shard in gained) acquire(keyspace, shard, map.epoch, prevEpoch)
            } finally {
                metrics.onHandoffDelta(-(dropped.size + gained.size))
            }
        }
        if (changed && prev != null) metrics.onReshardDuration(System.nanoTime() - startNanos)
        last = map
    }

    private suspend fun acquire(keyspace: Keyspace, shard: ShardId, epoch: Epoch, fromEpoch: Epoch?) {
        val key = keyspace to shard
        val existing = leases[key]
        if (existing != null && existing.isValid && existing.epoch >= epoch) return
        val lease = coordinator.acquire(keyspace, shard, epoch)
        if (lease != null) {
            leases[key] = lease
            metrics.onOwnershipMove()
            logger.info(
                "shard acquired shardId={} dimension=room fromEpoch={} toEpoch={} toNode={} fencingToken={}",
                shard.value, fromEpoch?.value, epoch.value, self.value, lease.fencingToken,
            )
        } else {
            // A higher-or-equal-epoch holder exists elsewhere; the map disagrees transiently.
            metrics.onLeaseExpiry()
            logger.warn(
                "shard acquire denied (fenced) shardId={} dimension=room epoch={} node={}",
                shard.value, epoch.value, self.value,
            )
        }
    }

    private suspend fun release(keyspace: Keyspace, shard: ShardId, fromEpoch: Epoch?, toEpoch: Epoch) {
        val key = keyspace to shard
        val lease = leases.remove(key) ?: return
        runCatching { lease.release() }
            .onFailure { logger.warn("lease release failed shardId={}", shard.value, it) }
        metrics.onOwnershipMove()
        logger.info(
            "shard released shardId={} dimension=room fromEpoch={} toEpoch={} fromNode={} fencingToken={}",
            shard.value, fromEpoch?.value, toEpoch.value, self.value, lease.fencingToken,
        )
    }

    private fun ownedShards(map: ShardMap, keyspace: Keyspace): Set<ShardId> {
        val count = map.counts.forKeyspace(keyspace)
        val owned = LinkedHashSet<ShardId>()
        for (s in 0 until count) {
            val shard = ShardId(s)
            if (map.assignment.owner(keyspace, shard) == self) owned.add(shard)
        }
        return owned
    }

    /** Releases every held lease (ordered drain on node leave). */
    suspend fun releaseAll() = mutex.withLock {
        for ((key, lease) in leases) {
            runCatching { lease.release() }
                .onFailure { logger.warn("lease release failed on drain shardId={}", key.second.value, it) }
        }
        leases.clear()
    }
}

/**
 * The sharding meters [OwnerLifecycle] drives (§7): `lockers.shard.ownership.moves` (each
 * acquire/release), `lockers.shard.lease.expiries` (a fenced/denied acquire), `lockers.shard.inhandoff`
 * (gauge of shards mid-handoff, driven by [onHandoffDelta]), and `lockers.reshard.duration` (timer over
 * a reconcile that changed ownership). A no-op default keeps the lifecycle testable without a registry.
 */
interface OwnerLifecycleMetrics {
    fun onOwnershipMove()
    fun onLeaseExpiry()
    fun onHandoffDelta(delta: Int)
    fun onReshardDuration(nanos: Long)

    companion object {
        val NONE: OwnerLifecycleMetrics = object : OwnerLifecycleMetrics {
            override fun onOwnershipMove() {}
            override fun onLeaseExpiry() {}
            override fun onHandoffDelta(delta: Int) {}
            override fun onReshardDuration(nanos: Long) {}
        }
    }
}
