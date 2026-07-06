package com.latenighthack.lockers.sharding.inmem

import com.latenighthack.lockers.sharding.Epoch
import com.latenighthack.lockers.sharding.HashPartitionFunction
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.PartitionFunction
import com.latenighthack.lockers.sharding.PeerAddress
import com.latenighthack.lockers.sharding.RingAssignment
import com.latenighthack.lockers.sharding.ShardCounts
import com.latenighthack.lockers.sharding.ShardMap
import com.latenighthack.lockers.sharding.spi.Membership
import com.latenighthack.lockers.sharding.spi.PeerLocator
import com.latenighthack.lockers.sharding.spi.ShardMapSource
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.emptyFlow
import java.util.concurrent.atomic.AtomicLong

/** [Membership] backed by a shared [StateFlow] of the node set (the whole "control plane"). */
class InMemoryMembership(
    override val self: NodeId,
    private val registry: StateFlow<Set<NodeId>>,
) : Membership {
    override fun current(): Set<NodeId> = registry.value
    override fun changes(): Flow<Set<NodeId>> = registry
}

/** [PeerLocator] over a fixed node->address table (blueprint-style static discovery). */
class StaticPeerLocator(private val addresses: Map<NodeId, PeerAddress>) : PeerLocator {
    override suspend fun addressOf(node: NodeId): PeerAddress? = addresses[node]
}

/**
 * Derives a [ShardMap] from the current [Membership] and the current [ShardCounts]. A change to
 * either the node set (elastic add/remove — M5) **or** the shard counts (online per-keyspace
 * repartition — M7) produces a new [ShardMap] under a strictly increasing epoch. [bind] must be
 * called to start reacting to membership changes; [updateCounts] drives an online count change.
 *
 * The two axes are decoupled exactly as the plan's Level-1 (partition counts) / Level-2
 * (assignment) split intends: a count change re-partitions only the affected keyspace's rooms
 * (others keep their shard id and owner), while a node change re-assigns shards without touching
 * partition output. Both share the one epoch sequence, so ordering across the two is total.
 */
class InMemoryShardMapSource(
    private val membership: Membership,
    initialCounts: ShardCounts,
    private val vnodes: Int = RingAssignment.DEFAULT_VNODES,
    private val partition: PartitionFunction = HashPartitionFunction(),
    /**
     * Optional shared stream of [ShardCounts] (the M7 control plane). When supplied, [bind] tracks
     * it so every node's source repartitions in lock-step off one authority — the counts analogue
     * of the shared membership registry. Manual [updateCounts] remains for single-source tests.
     */
    private val countsChanges: Flow<ShardCounts> = emptyFlow(),
) : ShardMapSource {
    private val epochSeq = AtomicLong(0)

    @Volatile
    private var lastNodes: Set<NodeId> = membership.current()

    @Volatile
    private var counts: ShardCounts = initialCounts

    // Serialises the two mutation paths (membership watch vs. updateCounts) so a concurrent
    // node-change and count-change can't both read a stale (nodes, counts) pair and clobber.
    private val lock = Any()
    private val state = MutableStateFlow(build(lastNodes, counts))

    private fun build(nodes: Set<NodeId>, counts: ShardCounts): ShardMap = ShardMap(
        epoch = Epoch(epochSeq.getAndIncrement()),
        counts = counts,
        assignment = RingAssignment(nodes, vnodes),
        partition = partition,
    )

    /**
     * Rebuilds (and bumps the epoch) whenever the node set actually changes. Guarding on the
     * node set (rather than dropping the first emission) is robust to the collector starting
     * after an early membership mutation — the initial set never double-bumps the epoch.
     */
    fun bind(scope: CoroutineScope) {
        membership.changes()
            .onEach { nodes ->
                synchronized(lock) {
                    if (nodes.isNotEmpty() && nodes != lastNodes) {
                        lastNodes = nodes
                        state.value = build(nodes, counts)
                    }
                }
            }
            .launchIn(scope)
        countsChanges
            .onEach { updateCounts(it) }
            .launchIn(scope)
    }

    /**
     * Online shard-count change (M7): swap in [newCounts] and emit a [ShardMap] at a higher epoch
     * over the unchanged node set. No-op (no epoch bump) if the counts are unchanged, mirroring the
     * membership guard so a redundant call never perturbs epochs. Keyspaces whose count is unchanged
     * keep byte-identical routing; only the repartitioned keyspace's rooms may move.
     */
    fun updateCounts(newCounts: ShardCounts) {
        synchronized(lock) {
            if (newCounts != counts) {
                counts = newCounts
                state.value = build(lastNodes, newCounts)
            }
        }
    }

    override suspend fun current(): ShardMap = state.value
    override fun watch(): Flow<ShardMap> = state
}
