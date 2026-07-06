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
 * Derives a [ShardMap] from the current [Membership]: any node-set change produces a new
 * [RingAssignment] under a strictly increasing epoch (the elastic-node model — shard counts
 * are fixed). [bind] must be called to start reacting to membership changes.
 */
class InMemoryShardMapSource(
    private val membership: Membership,
    private val counts: ShardCounts,
    private val vnodes: Int = RingAssignment.DEFAULT_VNODES,
    private val partition: PartitionFunction = HashPartitionFunction(),
) : ShardMapSource {
    private val epochSeq = AtomicLong(0)

    @Volatile
    private var lastNodes: Set<NodeId> = membership.current()
    private val state = MutableStateFlow(build(lastNodes))

    private fun build(nodes: Set<NodeId>): ShardMap = ShardMap(
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
                if (nodes.isNotEmpty() && nodes != lastNodes) {
                    lastNodes = nodes
                    state.value = build(nodes)
                }
            }
            .launchIn(scope)
    }

    override suspend fun current(): ShardMap = state.value
    override fun watch(): Flow<ShardMap> = state
}
