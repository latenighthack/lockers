package com.latenighthack.lockers.sharding

/**
 * A complete, immutable routing snapshot of ONE ring at one [epoch]: per-keyspace shard
 * [counts] (level 1) plus the shard->node [assignment] (level 2). Snapshots are compared by
 * epoch; every routing decision and peer RPC carries the epoch it was computed under.
 *
 * The room axis and the session/gateway axis are each their own [ShardMap] (their own node
 * set, epoch, and shard counts), so they shard and scale independently — see [ShardRouter].
 * The room ring routes by a room's `LockerKeyspace`; the session ring routes every session
 * under [Keyspace.SESSION].
 */
class ShardMap(
    val epoch: Epoch,
    val counts: ShardCounts,
    val assignment: Assignment,
    private val partition: PartitionFunction = HashPartitionFunction(),
) {
    val nodes: Set<NodeId> get() = assignment.nodes

    fun shard(keyspace: Keyspace, key: ByteArray): ShardId =
        partition.partition(keyspace, key, counts.forKeyspace(keyspace))

    fun owner(keyspace: Keyspace, key: ByteArray): NodeId =
        assignment.owner(keyspace, shard(keyspace, key))

    /**
     * Within a single [keyspace], the shards whose owning node differs from [prev]. Drives
     * lease handoff on a topology or shard-count change. Covers both a pure reassignment
     * (same counts, node set changed) and a repartition (counts changed) for that keyspace.
     */
    fun movedShards(prev: ShardMap, keyspace: Keyspace): Set<ShardId> {
        val now = counts.forKeyspace(keyspace)
        val was = prev.counts.forKeyspace(keyspace)
        val moved = LinkedHashSet<ShardId>()
        for (s in 0 until maxOf(now, was)) {
            val shard = ShardId(s)
            val nowOwner = if (s < now) assignment.owner(keyspace, shard) else null
            val wasOwner = if (s < was) prev.assignment.owner(keyspace, shard) else null
            if (nowOwner != wasOwner) moved.add(shard)
        }
        return moved
    }
}
