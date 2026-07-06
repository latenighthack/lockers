package com.latenighthack.lockers.sharding

/**
 * Level 2 of the two-level sharding map: places a [ShardId] onto a [NodeId]. Given a fixed
 * node set the mapping is deterministic across nodes, so every node agrees on the owner.
 */
interface Assignment {
    val nodes: Set<NodeId>
    fun owner(keyspace: Keyspace, shard: ShardId): NodeId
}

/**
 * Consistent-hash ring placement with virtual nodes. Adding/removing a node reassigns only
 * the shards in the arcs adjacent to that node's vnodes (expected ~`shards/nodes` movement),
 * and a high [vnodesPerNode] keeps the per-node split even. Ties break on [NodeId] so the ring
 * order is identical on every node regardless of set iteration order.
 */
class RingAssignment(
    override val nodes: Set<NodeId>,
    private val vnodesPerNode: Int = DEFAULT_VNODES,
    private val hash: (ByteArray) -> Long = ::fnv1a64,
) : Assignment {
    private val ringHashes: LongArray
    private val ringNodes: Array<NodeId>

    init {
        require(nodes.isNotEmpty()) { "assignment requires at least one node" }
        require(vnodesPerNode > 0) { "vnodesPerNode must be positive: $vnodesPerNode" }

        val points = ArrayList<Pair<Long, NodeId>>(nodes.size * vnodesPerNode)
        for (node in nodes) {
            for (v in 0 until vnodesPerNode) {
                points.add(vnodeHash(node, v) to node)
            }
        }
        points.sortWith(compareBy({ it.first }, { it.second.value }))
        ringHashes = LongArray(points.size) { points[it].first }
        ringNodes = Array(points.size) { points[it].second }
    }

    override fun owner(keyspace: Keyspace, shard: ShardId): NodeId {
        val point = hash(keyspaceTag(keyspace.value) + intBytes(shard.value))
        return ringNodes[clockwiseIndex(point)]
    }

    private fun vnodeHash(node: NodeId, vnode: Int): Long =
        hash(node.value.encodeToByteArray() + VNODE_SEP + intBytes(vnode))

    /** Index of the first ring point with hash >= [point], wrapping to 0. */
    private fun clockwiseIndex(point: Long): Int {
        var lo = 0
        var hi = ringHashes.size
        while (lo < hi) {
            val mid = (lo + hi) ushr 1
            if (ringHashes[mid] < point) lo = mid + 1 else hi = mid
        }
        return if (lo == ringHashes.size) 0 else lo
    }

    companion object {
        const val DEFAULT_VNODES = 128
        private val VNODE_SEP = byteArrayOf('#'.code.toByte())
    }
}
