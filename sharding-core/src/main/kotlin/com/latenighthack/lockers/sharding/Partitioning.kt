package com.latenighthack.lockers.sharding

/**
 * Level 1 of the two-level sharding map: a deterministic, config-only assignment of a
 * partition key to a shard in `[0, shardCount)`. Takes no membership input, so the same
 * `(keyspace, key, shardCount)` yields the same [ShardId] on every node and every run.
 */
fun interface PartitionFunction {
    fun partition(keyspace: Keyspace, key: ByteArray, shardCount: Int): ShardId
}

/** Default [PartitionFunction]: hashes `keyspaceTag ++ key` and takes an unsigned modulo. */
class HashPartitionFunction(
    private val hash: (ByteArray) -> Long = ::fnv1a64,
) : PartitionFunction {
    override fun partition(keyspace: Keyspace, key: ByteArray, shardCount: Int): ShardId {
        require(shardCount > 0) { "shardCount must be positive: $shardCount" }
        val h = hash(keyspaceTag(keyspace.value) + key)
        return ShardId(java.lang.Long.remainderUnsigned(h, shardCount.toLong()).toInt())
    }
}
