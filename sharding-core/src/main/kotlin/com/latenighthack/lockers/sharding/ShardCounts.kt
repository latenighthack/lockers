package com.latenighthack.lockers.sharding

/**
 * Per-keyspace shard counts with a global [default]. Immutable and validated. Different
 * keyspaces may be sharded into different counts; unlisted keyspaces use [default].
 */
class ShardCounts(
    val default: Int,
    val perKeyspace: Map<Keyspace, Int> = emptyMap(),
) {
    init {
        require(default > 0) { "default shard count must be positive: $default" }
        perKeyspace.forEach { (ks, n) ->
            require(n > 0) { "shard count for keyspace ${ks.value} must be positive: $n" }
        }
    }

    fun forKeyspace(keyspace: Keyspace): Int = perKeyspace[keyspace] ?: default

    override fun equals(other: Any?): Boolean =
        other is ShardCounts && default == other.default && perKeyspace == other.perKeyspace

    override fun hashCode(): Int = 31 * default + perKeyspace.hashCode()

    override fun toString(): String = "ShardCounts(default=$default, perKeyspace=$perKeyspace)"

    companion object {
        /**
         * Parses a spec like `"1=512,30=128"` (keyspace=count, comma-separated) over a global
         * [default]. Null/blank yields no overrides. Throws on malformed input so the server
         * fails fast at startup rather than silently mis-sharding.
         */
        fun parse(default: Int, spec: String?): ShardCounts {
            if (spec.isNullOrBlank()) return ShardCounts(default)
            val overrides = spec.split(',').filter { it.isNotBlank() }.associate { entry ->
                val parts = entry.split('=')
                require(parts.size == 2) {
                    "malformed keyspace shard-count entry: '$entry' (expected keyspace=count)"
                }
                val ks = parts[0].trim().toLongOrNull()
                    ?: throw IllegalArgumentException("invalid keyspace in '$entry'")
                val count = parts[1].trim().toIntOrNull()
                    ?: throw IllegalArgumentException("invalid count in '$entry'")
                Keyspace(ks) to count
            }
            return ShardCounts(default, overrides)
        }
    }
}
