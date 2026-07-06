package com.latenighthack.lockers.sharding

/** A shard index within a keyspace's partition space, in `[0, shardCount)`. */
@JvmInline
value class ShardId(val value: Int) {
    init {
        require(value >= 0) { "shardId must be non-negative: $value" }
    }
}

/**
 * A locker keyspace (mirrors the protobuf `LockerKeyspace.value`). The reserved
 * [SESSION] keyspace routes the session axis on the same ring as rooms.
 */
@JvmInline
value class Keyspace(val value: Long) {
    companion object {
        /** Reserved keyspace used to route the session axis (by sessionId). */
        val SESSION = Keyspace(-1L)
    }
}

/** A stable, logical cluster node identity — NOT a network address (see [PeerAddress]). */
@JvmInline
value class NodeId(val value: String) {
    init {
        require(value.isNotEmpty()) { "nodeId must be non-empty" }
    }
}

/** A monotonically increasing version stamp on a [ShardMap]. */
@JvmInline
value class Epoch(val value: Long) : Comparable<Epoch> {
    override fun compareTo(other: Epoch): Int = value.compareTo(other.value)
    override fun toString(): String = "epoch=$value"
}

/** A network address a peer node is reachable at for east-west RPC. */
data class PeerAddress(val host: String, val port: Int) {
    /** The `host:port` form used in redirects and topology (empty for a null address). */
    fun hostPort(): String = "$host:$port"
}
