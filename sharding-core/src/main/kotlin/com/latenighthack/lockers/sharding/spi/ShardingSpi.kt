package com.latenighthack.lockers.sharding.spi

import com.latenighthack.lockers.sharding.Epoch
import com.latenighthack.lockers.sharding.Keyspace
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.PeerAddress
import com.latenighthack.lockers.sharding.ShardId
import com.latenighthack.lockers.sharding.ShardMap
import kotlinx.coroutines.flow.Flow

/**
 * Pluggable seams the deployment blueprints implement. The sharding core depends only on
 * these interfaces, never on a concrete transport/coordination technology, so alternate
 * backends (in-memory, Postgres, Kubernetes, ...) can be swapped without touching routing.
 */

/** Who is in the cluster, who am I, and a stream of changes to the node set. */
interface Membership {
    val self: NodeId
    fun current(): Set<NodeId>
    fun changes(): Flow<Set<NodeId>>
}

/** The source of truth for the [ShardMap]. Each [watch] emission carries a higher epoch. */
interface ShardMapSource {
    suspend fun current(): ShardMap
    fun watch(): Flow<ShardMap>
}

/**
 * Fencing: grants at most one live [ShardLease] per `(keyspace, shard)` cluster-wide. An
 * [acquire] at a higher [Epoch] preempts a lower-epoch holder, enabling a fenced handoff.
 */
interface OwnershipCoordinator {
    suspend fun acquire(keyspace: Keyspace, shard: ShardId, epoch: Epoch): ShardLease?
}

/** A held ownership lease. Check [isValid] before performing authoritative writes. */
interface ShardLease {
    val keyspace: Keyspace
    val shard: ShardId
    val epoch: Epoch

    /** Monotonically increasing fence stamp; the store rejects writes bearing a lower token. */
    val fencingToken: Long

    /** False once the lease is revoked (preempted by a higher epoch) or released. */
    val isValid: Boolean

    suspend fun release()
}

/** Resolves a logical [NodeId] to a callable [PeerAddress]; keeps the ring transport-free. */
fun interface PeerLocator {
    suspend fun addressOf(node: NodeId): PeerAddress?
}
