package com.latenighthack.lockers.server.cluster

import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktbuf.net.ServerDescriptor
import com.latenighthack.lockers.cluster.v1.ClusterServer
import com.latenighthack.lockers.cluster.v1.GetTopologyRequest
import com.latenighthack.lockers.cluster.v1.GetTopologyResponse
import com.latenighthack.lockers.cluster.v1.GetTopologyResponse_RingBuilder
import com.latenighthack.lockers.server.tools.GrpcRouteProvider
import com.latenighthack.lockers.sharding.RingTopology
import com.latenighthack.lockers.sharding.ShardRouter

/**
 * Read-only view of the cluster's sharding topology, backed by the [ShardRouter]'s two rings.
 * A smart client (`RoutingRpcClient`) or a cluster-aware keymaster calls `GetTopology` to learn
 * the current per-ring epoch, shard counts, and node addresses, so it can target the owning node
 * up front rather than bouncing off a `NOT_OWNER` / `EPOCH_STALE` redirect on every call.
 *
 * Only wired in cluster mode (a [ShardRouter] exists); the monolith mounts nothing here. Mounted
 * on the internal admin port — the same operator-only surface as the push/broadcast admin services.
 */
class ClusterServiceImpl(private val router: ShardRouter) : ClusterServer {
    override suspend fun getTopology(
        context: GrpcRequestContext,
        request: GetTopologyRequest,
    ): GetTopologyResponse {
        val topology = router.topology()
        return GetTopologyResponse {
            room { fill(topology.room) }
            session { fill(topology.session) }
        }
    }

    private fun GetTopologyResponse_RingBuilder.fill(ring: RingTopology) {
        epoch = ring.epoch.value
        defaultShardCount = ring.counts.default
        keyspaceShardCounts {
            for ((keyspace, count) in ring.counts.perKeyspace) {
                addKeyspaceShardCount {
                    this.keyspace = keyspace.value
                    shardCount = count
                }
            }
        }
        nodes {
            for (member in ring.nodes) {
                addNode {
                    nodeId = member.node.value
                    address = member.address?.hostPort() ?: ""
                }
            }
        }
    }
}

/**
 * Mounts [ClusterServiceImpl] as a [GrpcRouteProvider]. Unlike the kotlin-inject service modules
 * this is a plain wrapper — the cluster service depends only on the [ShardRouter], which lives
 * outside the DI graph — added to the monolith's admin services when a cluster context is present.
 */
class ClusterServiceModule(router: ShardRouter) : GrpcRouteProvider<ClusterServer> {
    override val server: ClusterServer = ClusterServiceImpl(router)
    override val descriptor: ServerDescriptor = ClusterServer.Descriptor
}
