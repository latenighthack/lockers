package com.latenighthack.lockers.sharding.inmem

import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.PeerAddress
import com.latenighthack.lockers.sharding.RingAssignment
import com.latenighthack.lockers.sharding.ShardCounts
import com.latenighthack.lockers.sharding.ShardRouter
import com.latenighthack.lockers.sharding.spi.Membership
import com.latenighthack.lockers.sharding.spi.OwnershipCoordinator
import com.latenighthack.lockers.sharding.spi.PeerLocator
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.update

/**
 * An in-process control plane simulating a cluster with two independent rings — a room ring and
 * a session/gateway ring — plus one ownership coordinator. Lets tests (and, later, the server
 * test harness) drive node add/remove on either ring and observe routing and fenced handoff with
 * zero infrastructure. By default the session ring spans the same nodes as the room ring; pass
 * distinct [initialSessionNodes] to model separate WebSocket servers.
 */
class SimCluster(
    initialRoomNodes: Collection<NodeId>,
    private val roomCounts: ShardCounts,
    initialSessionNodes: Collection<NodeId> = initialRoomNodes,
    private val sessionCounts: ShardCounts = roomCounts,
    private val vnodes: Int = RingAssignment.DEFAULT_VNODES,
) {
    private val roomRegistry = MutableStateFlow(initialRoomNodes.toSet())
    private val sessionRegistry = MutableStateFlow(initialSessionNodes.toSet())
    private val ownership = InMemoryOwnershipCoordinator()

    // Room-ring topology controls (addNode/removeNode kept as the room-ring aliases).
    fun addNode(node: NodeId) = addRoomNode(node)
    fun removeNode(node: NodeId) = removeRoomNode(node)
    fun addRoomNode(node: NodeId) = roomRegistry.update { it + node }
    fun removeRoomNode(node: NodeId) = roomRegistry.update { it - node }
    fun roomNodes(): Set<NodeId> = roomRegistry.value

    // Session/gateway-ring topology controls (scale WebSocket servers independently).
    fun addSessionNode(node: NodeId) = sessionRegistry.update { it + node }
    fun removeSessionNode(node: NodeId) = sessionRegistry.update { it - node }
    fun sessionNodes(): Set<NodeId> = sessionRegistry.value

    fun coordinatorFor(node: NodeId): OwnershipCoordinator = ownership.coordinatorFor(node)

    /** A locator that derives a stable fake address from the node id (in-process routing). */
    fun peerLocator(): PeerLocator = PeerLocator { node -> PeerAddress(node.value, 0) }

    fun roomMembershipFor(self: NodeId): Membership = InMemoryMembership(self, roomRegistry)
    fun sessionMembershipFor(self: NodeId): Membership = InMemoryMembership(self, sessionRegistry)

    fun roomSourceFor(self: NodeId, scope: CoroutineScope): InMemoryShardMapSource =
        InMemoryShardMapSource(roomMembershipFor(self), roomCounts, vnodes).also { it.bind(scope) }

    fun sessionSourceFor(self: NodeId, scope: CoroutineScope): InMemoryShardMapSource =
        InMemoryShardMapSource(sessionMembershipFor(self), sessionCounts, vnodes).also { it.bind(scope) }

    /** Room-ring shard-map source alias (back-compat with room-axis tests). */
    fun shardMapSourceFor(self: NodeId, scope: CoroutineScope): InMemoryShardMapSource =
        roomSourceFor(self, scope)

    /** A fully wired [ShardRouter] for [self] tracking both this cluster's rings. */
    suspend fun routerFor(self: NodeId, scope: CoroutineScope): ShardRouter =
        ShardRouter.create(
            roomMembership = roomMembershipFor(self),
            roomSource = roomSourceFor(self, scope),
            sessionMembership = sessionMembershipFor(self),
            sessionSource = sessionSourceFor(self, scope),
            locator = peerLocator(),
            scope = scope,
        )
}
