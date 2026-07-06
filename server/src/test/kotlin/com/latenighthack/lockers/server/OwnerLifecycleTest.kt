package com.latenighthack.lockers.server

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isLessThanOrEqualTo
import assertk.assertions.isTrue
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.lockers.common.v1.Locker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.room.v1.LocalRoomServiceRpc
import com.latenighthack.lockers.room.v1.PostLockerChangeRequest
import com.latenighthack.lockers.room.v1.PostLockerChangeResponse
import com.latenighthack.lockers.room.v1.SubscriptionRequest
import com.latenighthack.lockers.room.v1.SubscriptionResponse
import com.latenighthack.lockers.server.agents.ExampleLockerAgent
import com.latenighthack.lockers.server.cluster.OwnerLifecycle
import com.latenighthack.lockers.server.cluster.RingRoomOwnership
import com.latenighthack.lockers.server.services.room.v1.LockStoreImpl
import com.latenighthack.lockers.server.services.room.v1.LockerStoreImpl
import com.latenighthack.lockers.server.services.room.v1.RoomServiceImpl
import com.latenighthack.lockers.server.services.room.v1.SubscriptionStoreImpl
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.session.v1.PostEventRequest
import com.latenighthack.lockers.session.v1.PostEventResponse
import com.latenighthack.lockers.session.v1.SessionGatewayService
import com.latenighthack.lockers.sharding.Keyspace
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.ShardCounts
import com.latenighthack.lockers.sharding.ShardId
import com.latenighthack.lockers.sharding.ShardRouter
import com.latenighthack.lockers.sharding.inmem.SimCluster
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test

/**
 * M5 — elastic reshard over the in-process [SimCluster] and one shared [InMemoryStoreDelegate].
 * Proves: (1) a node add/remove moves ownership only for the affected shards; (2) at most one live
 * lease per `(keyspace, shard)`; (3) a write on a node that lost its lease is rejected NOT_OWNER;
 * (4) after handoff the new owner rebuilds room→session routing from the durable store and delivery
 * continues. Reject-and-reconnect, ownership only — no durable data moves.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class OwnerLifecycleTest {

    private val roomKeyspace = Keyspace(1L)
    private val shardCount = 32

    /** Records every gateway fan-out (which node originated it, for which session). */
    private class RecordingGatewayDiscovery(
        private val node: NodeId,
        private val deliveries: MutableList<Pair<NodeId, String>>,
    ) : SessionGatewayDiscovery {
        override suspend fun findServer(sessionId: SessionId): SessionGatewayService =
            object : SessionGatewayService {
                override suspend fun postEvent(request: PostEventRequest): PostEventResponse {
                    deliveries.add(node to sessionId.rawValue.decodeToString())
                    return PostEventResponse { }
                }
            }
    }

    private inner class Node(
        val id: NodeId,
        val router: ShardRouter,
        val lifecycle: OwnerLifecycle,
        val impl: RoomServiceImpl,
        val evictions: AtomicInteger,
        val deliveries: MutableList<Pair<NodeId, String>>,
    ) {
        val rpc = LocalRoomServiceRpc(impl)
        suspend fun reconcile() = lifecycle.reconcile(router.roomMap())
    }

    /**
     * The one shared durable store all nodes coordinate over (Model A). Built once — `prepare()`
     * every store, then `createStores()` — and handed to every node, exactly like the shared
     * Postgres in production.
     */
    private class SharedStore(
        val subs: SubscriptionStoreImpl,
        val lockers: LockerStoreImpl,
        val locks: LockStoreImpl,
    )

    private suspend fun sharedStore(delegate: InMemoryStoreDelegate): SharedStore {
        val subs = SubscriptionStoreImpl(delegate).also { it.prepare() }
        val lockers = LockerStoreImpl(delegate).also { it.prepare() }
        val locks = LockStoreImpl(delegate).also { it.prepare() }
        delegate.createStores()
        return SharedStore(subs, lockers, locks)
    }

    private suspend fun node(id: NodeId, sim: SimCluster, store: SharedStore, scope: CoroutineScope): Node {
        val subs = store.subs
        val lockers = store.lockers
        val locks = store.locks
        val router = sim.routerFor(id, scope)

        // Break the impl<->lifecycle cycle: the eviction hook reads a holder set after impl exists.
        val implHolder = arrayOfNulls<RoomServiceImpl>(1)
        val evictions = AtomicInteger(0)
        val lifecycle = OwnerLifecycle(
            self = id,
            coordinator = sim.coordinatorFor(id),
            keyspaces = listOf(roomKeyspace),
            onShardsDropped = {
                evictions.incrementAndGet()
                implHolder[0]?.evictRoomCaches()
            },
        )
        val deliveries = CopyOnWriteArrayList<Pair<NodeId, String>>()
        val impl = RoomServiceImpl(
            subs, lockers, locks,
            RecordingGatewayDiscovery(id, deliveries),
            RingRoomOwnership(router, lifecycle),
            ExampleLockerAgent(), SimpleMeterRegistry(), LockersConfig.defaults(),
        )
        implHolder[0] = impl
        lifecycle.reconcile(router.roomMap()) // acquire initially-owned shards
        return Node(id, router, lifecycle, impl, evictions, deliveries)
    }

    private fun post(room: ByteArray, locker: ByteArray, version: Long = 0L) = PostLockerChangeRequest {
        roomId = RoomId(room)
        lockerId = LockerId {
            rawValue = locker
            keyspace = LockerKeyspace { value = roomKeyspace.value }
        }
        this.locker = Locker { }
        parentVersion = version
    }

    private suspend fun doSubscribe(node: Node, room: ByteArray, session: String): SubscriptionResponse =
        node.rpc.subscription(SubscriptionRequest {
            roomId = RoomId(room)
            sessionId = SessionId(session.encodeToByteArray())
            kind.subscribe { }
        })

    private fun firstRoomOwnedBy(node: Node, owner: NodeId): ByteArray {
        for (i in 0 until 10_000) {
            val room = "room-$i".encodeToByteArray()
            if (node.router.roomMap().owner(roomKeyspace, room) == owner) return room
        }
        error("no room owned by $owner found")
    }

    /** Spin (bounded) until [router]'s room map advances past [fromEpoch]. */
    private suspend fun waitForEpochBump(router: ShardRouter, fromEpoch: Long) {
        repeat(500) {
            if (router.roomMap().epoch.value > fromEpoch) return
            delay(5)
        }
        error("epoch did not advance past $fromEpoch")
    }

    private fun assertSingleOwnerPerShard(count: Int, vararg nodes: Node) {
        for (s in 0 until count) {
            val shard = ShardId(s)
            val holders = nodes.count { it.lifecycle.leaseFor(roomKeyspace, shard) != null }
            assertThat(holders).isLessThanOrEqualTo(1)
        }
    }

    @Test
    fun `node add moves ownership only for affected shards`() = runBlocking {
        val scope = CoroutineScope(Job())
        val sim = SimCluster(listOf(NodeId("a"), NodeId("b")), ShardCounts(shardCount))
        val a = node(NodeId("a"), sim, sharedStore(InMemoryStoreDelegate()), scope)

        val before = a.router.roomMap()
        sim.addNode(NodeId("c"))
        waitForEpochBump(a.router, before.epoch.value)
        val after = a.router.roomMap()

        val moved = after.movedShards(before, roomKeyspace)
        val count = after.counts.forKeyspace(roomKeyspace)
        // movedShards is EXACTLY the set of shards whose owner changed — no more, no fewer.
        for (s in 0 until count) {
            val shard = ShardId(s)
            val changed = before.assignment.owner(roomKeyspace, shard) != after.assignment.owner(roomKeyspace, shard)
            assertThat(moved.contains(shard)).isEqualTo(changed)
        }
        assertThat(moved.size).isGreaterThan(0)
        assertThat(moved.size).isLessThanOrEqualTo(count)
        scope.cancel()
    }

    @Test
    fun `at most one node holds a live lease per shard across a handoff`() = runBlocking {
        val scope = CoroutineScope(Job())
        val sim = SimCluster(listOf(NodeId("a"), NodeId("b")), ShardCounts(shardCount))
        val store = sharedStore(InMemoryStoreDelegate())
        val a = node(NodeId("a"), sim, store, scope)
        val b = node(NodeId("b"), sim, store, scope)

        assertSingleOwnerPerShard(shardCount, a, b)

        val before = a.router.roomMap().epoch.value
        sim.addNode(NodeId("c"))
        waitForEpochBump(a.router, before)
        val c = node(NodeId("c"), sim, store, scope)
        a.reconcile(); b.reconcile(); c.reconcile()

        assertSingleOwnerPerShard(shardCount, a, b, c)
        scope.cancel()
    }

    @Test
    fun `write on a node that lost its lease is rejected NOT_OWNER`() = runBlocking {
        val scope = CoroutineScope(Job())
        val sim = SimCluster(listOf(NodeId("a"), NodeId("b")), ShardCounts(shardCount))
        val store = sharedStore(InMemoryStoreDelegate())
        val a = node(NodeId("a"), sim, store, scope)
        val b = node(NodeId("b"), sim, store, scope)

        val room = firstRoomOwnedBy(a, NodeId("a"))
        val ok = a.rpc.postLockerChange(post(room, byteArrayOf(9)))
        assertThat(ok.result is PostLockerChangeResponse.Result.OK).isTrue()

        // Remove a from the ring: b picks up a's shards, a drains its leases.
        val before = b.router.roomMap().epoch.value
        sim.removeNode(NodeId("a"))
        waitForEpochBump(b.router, before)
        b.reconcile()
        a.reconcile() // a sees itself gone → releases every lease

        // A write on a is now rejected: it no longer holds the shard's lease.
        val rejected = a.rpc.postLockerChange(post(room, byteArrayOf(9), version = 1L))
        assertThat(rejected.result is PostLockerChangeResponse.Result.NOT_OWNER).isTrue()
        // And it evicted its caches during the drop (quiesce).
        assertThat(a.evictions.get()).isGreaterThan(0)
        scope.cancel()
    }

    @Test
    fun `after handoff the new owner rebuilds routing from the store and delivery continues`() = runBlocking {
        val scope = CoroutineScope(Job())
        val sim = SimCluster(listOf(NodeId("a"), NodeId("b")), ShardCounts(shardCount))
        val store = sharedStore(InMemoryStoreDelegate())
        val a = node(NodeId("a"), sim, store, scope)
        val b = node(NodeId("b"), sim, store, scope)

        val room = firstRoomOwnedBy(a, NodeId("a"))
        // Subscribe on the current owner; the subscription is durable in the shared store.
        assertThat(doSubscribe(a, room, "sess-1").result is SubscriptionResponse.Result.OK).isTrue()
        a.rpc.postLockerChange(post(room, byteArrayOf(7)))
        assertThat(a.deliveries.map { it.second }).contains("sess-1")

        // Reshard so b owns the room; a releases (and evicts), b acquires.
        val before = b.router.roomMap().epoch.value
        sim.removeNode(NodeId("a"))
        waitForEpochBump(b.router, before)
        a.reconcile()
        b.reconcile()

        // b never cached this room; it rebuilds room→session routing from SubscriptionStore on the
        // fan-out path, and the event is delivered for the same durable subscription. a's first write
        // persisted the locker at version 0, so parentVersion=0 makes the CAS pass on the new owner.
        val delivered = b.rpc.postLockerChange(post(room, byteArrayOf(7), version = 0L))
        assertThat(delivered.result is PostLockerChangeResponse.Result.OK).isTrue()
        assertThat(b.deliveries.map { it.second }).contains("sess-1")
        scope.cancel()
    }
}
