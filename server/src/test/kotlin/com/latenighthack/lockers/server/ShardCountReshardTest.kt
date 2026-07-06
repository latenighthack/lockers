package com.latenighthack.lockers.server

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
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
 * M7 — online shard-count change (split-range) end to end over the sim cluster and one shared store.
 * Changing keyspace K's room count repartitions only its rooms; a room that moves to a new owner is
 * handed off with fenced ownership, the new owner rebuilds room→session routing from the durable
 * `SubscriptionStore`, and delivery continues with no event loss. The version CAS backstops any
 * ragged dual-coordination during cutover (`lockers.reshard.cas.conflicts`).
 */
@OptIn(ExperimentalCoroutinesApi::class)
class ShardCountReshardTest {

    private val roomKeyspace = Keyspace(1L)

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

    private class Node(
        val id: NodeId,
        val router: ShardRouter,
        val lifecycle: OwnerLifecycle,
        val impl: RoomServiceImpl,
        val registry: SimpleMeterRegistry,
        val deliveries: MutableList<Pair<NodeId, String>>,
    ) {
        val rpc = LocalRoomServiceRpc(impl)
        suspend fun reconcile() = lifecycle.reconcile(router.roomMap())
        fun casConflicts(): Double = registry.counter("lockers.reshard.cas.conflicts").count()
        fun roomsRebuilt(): Double = registry.counter("lockers.reshard.rooms.rebuilt").count()
    }

    private suspend fun node(
        id: NodeId,
        sim: SimCluster,
        subs: SubscriptionStoreImpl,
        lockers: LockerStoreImpl,
        locks: LockStoreImpl,
        scope: CoroutineScope,
    ): Node {
        val router = sim.routerFor(id, scope)
        val implHolder = arrayOfNulls<RoomServiceImpl>(1)
        val lifecycle = OwnerLifecycle(
            self = id,
            coordinator = sim.coordinatorFor(id),
            keyspaces = listOf(roomKeyspace),
            onShardsDropped = { implHolder[0]?.evictRoomCaches() },
        )
        val deliveries = CopyOnWriteArrayList<Pair<NodeId, String>>()
        val registry = SimpleMeterRegistry()
        val impl = RoomServiceImpl(
            subs, lockers, locks,
            RecordingGatewayDiscovery(id, deliveries),
            RingRoomOwnership(router, lifecycle),
            ExampleLockerAgent(), registry, LockersConfig.defaults(),
        )
        implHolder[0] = impl
        lifecycle.reconcile(router.roomMap())
        return Node(id, router, lifecycle, impl, registry, deliveries)
    }

    private fun post(room: ByteArray, locker: ByteArray, version: Long) = PostLockerChangeRequest {
        roomId = RoomId(room)
        lockerId = LockerId {
            rawValue = locker
            keyspace = LockerKeyspace { value = roomKeyspace.value }
        }
        this.locker = Locker { }
        parentVersion = version
    }

    private suspend fun subscribe(node: Node, room: ByteArray, session: String) =
        node.rpc.subscription(SubscriptionRequest {
            roomId = RoomId(room)
            sessionId = SessionId(session.encodeToByteArray())
            kind.subscribe { }
        })

    private suspend fun waitForEpochBump(router: ShardRouter, fromEpoch: Long) {
        repeat(500) {
            if (router.roomMap().epoch.value > fromEpoch) return
            delay(5)
        }
        error("epoch did not advance past $fromEpoch")
    }

    /**
     * Finds a room whose owner changes from `a` to `b` when K's count goes from [fromCount] to
     * [toCount] on the given node set — the split-range room we hand off.
     */
    private fun repartitionedRoom(a: Node, fromCount: Int, toCount: Int): ByteArray {
        val nodes = a.router.roomMap().nodes
        val from = ShardCounts(default = 256, perKeyspace = mapOf(roomKeyspace to fromCount))
        val to = ShardCounts(default = 256, perKeyspace = mapOf(roomKeyspace to toCount))
        val ring = a.router.roomMap().assignment
        for (i in 0 until 20_000) {
            val room = "room-$i".encodeToByteArray()
            val partFrom = com.latenighthack.lockers.sharding.HashPartitionFunction()
                .partition(roomKeyspace, room, from.forKeyspace(roomKeyspace))
            val partTo = com.latenighthack.lockers.sharding.HashPartitionFunction()
                .partition(roomKeyspace, room, to.forKeyspace(roomKeyspace))
            val ownerFrom = ring.owner(roomKeyspace, partFrom)
            val ownerTo = ring.owner(roomKeyspace, partTo)
            if (ownerFrom == a.id && ownerTo != a.id) return room
        }
        error("no room found that repartitions away from ${a.id}")
    }

    @Test
    fun `count change hands off a split-range room and delivery continues with no loss`() = runBlocking {
        val scope = CoroutineScope(Job())
        val fromCount = 256
        val toCount = 128
        val sim = SimCluster(
            listOf(NodeId("a"), NodeId("b")),
            ShardCounts(default = 256, perKeyspace = mapOf(roomKeyspace to fromCount)),
        )
        val delegate = InMemoryStoreDelegate()
        val subs = SubscriptionStoreImpl(delegate).also { it.prepare() }
        val lockers = LockerStoreImpl(delegate).also { it.prepare() }
        val locks = LockStoreImpl(delegate).also { it.prepare() }
        delegate.createStores()

        val a = node(NodeId("a"), sim, subs, lockers, locks, scope)
        val b = node(NodeId("b"), sim, subs, lockers, locks, scope)

        // A room that a owns at 256 but b will own at 128.
        val room = repartitionedRoom(a, fromCount, toCount)
        assertThat(a.router.roomMap().owner(roomKeyspace, room)).isEqualTo(NodeId("a"))

        // Subscribe (durable) and write once on the current owner a; delivery lands.
        assertThat(subscribe(a, room, "sess-1").result is com.latenighthack.lockers.room.v1.SubscriptionResponse.Result.OK).isTrue()
        val first = a.rpc.postLockerChange(post(room, byteArrayOf(5), version = 0L))
        assertThat(first.result is PostLockerChangeResponse.Result.OK).isTrue()
        assertThat(a.deliveries.map { it.second }).contains("sess-1")

        // Online repartition K: 256 -> 128. Same node set, higher epoch.
        val before = a.router.roomMap().epoch.value
        sim.setRoomCounts(ShardCounts(default = 256, perKeyspace = mapOf(roomKeyspace to toCount)))
        waitForEpochBump(a.router, before)
        // Fenced handoff: old owner releases (evicting caches), new owner acquires.
        a.reconcile()
        b.reconcile()
        assertThat(b.router.roomMap().owner(roomKeyspace, room)).isEqualTo(NodeId("b"))

        // Old owner now rejects the write with NOT_OWNER (it dropped the lease); no dual write.
        val rejected = a.rpc.postLockerChange(post(room, byteArrayOf(5), version = 0L))
        assertThat(rejected.result is PostLockerChangeResponse.Result.NOT_OWNER).isTrue()

        // New owner b never cached this room; it rebuilds routing from SubscriptionStore and delivers
        // for the same durable subscription — no event loss across the count change. a's first write
        // persisted the locker at version 0, so parentVersion=0 makes the CAS pass on the new owner.
        val rebuiltBefore = b.roomsRebuilt()
        val delivered = b.rpc.postLockerChange(post(room, byteArrayOf(5), version = 0L))
        assertThat(delivered.result is PostLockerChangeResponse.Result.OK).isTrue()
        assertThat(b.deliveries.map { it.second }).contains("sess-1")
        assertThat(b.roomsRebuilt()).isGreaterThan(rebuiltBefore)
        scope.cancel()
    }

    @Test
    fun `a stale write on the old owner increments the CAS conflict backstop`() = runBlocking {
        val scope = CoroutineScope(Job())
        val sim = SimCluster(listOf(NodeId("a")), ShardCounts(default = 64, perKeyspace = mapOf(roomKeyspace to 64)))
        val delegate = InMemoryStoreDelegate()
        val subs = SubscriptionStoreImpl(delegate).also { it.prepare() }
        val lockers = LockerStoreImpl(delegate).also { it.prepare() }
        val locks = LockStoreImpl(delegate).also { it.prepare() }
        delegate.createStores()
        val a = node(NodeId("a"), sim, subs, lockers, locks, scope)

        val room = "room-cas".encodeToByteArray()
        // First write persists the locker at version 0. A subsequent write bearing a stale
        // parentVersion (5 != 0) must lose the CAS (UPDATE_LOCAL_VERSION) and bump
        // reshard.cas.conflicts — the dual-coordination detector two racing owners would trip.
        assertThat(a.rpc.postLockerChange(post(room, byteArrayOf(1), version = 0L)).result is PostLockerChangeResponse.Result.OK).isTrue()
        val conflictsBefore = a.casConflicts()
        val stale = a.rpc.postLockerChange(post(room, byteArrayOf(1), version = 5L))
        assertThat(stale.result is PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION).isTrue()
        assertThat(a.casConflicts()).isGreaterThan(conflictsBefore)
        scope.cancel()
    }
}
