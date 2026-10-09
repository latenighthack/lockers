package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.server.cluster.*
import com.latenighthack.lockers.server.services.room.v1.RoomOwner
import com.latenighthack.lockers.sharding.*
import com.latenighthack.lockers.sharding.inmem.SimCluster
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.*
import kotlin.test.*

@OptIn(ExperimentalCoroutinesApi::class)
class WholeRoomOwnershipTest {
    @Test fun `every locker keyspace shares the room authority lease`() = runTest {
        val self = NodeId("a")
        val sim = SimCluster(listOf(self, NodeId("b")), ShardCounts(8))
        val router = sim.routerFor(self, backgroundScope)
        runCurrent()
        val owner = OwnerLifecycle(self, sim.coordinatorFor(self), listOf(Keyspace(0)))
        owner.reconcile(router.roomMap())
        val room = (0..1000).map { RoomId("room-$it".encodeToByteArray()) }.first { router.roomMap().owner(Keyspace(0), it.rawValue) == self }
        val ownership = RingRoomOwnership(router, owner)
        for (keyspace in listOf(0L, 1L, 30L, 31L, 999L)) assertIs<RoomOwner.Local>(ownership.resolve(keyspace, room), "keyspace $keyspace")
        owner.releaseAll()
    }
}
