package com.latenighthack.lockers.server

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
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
import com.latenighthack.lockers.server.agents.ExampleLockerAgent
import com.latenighthack.lockers.server.cluster.RingRoomOwnership
import com.latenighthack.lockers.server.services.room.v1.LockStoreImpl
import com.latenighthack.lockers.server.services.room.v1.LockerStoreImpl
import com.latenighthack.lockers.server.services.room.v1.RoomOwner
import com.latenighthack.lockers.server.services.room.v1.RoomOwnership
import com.latenighthack.lockers.server.services.room.v1.RoomServiceImpl
import com.latenighthack.lockers.server.services.room.v1.SubscriptionStoreImpl
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.session.v1.SessionGatewayService
import com.latenighthack.lockers.sharding.Keyspace
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.ShardCounts
import com.latenighthack.lockers.sharding.inmem.SimCluster
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

@OptIn(ExperimentalCoroutinesApi::class)
class RoomOwnershipTest {

    private val noSessionGateway = object : SessionGatewayDiscovery {
        override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
    }

    private suspend fun roomServiceWith(ownership: RoomOwnership): LocalRoomServiceRpc {
        val delegate = InMemoryStoreDelegate()
        val subs = SubscriptionStoreImpl(delegate).also { it.prepare() }
        val lockers = LockerStoreImpl(delegate).also { it.prepare() }
        val locks = LockStoreImpl(delegate).also { it.prepare() }
        delegate.createStores()
        val impl = RoomServiceImpl(
            subs, lockers, locks, noSessionGateway, ownership,
            ExampleLockerAgent(), SimpleMeterRegistry(), LockersConfig.defaults(),
        )
        return LocalRoomServiceRpc(impl)
    }

    private fun postRequest() = PostLockerChangeRequest {
        roomId = RoomId(byteArrayOf(1, 2, 3))
        lockerId = LockerId {
            rawValue = byteArrayOf(9)
            keyspace = LockerKeyspace { value = 1 }
        }
        locker = Locker { }
    }

    @Test
    fun `write to a non-owner node is rejected with NOT_OWNER and a redirect`() = runBlocking {
        val client = roomServiceWith(object : RoomOwnership {
            override suspend fun resolve(keyspace: Long, roomId: RoomId) =
                RoomOwner.Remote(address = "peer-b:8080", epoch = 7)
        })

        val response = client.postLockerChange(postRequest())

        assertThat(response.result is PostLockerChangeResponse.Result.NOT_OWNER).isTrue()
        assertThat(response.redirect?.ownerAddress).isEqualTo("peer-b:8080")
        assertThat(response.redirect?.epoch).isEqualTo(7L)
    }

    @Test
    fun `write to the owning node proceeds`() = runBlocking {
        val client = roomServiceWith(object : RoomOwnership {
            override suspend fun resolve(keyspace: Long, roomId: RoomId) = RoomOwner.Local
        })

        val response = client.postLockerChange(postRequest())

        assertThat(response.result is PostLockerChangeResponse.Result.NOT_OWNER).isFalse()
        assertThat(response.result is PostLockerChangeResponse.Result.OK).isTrue()
    }

    @Test
    fun `RingRoomOwnership reports local vs remote exactly per the room ring`() = runTest {
        val sim = SimCluster(listOf(NodeId("a"), NodeId("b"), NodeId("c")), ShardCounts(64))
        val router = sim.routerFor(NodeId("a"), backgroundScope)
        runCurrent()
        val ownership = RingRoomOwnership(router)

        var localSeen = 0
        var remoteSeen = 0
        repeat(300) { i ->
            val roomId = RoomId("room-$i".encodeToByteArray())
            val resolved = ownership.resolve(1L, roomId)
            val route = router.routeRoom(Keyspace(1), roomId.rawValue)
            if (route.isLocal) {
                assertThat(resolved).isEqualTo(RoomOwner.Local)
                localSeen++
            } else {
                assertThat(resolved is RoomOwner.Remote).isTrue()
                val remote = resolved as RoomOwner.Remote
                assertThat(remote.address).isEqualTo("${route.address!!.host}:${route.address!!.port}")
                remoteSeen++
            }
        }
        assertThat(localSeen).isGreaterThan(0)
        assertThat(remoteSeen).isGreaterThan(0)
    }
}
