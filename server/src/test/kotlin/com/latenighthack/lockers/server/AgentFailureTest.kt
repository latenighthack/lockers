package com.latenighthack.lockers.server

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isTrue
import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.common.v1.Locker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.room.v1.LocalRoomServiceRpc
import com.latenighthack.lockers.room.v1.PostLockerChangeRequest
import com.latenighthack.lockers.room.v1.PostLockerChangeResponse
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.room.v1.LockStoreImpl
import com.latenighthack.lockers.server.services.room.v1.LockerStoreImpl
import com.latenighthack.lockers.server.services.room.v1.RoomOwner
import com.latenighthack.lockers.server.services.room.v1.RoomOwnership
import com.latenighthack.lockers.server.services.room.v1.RoomServiceImpl
import com.latenighthack.lockers.server.services.room.v1.SubscriptionStoreImpl
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.server.storage.v1.ServerLockerId
import com.latenighthack.lockers.server.storage.v1.ServerRoomId
import com.latenighthack.lockers.session.v1.SessionGatewayService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.AfterTest

/**
 * An agent runs AFTER the client's write is persisted and fanned out; if it
 * throws, the RPC must still return OK — 500ing a durable write leaves clients
 * unable to converge (seen in prod: wedged game lobbies with started=true and
 * no frames).
 */
class AgentFailureTest {
    private val services = mutableListOf<RoomServiceImpl>()
    private val databases = mutableListOf<Database>()
    @AfterTest fun closeFixtures() = runBlocking {
        try { for (service in services) service.closeAndJoin() }
        finally { databases.forEach { it.close() } }
    }


    private val noSessionGateway = object : SessionGatewayDiscovery {
        override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
    }

    private val localOwnership = object : RoomOwnership {
        override suspend fun resolve(keyspace: Long, roomId: RoomId) = RoomOwner.Local()
    }

    private val throwingAgent = object : LockerAgentRegistry {
        override suspend fun processPayload(
            roomId: RoomId,
            lockerId: LockerId,
            locker: Locker
        ): List<LockerAgentRegistry.LockerWrite> = throw IllegalStateException("agent boom")
    }

    @Test
    fun `agent exception does not fail the already-persisted write`() = runBlocking {
        val delegate = ServerStorage.inMemory().also { databases.add(it) }
        val subs = SubscriptionStoreImpl(delegate).also { it.prepare() }
        val lockers = LockerStoreImpl(delegate).also { it.prepare() }
        val locks = LockStoreImpl(delegate).also { it.prepare() }
        delegate.open()
        val registry = SimpleMeterRegistry()
        val client = LocalRoomServiceRpc(
            RoomServiceImpl(
                subs, lockers, locks, noSessionGateway, localOwnership,
                throwingAgent, registry, LockersConfig.defaults(),
            ).also { services.add(it) }
        )

        val response = client.postLockerChange(PostLockerChangeRequest {
            roomId = RoomId(byteArrayOf(1, 2, 3))
            lockerId = LockerId {
                rawValue = byteArrayOf(9)
                keyspace = LockerKeyspace { value = 31 }
            }
            locker = Locker { open { encodedPayload = byteArrayOf(1) } }
        })

        assertThat(response.result is PostLockerChangeResponse.Result.OK).isTrue()
        assertThat(registry.get("lockers.agent.invocations").tag("outcome", "error").counter().count()).isEqualTo(1.0)
        assertThat(registry.get("lockers.rpc.requests").tags("service", "room", "operation", "write", "outcome", "ok").counter().count()).isEqualTo(1.0)

        val stored = lockers.getLocker(ServerRoomId(byteArrayOf(1, 2, 3)), 31L, ServerLockerId(byteArrayOf(9)))
        assertThat(stored).isNotNull()
        assertThat(stored!!.version).isEqualTo(response.version)
    }
}
