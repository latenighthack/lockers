package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class SubscriptionFreshnessTest {
    @Test fun `legacy fanout sees subscriptions changed on another replica`() = runBlocking {
        val database = ServerStorage.inMemory()
        val subscriptions = SubscriptionStoreImpl(database).also { it.prepare() }
        val lockers = LockerStoreImpl(database).also { it.prepare() }
        val locks = LockStoreImpl(database).also { it.prepare() }
        database.open()
        val delivered = mutableListOf<SessionId>()
        val discovery = object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService = object : SessionGatewayService {
                override suspend fun postEvent(request: PostEventRequest) = PostEventResponse(result = PostEventResponse.Result.OK).also { delivered.addAll(request.sessionIds) }
                override suspend fun postEvents(request: PostEventsRequest) = PostEventsResponse(request.groups.map { postEvent(it) })
            }
        }
        val config = LockersConfig.defaults().copy(deliveryOutboxEnabled = false)
        val nodes = List(2) { RoomServiceImpl(subscriptions, lockers, locks, discovery, LocalRoomOwnership(), LockerAgentRegistry.None, SimpleMeterRegistry(), config) }
        val room = RoomId(byteArrayOf(1))
        val sid = SessionId(byteArrayOf(2))
        fun request(id: Byte) = PostLockerChangeRequest(roomId = room, lockerId = LockerId(byteArrayOf(id)), locker = Locker { open { encodedPayload = byteArrayOf(1) } })
        try {
            val a = LocalRoomServiceRpc(nodes[0]); val b = LocalRoomServiceRpc(nodes[1])
            a.postLockerChange(request(1)) // Cache the empty recipient set on A.
            b.subscription(SubscriptionRequest { roomId = room; sessionId = sid; kind.subscribe {} })
            a.postLockerChange(request(2))
            assertEquals(listOf(sid), delivered)
            delivered.clear()
            b.subscription(SubscriptionRequest { roomId = room; sessionId = sid; kind.unsubscribe {} })
            a.postLockerChange(request(3))
            assertEquals(emptyList(), delivered)
        } finally { nodes.forEach { it.close() } }
    }
}
