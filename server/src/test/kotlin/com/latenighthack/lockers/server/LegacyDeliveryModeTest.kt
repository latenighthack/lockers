package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import kotlin.test.*

class LegacyDeliveryModeTest {
    @Test fun `single writes deliver source and derived events in both capability modes`(): Unit = runBlocking {
        for (modern in listOf(false, true)) {
            val db = ServerStorage.inMemory(); val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
            val subs = SubscriptionStoreImpl(db).also { it.prepare() }; val lockers = LockerStoreImpl(db).also { it.prepare() }; val locks = LockStoreImpl(db).also { it.prepare() }
            db.open()
            val room = RoomId(byteArrayOf(1)); val sid = SessionId(byteArrayOf(3)); val source = LockerId(byteArrayOf(2)); val derived = LockerId(byteArrayOf(4))
            subs.addSubscription(ServerSessionId(sid.rawValue), ServerRoomId(room.rawValue))
            val delivered = java.util.concurrent.CopyOnWriteArrayList<Event>()
            val gateway = object: SessionGatewayService {
                override suspend fun postEvent(request: PostEventRequest): PostEventResponse { delivered.add(request.event!!); return PostEventResponse() }
                override suspend fun postEvents(request: PostEventsRequest) = PostEventsResponse(request.groups.map { postEvent(it) })
            }
            val registry = object: IdempotentLockerAgentRegistry {
                override val agentVersion = "legacy-delivery/v1"
                override suspend fun processPayload(effectKey: ByteArray, roomId: RoomId, lockerId: LockerId, locker: Locker) = listOf(LockerAgentRegistry.LockerWrite(derived, Locker { open { encodedPayload = byteArrayOf(5) } }))
            }
            val service = RoomServiceImpl(subs, lockers, locks, object: SessionGatewayDiscovery {
                override suspend fun findServer(sessionId: SessionId): SessionGatewayService = gateway
            }, LocalRoomOwnership(), registry, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryOutboxEnabled = modern, deliveryWorkerEnabled = true), outbox)
            try {
                service.start()
                val response = LocalRoomServiceRpc(service).postLockerChange(PostLockerChangeRequest(roomId = room, lockerId = source, locker = Locker { open { encodedPayload = byteArrayOf(6) } }))
                assertTrue(response.result.isOk()); assertFalse(response.agentPending); assertFalse(response.agentFailed)
                withTimeout(5000) { while (delivered.size < 2 || outbox.pendingCount() > 0) delay(20) }
                assertEquals(setOf(source.rawValue.toList(), derived.rawValue.toList()), delivered.map { it.locker!!.lockerId!!.rawValue.toList() }.toSet())
                assertEquals(listOf(1L, 2L), delivered.map { it.roomSequence })
            } finally { service.closeAndJoin(); db.close() }
        }
    }
}
