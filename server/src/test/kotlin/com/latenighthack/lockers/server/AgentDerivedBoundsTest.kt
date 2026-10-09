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

class AgentDerivedBoundsTest {
    private val room = RoomId(byteArrayOf(1))
    private val firstId = LockerId(byteArrayOf(8)); private val secondId = LockerId(byteArrayOf(9))
    private fun body() = Locker { open { encodedPayload = byteArrayOf(1) } }
    private suspend fun verify(writes: List<LockerAgentRegistry.LockerWrite>, seedMax: Boolean = false) {
        val db = ServerStorage.inMemory()
        val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
        val lockers = LockerStoreImpl(db).also { it.prepare() }; val locks = LockStoreImpl(db).also { it.prepare() }; val subs = SubscriptionStoreImpl(db).also { it.prepare() }
        db.open()
        if (seedMax) lockers.updateLocker(ServerLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(secondId.rawValue), body().toByteArray(), Long.MAX_VALUE))
        val agent = object: IdempotentLockerAgentRegistry {
            override val agentVersion = "bounds/v1"
            override suspend fun processPayload(effectKey: ByteArray, roomId: RoomId, lockerId: LockerId, locker: Locker) = writes
        }
        val service = RoomServiceImpl(subs, lockers, locks, object: SessionGatewayDiscovery { override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null },
            LocalRoomOwnership(), agent, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryWorkerEnabled = false, maxLockerPayloadBytes = ProtocolValidation.MAX_ENVELOPE_BYTES), outbox)
        try {
            service.start()
            val result = LocalRoomServiceRpc(service).postLockerChanges(PostLockerChangesRequest(roomId = room, writeRequestId = ByteArray(16) { 5 }, changes = listOf(
                PostLockerChangeRequest(roomId = room, lockerId = LockerId(byteArrayOf(2)), locker = body()))))
            assertTrue(result.result.isOk()); assertTrue(result.agentFailed)
            assertNull(lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(firstId.rawValue)))
            if (seedMax) assertEquals(Long.MAX_VALUE, lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(secondId.rawValue))!!.version)
            assertEquals(1L, outbox.watermark(room))
        } finally { service.closeAndJoin(); db.close() }
    }
    @Test fun `invalid derived identity fails before any output is applied`(): Unit = runBlocking {
        verify(listOf(LockerAgentRegistry.LockerWrite(firstId, body()), LockerAgentRegistry.LockerWrite(LockerId(ByteArray(129)), body())))
    }
    @Test fun `derived event budget includes its protocol framing`(): Unit = runBlocking {
        val large = Locker { open { encodedPayload = ByteArray(ProtocolValidation.MAX_ENVELOPE_BYTES - 16 * 1024 - 4) } }
        verify(listOf(LockerAgentRegistry.LockerWrite(firstId, large)))
    }
    @Test fun `derived batch count cannot bypass admission through a trusted agent`(): Unit = runBlocking {
        verify((0 until 65).map { LockerAgentRegistry.LockerWrite(LockerId(byteArrayOf(it.toByte())), body()) })
    }
    @Test fun `derived version exhaustion rolls back all derived lockers and events`(): Unit = runBlocking {
        verify(listOf(LockerAgentRegistry.LockerWrite(firstId, body()), LockerAgentRegistry.LockerWrite(secondId, body())), seedMax = true)
    }
}
