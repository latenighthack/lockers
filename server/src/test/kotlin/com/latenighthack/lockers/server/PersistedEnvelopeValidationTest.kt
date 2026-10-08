package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.SessionGatewayService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class PersistedEnvelopeValidationTest {
    private val room = RoomId(byteArrayOf(1)); private val id = LockerId(byteArrayOf(2))
    private fun open() = Locker { open { encodedPayload = byteArrayOf(4) } }
    private fun ambiguous() = Locker { open { encodedPayload = byteArrayOf(4) }; sealed { payload { enclosure { innerPayload = byteArrayOf(5) } } } }
    private suspend fun fixture(agent: LockerAgentRegistry = LockerAgentRegistry.None): Triple<com.latenighthack.ktstore.Database, LockerStoreImpl, RoomServiceImpl> {
        val db = ServerStorage.inMemory(); db.open()
        val lockers = LockerStoreImpl(db)
        val service = RoomServiceImpl(SubscriptionStoreImpl(db), lockers, LockStoreImpl(db), object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
        }, LocalRoomOwnership(), agent, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryWorkerEnabled = false))
        return Triple(db, lockers, service)
    }
    @Test fun corruptAndAmbiguousReadsReturnRepairIdentityAndVersion() = runBlocking {
        val (db, lockers, service) = fixture(); val rpc = LocalRoomServiceRpc(service)
        try {
            for (bytes in listOf(byteArrayOf(0x0A, 0x7F), ambiguous().toByteArray())) {
                lockers.updateLocker(ServerLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue), bytes, 7))
                assertNotNull(lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue)))
                val one = rpc.getLocker(GetLockerRequest(room, id))
                assertEquals(2, one.result.value, "response=$one")
                assertEquals(7, one.locker?.version)
                assertNull(one.locker?.locker)
                assertEquals(id.rawValue.toList(), one.locker?.lockerId?.rawValue?.toList())
                val many = rpc.getLockers(GetLockersRequest(room, listOf(id)))
                assertEquals(2, many.results.single().result.value)
                val all = rpc.getAllLockers(GetAllLockersRequest(room))
                assertEquals(2, all.result.value)
                assertEquals(7, all.lockers.single().version)
                val snapshot = rpc.subscribeAndSnapshot(SubscribeAndSnapshotRequest(roomId = room, sessionId = SessionId(byteArrayOf(8))))
                assertEquals(2, snapshot.result.value)
                assertEquals(7, snapshot.lockers.single().version)
            }
        } finally { service.close(); db.close() }
    }
    @Test fun ambiguousAgentOutputNeverCommitsDerivedRowsOrDelivery() = runBlocking {
        val derived = LockerId(byteArrayOf(3))
        val agent = object : LockerAgentRegistry {
            override suspend fun processPayload(roomId: RoomId, lockerId: LockerId, locker: Locker) =
                listOf(LockerAgentRegistry.LockerWrite(derived, ambiguous()))
        }
        val (db, lockers, service) = fixture(agent)
        try {
            val response = LocalRoomServiceRpc(service).postLockerChanges(PostLockerChangesRequest(roomId = room,
                writeRequestId = ByteArray(16) { 8 }, changes = listOf(PostLockerChangeRequest(roomId = room, lockerId = id, locker = open()))))
            assertTrue(response.result.isOk())
            assertTrue(response.agentFailed)
            assertNotNull(lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue)))
            assertNull(lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(derived.rawValue)))
        } finally { service.close(); db.close() }
    }
}
