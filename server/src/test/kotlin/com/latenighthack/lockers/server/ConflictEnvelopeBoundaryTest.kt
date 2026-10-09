package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
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

class ConflictEnvelopeBoundaryTest {
    private suspend fun fixture(ownership: RoomOwnership = LocalRoomOwnership()): Triple<com.latenighthack.ktstore.Database, LockerStoreImpl, RoomServiceImpl> {
        val db = ServerStorage.inMemory(); db.open(); val store = LockerStoreImpl(db)
        val service = RoomServiceImpl(SubscriptionStoreImpl(db), store, LockStoreImpl(db), object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
        }, ownership, LockerAgentRegistry.None, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryWorkerEnabled = false))
        return Triple(db, store, service)
    }
    @Test fun contradictoryStoredEnvelopeNeverEscapesThroughConflictReplies() = runBlocking {
        val (db, store, service) = fixture(); val rpc = LocalRoomServiceRpc(service); val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2))
        try {
            val bad = Locker { open { encodedPayload = byteArrayOf(1) }; sealed { payload { enclosure { innerPayload = byteArrayOf(2) } } } }
            store.updateLocker(ServerLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue), bad.toByteArray(), 7))
            val error = assertFailsWith<RpcResponseException> {
                rpc.postLockerChange(PostLockerChangeRequest(roomId = room, lockerId = id, locker = Locker { open { encodedPayload = byteArrayOf(3) } }))
            }
            assertEquals(Codes.DATA_LOSS, error.code); assertTrue(error.errorMessage.contains("version 7"))
            val delete = assertFailsWith<RpcResponseException> { rpc.deleteLocker(DeleteLockerRequest(roomId = room, lockerId = id)) }
            assertEquals(Codes.DATA_LOSS, delete.code); assertTrue(delete.errorMessage.contains("version 7"))
            assertContentEquals(bad.toByteArray(), store.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue))!!.locker)
        } finally { service.close(); db.close() }
    }
}
