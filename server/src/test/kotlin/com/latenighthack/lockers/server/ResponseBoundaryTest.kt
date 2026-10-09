package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.claim.RoomClaimCapacityExceeded
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.SessionGatewayService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class ResponseBoundaryTest {
    private suspend fun fixture(ownership: RoomOwnership = LocalRoomOwnership()): Triple<com.latenighthack.ktstore.Database, LockerStoreImpl, RoomServiceImpl> {
        val db = ServerStorage.inMemory(); db.open(); val store = LockerStoreImpl(db)
        val service = RoomServiceImpl(SubscriptionStoreImpl(db), store, LockStoreImpl(db), object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
        }, ownership, LockerAgentRegistry.None, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryWorkerEnabled = false))
        return Triple(db, store, service)
    }
    @Test fun completeBulkReadAndConflictReplyCannotExceedEnvelopeAndPagingRemainsComplete() = runBlocking {
        val (db, store, service) = fixture(); val rpc = LocalRoomServiceRpc(service); val room = RoomId(byteArrayOf(1))
        val ids = (1..20).map { LockerId(byteArrayOf(it.toByte())) }
        try {
            val large = Locker { open { encodedPayload = ByteArray(1024 * 1024) } }
            store.updateLockers(ids.map { ServerLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(it.rawValue), large.toByteArray(), 1) })
            assertEquals(Codes.OUT_OF_RANGE, assertFailsWith<RpcResponseException> { rpc.getLockers(GetLockersRequest(room, ids)) }.code)
            assertEquals(Codes.OUT_OF_RANGE, assertFailsWith<RpcResponseException> { rpc.getAllLockers(GetAllLockersRequest(room)) }.code)
            val change = Locker { open { encodedPayload = byteArrayOf(1) } }
            assertEquals(Codes.OUT_OF_RANGE, assertFailsWith<RpcResponseException> {
                rpc.postLockerChanges(PostLockerChangesRequest(roomId = room, writeRequestId = ByteArray(16) { 2 },
                    changes = ids.map { PostLockerChangeRequest(roomId = room, lockerId = it, locker = change) }))
            }.code)
            assertTrue(store.getAllLockers(ServerRoomId(room.rawValue)).all { it.version == 1L })
            assertEquals(0, DeliveryOutboxStore(db).pendingCountLong())
            var page = rpc.getAllLockers(GetAllLockersRequest(roomId = room, pageSize = 64)); var count = page.lockers.size
            while (page.nextPageToken.isNotEmpty()) {
                assertTrue(page.toByteArray().size <= ProtocolValidation.MAX_ENVELOPE_BYTES)
                page = rpc.getAllLockers(GetAllLockersRequest(roomId = room, pageSize = 64, pageToken = page.nextPageToken)); count += page.lockers.size
            }
            assertEquals(ids.size, count)
        } finally { service.close(); db.close() }
    }
    @Test fun permanentClaimCapacityMapsToFailedPreconditionWithoutSavingWrite() = runBlocking {
        val (db, store, service) = fixture(object : RoomOwnership {
            override suspend fun resolve(keyspace: Long, roomId: RoomId): RoomOwner = throw RoomClaimCapacityExceeded()
        })
        val rpc = LocalRoomServiceRpc(service); val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2))
        try {
            assertEquals(Codes.FAILED_PRECONDITION, assertFailsWith<RpcResponseException> {
                rpc.postLockerChanges(PostLockerChangesRequest(roomId = room, writeRequestId = ByteArray(16) { 3 },
                    changes = listOf(PostLockerChangeRequest(roomId = room, lockerId = id, locker = Locker { open { encodedPayload = byteArrayOf(1) } }))))
            }.code)
            assertNull(store.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue)))
        } finally { service.close(); db.close() }
    }
}
