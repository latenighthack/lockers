package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.session.v1.SessionGatewayService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class RequestPreflightTest {
    @Test fun malformedWritesDoNotReachRoomAdmission(): Unit = runBlocking {
        fixture { calls, rpc ->
            val valid = PostLockerChangeRequest(roomId = room, lockerId = id, locker = Locker { open { encodedPayload = byteArrayOf(1) } })
            for (request in listOf(
                valid.copy(roomId = RoomId(byteArrayOf())),
                valid.copy(lockerId = LockerId(ByteArray(129))),
                valid.copy(parentVersion = -1),
                valid.copy(notification = Notification(payload = Payload(ByteArray(65 * 1024)))),
                valid.copy(writeSignature = Signature(signature = ByteArray(1024))),
                valid.copy(locker = Locker()),
            )) {
                rpc.postLockerChange(request)
                assertEquals(0, calls(), "Malformed packet reached permanent room claim admission")
            }
        }
    }
    @Test fun malformedAuthorityPacketsDoNotReachRoomAdmission(): Unit = runBlocking {
        fixture { calls, rpc ->
            rpc.lockLocker(LockLockerRequest(roomId = room, grant = LockGrant(scope = LockScope(), publicKey = Secp256R1Key.PublicKey(byteArrayOf(1)))))
            assertEquals(0, calls())
            rpc.unlockLocker(UnlockLockerRequest(roomId = room, scope = LockScope(), signature = Signature(signature = ByteArray(1024))))
            assertEquals(0, calls())
            rpc.deleteLocker(DeleteLockerRequest(roomId = room, lockerId = id, writeSignature = Signature(signature = ByteArray(1024))))
            assertEquals(0, calls())
        }
    }
    private val room = RoomId(byteArrayOf(1))
    private val id = LockerId(byteArrayOf(2))
    private suspend fun fixture(block: suspend (() -> Int, RoomService) -> Unit) {
        val db = ServerStorage.inMemory()
        val subs = SubscriptionStoreImpl(db).also { it.prepare() }
        val lockers = LockerStoreImpl(db).also { it.prepare() }
        val locks = LockStoreImpl(db).also { it.prepare() }
        db.open()
        var calls = 0
        val service = RoomServiceImpl(subs, lockers, locks, object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
        }, object : RoomOwnership {
            override suspend fun resolve(keyspace: Long, roomId: RoomId): RoomOwner { calls++; return RoomOwner.Local() }
        }, object : LockerAgentRegistry {
            override suspend fun processPayload(roomId: RoomId, lockerId: LockerId, locker: Locker) = emptyList<LockerAgentRegistry.LockerWrite>()
        }, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryWorkerEnabled = false))
        try { block({ calls }, LocalRoomServiceRpc(service)) }
        finally { service.closeAndJoin(); db.close() }
    }
}
