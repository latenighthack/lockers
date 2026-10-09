package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.session.v1.SessionGatewayService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class ReadBoundaryAdmissionTest {
    @Test fun invalidIdentityAndReadExhaustionFailBeforeStorage() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val limits = ServerResourceLimits(globalReadsPerSecond = 1, globalReadBurst = 1, roomReadsPerSecond = 1, roomReadBurst = 1)
        val service = RoomServiceImpl(SubscriptionStoreImpl(db), LockerStoreImpl(db), LockStoreImpl(db), object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
        }, LocalRoomOwnership(), LockerAgentRegistry.None, SimpleMeterRegistry(), LockersConfig.defaults().copy(resourceLimits = limits, deliveryWorkerEnabled = false))
        val rpc = LocalRoomServiceRpc(service)
        try {
            assertEquals(Codes.INVALID_ARGUMENT, assertFailsWith<RpcResponseException> {
                rpc.getLocker(GetLockerRequest(RoomId(byteArrayOf()), LockerId(byteArrayOf(1))))
            }.code)
            assertTrue(rpc.getLocker(GetLockerRequest(RoomId(byteArrayOf(1)), LockerId(byteArrayOf(2)))).result.isOk())
            assertEquals(Codes.RESOURCE_EXHAUSTED, assertFailsWith<RpcResponseException> {
                rpc.getLocker(GetLockerRequest(RoomId(byteArrayOf(2)), LockerId(byteArrayOf(2))))
            }.code)
        } finally { service.close(); db.close() }
    }
}
