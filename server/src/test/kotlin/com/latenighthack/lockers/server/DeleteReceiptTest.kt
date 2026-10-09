package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class DeleteReceiptTest {
    private val room = RoomId(byteArrayOf(11))
    private val id = LockerId(byteArrayOf(12), LockerKeyspace(0))

    @Test fun lostDeleteReplyReplayPreservesRecreatedLockerAndDeliveryCount(): Unit = runBlocking {
        fixture { core, rpc ->
            assertEquals(1, rpc.postLockerChange(write(0)).version)
            val request = DeleteLockerRequest(roomId = room, lockerId = id, parentVersion = 1, writeRequestId = ByteArray(32) { 4 })
            val first = rpc.deleteLocker(request)
            assertEquals(DeleteLockerResponse.Result.OK, first.result)
            assertEquals(2, first.version)
            assertEquals(3, rpc.postLockerChange(write(2)).version)
            val watermark = core.deliveryOutbox!!.watermark(room)
            val replay = rpc.deleteLocker(request)
            assertEquals(first, replay, "lost reply must replay its immutable receipt")
            assertEquals(watermark, core.deliveryOutbox!!.watermark(room))
            val current = rpc.getLocker(GetLockerRequest(roomId = room, lockerId = id))
            assertEquals(3, current.locker!!.version)
            assertNotNull(current.locker!!.locker)
            val outcome = rpc.getWriteOutcome(GetWriteOutcomeRequest(roomId = room, writeRequestId = request.writeRequestId)).outcome!!
            assertEquals(WriteOutcome.AgentState.APPLIED, outcome.agentState)
            assertEquals(id, outcome.sourceVersions.single().lockerId)
            assertEquals(2, outcome.sourceVersions.single().version)
        }
    }

    @Test fun reusedDeleteIdentityRejectsDifferentPacket(): Unit = runBlocking {
        fixture { _, rpc ->
            val request = DeleteLockerRequest(roomId = room, lockerId = id, parentVersion = 0, writeRequestId = ByteArray(32) { 5 })
            assertEquals(DeleteLockerResponse.Result.OK, rpc.deleteLocker(request).result)
            assertEquals(DeleteLockerResponse.Result.REQUEST_ID_REUSED, rpc.deleteLocker(request.copy(parentVersion = 1)).result)
        }
    }

    @Test fun deletingAnExistingTombstoneDoesNotAdvanceVersionOrFanout(): Unit = runBlocking {
        fixture { core, rpc ->
            assertEquals(1, rpc.deleteLocker(DeleteLockerRequest(roomId = room, lockerId = id)).version)
            val watermark = core.deliveryOutbox!!.watermark(room)
            val second = rpc.deleteLocker(DeleteLockerRequest(roomId = room, lockerId = id, parentVersion = 1))
            assertEquals(DeleteLockerResponse.Result.OK, second.result)
            assertEquals(1, second.version)
            assertEquals(watermark, core.deliveryOutbox!!.watermark(room))
        }
    }

    private fun write(parent: Long) = PostLockerChangeRequest(roomId = room, lockerId = id, parentVersion = parent,
        locker = Locker { open { encodedPayload = byteArrayOf(42) } })
    private suspend fun fixture(block: suspend (ServerCore, RoomService) -> Unit) {
        val db = ServerStorage.inMemory()
        val core = ServerCore::class.create(LockersConfig.defaults().copy(deliveryWorkerEnabled = false), db)
        core.overridePushProviders = emptyList()
        core.setup()
        val component = MonolithComponent(core)
        try { block(core, LocalRoomServiceRpc(component.roomServiceModule.serverImpl)) }
        finally { component.closeAndJoin(); db.close() }
    }
}
