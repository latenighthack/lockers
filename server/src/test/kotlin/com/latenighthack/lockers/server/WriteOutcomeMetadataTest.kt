package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class WriteOutcomeMetadataTest {
    @Test fun `historical receipts expose versions and uncertain agents without replay`(): Unit = runBlocking {
        val core = ServerCore::class.create(LockersConfig.defaults().copy(deliveryWorkerEnabled = false), ServerStorage.inMemory())
        core.overridePushProviders = emptyList()
        core.setup()
        val component = MonolithComponent(core)
        val rpc = LocalRoomServiceRpc(component.roomServiceModule.serverImpl)
        val room = RoomId(byteArrayOf(1))
        val writeId = ByteArray(16) { 2 }
        try {
            assertEquals(GetWriteOutcomeResponse.Result.INVALID, rpc.getWriteOutcome(GetWriteOutcomeRequest(roomId = room, writeRequestId = byteArrayOf())).result)
            assertEquals(GetWriteOutcomeResponse.Result.NOT_FOUND, rpc.getWriteOutcome(GetWriteOutcomeRequest(roomId = room, writeRequestId = writeId)).result)
            for ((response, state) in listOf(
                PostLockerChangesResponse(agentPending = true) to WriteOutcome.AgentState.INDETERMINATE,
                PostLockerChangesResponse(agentFailed = true) to WriteOutcome.AgentState.FAILED,
                PostLockerChangesResponse() to WriteOutcome.AgentState.APPLIED,
            )) {
                core.deliveryOutbox!!.saveReceipt(ServerWriteReceipt(requestId = writeId, roomId = ServerRoomId(room.rawValue),
                    encodedOutcome = response.copy(changes = listOf(PostLockerChangeResponse(version = 5))).toByteArray()))
                val outcome = rpc.getWriteOutcome(GetWriteOutcomeRequest(roomId = room, writeRequestId = writeId)).outcome!!
                assertEquals(room, outcome.roomId)
                assertContentEquals(writeId, outcome.writeRequestId)
                assertEquals(state, outcome.agentState)
                assertEquals(5, outcome.sourceVersions.single().version)
                assertNull(outcome.sourceVersions.single().lockerId) // Historical receipt did not retain it.
            }
        } finally { component.closeAndJoin() }
    }
}
