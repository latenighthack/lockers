package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class AgentRetentionTest {
    @Test fun `completed receipts expire after31days while pending work and its inputs remain`(): Unit = runBlocking {
        var now = 1L
        val db = ServerStorage.inMemory(); val outbox = DeliveryOutboxStore(db, clock = { now }).also { it.prepareStores() }; db.open()
        val room = RoomId(byteArrayOf(1))
        try {
            for (n in 1..2) {
                val req = PostLockerChangesRequest(roomId = room, writeRequestId = ByteArray(16) { n.toByte() })
                val response = PostLockerChangesResponse(agentPending = n == 2)
                outbox.atomic(room) {
                    outbox.agentWork.create(room, req, response, "fixture/v1", n.toLong(), applied = n == 1)
                    outbox.saveReceipt(ServerWriteReceipt(roomId = ServerRoomId(room.rawValue), requestId = req.writeRequestId, encodedOutcome = response.toByteArray()))
                }
            }
            now += 31L * 24 * 60 * 60 * 1000
            outbox.pruneAgentReceipts()
            assertNull(outbox.receipt(room, ByteArray(16) { 1 }))
            assertNull(outbox.agentWork.find(room, ByteArray(16) { 1 }))
            assertNotNull(outbox.receipt(room, ByteArray(16) { 2 }))
            assertNotNull(outbox.agentWork.find(room, ByteArray(16) { 2 }))
        } finally { db.close() }
    }
}
