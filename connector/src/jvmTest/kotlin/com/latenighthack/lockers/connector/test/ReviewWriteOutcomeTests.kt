package com.latenighthack.lockers.connector.test
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.connector.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.test.*
class ReviewWriteOutcomeTests {
    @Test fun `indeterminate agent status reports an already committed source`() = runBlocking {
        val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2))
        var requestId = byteArrayOf()
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true, writeOutcomes = true).toByteArray()
            "PostLockerChange" -> { requestId = PostLockerChangeRequest.fromByteArray(bytes).writeRequestId; PostLockerChangeResponse(version = 7, agentIndeterminate = true, writeRequestId = requestId).toByteArray() }
            else -> error(method.methodName)
        } })
        try {
            val committed = assertFailsWith<LockerSourceCommittedException> { client.updateLocker(room, id) { byteArrayOf(3) } }
            assertTrue(committed.agentIndeterminate)
            assertContentEquals(requestId, committed.writeRequestId)
            assertEquals(7, committed.sourceVersions.single().version)
            committed.writeRequestId.fill(0)
            assertContentEquals(requestId, committed.writeRequestId)
            val mutated = committed.sourceVersions; mutated.single().lockerId!!.rawValue.fill(0)
            assertContentEquals(id.rawValue, committed.sourceVersions.single().lockerId!!.rawValue)
            assertEquals(7, client.getLocker(room, id, revalidate = false)!!.version)
        } finally { client.closeAndJoin() }
    }
    @Test fun `receipt polling is cold distinct cancellable and stops at the terminal outcome`() = runBlocking {
        var calls = 0
        val room = RoomId(byteArrayOf(1)); val requestId = ByteArray(16) { 2 }
        val client = reviewClient(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeOutcomes = true).toByteArray()
            "GetWriteOutcome" -> {
                calls++
                GetWriteOutcomeResponse(outcome = WriteOutcome(room, requestId, agentState = if (calls <= 2) WriteOutcome.AgentState.PENDING else WriteOutcome.AgentState.APPLIED)).toByteArray()
            }
            else -> error(method.methodName)
        } })
        try {
            val observations = client.writeOutcomes(room, requestId, pollIntervalMillis = 1)
            assertEquals(0, calls)
            val seen = withTimeout(2_000) { observations.toList() }
            assertEquals(2, seen.size)
            assertEquals(3, calls)
            assertTrue(seen.last().terminal)
        } finally { client.closeAndJoin() }
    }
    @Test fun `unavailable receipts expire under a configurable deadline`() = runBlocking {
        val client = reviewClient(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeOutcomes = true).toByteArray()
            "GetWriteOutcome" -> GetWriteOutcomeResponse(result = GetWriteOutcomeResponse.Result.NOT_FOUND).toByteArray()
            else -> error(method.methodName)
        } })
        try {
            val seen = client.writeOutcomes(RoomId(byteArrayOf(1)), ByteArray(16) { 2 }, pollIntervalMillis = 5, timeoutMillis = 30).toList()
            assertEquals(2, seen.size)
            assertTrue((seen.first() as WriteOutcomeObservation.Response).response.result is GetWriteOutcomeResponse.Result.NOT_FOUND)
            assertEquals(WriteOutcomeObservation.UnavailableReason.DEADLINE_EXCEEDED, (seen.last() as WriteOutcomeObservation.Unavailable).reason)
        } finally { client.closeAndJoin() }
    }
    @Test fun `poll deadline cancels a held RPC instead of leaving detached work`() = runBlocking {
        val entered = CompletableDeferred<Unit>(); val cancelled = CompletableDeferred<Unit>()
        val client = reviewClient(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeOutcomes = true).toByteArray()
            "GetWriteOutcome" -> { entered.complete(Unit); try { awaitCancellation() } finally { cancelled.complete(Unit) } }
            else -> error(method.methodName)
        } })
        try {
            val terminal = client.writeOutcomes(RoomId(byteArrayOf(1)), ByteArray(16) { 2 }, timeoutMillis = 30).last()
            assertTrue(entered.isCompleted); assertTrue(cancelled.isCompleted)
            assertEquals(WriteOutcomeObservation.UnavailableReason.DEADLINE_EXCEEDED, (terminal as WriteOutcomeObservation.Unavailable).reason)
        } finally { client.closeAndJoin() }
    }
}
