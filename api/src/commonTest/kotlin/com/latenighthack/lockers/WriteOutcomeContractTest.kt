package com.latenighthack.lockers

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import kotlin.test.*

class WriteOutcomeContractTest {
    @Test fun `write outcome exposes immutable metadata and preserves indeterminate state`() {
        val outcome = WriteOutcome(roomId = RoomId(byteArrayOf(1)), writeRequestId = ByteArray(16) { 2 },
            sourceVersions = listOf(WriteSourceVersion(lockerId = LockerId(rawValue = "source".encodeToByteArray(), keyspace = LockerKeyspace(1)), version = 4)),
            agentState = WriteOutcome.AgentState.INDETERMINATE)
        assertEquals(outcome, WriteOutcome.fromByteArray(outcome.toByteArray()))
        assertEquals(outcome, GetWriteOutcomeResponse.fromByteArray(GetWriteOutcomeResponse(outcome = outcome).toByteArray()).outcome)
        assertEquals(outcome.sourceVersions, PostLockerChangesResponse.fromByteArray(PostLockerChangesResponse(sourceVersions = outcome.sourceVersions).toByteArray()).sourceVersions)
        assertEquals(outcome.sourceVersions, PostLockerChangeResponse.fromByteArray(PostLockerChangeResponse(sourceVersions = outcome.sourceVersions).toByteArray()).sourceVersions)
    }
}
