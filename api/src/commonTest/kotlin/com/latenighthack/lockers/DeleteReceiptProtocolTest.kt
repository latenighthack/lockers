package com.latenighthack.lockers

import com.latenighthack.lockers.room.v1.*
import kotlin.test.*

class DeleteReceiptProtocolTest {
    @Test fun immutableDeleteIdentityRoundTripsWithoutChangingLegacyDefaults() {
        val id = ByteArray(32) { it.toByte() }
        assertContentEquals(id, DeleteLockerRequest.fromByteArray(DeleteLockerRequest(writeRequestId = id).toByteArray()).writeRequestId)
        val decoded = DeleteLockerResponse.fromByteArray(DeleteLockerResponse(result = DeleteLockerResponse.Result.REQUEST_ID_REUSED, writeRequestId = id).toByteArray())
        assertEquals(DeleteLockerResponse.Result.REQUEST_ID_REUSED, decoded.result)
        assertContentEquals(id, decoded.writeRequestId)
        assertTrue(DeleteLockerRequest().writeRequestId.isEmpty())
    }
}
