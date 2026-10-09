package com.latenighthack.lockers.connector.test
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.connector.*
import kotlinx.coroutines.runBlocking
import kotlin.test.*
class ReviewCacheIsolationTests {
    @Test fun `caller owned write and read payloads cannot mutate committed cache`() = runBlocking {
        val client = reviewClient(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true).toByteArray()
            "PostLockerChange" -> PostLockerChangeResponse(version = 1).toByteArray()
            else -> error(method.methodName)
        } })
        val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2))
        try {
            val written = client.updateLocker(room, id) { byteArrayOf(4) }!!
            written.open!!.encodedPayload.fill(8)
            val read = client.getLocker(room, id, false)!!
            assertContentEquals(byteArrayOf(4), read.locker!!.open!!.encodedPayload)
            read.locker!!.open!!.encodedPayload.fill(9)
            client.getAllKnownLockers().single().payload.fill(10)
            assertContentEquals(byteArrayOf(4), client.getLocker(room, id, false)!!.locker!!.open!!.encodedPayload)
        } finally { client.closeAndJoin() }
    }
}
