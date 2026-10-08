package com.latenighthack.lockers.common

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import kotlin.test.*

class SnapshotPagingProtocolTest {
    @Test fun pagingIsExplicitAndRoomWatermarkSurvivesSerialization() {
        assertFalse(CapabilitiesResponse().snapshotPaging)
        val token = byteArrayOf(1, 2, 3)
        val request = GetAllLockersRequest(roomId = RoomId(byteArrayOf(4)), keyspace = LockerKeyspace(0),
            pageSize = 64, pageToken = token)
        val decoded = GetAllLockersRequest.fromByteArray(request.toByteArray())
        assertEquals(64, decoded.pageSize); assertContentEquals(token, decoded.pageToken)
        val response = GetAllLockersResponse(roomSequence = 7, nextPageToken = token)
        val result = GetAllLockersResponse.fromByteArray(response.toByteArray())
        assertEquals(7, result.roomSequence); assertContentEquals(token, result.nextPageToken)
        assertEquals(0, GetAllLockersRequest(roomId = RoomId(byteArrayOf(4))).pageSize)
    }
}
