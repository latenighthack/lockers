package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.RoomId
import kotlin.test.*

class ReadAdmissionTest {
    @Test fun globalBudgetSurvivesRoomRotationAndCacheEvictionAndTimeRefills() {
        var now = 0L
        val limits = ServerResourceLimits(globalReadsPerSecond = 1, globalReadBurst = 3,
            roomReadsPerSecond = 1, roomReadBurst = 1, maxTrackedReadRooms = 2)
        val limiter = ReadAdmission(limits) { now }
        val a = RoomId(byteArrayOf(1)); val b = RoomId(byteArrayOf(2)); val c = RoomId(byteArrayOf(3))
        assertTrue(limiter.tryAcquire(a)); assertFalse(limiter.tryAcquire(a))
        assertTrue(limiter.tryAcquire(b)); assertFalse(limiter.tryAcquire(c))
        assertEquals(2, limiter.trackedRoomCount)
        now = 2_000_000_000
        assertTrue(limiter.tryAcquire(c)); assertEquals(2, limiter.trackedRoomCount)
        assertTrue(limiter.tryAcquire(a)); assertFalse(limiter.tryAcquire(b))
    }
}
