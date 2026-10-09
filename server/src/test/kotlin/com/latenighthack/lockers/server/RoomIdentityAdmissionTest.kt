package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.tools.RoomRateLimiter
import java.util.concurrent.Executors
import kotlin.test.*

class RoomIdentityAdmissionTest {
    @Test fun unknownWireFieldsCannotResetReadOrWriteRoomBudget() {
        val room = RoomId(byteArrayOf(1))
        val alias = RoomId.fromByteArray(room.toByteArray() + byteArrayOf(0xA0.toByte(), 0x06, 0x01))
        val reads = ReadAdmission(ServerResourceLimits(globalReadBurst = 100, roomReadBurst = 1)) { 0 }
        assertTrue(reads.tryAcquire(room)); assertFalse(reads.tryAcquire(alias)); assertEquals(1, reads.trackedRoomCount)
        val writes = RoomRateLimiter(1, 1, nanoTime = { 0 })
        assertTrue(writes.tryAcquire(room)); assertFalse(writes.tryAcquire(alias))
    }
    @Test fun concurrentFirstWritesShareTheSameBucket() {
        val limiter = RoomRateLimiter(1, 1, nanoTime = { 0 })
        val executor = Executors.newFixedThreadPool(8)
        try {
            val start = java.util.concurrent.CountDownLatch(1)
            val results = (1..32).map { executor.submit<Boolean> { start.await(); limiter.tryAcquire(RoomId(byteArrayOf(1))) } }
            start.countDown(); assertEquals(1, results.count { it.get() })
        } finally { executor.shutdownNow() }
    }
}
