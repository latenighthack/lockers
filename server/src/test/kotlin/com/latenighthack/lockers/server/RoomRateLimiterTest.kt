package com.latenighthack.lockers.server

import assertk.assertThat
import assertk.assertions.isFalse
import assertk.assertions.isTrue
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.server.tools.RoomRateLimiter
import kotlin.test.Test

class RoomRateLimiterTest {
    private fun roomId(b: Byte) = RoomId(byteArrayOf(b))

    @Test
    fun allowsUpToBurstThenRejects() {
        var now = 0L
        val limiter = RoomRateLimiter(permitsPerSecond = 10, burst = 3, nanoTime = { now })
        val room = roomId(1)
        assertThat(limiter.tryAcquire(room)).isTrue()
        assertThat(limiter.tryAcquire(room)).isTrue()
        assertThat(limiter.tryAcquire(room)).isTrue()
        assertThat(limiter.tryAcquire(room)).isFalse()
    }

    @Test
    fun refillsOverTime() {
        var now = 0L
        val limiter = RoomRateLimiter(permitsPerSecond = 10, burst = 1, nanoTime = { now })
        val room = roomId(2)
        assertThat(limiter.tryAcquire(room)).isTrue()
        assertThat(limiter.tryAcquire(room)).isFalse()
        now += 200_000_000L // 0.2s at 10 permits/s refills a full token (capped at burst)
        assertThat(limiter.tryAcquire(room)).isTrue()
    }

    @Test
    fun limitsAreIndependentPerRoom() {
        var now = 0L
        val limiter = RoomRateLimiter(permitsPerSecond = 1, burst = 1, nanoTime = { now })
        assertThat(limiter.tryAcquire(roomId(3))).isTrue()
        assertThat(limiter.tryAcquire(roomId(4))).isTrue()
        assertThat(limiter.tryAcquire(roomId(3))).isFalse()
    }

    @Test
    fun nonPositiveRateDisablesLimiting() {
        val limiter = RoomRateLimiter(permitsPerSecond = 0, burst = 0)
        repeat(1000) { assertThat(limiter.tryAcquire(roomId(5))).isTrue() }
    }
}
