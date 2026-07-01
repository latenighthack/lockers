package com.latenighthack.lockers.server.tools

import com.latenighthack.lockers.common.v1.RoomId
import io.github.reactivecircus.cache4k.Cache
import kotlin.time.Duration.Companion.minutes

/**
 * Per-room token-bucket rate limiter guarding writes. Each room refills at
 * [permitsPerSecond] up to a ceiling of [burst] tokens; [tryAcquire] consumes a
 * token and returns false when a room has exhausted its budget so callers can
 * reject the write.
 *
 * Buckets live in a size- and time-bounded cache so a flood of distinct rooms
 * cannot grow this limiter without bound (which would defeat its purpose).
 * A [permitsPerSecond] <= 0 disables limiting entirely.
 */
class RoomRateLimiter(
    private val permitsPerSecond: Int,
    private val burst: Int,
    maxTrackedRooms: Long = 100_000,
    private val nanoTime: () -> Long = System::nanoTime,
) {
    private class Bucket(var tokens: Double, var lastRefillNanos: Long)

    private val buckets = Cache.Builder<RoomId, Bucket>()
        .maximumCacheSize(maxTrackedRooms)
        .expireAfterAccess(10.minutes)
        .build()

    fun tryAcquire(roomId: RoomId): Boolean {
        if (permitsPerSecond <= 0) return true

        val now = nanoTime()
        val bucket = buckets.get(roomId) ?: Bucket(burst.toDouble(), now).also { buckets.put(roomId, it) }

        return synchronized(bucket) {
            val elapsedSeconds = (now - bucket.lastRefillNanos).coerceAtLeast(0) / 1_000_000_000.0
            bucket.tokens = minOf(burst.toDouble(), bucket.tokens + elapsedSeconds * permitsPerSecond)
            bucket.lastRefillNanos = now
            if (bucket.tokens >= 1.0) {
                bucket.tokens -= 1.0
                true
            } else {
                false
            }
        }
    }
}
