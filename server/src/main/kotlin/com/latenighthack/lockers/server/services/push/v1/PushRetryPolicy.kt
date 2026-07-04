package com.latenighthack.lockers.server.services.push.v1

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Governs how a transient push failure is retried before it is dead-lettered.
 * [maxAttempts] is the total number of delivery attempts (across restarts, since
 * the attempt count is persisted); backoff is exponential from [baseDelay] up to
 * [maxDelay]. Tests inject a fast policy so the dead-letter path is exercised
 * without real waits.
 */
data class PushRetryPolicy(
    val maxAttempts: Int = 5,
    val baseDelay: Duration = 1.seconds,
    val maxDelay: Duration = 60.seconds,
) {
    companion object {
        val DEFAULT = PushRetryPolicy()
    }
}
