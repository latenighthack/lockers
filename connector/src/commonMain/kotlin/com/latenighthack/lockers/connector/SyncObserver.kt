package com.latenighthack.lockers.connector

import kotlinx.coroutines.CancellationException
import kotlin.time.TimeSource

private const val NANOS_PER_SECOND = 1_000_000_000.0

/** Optional, privacy-safe observations. Implementations must enqueue without blocking.
 * Stages are a finite vocabulary; no room, locker, user, payload or event identifiers cross this seam.
 */
fun interface SyncObserver {
    fun observe(stage: String, outcome: String, seconds: Double, depth: Int, capacity: Int)

    companion object {
        val NONE = SyncObserver { _, _, _, _, _ -> }
    }
}

internal fun SyncObserver.record(
    stage: String,
    outcome: String = "success",
    seconds: Double = 0.0,
    depth: Int = 0,
    capacity: Int = 0,
) {
    runCatching { observe(stage, outcome, seconds, depth, capacity) }
}

internal suspend fun <T> SyncObserver.measure(stage: String, block: suspend () -> T): T {
    val start = TimeSource.Monotonic.markNow()
    var outcome = "failure"
    try {
        return block().also { outcome = "success" }
    } catch (
        cancelled: CancellationException,
    ) {
        outcome = "cancelled"
        throw cancelled
    } finally {
        record(stage, outcome, start.elapsedNow().inWholeNanoseconds / NANOS_PER_SECOND)
    }
}
