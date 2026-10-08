package com.latenighthack.lockers.server.tools

import com.latenighthack.lockers.observability.*
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CancellationException
import java.util.concurrent.TimeUnit

/** Parent registry/exporter failures must not change a library operation's result. */
fun MeterRegistry.safeMeters(block: MeterRegistry.() -> Unit) { try { block() } catch (_: Exception) {} }

fun rpcOutcome(result: String): TelemetryOutcome = when {
    result == "OK" -> TelemetryOutcome.OK
    result.contains("CONFLICT") || result.contains("VERSION") -> TelemetryOutcome.CONFLICT
    result.contains("NOT_OWNER") || result.contains("EPOCH_STALE") -> TelemetryOutcome.REDIRECT
    else -> TelemetryOutcome.REJECTED
}
suspend fun <T> MeterRegistry.trackRpc(operation: TelemetryOperation, telemetry: LockersTelemetry = LockersTelemetry.NONE,
    outcome: (T) -> TelemetryOutcome = { TelemetryOutcome.OK }, block: suspend () -> T): T {
    val start = System.nanoTime()
    var result = TelemetryOutcome.ERROR
    try { return telemetry.observe(operation, outcome) { block() }.also { result = outcome(it) } }
    catch (cancelled: CancellationException) { result = TelemetryOutcome.CANCELLED; throw cancelled }
    finally {
        val tags = arrayOf("service", operation.component, "operation", operation.operation, "outcome", result.name.lowercase())
        safeMeters {
            counter("lockers.rpc.requests", *tags).increment()
            timer("lockers.rpc.duration", *tags).record(System.nanoTime() - start, TimeUnit.NANOSECONDS)
        }
    }
}
