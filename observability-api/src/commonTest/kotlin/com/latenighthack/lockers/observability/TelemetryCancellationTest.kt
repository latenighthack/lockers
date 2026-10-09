package com.latenighthack.lockers.observability

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class TelemetryCancellationTest {
    @Test fun spanContextCannotReplaceTheOperationLifetime() = runTest {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val telemetry = object : LockersTelemetry {
            override suspend fun startSpan(operation: TelemetryOperation) = object : TelemetrySpan {
                override val context = NonCancellable
                override fun finish(outcome: TelemetryOutcome) {}
            }
        }
        val operation = backgroundScope.launch {
            telemetry.observe(TelemetryOperation.ROOM_WRITE) { entered.complete(Unit); release.await() }
        }
        entered.await()
        try { operation.cancel(); withTimeout(100) { operation.join() } }
        finally { release.complete(Unit); operation.join() }
    }
    @Test fun cancelledSpanStartupDoesNotExecuteTheOperation() = runTest {
        val cancelled = CancellationException("span startup cancelled")
        val telemetry = object : LockersTelemetry {
            override suspend fun startSpan(operation: TelemetryOperation): TelemetrySpan? = throw cancelled
        }
        var invoked = false
        val caught = assertFailsWith<CancellationException> {
            telemetry.observe(TelemetryOperation.ROOM_WRITE) { invoked = true }
        }
        assertSame(cancelled, caught)
        assertFalse(invoked)
    }
}
