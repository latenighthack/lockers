package com.latenighthack.lockers.observability
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import kotlin.test.*
class TelemetryTest {
    @Test fun lifecycleEventsCarryTheActiveTraceWithoutOperationData() = runTest {
        val events = mutableListOf<TelemetryEvent>()
        val telemetry = object : LockersTelemetry {
            override suspend fun startSpan(operation: TelemetryOperation) = object : TelemetrySpan {
                override val traceId = "12345678901234567890123456789012"
                override val spanId = "1234567890123456"
                override fun finish(outcome: TelemetryOutcome) {}
            }
            override fun event(event: TelemetryEvent) { events += event }
        }
        telemetry.observe(TelemetryOperation.SESSION_OPEN) {
            assertEquals("1234567890123456", currentCoroutineContext()[TelemetryContext]?.spanId)
        }
        assertEquals(TelemetryOutcome.OK, events.single().outcome)
        assertEquals("12345678901234567890123456789012", events.single().traceId)
    }
    @Test fun observersCannotFailTheOperation() = runTest {
        val telemetry = object : LockersTelemetry {
            override suspend fun startSpan(operation: TelemetryOperation): TelemetrySpan = error("exporter unavailable")
            override fun record(operation: TelemetryOperation, outcome: TelemetryOutcome, elapsedNanos: Long) = error("metrics unavailable")
            override fun event(event: TelemetryEvent) = error("logs unavailable")
        }
        assertEquals(42, telemetry.observe(TelemetryOperation.CONNECTOR_WRITE) { 42 })
        val original = IllegalStateException("operation failed")
        assertSame(original, assertFailsWith<IllegalStateException> { telemetry.observe(TelemetryOperation.CONNECTOR_WRITE) { throw original } })
    }
    @Test fun cancellationIsPreservedAndMeasured() = runTest {
        var outcome: TelemetryOutcome? = null
        val telemetry = object : LockersTelemetry { override fun record(operation: TelemetryOperation, result: TelemetryOutcome, elapsedNanos: Long) { outcome = result } }
        assertFailsWith<CancellationException> { telemetry.observe(TelemetryOperation.CONNECTOR_WRITE) { throw CancellationException() } }
        assertEquals(TelemetryOutcome.CANCELLED, outcome)
    }
    @Test fun ingestionRejectsUnknownOrUnboundedData() {
        val valid = ClientTelemetryBatch(platform = ClientPlatform.IOS, measurements = listOf(ClientMeasurement(TelemetryOperation.CONNECTOR_WRITE, TelemetryOutcome.OK, 1, .01, List(15) { if(it == 2) 1 else 0 })))
        assertEquals(valid, TelemetryContract.decode(TelemetryContract.encode(valid)))
        assertFailsWith<IllegalArgumentException> { TelemetryContract.validate(valid.copy(schemaVersion = 99)) }
        assertFailsWith<IllegalArgumentException> { TelemetryContract.validate(valid.copy(measurements = valid.measurements + valid.measurements)) }
        assertFailsWith<IllegalArgumentException> { TelemetryContract.validate(valid.copy(measurements = valid.measurements.map { it.copy(operation = TelemetryOperation.ROOM_WRITE) })) }
        assertFailsWith<IllegalArgumentException> { TelemetryContract.validate(valid.copy(measurements = valid.measurements.map { it.copy(sumSeconds = Double.NaN) })) }
        assertFailsWith<IllegalArgumentException> { TelemetryContract.validate(valid.copy(measurements = valid.measurements.map { it.copy(count = 2) })) }
        assertFailsWith<IllegalArgumentException> { TelemetryContract.decode("{\"platform\":\"IOS\",\"roomId\":\"private\"}") }
        assertFailsWith<IllegalArgumentException> { TelemetryContract.decode("x".repeat(TelemetryContract.MAX_BYTES + 1)) }
    }
}
