package com.latenighthack.lockers.observability.connector
import com.latenighthack.lockers.observability.*
import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import kotlin.test.*
@OptIn(ExperimentalCoroutinesApi::class)
class ConnectorTelemetryTest {
    @Test fun aggregatesAllSamplesAndNeverRetriesFailedUploads() = runTest {
        val batches = mutableListOf<ClientTelemetryBatch>(); var fail = true
        val collector = ConnectorTelemetry(ClientPlatform.JVM, ClientTelemetryTransport { batches += it; if(fail) error("offline") }, scope = backgroundScope)
        repeat(300) { collector.event(TelemetryEvent(TelemetryOperation.CONNECTOR_TERMINAL, TelemetryOutcome.ERROR)) }
        repeat(1000) { collector.record(TelemetryOperation.CONNECTOR_GET, TelemetryOutcome.OK, 10_000_000) }
        collector.flushMetrics()
        assertEquals(1, batches.size); assertEquals(1000, batches.single().measurements.single().count)
        assertEquals(1000, batches.single().measurements.single().buckets[2]); assertEquals(10.0, batches.single().measurements.single().sumSeconds, .000001)
        collector.flushMetrics()
        assertEquals(2, batches.size); assertTrue(batches.last().measurements.isEmpty())
        fail = false; collector.flushMetrics()
        assertEquals(3, batches.size); assertTrue(batches.last().measurements.isEmpty())
        assertEquals(2, batches.last().losses.single { it.reason == TelemetryLoss.UPLOAD }.count)
        assertEquals(44, batches.last().losses.single { it.reason == TelemetryLoss.OVERFLOW }.count)
        collector.close(); collector.awaitClosed()
    }
    @Test fun diagnosticsAreBoundedAndCloseDrainsWithoutWaitingForTheInterval() = runTest {
        val batches = mutableListOf<ClientTelemetryBatch>()
        val collector = ConnectorTelemetry(ClientPlatform.JS, ClientTelemetryTransport { batches += it }, scope = backgroundScope)
        repeat(300) { collector.event(TelemetryEvent(TelemetryOperation.CONNECTOR_TERMINAL, TelemetryOutcome.ERROR)) }
        collector.record(TelemetryOperation.CONNECTOR_OPEN, TelemetryOutcome.OK, 0)
        collector.close(); collector.awaitClosed()
        assertEquals(256, batches.sumOf { it.diagnostics.size })
        assertTrue(batches.all { it.diagnostics.size <= 32 })
        assertEquals(44, batches.flatMap { it.losses }.single().count)
        assertEquals(1, batches.flatMap { it.measurements }.single().count)
        collector.record(TelemetryOperation.CONNECTOR_OPEN, TelemetryOutcome.OK, 0)
        val size = batches.size; collector.close(); assertEquals(size, batches.size)
    }
    @Test fun hungExporterCannotBlockShutdown() = runTest {
        val collector = ConnectorTelemetry(ClientPlatform.IOS, ClientTelemetryTransport { awaitCancellation() }, scope = backgroundScope)
        collector.record(TelemetryOperation.CONNECTOR_OPEN, TelemetryOutcome.OK, 0)
        collector.close(); collector.awaitClosed()
        assertEquals(2000, testScheduler.currentTime)
    }
    @Test fun simultaneousMetricsAndDiagnosticFlushesUseOneTransportAttemptAtATime() = runTest {
        var active = 0; var peak = 0
        val collector = ConnectorTelemetry(ClientPlatform.ANDROID, ClientTelemetryTransport { active++; peak = maxOf(peak,active); delay(10); active-- }, scope = backgroundScope)
        collector.record(TelemetryOperation.CONNECTOR_OPEN, TelemetryOutcome.OK, 0)
        collector.event(TelemetryEvent(TelemetryOperation.CONNECTOR_OPEN, TelemetryOutcome.OK))
        coroutineScope { launch { collector.flushMetrics() }; launch { collector.flushDiagnostics() } }
        assertEquals(1,peak); collector.close(); collector.awaitClosed()
    }
}
