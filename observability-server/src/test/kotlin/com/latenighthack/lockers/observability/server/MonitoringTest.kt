package com.latenighthack.lockers.observability.server

import com.latenighthack.lockers.observability.*
import com.latenighthack.lockers.server.*
import com.latenighthack.lockers.server.tools.*
import com.latenighthack.ktstore.*
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import io.micrometer.prometheus.*
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import kotlinx.coroutines.*
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.*

class MonitoringTest {
    @Test fun backlogSamplesAreCachedBecomeUnhealthyAndOwnedGaugesAreRemoved(): Unit = runBlocking {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val database = ServerStorage.inMemory(meterRegistry = registry)
        val core = ServerCore::class.create(LockersConfig.defaults().copy(claimRenewMs = 100), database)
        val monitoring = LockersMonitoring.attach(core, registry, registry, options = MonitoringOptions(snapshotIntervalMillis = 60_000))
        try {
            assertEquals(-1.0, registry.get("lockers.backlog.pending").tag("queue", "room").gauge().value())
            core.setup(); monitoring.refresh()
            val timestamp = registry.get("lockers.backlog.sample.timestamp").gauge().value()
            assertTrue(timestamp > 0)
            assertEquals(.1, registry.get("lockers.claim.renew.interval.seconds").gauge().value())
            fun reads() = registry.find("lockers.storage.operations").counters().sumOf { it.count() }
            val beforeScraping = reads()
            repeat(3) { registry.scrape() }
            assertEquals(beforeScraping, reads(), "Scraping must perform no database work")
            database.close(); monitoring.refresh()
            assertEquals(0.0, registry.get("lockers.backlog.sample.healthy").gauge().value())
            assertEquals(timestamp, registry.get("lockers.backlog.sample.timestamp").gauge().value())
            assertEquals(0.0, registry.get("lockers.backlog.pending").tag("queue", "room").gauge().value())
            assertTrue(registry.get("lockers.backlog.refresh.failures").counter().count() >= 1)
            monitoring.close()
            assertNull(registry.find("lockers.backlog.pending").gauge())
            assertNull(registry.find("lockers.monitoring.info").gauge())
            registry.counter("parent.still.usable").increment()
            assertEquals(1.0, registry.get("parent.still.usable").counter().count())
        } finally { monitoring.close(); registry.close() }
    }

    @Test fun failedMetricRegistrationCannotOverrideRpcResultsOrFailures() = runBlocking {
        val registry = io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        registry.config().meterFilter(object : io.micrometer.core.instrument.config.MeterFilter {
            override fun map(id: io.micrometer.core.instrument.Meter.Id): io.micrometer.core.instrument.Meter.Id = error("parent exporter failed")
        })
        assertEquals(42, registry.trackRpc(TelemetryOperation.ROOM_WRITE) { 42 })
        val failure = IllegalStateException("operation failed")
        val thrown = assertFailsWith<IllegalStateException> { registry.trackRpc(TelemetryOperation.ROOM_WRITE) { throw failure } }
        assertSame(failure, thrown)
        registry.close()
    }
    @Test fun compositeRegistryExposesFailuresBucketsAndPreservesParentMeters(): Unit = runBlocking {
        val prometheus = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val composite = CompositeMeterRegistry().also { it.add(prometheus) }
        composite.counter("parent.requests").increment()
        val core = ServerCore::class.create(LockersConfig.defaults(), ServerStorage.inMemory(meterRegistry = composite))
        val monitoring = LockersMonitoring.attach(core, composite, prometheus, options = MonitoringOptions(serviceName = "sample", environment = "test"))
        try {
            core.setup(); monitoring.refresh()
            composite.trackRpc(TelemetryOperation.ROOM_WRITE, core.telemetry) { 42 }
            assertFailsWith<IllegalStateException> { composite.trackRpc(TelemetryOperation.ROOM_WRITE, core.telemetry) { error("failure") } }
            assertFailsWith<CancellationException> { composite.trackRpc(TelemetryOperation.ROOM_WRITE, core.telemetry) { throw CancellationException() } }
            for (outcome in listOf("ok", "error", "cancelled")) assertEquals(1.0, composite.get("lockers.rpc.requests").tags("service","room","operation","write","outcome",outcome).counter().count())
            val scrape = prometheus.scrape()
            assertContains(scrape,"lockers_rpc_duration_seconds_bucket"); assertContains(scrape,"le=\"0.1\"")
            assertContains(scrape,"lockers_backlog_pending"); assertContains(scrape,"contract_version=\"1\"")
            assertEquals(emptyList(), composite.get("parent.requests").counter().id.tags)
            assertEquals(0L, core.backlogCounts().getValue("room"))
            assertFailsWith<IllegalStateException> { LockersMonitoring.attach(core, composite, prometheus) }
            Files.createDirectories(Path.of("build/monitoring")); Files.writeString(Path.of("build/monitoring/healthy.prom"),scrape)
        } finally { monitoring.close(); composite.close(); prometheus.close() }
    }
    @Test fun importedHistogramsMergeIntoCumulativePrometheusFamiliesAndResetWithTheProcess() = runBlocking {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val ingester = ClientTelemetryIngester(registry,"sample","test")
        val sample = ClientMeasurement(TelemetryOperation.CONNECTOR_WRITE,TelemetryOutcome.OK,2,.11,List(15) { if(it==2||it==5) 1 else 0 })
        val batch = ClientTelemetryBatch(platform=ClientPlatform.IOS,measurements=listOf(sample))
        ingester.accept(batch); ingester.accept(batch)
        val scrape=registry.scrape()
        assertTrue(Regex("lockers_connector_operations_total[^\n]* 4.0").containsMatchIn(scrape))
        assertTrue(Regex("lockers_connector_duration_seconds_bucket[^\n]*le=\"0.1\"[^\n]* 4.0").containsMatchIn(scrape))
        assertTrue(Regex("lockers_connector_duration_seconds_sum[^\n]* 0.22").containsMatchIn(scrape))
        assertFailsWith<IllegalArgumentException> { ingester.accept(batch.copy(schemaVersion=2)) }
        assertEquals(scrape,registry.scrape())
        ingester.close(); assertFalse(registry.scrape().contains("lockers_connector_duration_seconds_bucket"))
        ClientTelemetryIngester(registry,"sample","test").use { assertFalse(registry.scrape().contains("lockers_connector_operations_total{")) }
        registry.close()
    }
    @Test fun internalSpansReuseParentTraceAndFinishWhenTheOperationFails() = runBlocking {
        val exporter=InMemorySpanExporter.create();val provider=SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build()
        val sdk=OpenTelemetrySdk.builder().setTracerProvider(provider).build()
        val diagnostics=LockersMonitoring.diagnostics(sdk)
        diagnostics.observe(TelemetryOperation.ROOM_WRITE) { diagnostics.observe(TelemetryOperation.AGENT_EXECUTE) { 42 } }
        assertFailsWith<IllegalStateException> { diagnostics.observe(TelemetryOperation.STORAGE_READ) { error("secret failure") } }
        val spans=exporter.finishedSpanItems
        val room=spans.single { it.name=="lockers.room.write" };val agent=spans.single { it.name=="lockers.agent.execute" }
        assertEquals(room.traceId,agent.traceId);assertEquals(room.spanId,agent.parentSpanId)
        assertTrue(spans.all { it.events.isEmpty() });provider.close();sdk.close()
    }
    @Test fun storageOperationsOutsideWriteTracingAreMeasuredAndExtensionStoresAreExcluded() = runBlocking {
        val registry=PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val db=ServerStorage.inMemory(meterRegistry=registry)
        val outbox=com.latenighthack.lockers.server.services.room.v1.DeliveryOutboxStore(db)
        outbox.prepareStores();db.open()
        assertEquals(0L,outbox.pendingCountLong())
        assertEquals(1.0,registry.get("lockers.storage.operations").tags("store","delivery_outbox","operation","read","outcome","ok").counter().count())
        registry.close()
    }
}
