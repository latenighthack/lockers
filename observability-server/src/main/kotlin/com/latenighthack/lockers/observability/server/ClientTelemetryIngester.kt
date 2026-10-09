package com.latenighthack.lockers.observability.server

import com.latenighthack.lockers.observability.*
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.prometheus.client.Collector
import java.util.concurrent.atomic.AtomicBoolean

/** Parent implementation exports the original IDs/timestamps to OTLP, without inventing new spans. */
fun interface ClientSpanExporter { suspend fun export(platform: ClientPlatform, spans: List<ClientTelemetrySpan>) }

class ClientTelemetryIngester(
    private val registry: PrometheusMeterRegistry,
    private val serviceName: String = "lockers", private val environment: String = "default",
    private val spanExporter: ClientSpanExporter? = null,
) : AutoCloseable {
    private val closed = AtomicBoolean()
    private data class Key(val operation: TelemetryOperation, val outcome: TelemetryOutcome, val platform: ClientPlatform)
    private data class Aggregate(var count: Double = 0.0, var sum: Double = 0.0, val buckets: DoubleArray = DoubleArray(TelemetryContract.latencyBucketsSeconds.size + 1))
    private val measurements = mutableMapOf<Key, Aggregate>()
    private val losses = mutableMapOf<Pair<ClientPlatform, TelemetryLoss>, Double>()
    private val received = ClientPlatform.entries.associateWith { 0.0 }.toMutableMap()
    private val droppedDiagnostics = ClientPlatform.entries.associateWith { 0.0 }.toMutableMap()
    private val collector: Collector = object : Collector() {
        override fun collect(): List<MetricFamilySamples> = synchronized(measurements) {
            val labels = listOf("operation", "outcome", "platform", "service_name", "environment")
            val operations = mutableListOf<MetricFamilySamples.Sample>(); val duration = mutableListOf<MetricFamilySamples.Sample>()
            measurements.forEach { (key, aggregate) ->
                val values = listOf(key.operation.operation, key.outcome.name.lowercase(), key.platform.name.lowercase(), serviceName, environment)
                operations += MetricFamilySamples.Sample("lockers_connector_operations_total", labels, values, aggregate.count)
                var cumulative = 0.0
                aggregate.buckets.forEachIndexed { i, n ->
                    cumulative += n
                    val bound = TelemetryContract.latencyBucketsSeconds.getOrNull(i)?.toString() ?: "+Inf"
                    duration += MetricFamilySamples.Sample("lockers_connector_duration_seconds_bucket", labels + "le", values + bound, cumulative)
                }
                duration += MetricFamilySamples.Sample("lockers_connector_duration_seconds_count", labels, values, aggregate.count)
                duration += MetricFamilySamples.Sample("lockers_connector_duration_seconds_sum", labels, values, aggregate.sum)
            }
            val dropped = losses.map { (key, count) -> MetricFamilySamples.Sample("lockers_connector_telemetry_dropped_total", listOf("platform", "reason", "service_name", "environment"), listOf(key.first.name.lowercase(), key.second.name.lowercase(), serviceName, environment), count) }
            val batches = received.map { (platform, count) -> MetricFamilySamples.Sample("lockers_connector_telemetry_batches_total", listOf("platform", "service_name", "environment"), listOf(platform.name.lowercase(), serviceName, environment), count) }
            val diagnostics = droppedDiagnostics.map { (platform, count) -> MetricFamilySamples.Sample("lockers_connector_diagnostics_dropped_total", listOf("platform", "service_name", "environment"), listOf(platform.name.lowercase(), serviceName, environment), count) }
            listOf(MetricFamilySamples("lockers_connector_operations", Type.COUNTER, "Received connector operation deltas; best effort.", operations),
                MetricFamilySamples("lockers_connector_duration_seconds", Type.HISTOGRAM, "Received connector operation duration distributions in seconds.", duration),
                MetricFamilySamples("lockers_connector_telemetry_dropped", Type.COUNTER, "Client-reported monitoring loss.", dropped),
                MetricFamilySamples("lockers_connector_telemetry_batches", Type.COUNTER, "Accepted connector monitoring batches.", batches),
                MetricFamilySamples("lockers_connector_diagnostics_dropped", Type.COUNTER, "Client spans received without a configured exporter.", diagnostics))
        }
    }
    init { registry.prometheusRegistry.register(collector) }
    suspend fun acceptJson(body: String) = accept(TelemetryContract.decode(body))
    suspend fun accept(batch: ClientTelemetryBatch) {
        TelemetryContract.validate(batch)
        require(TelemetryContract.encode(batch).encodeToByteArray().size <= TelemetryContract.MAX_BYTES)
        synchronized(measurements) {
            batch.measurements.forEach { measurement ->
                val aggregate = measurements.getOrPut(Key(measurement.operation, measurement.outcome, batch.platform)) { Aggregate() }
                aggregate.count += measurement.count; aggregate.sum += measurement.sumSeconds
                measurement.buckets.forEachIndexed { i, n -> aggregate.buckets[i] += n }
            }
            batch.losses.forEach { loss -> val key = batch.platform to loss.reason; losses[key] = (losses[key] ?: 0.0) + loss.count }
            received[batch.platform] = received.getValue(batch.platform) + 1
        }
        batch.diagnostics.mapNotNull { it.event }.forEach { logEvent(it, batch.platform.name.lowercase()) }
        val spans = batch.diagnostics.mapNotNull { it.span }
        if (spans.isNotEmpty()) {
            if (spanExporter != null) spanExporter.export(batch.platform, spans)
            else synchronized(measurements) { droppedDiagnostics[batch.platform] = droppedDiagnostics.getValue(batch.platform) + spans.size }
        }
    }
    override fun close() { if (closed.compareAndSet(false, true)) registry.prometheusRegistry.unregister(collector) }
}
