package com.latenighthack.lockers.observability.server

import com.latenighthack.lockers.observability.*
import com.latenighthack.lockers.server.ServerCore
import io.micrometer.core.instrument.*
import io.micrometer.core.instrument.config.MeterFilter
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import kotlinx.coroutines.*
import kotlinx.serialization.json.*
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicBoolean

/** Logical names, never JDBC URLs or secrets. Bind identical resource labels in Loki/Tempo. */
data class MonitoringOptions(val storageId: String = "default", val serviceName: String = "lockers", val environment: String = "default", val clientServiceName: String = "lockers-client", val libraryVersion: String = LOCKERS_LIBRARY_VERSION, val snapshotIntervalMillis: Long = 15_000) {
    init { require(listOf(storageId, serviceName, environment, clientServiceName, libraryVersion).all { it.matches(Regex("[A-Za-z0-9_.-]{1,128}")) }); require(snapshotIntervalMillis > 0) }
}

/** Owns only its polling coroutine and meters. Never closes parent registries or the parent SDK. */
class LockersMonitoring private constructor(
    private val core: ServerCore, private val meters: MeterRegistry, val ingester: ClientTelemetryIngester,
    private val options: MonitoringOptions,
) : AutoCloseable {
    private val closed = AtomicBoolean()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val backlog = listOf("room", "push_delivery", "push", "deadletter").associateWith { AtomicLong(-1) }
    private val snapshotAt = AtomicLong(0)
    private val snapshotHealthy = AtomicLong(0)
    private val ownedMeters = mutableListOf<Meter>()
    init {
        fun gauge(name: String, value: AtomicLong, vararg tags: String) {
            ownedMeters += Gauge.builder(name, value) { it.get().toDouble() }.tags(*tags).strongReference(true).register(meters)
        }
        backlog.forEach { (queue, value) -> gauge("lockers.backlog.pending", value, "queue", queue, "storage", options.storageId) }
        gauge("lockers.backlog.sample.timestamp", snapshotAt, "storage", options.storageId)
        gauge("lockers.backlog.sample.healthy", snapshotHealthy, "storage", options.storageId)
        gauge("lockers.monitoring.info", AtomicLong(1), "contract_version", TelemetryContract.VERSION.toString(), "library_version", options.libraryVersion, "client_service_name", options.clientServiceName)
        val config = core.config
        mapOf("room" to true, "session" to true, "storage" to true, "push" to true, "agent" to true,
            "delivery" to config.deliveryOutboxEnabled, "delivery_worker" to config.deliveryWorkerEnabled,
            "push_worker" to config.pushWorkerEnabled, "claim" to (config.roomOwnership == "claim"), "ring" to (config.roomOwnership == "ring"), "local" to (config.roomOwnership == "local"), "connector" to true)
            .forEach { (component, enabled) -> gauge("lockers.monitoring.component.enabled", AtomicLong(if (enabled) 1 else 0), "component", component) }
        ownedMeters += Gauge.builder("lockers.claim.renew.interval.seconds", config) { it.claimRenewMs / 1000.0 }.strongReference(true).register(meters)
        scope.launch { while (isActive) { refresh(); delay(options.snapshotIntervalMillis) } }
    }
    /** Callable for startup checks and deterministic tests; no query runs during scrape(). */
    suspend fun refresh() {
        if (!core.setupStarted) return
        try {
            val snapshot = core.backlogCounts()
            snapshot.forEach { (queue, count) -> backlog.getValue(queue).set(count) }
            snapshotAt.set(System.currentTimeMillis() / 1000); snapshotHealthy.set(1)
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (_: Exception) { snapshotHealthy.set(0); meters.counter("lockers.backlog.refresh.failures", "storage", options.storageId).increment() }
    }
    override fun close() { if (closed.compareAndSet(false, true)) { scope.cancel(); ingester.close(); ownedMeters.forEach(meters::remove) } }
    companion object {
        fun diagnostics(openTelemetry: OpenTelemetry): LockersTelemetry = object : LockersTelemetry {
            override suspend fun startSpan(operation: TelemetryOperation): TelemetrySpan {
                val span = openTelemetry.getTracer("com.latenighthack.lockers").spanBuilder("lockers.${operation.component}.${operation.operation}")
                    .setAttribute("lockers.component", operation.component).setAttribute("lockers.operation", operation.operation).startSpan()
                return object : TelemetrySpan {
                    override val context = Context.current().with(span).asContextElement()
                    override val traceId = span.spanContext.takeIf { it.isValid }?.traceId
                    override val spanId = span.spanContext.takeIf { it.isValid }?.spanId
                    override fun finish(outcome: TelemetryOutcome) { if (outcome == TelemetryOutcome.ERROR) span.setStatus(StatusCode.ERROR); span.setAttribute("lockers.outcome", outcome.name.lowercase()); span.end() }
                }
            }
            override fun event(event: TelemetryEvent) = logEvent(event)
        }
        fun attach(core: ServerCore, meterRegistry: MeterRegistry, prometheusRegistry: PrometheusMeterRegistry,
            openTelemetry: OpenTelemetry = OpenTelemetry.noop(), options: MonitoringOptions = MonitoringOptions(),
            spanExporter: ClientSpanExporter? = null): LockersMonitoring {
            check(!core.setupStarted) { "Attach monitoring before core setup" }
            check(meterRegistry.meters.none { it.id.name.startsWith("lockers.") }) { "Attach monitoring before constructing services or registering Lockers meters" }
            meterRegistry.config().meterFilter(object : MeterFilter {
                override fun map(id: Meter.Id): Meter.Id = if (id.name.startsWith("lockers.")) id.withTags(listOf(Tag.of("service_name", options.serviceName), Tag.of("environment", options.environment))) else id
                override fun configure(id: Meter.Id, config: DistributionStatisticConfig): DistributionStatisticConfig =
                    if (id.name.startsWith("lockers.") && id.type == Meter.Type.TIMER) DistributionStatisticConfig.builder()
                        .serviceLevelObjectives(*TelemetryContract.latencyBucketsSeconds.map { it * 1_000_000_000.0 }.toDoubleArray()).build().merge(config) else config
            })
            TelemetryOperation.entries.filter { it.component in setOf("room", "session", "push") && it != TelemetryOperation.PUSH_SEND }.forEach { operation ->
                TelemetryOutcome.entries.forEach { outcome -> meterRegistry.counter("lockers.rpc.requests", "service", operation.component, "operation", operation.operation, "outcome", outcome.name.lowercase()) }
            }
            for (backend in listOf("apns", "fcm", "web_push")) meterRegistry.counter("lockers.push.sent", "backend", backend)
            meterRegistry.counter("lockers.room.agent.failures")
            core.overrideMeterRegistry = meterRegistry
            core.overrideTelemetry = diagnostics(openTelemetry)
            val ingester = ClientTelemetryIngester(prometheusRegistry, options.serviceName, options.environment, spanExporter)
            return LockersMonitoring(core, meterRegistry, ingester, options)
        }
    }
}

internal fun logEvent(event: TelemetryEvent, platform: String? = null) {
    val fields = mapOf("lockers_component" to event.operation.component, "lockers_operation" to event.operation.operation,
        "lockers_outcome" to event.outcome.name.lowercase(), "trace_id" to event.traceId, "span_id" to event.spanId, "client_platform" to platform)
    val previous = fields.mapValues { MDC.get(it.key) }
    try { fields.forEach { (key, value) -> if (value != null) MDC.put(key, value) else MDC.remove(key) }; LoggerFactory.getLogger("com.latenighthack.lockers.monitoring").info(buildJsonObject { fields.forEach { (key, value) -> if (value != null) put(key, value) }; put("event", "lockers_operation") }.toString()) }
    finally { previous.forEach { (key, value) -> if (value == null) MDC.remove(key) else MDC.put(key, value) } }
}
