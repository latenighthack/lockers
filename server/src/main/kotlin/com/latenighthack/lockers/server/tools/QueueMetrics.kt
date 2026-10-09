package com.latenighthack.lockers.server.tools

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

private const val MILLIS_PER_SECOND = 1000.0

/** Shared-storage gauges are a snapshot per database, never additive across replicas. */
class QueueMetrics(private val registry: MeterRegistry, val queue: String) {
    private val snapshot = AtomicReference<QueueSnapshot?>()
    private val collectedAt = AtomicLong()
    private val lastHeartbeat = AtomicLong()
    private val lastProgress = AtomicLong()
    val active = AtomicInteger()
    val capacity = AtomicInteger()
    val enabled = AtomicInteger()
    val collectionFailures get() = registry.counter("fullhouse.queue.collection.failures", "queue", queue)
    val wait get() = registry.timer("fullhouse.queue.wait", "queue", queue)
    val processing get() = registry.timer("fullhouse.queue.processing", "queue", queue)
    val gateway get() = registry.timer("fullhouse.queue.gateway", "queue", queue)
    val claim get() = registry.timer("fullhouse.queue.claim", "queue", queue)

    init {
        registry.safeMeters {
            fun shared(name: String, value: (QueueSnapshot) -> Double) {
                Gauge
                    .builder("fullhouse.queue.$name") { snapshot.get()?.let(value) ?: Double.NaN }
                    .tag("queue", queue)
                    .tag("scope", "shared")
                    .register(registry)
            }
            shared("depth") { it.depth.toDouble() }
            shared("recipients") { it.recipients.toDouble() }
            shared("unknown.age") { it.unknownAge.toDouble() }
            shared("oldest.age.seconds") {
                if (it.depth ==
                    0L
                ) {
                    0.0
                } else {
                    it.oldestAt?.let { at ->
                        maxOf(
                            0.0,
                            (System.currentTimeMillis() - at) / MILLIS_PER_SECOND,
                        )
                    } ?: Double.NaN
                }
            }
            shared("eligible.depth") { it.eligibleDepth?.toDouble() ?: Double.NaN }
            shared("eligible.oldest.age.seconds") {
                if (it.eligibleDepth ==
                    0L
                ) {
                    0.0
                } else {
                    it.eligibleAt?.let { at ->
                        maxOf(
                            0.0,
                            (System.currentTimeMillis() - at) / MILLIS_PER_SECOND,
                        )
                    } ?: Double.NaN
                }
            }
            registry.gauge(
                "fullhouse.queue.collected.timestamp.seconds",
                listOf(Tag.of("queue", queue)),
                collectedAt,
            ) { it.get() / MILLIS_PER_SECOND }
            registry.gauge(
                "fullhouse.queue.heartbeat.timestamp.seconds",
                listOf(Tag.of("queue", queue)),
                lastHeartbeat,
            ) { it.get() / MILLIS_PER_SECOND }
            registry.gauge(
                "fullhouse.queue.progress.timestamp.seconds",
                listOf(Tag.of("queue", queue)),
                lastProgress,
            ) { it.get() / MILLIS_PER_SECOND }
            registry.gauge("fullhouse.queue.active", listOf(Tag.of("queue", queue)), active) { it.get().toDouble() }
            registry.gauge("fullhouse.queue.capacity", listOf(Tag.of("queue", queue)), capacity) { it.get().toDouble() }
            registry.gauge("fullhouse.queue.worker.enabled", listOf(Tag.of("queue", queue)), enabled) {
                it.get().toDouble()
            }
        }
    }

    fun update(value: QueueSnapshot) {
        snapshot.set(value)
        collectedAt.set(System.currentTimeMillis())
    }

    fun heartbeat() {
        lastHeartbeat.set(System.currentTimeMillis())
    }

    fun progress() {
        lastProgress.set(System.currentTimeMillis())
    }

    fun event(event: String, amount: Double = 1.0) {
        registry.safeMeters { counter("fullhouse.queue.events", "queue", queue, "event", event).increment(amount) }
    }
}
