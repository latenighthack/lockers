package com.latenighthack.lockers.server.tools

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

private const val MILLIS_PER_SECOND = 1000.0

/** Database-wide classification: use max across replicas, never sum. */
class InboxPresenceMetrics(private val registry: MeterRegistry, configured: Boolean) {
    private val snapshot = AtomicReference<Map<String, QueueSnapshot>?>(null)
    private val collected = AtomicLong()
    val failures get() = registry.counter("fullhouse.session.inbox.collection.failures")

    init {
        registry.safeMeters {
            Gauge.builder("fullhouse.session.inbox.presence.configured") {
                if (configured) 1.0 else 0.0
            }.register(registry)
            Gauge.builder("fullhouse.session.inbox.collected.timestamp.seconds") {
                collected.get() / MILLIS_PER_SECOND
            }.register(registry)
            for (presence in listOf("online", "offline")) {
                fun shared(name: String, value: (QueueSnapshot) -> Double) {
                    Gauge
                        .builder("fullhouse.session.inbox.$name") {
                            snapshot.get()?.get(presence)?.let(value) ?: Double.NaN
                        }
                        .tag("presence", presence)
                        .tag("scope", "shared")
                        .register(registry)
                }
                shared("depth") { it.depth.toDouble() }
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
            }
        }
    }

    fun update(value: Map<String, QueueSnapshot>) {
        snapshot.set(value)
        collected.set(System.currentTimeMillis())
    }
}
