package com.latenighthack.lockers.server.cluster

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Binds the sharding/reshard meters (§7) to a Micrometer [registry] and adapts them to the
 * transport-free [OwnerLifecycleMetrics] the [OwnerLifecycle] drives. Registering them here (even in
 * a monolith, which never mutates them) keeps `/metrics` shape-stable across deployment modes.
 *
 * Meters:
 *  - `lockers.shard.ownership.moves` — counter, +1 per lease acquire or release;
 *  - `lockers.shard.inhandoff` — gauge, shards currently mid fenced-handoff on this node;
 *  - `lockers.shard.lease.expiries` — counter, a fenced/denied acquire (a higher-epoch holder won);
 *  - `lockers.reshard.duration` — timer over each ownership-changing reconcile.
 *
 * (`lockers.reshard.rooms.rebuilt` and `lockers.reshard.cas.conflicts` are owned by `RoomServiceImpl`,
 * which already holds the registry — they fire on the lazy store rebuild and the version-CAS reject.)
 */
class ShardMetrics(registry: MeterRegistry) : OwnerLifecycleMetrics {
    private val moves = registry.counter("lockers.shard.ownership.moves")
    private val leaseExpiries = registry.counter("lockers.shard.lease.expiries")
    private val inHandoff = AtomicInteger(0)
    private val reshardDuration: Timer = Timer.builder("lockers.reshard.duration")
        .publishPercentileHistogram()
        .register(registry)

    init {
        registry.gauge("lockers.shard.inhandoff", inHandoff) { it.get().toDouble() }
    }

    override fun onOwnershipMove() {
        moves.increment()
    }

    override fun onLeaseExpiry() {
        leaseExpiries.increment()
    }

    override fun onHandoffDelta(delta: Int) {
        inHandoff.addAndGet(delta)
    }

    override fun onReshardDuration(nanos: Long) {
        reshardDuration.record(nanos, TimeUnit.NANOSECONDS)
    }
}
