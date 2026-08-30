package com.latenighthack.lockers.server.claim

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Binds the claim-ownership meters to a Micrometer [registry] (`ShardMetrics` pattern; registered
 * even in monolith mode so the `/metrics` shape is stable across deployment modes):
 *
 *  - `lockers.claim.acquires`  — counter, +1 when this node becomes a room's owner;
 *  - `lockers.claim.steals`    — counter, the subset of acquires that took over an expired claim;
 *  - `lockers.claim.lost`      — counter, claims missing from a renew round (stolen after expiry);
 *  - `lockers.claim.redirects` — counter, resolves answered with another node's address;
 *  - `lockers.claim.renew.duration`     — timer over each batched renew round;
 *  - `lockers.claim.rooms.owned`        — gauge, rooms currently owned by this node;
 *  - `lockers.gateway.registry.misses`  — counter, session_gateway lookups that found no live row.
 */
class ClaimMetrics(registry: MeterRegistry) {
    val acquires: Counter = registry.counter("lockers.claim.acquires")
    val steals: Counter = registry.counter("lockers.claim.steals")
    val lost: Counter = registry.counter("lockers.claim.lost")
    val redirects: Counter = registry.counter("lockers.claim.redirects")
    val registryMisses: Counter = registry.counter("lockers.gateway.registry.misses")
    val renewDuration: Timer = Timer.builder("lockers.claim.renew.duration")
        .publishPercentileHistogram()
        .register(registry)
    private val roomsOwned = AtomicInteger(0)

    init {
        registry.gauge("lockers.claim.rooms.owned", roomsOwned) { it.get().toDouble() }
    }

    fun onRoomsOwned(count: Int) {
        roomsOwned.set(count)
    }

    fun recordRenew(nanos: Long) {
        renewDuration.record(nanos, TimeUnit.NANOSECONDS)
    }
}
