package com.latenighthack.lockers.server.claim

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.latenighthack.ktstore.InMemoryStoreDelegate
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions
import kotlin.test.Test
import kotlin.time.Duration.Companion.seconds

/**
 * On-demand in-process load soak (design-doc load plan, option-1 fidelity): R rooms across an
 * N-node claim cluster over real loopback HTTP at a target aggregate write rate, followed by a
 * single-node monolith baseline over the same transport. Everything shares one JVM, so absolute
 * latency numbers are indicative only — the printed report is the product; assertions are limited
 * to health invariants (zero failed writes, zero lost claims, sane renew rounds).
 *
 * Gated off `LOCKERS_LOAD_TEST=true` (not run in CI). Knobs: LOCKERS_LOAD_NODES (2),
 * LOCKERS_LOAD_ROOMS (1000), LOCKERS_LOAD_WPS (50), LOCKERS_LOAD_DURATION_SEC (30; the doc's
 * full soak is 600). Keep writes ≥ rooms (wps × duration ≥ rooms) so every room is exercised.
 */
class ClaimLoadSoakTest {
    @Test
    fun `two-node claim cluster sustains the target write load`() {
        Assumptions.assumeTrue(
            System.getenv("LOCKERS_LOAD_TEST")?.toBoolean() == true,
            "LOCKERS_LOAD_TEST not set; skipping load soak",
        )
        runBlocking {
            val nodes = LoadKnobs.nodes()
            val rooms = LoadKnobs.rooms()
            val wps = LoadKnobs.writesPerSecond()
            val duration = LoadKnobs.durationSeconds().seconds

            // Production TTLs: the renew cadence under load is part of what's being measured.
            val cluster = startClaimClusterOfSize(nodes, ttlMs = 15_000, renewMs = 5_000)
            val claimResult = cluster.use {
                ClaimLoadRunner(
                    targetAddrs = it.addrs,
                    rooms = rooms,
                    writesPerSecond = wps,
                    duration = duration,
                ).run().also { result ->
                    println(result.report("claim $nodes-node ($rooms rooms, ${wps}w/s, $duration)"))
                    for (node in it.nodes) {
                        val renew = node.meterRegistry.find("lockers.claim.renew.duration").timer()
                        println(
                            "${node.nodeId}: rooms.owned=%.0f acquires=%.0f steals=%.0f lost=%.0f ".format(
                                node.meterRegistry.get("lockers.claim.rooms.owned").gauge().value(),
                                node.meterRegistry.get("lockers.claim.acquires").counter().count(),
                                node.meterRegistry.get("lockers.claim.steals").counter().count(),
                                node.meterRegistry.get("lockers.claim.lost").counter().count(),
                            ) + "renew.rounds=${renew?.count() ?: 0} renew.max=%.1fms".format(
                                renew?.max(java.util.concurrent.TimeUnit.MILLISECONDS) ?: 0.0,
                            )
                        )
                        // Health invariants (the doc's hard expectations, not latency targets):
                        assertThat(node.meterRegistry.get("lockers.claim.lost").counter().count())
                            .isEqualTo(0.0)
                    }
                    val totalOwned = it.nodes.sumOf { node ->
                        node.meterRegistry.get("lockers.claim.rooms.owned").gauge().value()
                    }
                    // Every touched room is owned exactly once across the cluster; with the
                    // default knobs (writes ≥ rooms) that is every room.
                    if (result.writes >= rooms) {
                        assertThat(totalOwned).isEqualTo(rooms.toDouble())
                    }
                }
            }
            assertThat(claimResult.errors).isEqualTo(0)

            // Baseline: same load against a single monolith node over the same loopback transport.
            val baseline = startLocalMonolithNode(InMemoryStoreDelegate())
            val baselineResult = try {
                ClaimLoadRunner(
                    targetAddrs = listOf(baseline.addr),
                    rooms = rooms,
                    writesPerSecond = wps,
                    duration = duration,
                ).run().also { println(it.report("monolith baseline")) }
            } finally {
                baseline.stopGracefully()
            }
            assertThat(baselineResult.errors).isEqualTo(0)

            println(
                "p99 delta (claim - monolith) = %.2fms  [doc target: < 5ms on real hardware; in-JVM numbers are indicative only]"
                    .format(claimResult.p99Ms - baselineResult.p99Ms)
            )
        }
    }
}
