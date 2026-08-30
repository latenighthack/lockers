package com.latenighthack.lockers.server.claim

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isLessThanOrEqualTo
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assumptions
import java.net.URI
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.time.Duration.Companion.seconds

/**
 * Option-2 load driver: drives EXTERNALLY-STARTED `lockers-server` processes (separate JVMs, real
 * Postgres for both locker storage and the claim tables — the shared-substrate configuration
 * production runs). Orchestrated by `scripts/claim-load.sh`; not meant to be run bare.
 *
 * Gated off `LOCKERS_LOAD_TARGETS` (comma-separated `host:port` list). Optional:
 *  - `LOCKERS_LOAD_LABEL`   — report label (default "external")
 *  - `LOCKERS_TEST_PG_URL`  — when set, asserts `room_claim` cardinality ≤ active rooms
 *  - knobs from [LoadKnobs]
 *
 * After the run it scrapes each target's `/metrics` for the renew-duration max and prints it
 * (the doc's target: renew round < 50ms).
 */
class ExternalClaimLoadDriverTest {
    @Test
    fun `drive external servers at the target write load`() {
        val targets = System.getenv("LOCKERS_LOAD_TARGETS")
            ?.split(',')?.map { it.trim() }?.filter { it.isNotEmpty() }
        Assumptions.assumeTrue(!targets.isNullOrEmpty(), "LOCKERS_LOAD_TARGETS not set; skipping")
        val label = System.getenv("LOCKERS_LOAD_LABEL") ?: "external"
        val rooms = LoadKnobs.rooms()

        runBlocking {
            val result = ClaimLoadRunner(
                targetAddrs = targets!!,
                rooms = rooms,
                writesPerSecond = LoadKnobs.writesPerSecond(),
                duration = LoadKnobs.durationSeconds().seconds,
            ).run()
            println(result.report("$label (${targets.size} node(s), $rooms rooms)"))
            assertThat(result.errors).isEqualTo(0)

            for (target in targets) {
                val renewMax = scrapeMetricMax("http://$target/metrics", "lockers_claim_renew_duration_seconds_max")
                if (renewMax != null) {
                    println("$target renew.max=%.1fms".format(renewMax * 1000))
                }
            }

            System.getenv("LOCKERS_TEST_PG_URL")?.takeIf { it.isNotBlank() }?.let { url ->
                DriverManager.getConnection(url).use { conn ->
                    conn.createStatement().use { st ->
                        st.executeQuery("SELECT count(*) FROM room_claim").use { rs ->
                            rs.next()
                            val rows = rs.getLong(1)
                            println("room_claim rows=$rows (active rooms=$rooms)")
                            // The doc's cardinality invariant: the table tracks active rooms, and
                            // never exceeds them.
                            assertThat(rows).isLessThanOrEqualTo(rooms.toLong())
                        }
                    }
                }
            }
        }
    }

    private fun scrapeMetricMax(url: String, metric: String): Double? = runCatching {
        URI(url).toURL().openStream().bufferedReader().useLines { lines ->
            lines.firstOrNull { it.startsWith(metric) }
                ?.substringAfterLast(' ')?.toDoubleOrNull()
        }
    }.getOrNull()
}
