package com.latenighthack.lockers.server.claim

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ClaimJdbcPoolMetricsPgTest {
    @Test fun lazyConnectionsAndCancelledAcquisitionAreReportedTruthfully() = runBlocking {
        val registry = SimpleMeterRegistry()
        val pool = ClaimJdbcPool(
            PgTestGate.urlOrSkip(),
            1,
            registry = registry,
            purpose =
            ClaimJdbcPool.Purpose.OPERATIONS,
        )

        fun gauge(state: String) = registry
            .get("fullhouse.database.connections")
            .tags("pool", "operations", "state", state)
            .gauge()
            .value()
        assertEquals(1.0, gauge("capacity"))
        assertEquals(0.0, gauge("idle"))
        assertEquals(0.0, gauge("total"))
        val entered = CompletableDeferred<Unit>()
        val release = CountDownLatch(1)
        val held =
            async(Dispatchers.IO) {
                pool.withConnection { conn ->
                    entered.complete(Unit)
                    check(release.await(3, TimeUnit.SECONDS))
                    conn.createStatement().use { it.executeQuery("SELECT 1").use { rows -> check(rows.next()) } }
                }
            }
        try {
            entered.await()
            assertEquals(1.0, gauge("active"))
            assertEquals(1.0, gauge("total"))
            assertEquals(0.0, gauge("idle"))
            assertFailsWith<TimeoutCancellationException> {
                withTimeout(
                    30,
                ) { pool.withConnection { error("must not acquire an occupied pool") } }
            }
            assertEquals(
                1L,
                registry
                    .get(
                        "fullhouse.database.duration",
                    ).tags("pool", "operations", "stage", "acquire", "outcome", "timeout")
                    .timer()
                    .count(),
            )
            release.countDown()
            held.await()
            assertEquals(0.0, gauge("active"))
            assertEquals(1.0, gauge("idle"))
        } finally {
            release.countDown()
            held.join()
            pool.close()
        }
        assertEquals(0.0, gauge("idle"))
        assertEquals(0.0, gauge("total"))
    }
}
