package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.server.LockersConfig
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Validation contract of [ClaimContext.fromConfig] — the single boot path for embedders and the
 * runnable server. All failure cases must throw BEFORE any JDBC pool is constructed, which is what
 * lets these run without Postgres; a passing full build is covered by the Pg-gated cluster tests.
 */
class ClaimContextFromConfigTest {
    private val registry = SimpleMeterRegistry()

    private fun config(vararg pairs: Pair<String, String>): LockersConfig =
        LockersConfig.fromEnv(mapOf(*pairs)::get)

    @Test
    fun `local mode yields no context`() = runTest {
        assertNull(ClaimContext.fromConfig(config(), registry))
        assertNull(ClaimContext.fromConfig(config("LOCKERS_ROOM_OWNERSHIP" to "local"), registry))
    }

    @Test
    fun `ring mode yields no context (its wiring is separate)`() = runTest {
        assertNull(ClaimContext.fromConfig(config("LOCKERS_ROOM_OWNERSHIP" to "ring"), registry))
    }

    @Test
    fun `claim without a db url names LOCKERS_DB_URL`() = runTest {
        val e = assertFailsWith<IllegalStateException> {
            ClaimContext.fromConfig(config("LOCKERS_ROOM_OWNERSHIP" to "claim"), registry)
        }
        assertTrue("LOCKERS_DB_URL" in e.message!!)
    }

    @Test
    fun `fallback jdbc url substitutes for LOCKERS_DB_URL`() = runTest {
        // With the fallback supplied, validation proceeds past the URL to the next requirement.
        val e = assertFailsWith<IllegalStateException> {
            ClaimContext.fromConfig(
                config("LOCKERS_ROOM_OWNERSHIP" to "claim"),
                registry,
                fallbackJdbcUrl = "jdbc:postgresql://localhost:5432/x",
            )
        }
        assertTrue("LOCKERS_NODE_ID" in e.message!!)
    }

    @Test
    fun `claim without advertise addr names LOCKERS_ADVERTISE_ADDR`() = runTest {
        val e = assertFailsWith<IllegalStateException> {
            ClaimContext.fromConfig(
                config(
                    "LOCKERS_ROOM_OWNERSHIP" to "claim",
                    "LOCKERS_DB_URL" to "jdbc:postgresql://localhost:5432/x",
                    "LOCKERS_NODE_ID" to "n1",
                ),
                registry,
            )
        }
        assertTrue("LOCKERS_ADVERTISE_ADDR" in e.message!!)
    }

    @Test
    fun `renew must be under half the ttl`() = runTest {
        val e = assertFailsWith<IllegalStateException> {
            ClaimContext.fromConfig(
                config(
                    "LOCKERS_ROOM_OWNERSHIP" to "claim",
                    "LOCKERS_DB_URL" to "jdbc:postgresql://localhost:5432/x",
                    "LOCKERS_NODE_ID" to "n1",
                    "LOCKERS_ADVERTISE_ADDR" to "n1:8081",
                    "LOCKERS_CLAIM_TTL_MS" to "1000",
                    "LOCKERS_CLAIM_RENEW_MS" to "500",
                ),
                registry,
            )
        }
        assertTrue("LOCKERS_CLAIM_RENEW_MS" in e.message!!)
    }
}
