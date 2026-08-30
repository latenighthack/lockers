package com.latenighthack.lockers.server.claim

import org.junit.jupiter.api.Assumptions

/**
 * Gate for tests that need a real Postgres (the claim upsert's ON CONFLICT/WHERE semantics are the
 * product; SQLite/H2 substitutes prove nothing). Locally, an unset `LOCKERS_TEST_PG_URL` skips the
 * suite; in CI `LOCKERS_TEST_PG_REQUIRED=true` turns an unset URL into a hard failure so these
 * tests can never green-by-skipping (the exact failure mode the ring path suffered).
 *
 * Local run: `docker run --rm -p 5432:5432 -e POSTGRES_USER=lockers -e POSTGRES_PASSWORD=lockers
 * -e POSTGRES_DB=lockers postgres:16`, then
 * `LOCKERS_TEST_PG_URL='jdbc:postgresql://localhost:5432/lockers?user=lockers&password=lockers'`.
 */
object PgTestGate {
    fun urlOrSkip(): String {
        val url = System.getenv("LOCKERS_TEST_PG_URL")?.takeIf { it.isNotBlank() }
        if (url == null) {
            check(System.getenv("LOCKERS_TEST_PG_REQUIRED")?.toBoolean() != true) {
                "LOCKERS_TEST_PG_REQUIRED=true but LOCKERS_TEST_PG_URL is unset"
            }
            Assumptions.assumeTrue(false, "LOCKERS_TEST_PG_URL not set; skipping Postgres claim tests")
        }
        return url!!
    }
}
