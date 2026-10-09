# F39: one required PostgreSQL test gate

The combined required-PostgreSQL suite silently skipped `NamespaceAdmissionPgTest` because this new regression used a different environment name (LOCKERS_POSTGRES_TEST_URL). All existing actual-PG suites use PgTestGate and LOCKERS_TEST_PG_URL / LOCKERS_TEST_PG_REQUIRED. The regression now uses that shared gate, so a required validation cannot silently omit it.

Actual two-handle reservation race passed against the dedicated PostgreSQL instance: twelve different rooms compete for a global three-locker budget and a global three-lock budget; exactly three succeed for each. Integrated inbox byte admission, session/snapshot revocation and bounded delivery frame regressions pass with the final immutable ktbuf output-runtime manifest. No tests skipped in the focused run.
