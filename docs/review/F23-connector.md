# F23 — whole committed cache snapshots

Reproduction: three cached lockers produced a first watcher emission of size one (`ReviewConnectorTests.watch first cached emission contains the whole snapshot`, `/tmp/connector-F23-repro.log`).

Fix: watchers observe immutable, complete cache reads under the acceptance mutex, using per-active-watch StateFlow wakeups. Cached history is emitted in one read; an empty cache completes initial hydration before its first snapshot. Hydration merges via stored versions, so live updates cannot be replaced by stale history. Active watcher entries are reference counted and removed on cancellation; there is no retained shared replay or mutable producer map to rebuild after idle periods.

Verification: the first cached emission contains all three lockers; all five review tests pass (`/tmp/connector-F23-fix.log`). Slow event collector isolation is tracked under F24.

Follow-up: returned write/read payload buffers could mutate the persisted in-memory cache. The cache isolation regression failed on that alias (`/tmp/connector-cache-isolation-repro.log`). Cache ingress and public snapshot/cache egress now copy identifiers/payload bytes, preserving committed data even if a caller mutates returned buffers. The new regression passes in the 42-test final set (`/tmp/connector-final-regressions.log`).
