# F23 — whole committed cache snapshots

Reproduction: three cached lockers produced a first watcher emission of size one (`ReviewConnectorTests.watch first cached emission contains the whole snapshot`, `/tmp/connector-F23-repro.log`).

Fix: watchers observe immutable, complete cache reads under the acceptance mutex, using per-active-watch StateFlow wakeups. Cached history is emitted in one read; an empty cache completes initial hydration before its first snapshot. Hydration merges via stored versions, so live updates cannot be replaced by stale history. Active watcher entries are reference counted and removed on cancellation; there is no retained shared replay or mutable producer map to rebuild after idle periods.

Verification: the first cached emission contains all three lockers; all five review tests pass (`/tmp/connector-F23-fix.log`). Slow event collector isolation is tracked under F24.
