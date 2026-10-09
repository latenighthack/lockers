# F25 — cancellation during a JDBC lease handoff

The actual PostgreSQL regression queues the successful IO result on a controlled
caller dispatcher, cancels the caller before that handoff, and attempts the same
advisory lock again. Baseline fails because the allocated connection and lock
are lost. The fixed gateway retains resource ownership until the handoff succeeds
and closes on failure under NonCancellable IO. Lease release is serialized,
remains invalid after its first release attempt, and allows retry if close fails.
Close errors propagate rather than being discarded.

One discovered real-PostgreSQL regression passes with required PG enabled.
Logs: /tmp/lockers-F25-advisory-red.log and
/tmp/lockers-F25-advisory-green.log. Full integration remains pending.
