# F25: coordination SQL cancellation

A real PostgreSQL regression held an advisory lock on one connection, waited for the claim pool's query to block, then cancelled the caller. Before the fix it missed a 1200 ms join deadline (one failure, zero skips). With a finite 250 ms test lock policy it joins and preserves `CancellationException`; a subsequent query verifies the slot remains usable.

Claim, advisory-lock, shard-map and readiness connections now share the bounded PostgreSQL initialization used by ktstore. Failed initialization closes the allocated connection. Claim pools reject nonpositive capacity. PostgreSQL operations have finite lock, statement and transport deadlines; cancellation can wait for the configured deadline rather than interrupting immediately.

The fixed run also passes actual PostgreSQL claim, advisory handoff, commit fence and global outbox quota tests: `/tmp/lockers-F25-claim-limits-green.log`. Dependency commit `ktstore:37cabbe` was published under a unique private workspace version through the Fullhouse CLI and compiled across JVM, JS, Android and Apple.
