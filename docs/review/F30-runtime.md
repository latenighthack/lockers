# F30 — desired and acquired ownership

Repro: OwnerRecoveryTest injects an initial denied lease acquisition, then allows it without changing the routing map. Baseline neither reacquired on repeated reconcile nor retried while watching an unchanged map; both regressions failed.

Fix: reconcile compares desired shards against actual valid leases each round. Unacquired/revoked leases remain desired and retry every 250 ms. Epoch changes release an earlier handle before replacement, matching PostgreSQL's nonpreemptive advisory locks. Watch/start is owned by one job, cancellation propagates, and stopAndRelease cancels/joins the watcher before releasing leases.

Validation: OwnerRecoveryTest red then green; OwnerLifecycleTest green (6 tests covering denial recovery and handoff behavior). Commit-time mutation fencing is tracked independently under F08.
