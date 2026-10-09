# F38 — bounded permanent room-claim history

New actual PostgreSQL and in-memory regressions first failed after adding only
capacity API parameters with baseline-equivalent behavior: all new IDs were
accepted and release freed history. Both now enforce permanent reservation
capacity. Two independent PostgreSQL claim-store instances admit exactly the
configured maximum under concurrent new identities; released claims retain their
slot while successors advance the original epoch.

An additive external room_claim_capacity singleton holds the reservation count.
New-ID admission locks it, checks again for a concurrent matching identity, and
increments it in the same transaction as the claim insert. Existing identities
can still renew or fail over when admission is exhausted. Legacy history is
counted once during idempotent prepare. No release or expiry deletes history or
returns quota. All replicas must use the same operator capacity setting.

The new regressions, both complete claim contracts and commit-time PostgreSQL
fence tests pass in /tmp/lockers-F38-claim-green.log; baseline failures are in
/tmp/lockers-F38-claim-red.log. Final protocol exhaustion mapping, config wiring
and integrated suites remain pending.
