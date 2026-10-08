# F18 — durable delivery is the production default

Repro: LockersConfigTest now requires delivery outbox and delivery worker enabled when environment variables are absent. Baseline failed because outbox defaulted false.

Fix: constructor and environment defaults enable durable delivery. Explicit feature opt-out remains visible for legacy negotiation; F08 unifies even legacy accepted single writes through the same transactional source/delivery-intent commit. Apply together with F08's same-database fallback for direct service constructors; custom stores must explicitly supply a transactional outbox rather than silently promise durable acceptance.

Validation: LockersConfigTest red then green (8 tests), including explicit opt-out. Complete durable-source/inbox recovery integration is validated with F08 and delivery worker suites at the root.
