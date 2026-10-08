# F12 — atomic session authority

Repro: SessionAtomicityTest rendezvous forces two independent service instances to read the same absent session or the same challenge before either continues. Baseline accepted both concurrent creates and both concurrent opens; both assertions failed. Tests use real generated keys/signatures, not scheduler timing.

Fix: SessionStore exposes atomic authority, create-if-absent and compare-and-rotate operations. Database-backed store uses the shared `lockers.session-authority` transaction key (also used by authorization/revocation), rechecks the persisted authorized key and challenge, and accepts exactly one contender. Losing open returns INVALID_SEQUENCE and the current recovery challenge; losing create returns SESSION_EXISTS. Custom stores must provide atomic authority rather than silently emulate unsafe get/save.

Validation: SessionAtomicityTest red then green; SessionOwnershipTest also passed (5 tests total). PostgreSQL transaction semantics and consumer integration are checked by the root's final verification.
