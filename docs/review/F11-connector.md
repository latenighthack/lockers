# F11 — durable ratchet key recovery

Reproduction: `ReviewConnectorTests.committed ratchet adopts the new key even when agent fails` failed on the snapshot baseline: the adoption callback was never called and the committed source was not cached. Log: `/tmp/connector-F11-repro.log`.

Fix: an additive `pending_ratchets` definition records the encoded immutable receipt request and new private key before network submission. Startup and subsequent mutation recover the exact request, adopt committed keys, accept source state, then remove the journal entry. A failed adoption keeps the journal. `LockerSourceCommittedException` reports agent failure/pending separately from source rejection. Ratchets require receipt capability; ambiguous legacy ratchets cannot provide this recovery guarantee. Private journal material must use the application's trusted/encrypted database.

Verification: both the agent failure case and client replacement after failed adoption pass. Replacement replays identical request bytes and restores the matching authority key. `:connector:jvmTest --tests '*ReviewConnectorTests*'` passes using the explicit Fullhouse dependency manifest and isolated Maven repository. Log: `/tmp/connector-F11-fix.log`. Whole-suite and multiplatform integration remain final gates.

Follow-up: a default no-op callback reproduced success with no recoverable adopted key (`/tmp/connector-F11-adoption-repro.log`). The client now confirms that `writeKeyFor` resolves the new public key before clearing a pending receipt, otherwise reports committed `RatchetAdoptionPendingException`. A separate latest-per-scope durable archive survives a volatile provider reset. Recovery checks server authority before adoption, and exact V2 scope history removes obsolete archive entries without replacing a newer provider key. `onRatcheted` documents persistence before return; the archive is retained as defense in depth and contains private key material requiring trusted storage. Indexed room/public-key lookup avoids loading the archive on every mutation.

Verification: no-op provider, replacement source recovery, volatile source reset, and obsolete authority tests pass (`/tmp/connector-F11-archive-tests.log`); the broader 22-test connector/authority/push/proof/fastpath set also passed (`/tmp/connector-F11-adoption-fix.log`).

## Expired receipt recovery

A real HTTP regression now holds a committed ratchet reply, cancels the caller,
removes its exact server receipt through an owned test database, and commits a
later payload. The baseline replacement cleared the only new private-key intent
without adopting it (`/tmp/connector-expired-ratchet-red.log`). Rejected persisted
replays now consult actual locker state and all three exact V2 scope histories;
a matching locked public key proves the atomic source/key transition. Recovery
caches the current server payload, archives the matching scope, verifies that
the private key matches its advertised public key, confirms provider adoption,
and only then removes the pending intent. It never installs the old request
payload at a later version.

If no current scope proves the transition, `RatchetRecoveryUnresolvedException`
retains the confidential intent and immutable request ID for trusted/manual
resolution. It does not claim that the write failed or submit a new mutation.
Inline retries after an ambiguous transport/forward attempt use the same rule;
a proven commit with an expired receipt throws `RatchetReceiptUnavailableException`
with the original atomic source version (`parentVersion + 1`) and receipt ID.
Agent status remains unavailable and can be observed with the bounded outcome
Flow. A first definitive, uncommitted CAS rejection retains normal fair-read
retries. Pending private intents are never age-pruned merely because a server
receipt expired; existing confidential-storage and finite-admission requirements
still apply.

Three regressions cover the real lost reply plus later payload, unknown current
authority with no transform rerun or intent deletion, and a live ancestor hidden
by a child authority. Whole JVM suite: 139 tests, zero failures
(`/tmp/connector-fixture-clock-fix.log`). Android compilation, Node tests, and
Apple simulator tests pass (`/tmp/connector-expired-ratchet-platforms.log`).
