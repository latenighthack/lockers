# F11 — durable ratchet key recovery

Reproduction: `ReviewConnectorTests.committed ratchet adopts the new key even when agent fails` failed on the snapshot baseline: the adoption callback was never called and the committed source was not cached. Log: `/tmp/connector-F11-repro.log`.

Fix: an additive `pending_ratchets` definition records the encoded immutable receipt request and new private key before network submission. Startup and subsequent mutation recover the exact request, adopt committed keys, accept source state, then remove the journal entry. A failed adoption keeps the journal. `LockerSourceCommittedException` reports agent failure/pending separately from source rejection. Ratchets require receipt capability; ambiguous legacy ratchets cannot provide this recovery guarantee. Private journal material must use the application's trusted/encrypted database.

Verification: both the agent failure case and client replacement after failed adoption pass. Replacement replays identical request bytes and restores the matching authority key. `:connector:jvmTest --tests '*ReviewConnectorTests*'` passes using the explicit Fullhouse dependency manifest and isolated Maven repository. Log: `/tmp/connector-F11-fix.log`. Whole-suite and multiplatform integration remain final gates.

Follow-up: a default no-op callback reproduced success with no recoverable adopted key (`/tmp/connector-F11-adoption-repro.log`). The client now confirms that `writeKeyFor` resolves the new public key before clearing a pending receipt, otherwise reports committed `RatchetAdoptionPendingException`. A separate latest-per-scope durable archive survives a volatile provider reset. Recovery checks server authority before adoption, and exact V2 scope history removes obsolete archive entries without replacing a newer provider key. `onRatcheted` documents persistence before return; the archive is retained as defense in depth and contains private key material requiring trusted storage. Indexed room/public-key lookup avoids loading the archive on every mutation.

Verification: no-op provider, replacement source recovery, volatile source reset, and obsolete authority tests pass (`/tmp/connector-F11-archive-tests.log`); the broader 22-test connector/authority/push/proof/fastpath set also passed (`/tmp/connector-F11-adoption-fix.log`).

## Expired receipt recovery

The actual HTTP lost-reply reproduction commits a ratchet, cancels its held reply,
removes the exact receipt, and commits a later payload. The initial recovery fix
restored the private key but incorrectly treated matching authority as proof of
the original source write. Independent verification found a counterexample:
another authorized operation can install the publicly known proposed key while
the original immutable source request never commits. The final policy below
supersedes that initial implementation; no expired-receipt path asserts a source
version from current authority or the old request's parent version.

## Final verifier follow-up: source uncertainty and independent recovery

Authority alone does not certify the source write. An unrelated authorized write plus a direct grant of the publicly known proposed key previously caused an expired/rejected ratchet to manufacture `parentVersion + 1`, publish SourceCommitted, archive the key with a false source version, and erase its confidential pending intent. Any-scope matching also accepted an unrelated scope's public key.

Recovery now persists the original exact scope, predecessor epoch and public key atomically beside the unchanged V1 private intent in an additive V5 database migration. Only the exact scope and expected successor epoch can restore a usable private key. Without a receipt the result remains `RatchetRecoveryUnresolvedException` with detached immutable identity, observed actual version and key-adoption status; no source version is asserted, no original body is accepted, and no V1 committed archive is fabricated. Legacy intents without the original scope remain unresolved. A default/no-op key provider cannot discard the private key: confirmation still requires lookup of the proposed key after its callback.

`acknowledgeRatchetSourceUncertainty(room, locker, writeID)` is an explicit, durable decision for the exact immutable intent. Invalid identities fail closed. ACK never certifies a commit; subsequent caller mutations first fetch actual content/version and create a fresh ID. ACKed recovery skips replaying the original source request, reloads current metadata inside the mutation lane, and re-confirms the key after provider reset. Its confidential pending key remains until exact scope history proves it obsolete. Applications must retain confidential/encrypted storage and handle the unresolved source outcome before using this seam.

Startup consumes private pending/archive entries through bounded one-row indexed Flows. Each intent/archive recovers independently; cancellation propagates while unresolved/transient entries continue owned retries without starving later entries. `ratchetRecoveryFailures` is a nonblocking StateFlow containing at most 1,024 detached identity/status entries, no causes, requests, payloads or private keys. Resolved failures disappear on the next pass.

Evidence: original baseline source-uncertainty failures `/tmp/connector-ratchet-source-unknown-red.log`; isolated original `7c1447e` actual HTTP unrelated-write/unlock/direct-public-grant and startup starvation both fail in `/tmp/connector-ratchet-adversarial-isolation-red.log`. Six fixed regressions pass in `/tmp/connector-ratchet-metadata-green.log`, including expired actual committed reply, provider reset after ACK, wrong-scope matching, exact overridden ancestor, unrelated direct grant and later recovery without restart. Whole connector JVM suite: 156 tests, zero failures (`/tmp/connector-ratchet-final-suite.log`). Android compilation, Node and Apple simulator tests pass (`/tmp/connector-ratchet-final-platforms.log`); the final diagnostic-only change was rechecked by targeted JVM tests. This supersedes earlier authority-as-source-proof text above.

### Completed-key archive ordering

Two further red regressions showed an older receipt for another locker in a shared authority scope overwriting a newer private-key archive, and an obsolete reader deleting the replacement during cleanup. Archives now compare authority epochs (source versions from different lockers are not comparable), reject conflicting keys at the same known epoch, detach input data, and remove only the exact row observed inside the keyed transaction. Legacy locker-only fallback archives compare source versions only when no epoch is available. Both tests fail in `/tmp/connector-archive-order-red.log` and pass alongside the six recovery tests in `/tmp/connector-archive-order-green.log`. Frozen V1 archive bytes remain unchanged.

### Exact successor authority is still not source provenance

The stronger actual HTTP adversarial regression intercepts the original ratchet before it reaches the server. A different authorized source write, using only the proposed public key and the old signing key, writes payload9 and rotates the exact original scope from epoch1 to2 under a different immutable write ID. Recovery safely restores the currently valid private key but reports Unknown, retains the private intent, creates no committed V1 archive and caches only payload9. Explicit ACK then permits a fresh caller write; closing/restarting with the old volatile provider restores the key without ever replaying the ACKed original source. This fails against original7c1447e (`/tmp/connector-ratchet-exact-epoch-red.log`) and passes with the seven recovery regressions (`/tmp/connector-ratchet-exact-epoch-green.log`).

Compatibility correction: preserve the exact frozen V1 scope-index extractor, including historical protobuf unknown fields. The ordering/CAS fix does not rekey that store; normalized authority proof remains client-local. Any future canonical archive index requires an additive definition and migration.
