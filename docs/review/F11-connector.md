# F11 — durable ratchet key recovery

Reproduction: `ReviewConnectorTests.committed ratchet adopts the new key even when agent fails` failed on the snapshot baseline: the adoption callback was never called and the committed source was not cached. Log: `/tmp/connector-F11-repro.log`.

Fix: an additive `pending_ratchets` definition records the encoded immutable receipt request and new private key before network submission. Startup and subsequent mutation recover the exact request, adopt committed keys, accept source state, then remove the journal entry. A failed adoption keeps the journal. `LockerSourceCommittedException` reports agent failure/pending separately from source rejection. Ratchets require receipt capability; ambiguous legacy ratchets cannot provide this recovery guarantee. Private journal material must use the application's trusted/encrypted database.

Verification: both the agent failure case and client replacement after failed adoption pass. Replacement replays identical request bytes and restores the matching authority key. `:connector:jvmTest --tests '*ReviewConnectorTests*'` passes using the explicit Fullhouse dependency manifest and isolated Maven repository. Log: `/tmp/connector-F11-fix.log`. Whole-suite and multiplatform integration remain final gates.
