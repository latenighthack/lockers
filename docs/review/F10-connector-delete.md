# F10 / F42 — immutable delete retries and explicit compare-and-set conflicts

Baseline `ReviewDeleteReceiptTests` reproduced both missing negotiated delete receipts and a legacy lost reply followed by version conflict automatically deleting recreated version 3. Evidence: `/tmp/connector-F10-delete-repro.log` (two failures).

Delete now fetches once when uncached, freezes one complete request before submission (receipt ID, parent, signature, encoded notification), and reuses it for ambiguous retries and owner redirects. Delete receipts negotiate independently of post receipts. Conflicts refresh authoritative cache and throw `LockerDeleteConflictException(expectedVersion, actualVersion, mayHaveCommitted)` without rebasing; the legacy ambiguity flag warns that the original delete may have committed. Receipt reuse rejection is terminal. A replayed older successful delete cannot replace newer cached content. Invalid successful receipt metadata is reported as source committed, preserving identity and version for recovery.

Verification: four delete regressions plus four authority/immutable-post regressions passed with the final isolated ktbuf/ktstore dependencies (`/tmp/connector-F10-delete-regressions.log`). Notification encoding runs once even if the caller mutates its buffer after the first lost response.
