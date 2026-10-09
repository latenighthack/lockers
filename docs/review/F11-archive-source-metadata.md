# F11 — archive recovery source identity

A shared-scope key archive can originate from locker A and later restore its key for locker B. A public updateLocker(B) regression with a failed key-provider adoption callback showed the typed committed-source exception combining A's immutable write ID/version5 with B's locker ID (whose actual version is17). The original assertion fails in `evidence/lockers-ratchet-source-metadata-red.log` and XML. No B transform or source RPC has run.

The private adoptionPending helper now reports the archive's original canonical request locker and committed source version. The key callback still targets the locker being accessed. Source versions from different lockers are never substituted. The regression also asserts zero B transform/RPC calls and retention of A's immutable request ID.

Verification: all13 ratchet adoption/expired receipt/archive ordering regressions pass without skips; Android release, JS and Apple simulator source compilation pass using the indexeddb-final isolated dependency manifest. Evidence: `evidence/lockers-ratchet-source-metadata-green.log`. Final integrated and independent checks remain open.
