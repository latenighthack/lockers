# F42 — immutable retry bytes across caller-owned buffers

Reproduction: mutating the ByteArrays returned by a transform/notification builder after a lost response changed the second request's bytes while retaining its receipt id. `ReviewAuthorityV2Tests.ambiguous retries freeze caller-owned payload arrays` failed before the fix (`/tmp/connector-F42-freeze-repro.log`).

Fix: single and atomic batch writes deep-freeze the complete encoded request before first persistence/submission. Retries reuse that frozen request and receipt id without rerunning transforms/codecs. Transforms also receive copied cache bytes, so in-place work cannot mutate committed cache state before a write. New optimistic conflicts still run the documented pure/repeatable transform against a fresh version.

Verification: the new alias-mutation regression passes, as do the prior byte-identical retry, transform/codec-once, authority and write-outcome/ratchet tests (`/tmp/connector-F42-freeze-fix.log`). Documentation/build conventions remain owned by the build worker.
