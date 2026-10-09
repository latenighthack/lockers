# F26/F42 — frozen archive identity and bounded canonical adoption

The independent SQLite historical fixture failed on the restored V1 implementation:
logical locker-scope aliases with different protobuf unknown fields returned two
private archives, so choosing one could depend on row order. Canonicalizing the V1
extractor in place had also broken frozen index behavior. V1 is now retained exactly.

ConnectorStorage V6 adds an independent canonical archive V2 and progress store.
The V2 indexes use only known room/scope/keyspace/locker identity. Its records retain
the original V1 payload bytes, including private keys and unknown protobuf fields.
Schema migration only creates these stores. The archive adapter then adopts immutable
V1 rows in ordered pages of four, capped at 10,000 legacy rows and bounded row bytes.
Crypto validation derives the public key from the detached private key outside the
owner transaction. Only the merge and durable processed-rank update occur under the
connector-ratchet owner. Cancellation yields between pages; reopen skips already
committed ranks. The immutable V1 source remains unchanged by V6 APIs. Once complete,
normal reads neither rescan nor fall back to V1.

The highest scope authority epoch wins; source versions are compared only for an
epoch-zero legacy locker scope. Equal positive epochs with different public/private
keys fail closed, as do private/public mismatch and request/state authority mismatch.
All archive reads and adoption wait for completion. Conflicts retain every original
V1 byte and leave any already-copied V2 rows unusable through the archive API.

Actual SQLite regressions cover V3/V4/V5 close/reopen to V6, pending/archive byte
preservation, unknown scope/keyspace aliases, lower-epoch higher-source-version
ordering, cursor cancellation after four rows and reopen, no post-completion rescan,
partial-copy conflicts across reopen, and wrong private keys. Existing ratchet,
receipt-expiry and retention tests remain green (21 executed, zero failures/skips).
The actual browser IndexedDB, Android SQLite and Apple SQLite fixtures run the same
historical migration contract; those platform gates are in progress and will be
recorded before final integration/publication. Baseline red/green logs are
`/tmp/lockers-archive-v2-red.log` and `/tmp/lockers-archive-v2-safety.log`.

This commit does not infer that an archived authority certifies a particular source
write. F11 keeps those outcomes separate. Publication stays held for the final
subscription-intent ordering and platform gates.
