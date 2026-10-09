# F14 follow-up — logical identities ignore protobuf wrapper decoration

Three baseline regressions failed (`/tmp/connector-F14-alias-repro.log`): unknown RoomId/LockerId fields bypassed a held mutation lane; the same locker decorated with unknown fields bypassed duplicate-batch admission; mutable input arrays and unknown keyspace fields remained in canonical hash keys.

Connector-local normalization constructs fresh room/session IDs from copied raw bytes and locker IDs from copied bytes plus numeric keyspace (absent is zero). Coordination recursively normalizes composite keys before retention. Mutation arguments, read keys, hydration identities, snapshots, typed filters, duplicate checks, scope wrappers and subscription/push actor identities now use those semantics. Unknown fields on payload envelopes, event packets and frozen stored records are preserved. Future identity wrappers remain accepted.

All three focused regressions pass. Integrated connector JVM run executed 131 tests; 128 passed and exposed three unrelated obsolete routing/signed-subscription fixtures being corrected separately (`/tmp/connector-integration-suite.log`).
