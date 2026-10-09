# F35 — server broadcast contract

The existing server broadcast shape is intentionally a durable Event with an empty room identity, no locker, a fresh event identity and the complete Notification. No new locker/keyspace authority is invented. The F35 transport-decoding failure is reproduced and remedied by the connector's typed durable broadcast Flow (`F35-connector.md`); full queued/live metadata preservation is covered by the additive F34 server inbox work.

`BroadcastShapeTest` verifies a body-only push notification and payload remain byte-identical after rebuilding the inbox store, with no locker association and a valid16..64-byte event identity. `SessionNotificationMetadataTest` verifies queued and live notifications bind the actual canonical metadata. Both tests pass (zero skips), `/tmp/runtime-F35-green.log`. No server wire change is required; the explicit trusted server broadcast extension boundary is preserved.
