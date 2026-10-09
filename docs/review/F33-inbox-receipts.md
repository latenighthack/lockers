# F33 durable inbox delivery receipts

Reproduction: acknowledge an accepted event, then retry the same gateway request. The previous inbox row had been deleted, so it was inserted and pushed again. A second repro moves a socket between gateways after the first gateway acceptance: simply suppressing duplicates loses the wake at the new gateway.

Fix: accept the immutable full Event and its SHA-256 receipt in the same database owner as inbox and metadata. Matching retries are accepted without insertion or push; contradictory bytes reject. ACK and session inbox cleanup retain receipts. A matching retry with a pending row wakes the current stream to drain its durable inbox, with bounded per-stream duplicate tracking. Revoked session identities remain accepted discards. Receipts survive SQLite reopening, have finite global/per-session quotas, and only expired identities are pruned in bounded batches.

The sender retries automatically for at most 30 days; receipts remain protected for 31 days. Event identities must never be reused. Manual retries after parking must issue fresh event identities/sequence and retain the original evidence. Historical acknowledged V1 rows did not retain delivery identity and cannot be reconstructed retroactively. Pending V1 rows can be backfilled on a matching replay; lost push metadata cannot be recovered from the old row.

Validation: InboxDeliveryReceiptTest exercises ACK/retry, changed bytes, push suppression, and two-gateway handoff/repeated wake. InboxReceiptPersistenceTest exercises actual SQLite ACK/reopen, quota rejection, the exact retention boundary, and expired-only pruning. GatewayAdmissionTest and SessionNotificationMetadataTest also pass.
