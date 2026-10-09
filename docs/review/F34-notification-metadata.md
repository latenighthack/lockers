# F34 notification metadata survives inbox delivery

A regression reproduced title/body disappearing from the queued response. The inbox now atomically pairs its frozen V1 row with an additive V2 sidecar containing the entire Event protobuf. Live delivery carries the accepted full Event directly; queued and recovery reconstruction reads both records in one transaction, avoiding an ACK race. Payload, push metadata, locker metadata and unknown Event fields survive SQLite reopen. Single/batched ACK and deleteAllEvents remove both records atomically.

The regression binds its payload to the actual serialized push title/body, including a body-only notification whose title becomes the protobuf empty string. It exercises batched offline acceptance, stream open/replay and single live delivery. The SQLite regression covers restart, unknown bytes, ACK and revocation cleanup. Existing broadcast and session atomicity regressions pass (seven tests total). Connector decode already reads the persisted full Event context (F24); its encoding context must likewise use the built protobuf Notification (F03).

Legacy rows never persisted title/body and cannot reconstruct it. They retain explicit payload-only fallback; old pending context-authenticated notifications require owner reconciliation. Custom inbox extensions must implement full metadata persistence before accepting push-bearing notifications; the default refuses silent loss.

Validation: `:server:test --tests *SessionNotificationMetadataTest --tests *SessionInboxMetadataPersistenceTest --tests *BroadcastServiceTest --tests *SessionAtomicityTest` with the isolated immutable ktstore workspace manifest. Red/green logs are in evidence/F34.
