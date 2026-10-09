# F29 — Stream attachment lifecycle and incarnation fencing

Reproductions: a routing registry that throws during the first attachment leaked the active-stream count and cancellation entry; a queued detach removed an immediately reattached session on the same node. Both focused tests failed against the preceding implementation.

Fix: stream registration cleanup records registration before routing can fail; cancellation is conflated and nonblocking. Attachment rechecks the accepted challenge and live session in the shared authority transaction, preventing a delayed open from publishing stale routing. Every gateway attachment has a UUID persisted in an additive JDBC column; deletes compare that UUID. Registry writes and renewal/drain are serialized, and cancellation propagates. The server exposes immediate local stream closure and an owned one-second shared-store revocation check for remote streams.

Validation: `:server:test` targeted SessionAttachFailureTest, RegistryIncarnationTest, RegistrySessionGatewayDiscoveryTest, SessionAtomicityTest, SessionHeartbeatTest, SessionOwnershipTest, InMemorySessionGatewayStoreTest: 20 tests passed, including a shared-store revocation closing another instance's live stream. Legacy gateway store contracts remain supported; canonical registries require incarnation-aware store implementations.
