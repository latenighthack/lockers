# F28 — observable stream heartbeat

Repro: SessionHeartbeatTest opens a valid session, sends ping, and requires initial Open followed by Pong. Baseline timed out because the ping branch constructed a response without emitting it.

Fix: emit Pong and gate all post-initialization replies on publication of the opening snapshot, preserving protocol order even when a caller sends ping immediately after create.

Validation: SessionHeartbeatTest red then green; SessionAtomicityTest green (3 tests). Client heartbeat deadlines and receive-side liveness are tracked separately by the connector remediation.
