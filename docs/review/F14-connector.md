# F14 — canonical client mutation identity

Reproduction: a batch containing omitted and explicit-zero keyspaces reached transport instead of failing duplicate admission (`ReviewConnectorTests.null and zero keyspaces are rejected as duplicate batch identities`). `/tmp/connector-F14-repro.log` records the failing regression.

Fix: normalize batch entries before admission, signing and serialization; normalize single mutation mutex keys and requests, including pending-ratchet recovery. Omitted and zero keyspaces retain their signed/storage equivalence.

Verification: the alias batch is rejected before I/O; all three review connector regressions pass (`/tmp/connector-F14-fix.log`). Server canonicalization is tracked separately by root under F14.

## Detached observation and store boundaries

Six actual regressions mutate public .value, replayCache and collected session/status IDs, a room event seen by two collectors, subscription unknown-body bytes, sequence/ACK buffers and push credential/revision buffers. All six fail against original7c1447e (`/tmp/connector-public-observation-baseline-red.log`), including the real HTTP signed-session observation test. Public Stream.connection/session projections and push status sessions now detach IDs on each access/collector; room event copies are per collector, so one observer cannot mutate another observer or reducer map key. Subscription, ACK, sequence, push registration and revision store methods detach input/output arrays and preserve protobuf unknown BODY bytes. Frozen codecs/index extractors are unchanged. MappedStateFlow explicitly opts into the inheritance API it implements. The actor capacity hot path returns before copying all pending keys when its global budget is full.

All26 affected observation/controller/push regressions pass (`/tmp/connector-public-observation-green.log`). Whole169 JVM connector tests pass, zero failures/skips (`/tmp/connector-followups-final-suite.log`); Android compilation, Node and Apple simulator tests pass (`/tmp/connector-followups-final-platforms.log`).
