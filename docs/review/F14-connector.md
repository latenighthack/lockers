# F14 — canonical client mutation identity

Reproduction: a batch containing omitted and explicit-zero keyspaces reached transport instead of failing duplicate admission (`ReviewConnectorTests.null and zero keyspaces are rejected as duplicate batch identities`). `/tmp/connector-F14-repro.log` records the failing regression.

Fix: normalize batch entries before admission, signing and serialization; normalize single mutation mutex keys and requests, including pending-ratchet recovery. Omitted and zero keyspaces retain their signed/storage equivalence.

Verification: the alias batch is rejected before I/O; all three review connector regressions pass (`/tmp/connector-F14-fix.log`). Server canonicalization is tracked separately by root under F14.
