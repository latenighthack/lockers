# F25 — preserve coroutine cancellation

Reproduction: injecting a CancellationException from the post transport caused retries and a LockerWriteException instead of the original cancellation (`ReviewConnectorTests.transport cancellation escapes a write without retry or wrapping`, `/tmp/connector-F25-repro.log`).

Fix: reject cancellation before write retry classification; preserve it through push registration/unregistration error handling. Retry exhaustion still has its specific write-failed outcome.

Verification: injected cancellation escapes with identical object identity after exactly one RPC; all four targeted review tests pass (`/tmp/connector-F25-fix.log`). Server and dependency cancellation behavior remain separate root/runtime checks.
