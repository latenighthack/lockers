# F25 — preserve coroutine cancellation

Reproduction: injecting a CancellationException from the post transport caused retries and a LockerWriteException instead of the original cancellation (`ReviewConnectorTests.transport cancellation escapes a write without retry or wrapping`, `/tmp/connector-F25-repro.log`).

Fix: reject cancellation before write retry classification; preserve it through push registration/unregistration error handling. Retry exhaustion still has its specific write-failed outcome.

Verification: injected cancellation escapes with identical object identity after exactly one RPC; all four targeted review tests pass (`/tmp/connector-F25-fix.log`). Server and dependency cancellation behavior remain separate root/runtime checks.

Follow-up: collecting the durable event journal in an already-cancelled coroutine previously exited its `while(isActive)` loop normally, so `first()` threw NoSuchElementException instead of cancellation. A deterministic self-cancelling collector proves the wrong exception on the original source (`/tmp/connector-journal-cancel-red.log`). The loop now checks `ensureActive()` before every page and continues until cancellation throws. The regression and both owned HTTP fixture teardown tests pass (`/tmp/connector-journal-cancel-green.log`); cancellation never becomes empty successful Flow completion.
