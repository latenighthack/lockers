# F27 — one truthful connection state

Reproduction: closing a never-connected client left awaitConnected waiting until its timeout (`ReviewConnectorTests.awaitConnected reports a closed client instead of hanging`, `/tmp/connector-F27-repro.log`). The test explicitly distinguishes TimeoutCancellationException from a closure outcome.

Fix: Connecting, Connected(session, epoch), Retrying, Failed and Closed share one StateFlow. Session ID, Boolean connectivity and fatal error are synchronous projections of that state. Every transport attempt clears readiness on failure or clean close, and each successful open advances connection epoch. Closed is terminal; parent cancellation closes state too. Connection and subscription awaiters terminate on failure/closure; retry exhaustion is handled separately from caller cancellation.

Verification: eleven targeted tests pass, including immediate closed-client failure and clearing connectivity/session during transport backoff (`/tmp/connector-F27-fix.log`).
