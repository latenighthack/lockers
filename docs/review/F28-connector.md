# F28 — receive-based session liveness

Reproduction: a fake transport sends one successful open, accepts outgoing pings and remains silent. It never reconnects (`ReviewConnectorTests.a silent session expires its receive deadline and reconnects`, `/tmp/connector-F28-repro.log`). Interval/deadline constructor parameters provide bounded test timing without changing baseline liveness behavior.

Fix: every connection attempt owns a monotonic receive watchdog. Only received response frames refresh its deadline. Expiration aborts that attempt, clears readiness through the connection state, and enters reconnect backoff. The watchdog is cancelled and joined when its transport ends. Ping interval/deadline validation prevents busy timers; defaults are 60 seconds/120 seconds.

Verification: silent transport reconnects within the configured deadline; all twelve targeted tests pass (`/tmp/connector-F28-fix.log`). Server Pong emission is handled separately by runtime worker.
