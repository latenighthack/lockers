# F04: administrator HTTP credentials

Two actual HTTP regressions failed: an unconfigured admin router answered management requests with 200, and incorrect credentials were rejected inside handlers with 500. Internal-port deployment assumptions did not establish an authentication boundary.

`monolithAdmin` now exposes no management routes until `LOCKERS_ADMIN_TOKEN` is configured. Configured routes compare the credential before unary dispatch and return 401 for missing or incorrect tokens. Peer credentials remain separate. Direct in-process test modules retain their explicit trusted boundary.

Evidence: `/tmp/lockers-F04-admin-red.log` (2 failures); `/tmp/lockers-F04-admin-green.log` (admin and gateway HTTP tests pass; runnable server compiles).
