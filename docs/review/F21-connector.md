# F21 — retry desired push registration while connected

Reproduction: persist a registration before controller startup, fail its first RegisterSession RPC, and keep a fixed session ID. awaitRegistered times out with one pending request (`ReviewPushControllerTests.push registration retries a transient failure without changing session ID`, `/tmp/connector-F21-repro.log`).

Fix: retry transient registration errors with backoff and preserve cancellation. Terminal rejection/transport errors are observable to awaiters; closing the controller also ends awaiters. Successful retries settle pending state. Removing post-filter session deduplication also permits null → same-ID reconnects to resend, matching the canonical stream readiness state.

Verification: the fixed-session transient failure retries exactly once and persists a confirmed credential (`/tmp/connector-F21-fix.log`). Generation/session fencing and durable removal are handled next under F22.

Permanent local protobuf/parser/packet failures now use the same terminal retry
policy as permanent RPC status codes. A tiny real encoder ceiling regression
previously retried push registration until cancellation; it now records a
failure, throws it from the waiter after one attempt, and preserves the durable
credential intent. A new explicit credential revision clears the failure and
can complete after the boundary is repaired. Retained remote error text is
bounded while preserving its exact status code.
