# F36 — namespaced routing and owned bounded transports

Reproduction: three regressions failed before the fix: identical room/session bytes routed to the same owner; concurrent first calls allocated multiple delegates; the documented factory could not reach an actual bare/full HTTP redirect (`/tmp/connector-F36-repro.log`).

Fix: room/session namespaces are distinct and session redirects use their own API. Client creation is serialized, owner routes cap at 2,048, owned transport slots cap at 64, and each active delegate is leased until its call finishes. Idle transports are disposed before replacing slots; active transports never evict. Failed disposal retains ownership for shutdown rather than silently leaking. The seed remains caller-owned unless explicitly requested. Custom factories return independently owned transports and provide a disposer; HttpRpcClient delegates use the upstream close-and-join seam by default. Routing close stops admission, cancels/joins owned call scopes and disposes each retained resource once, including concurrent close callers.

The paired ktbuf immutable pin `1.1.10-fh.98302b247390dbaac850` normalizes HTTP/HTTPS consistently and owns cancellable bounded platform transports. It also contains the strict parser length/varint/budget fixes missing from the earlier runtime pin. Source commits `13d4640` and `e3d6caf`, plus isolated publication bootstrap correction, were tested and published locally through the Fullhouse CLI; no external repository publication occurred.

Verification: five routing regressions pass for namespace isolation, one delegate under 32 concurrent calls, real bare/full HTTP destinations, non-eviction of active transports with single disposal, and cancellation/join before concurrent shutdown disposal. The broader 42-test connector regression set passes (`/tmp/connector-final-regressions.log`) with the final paired ktstore/ktbuf manifest.

## Permanent protocol rejection follow-up

On final paired transport pin `1.1.10-fh.e4f6e9007a56260dc5d2`, three focused
regressions failed: a tiny local output ceiling retried the write transform;
permanent BIDI RPC/decoder/encoder errors retried session connections; and a
malformed nested protobuf frame reconnected instead of failing
(`/tmp/connector-terminal-protocol-red.log`). The SDK now uses the transport's
retry classification plus terminal malformed-wire/configuration exceptions.
Cancellation still propagates, temporary `RESOURCE_EXHAUSTED` RPCs reconnect,
and terminal stream failures expose `StreamFatalError.ProtocolRejected(cause)`.
The write transform executes once and never submits the oversized packet.
All six terminal-protocol, session-admission, and delayed-destroy regressions
pass (`/tmp/connector-terminal-protocol-green.log`).
