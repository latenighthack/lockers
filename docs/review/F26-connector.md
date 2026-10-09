# F26 — owned and awaited connector lifecycle

Reproduction: starting Stream twice opened two transports (`ReviewConnectorTests.starting a stream twice opens only one transport`, `/tmp/connector-F26-repro.log`).

Fix: component constructors accept parent coroutine contexts; SupervisorJobs inherit their parent and LockerClient no longer uses GlobalScope. Start is idempotent and refuses a closed component. The facade owns a child job of the caller's context (or supplied application context), rolls back component startup on failure, and exposes `closeAndJoin`. Every low-level component also exposes awaited close; synchronous close remains a cancellation convenience.

Verification: duplicate starts open exactly one transport; cancelling the supplied parent and joining it closes the transport and controller children. All nine targeted tests pass using the patched explicit Fullhouse manifest (`/tmp/connector-F26-fix.log`). Callers must close a client or cancel its owning scope; short-lived factory scopes should supply their long-lived application context.

Real-HTTP fixture clock follow-up: successive full JVM runs reproduced unrelated
15-second timeouts in retry-exhaustion, terminal-error, and ratchet integration
cases while background client loops inherited `runTest` virtual delays. The
owned HTTP fixture now defaults background clients to `Dispatchers.Default`,
retaining the caller's parent Job and honoring explicit test contexts. This
matches the owned Stream fixture and prevents virtual heartbeats/reconnects
from outrunning real sockets. All 139 JVM tests then passed in 24 seconds
(`/tmp/connector-fixture-clock-fix.log`); production coroutine ownership and
retry timing are unchanged.

A second ownership regression appeared on normal fixture return: the suspend
runner extension was invoked with the outer `runTest` CoroutineScope receiver,
so `launch` collectors escaped the runner Job inspected by cleanup. The fixture
now passes `CoroutineScope(currentCoroutineContext())` explicitly. The normal
return regression and complete 153-test JVM suite pass in 18 seconds
(`/tmp/connector-controller-final-suite.log`), with no relaxed deadlines.
