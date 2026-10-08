# F26 — owned and awaited connector lifecycle

Reproduction: starting Stream twice opened two transports (`ReviewConnectorTests.starting a stream twice opens only one transport`, `/tmp/connector-F26-repro.log`).

Fix: component constructors accept parent coroutine contexts; SupervisorJobs inherit their parent and LockerClient no longer uses GlobalScope. Start is idempotent and refuses a closed component. The facade owns a child job of the caller's context (or supplied application context), rolls back component startup on failure, and exposes `closeAndJoin`. Every low-level component also exposes awaited close; synchronous close remains a cancellation convenience.

Verification: duplicate starts open exactly one transport; cancelling the supplied parent and joining it closes the transport and controller children. All nine targeted tests pass using the patched explicit Fullhouse manifest (`/tmp/connector-F26-fix.log`). Callers must close a client or cancel its owning scope; short-lived factory scopes should supply their long-lived application context.
