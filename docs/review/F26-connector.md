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

## Observable subscription worker and storage failures

Baseline tests reproduced permanent room RPC retries, a startup read leaving
`started=true` without workers, and confirmation/removal storage exceptions
escaping a supervised child while subscription waiters hung
(`/tmp/connector-controller-failures-red.log`,
`/tmp/connector-subscription-storage-red.log`). The serialized reducer now owns
per-room failure state and startup/controller failure state, exposed through
`subscriptionFailures` and `subscriptionFailure` StateFlows. Waiters throw the
recorded failure promptly. Epoch/generation guards discard stale failures,
healthy rooms continue, temporary quotas retry, and explicit repeated subscribe
requests retry a failed intent while healthy/in-flight duplicates deduplicate.
An applied-intent acknowledgment prevents the retry waiter from seeing an old
failure before the new decision is reduced.

Confirmation/removal storage failures retain durable pending intent for an
explicit retry or replacement controller; no child failure is unhandled. Failed
new-room persistence fails its caller without retaining a ghost error key.
Startup storage failure is terminal and observable, and cancels the controller.
Successful unsubscribe removes generation and error metadata. Room identities
retain the existing 1..128-byte protocol bound. `maxSubscriptions` is configurable
in `ConnectorRetentionPolicy` (default 1024, finite maximum 100000); indexed
startup reads and atomic durable admission bound desired/failure maps. Remote
RPC text retained in status maps is capped at 2048 characters with exact status
codes preserved. Invalid stored snapshot data and malformed paging contracts
are permanent failures; the snapshot rollback regression still verifies no
partial cache publication.

Verification: full 153-test JVM suite passes in 18 seconds
(`/tmp/connector-controller-final-suite.log`), followed by Android compilation,
Node tests, and Apple simulator tests
(`/tmp/connector-controller-final-platforms.log`). Regressions include per-room
isolation, explicit retry, stale-session failure, startup/save/delete failures,
replacement recovery, finite room churn, quota admission and retained metadata.

### Retry budget is a domain failure

Upstream RetryLimitExceeded extends CancellationException. Two virtual-clock tests exhaust real repeatWithBackoff transport budgets after three RESOURCE_EXHAUSTED attempts; the old subscription/push actor silently canceled its worker, leaving waiters to time out. Domain boundaries now catch budget exhaustion before cancellation, verify the owning job is still active, and publish the exact observable failure while retaining durable desired intent. True parent/caller cancellation still propagates. Reducer/startup storage budget failures follow the same distinction. Both tests fail in `/tmp/connector-controller-budget-red.log`; budget, subscription and push regressions pass in `/tmp/connector-controller-budget-green.log`.

### Retiring subscription effects remain owned and bounded

Canceling/removing a job is not completion. A held NonCancellable RPC formerly left one effect alive per session change: 20 sessions retained20 effects despite two persisted intents. Removed rooms could similarly discard accounting while old effects remained alive. Both cases reproduce against baseline (`/tmp/connector-actor-retirement-red.log`, `/tmp/connector-actor-global-red.log`).

The actor now retains jobs until actual completion, allows at most two effects per room and globally twice maxSubscriptions including removed room identities, and conflates deferred changes to each room's latest desired generation. A dedicated completion channel has one slot per globally accounted effect, so user-command saturation cannot lose release notifications. Per-room counters keep admission linear instead of scanning every held effect. Healthy rooms progress while another room retires; completion starts only the latest deferred session. Cancellation still owns/joins cleanup. `subscriptionWork`/controller.work synchronously expose detached occupied/retiring counts and queued room identities; queueing is a capacity observation, not a permanent intent failure. Twelve subscription/budget regressions pass in `/tmp/connector-actor-work-state-green.log`.
