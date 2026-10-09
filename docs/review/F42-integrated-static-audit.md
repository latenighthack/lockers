# F42 — integrated static audit

Checkpoint: independent worktree from `274e7de`, with the reviewed source cleanup
in `230ffda`. This is a reviewed adoption baseline, not elimination of style debt.
The final integrated checkpoint still requires its complete platform/consumer gate.

## Reproduction and corrections

The first aggregate run reported 2,909 finding occurrences after the remediation
commits, predominantly line length, wildcard imports and numeric literals. Reviewing
the exception findings also identified silent shard-map poll diagnostics, unused
private arguments/fallback code, and fixture cleanup throws inside `finally`.

The corrections retain metric-registration initializers and their exact names,
types and order; remove only an uncalled private delivery helper and unused push
argument; require the single-write helper to receive explicit durable staging
buffers and its already-observed row; attach fixture cleanup failures to the primary
failure and throw after cleanup; log the private shard-map polling failure while
preserving failed status and the last known map. Expected cryptographic rejection,
already-sent stream rejection and sanitized forwarded RPC errors are named as such.
Their cancellation guards and response/state behavior are unchanged. Retry-budget
exhaustion remains visible as the typed fatal stream state; its log contains only
the exception class, because the upstream exception does not retain the last cause.

`RetryLimitExceeded` inherits `CancellationException`. The separate actor-exhaustion
regressions and domain handling belong to F27/F11; this audit does not mark that gap
resolved merely because the outer Stream catch records a failure.

## Generic boundary catches

The 58 production catch occurrences collapse to 38 additional exact baseline IDs
because detekt's identity omits source line numbers. All were inspected in context.
The exact IDs are recorded in `evidence/F42/integrated-static-audit.json`.

| Boundary group | Occurrences | Cancellation and failure contract reviewed |
| --- | ---: | --- |
| Client callback and write state: PushRegistration, LockerClient, Stream | 10 | Explicit cancellation propagation or original failure attached to typed observable state; recorded fatal outcomes are retained. Retry-budget subtype is handled separately from ordinary cancellation. |
| Client startup and transport shutdown: LockersClient, RoutingRpcClient | 2 | Cleanup runs in NonCancellable; original startup failure is rethrown and teardown failures are aggregated. |
| Server maintenance and producers: OwnerLifecycle, PostgresShardMapSource, AgentWorkflow, PushServiceImpl, ClaimSessionRegistry, ClaimRenewalService | 17 | Explicit cancellation guard or current-context activity check distinguishes caller cancellation from an owned deadline. Recoverable work remains durable; failures are logged or attached to observable state; joined cleanup retains original failure. |
| Owned peer transport: RemoteGatewayConnections | 3 | Closing errors are recorded, awaited and aggregated; transport children are joined. |
| Factory/JDBC/component lifecycle: AdvisoryLockCoordinator, PostgresConnections, MonolithComponent, ServerExtension, PushProviders, FcmPushProvider, RoomClaimStore, ClaimContext, server/run Main | 22 | Partial-startup rollback closes owned resources, cleanup errors are suppressed on the primary error, and the primary failure is rethrown. Component teardown aggregates failures in NonCancellable. |
| Operation, codec and provider boundaries: RoomServiceImpl, WebPushProvider, ApnsPushProvider | 4 | Caller cancellation propagates; transport/provider/extension failures enter their existing modeled rejection or retry path. Public forwarding diagnostics remain sanitized. |

The three reviewed `InstanceOfCheckForException` IDs classify domain outcomes,
including coroutine cancellation and unsupported extension capabilities. They do
not introduce a broader catch or hide an exception.

## Baseline policy and validation

Only reviewed formatting, complexity and the exact boundary IDs above were added.
All legacy and new `SwallowedException` and `ThrowingExceptionFromFinally` IDs were
removed. `GlobalCoroutineUsage` and `SuspendFunSwallowedCancellation` remain absent.
The source fixes resolve the remaining exception findings instead of suppressing
those rules. The JSON inventory records added and removed IDs by rule and module.

The aggregate `./gradlew detekt` passes for all ten handwritten-source modules.
Focused fixture, atomic batch/ratchet and shard-map tests passed: 17 executed, zero
failures; one separate PostgreSQL claim-capacity test was skipped because this
local focused command omitted its opt-in environment variable. Its required actual
PostgreSQL gate passed in the integrated root run. Raw logs:
`/tmp/lockers-static-final-verified.log`,
`/tmp/lockers-static-final-detekt-confirm.log`.

The compiler-proven redundant nullability operators in RoomServiceImpl were removed
and its compile/detekt checks pass without server warnings. Connector nullability and
unstable StateFlow implementation warnings remain visible for the owning follow-up;
this change does not add warning suppressions. The previous commonMain naming and GlobalScope probes remain
valid gate evidence in F42. Runtime cancellation tests supplement syntax analysis;
a clean detekt report alone does not prove coroutine correctness.


## V7 integrated follow-up

The independent `4bb5ea4` checkpoint includes V6 archive adoption and V7 ordered
subscription intent storage. The first aggregate run found 571 additional
occurrences: 373 line-length, 117 wildcard-import and 45 numeric-literal findings,
plus 36 complexity/exception findings. These remain documented style and complexity
debt; this audit does not claim that a larger adoption baseline improves readability.
The exact additional and stale IDs are in `evidence/F42/v7-static-audit.json`.

The six additional generic catch occurrences all map to one detekt identity,
`TooGenericExceptionCaught:Stream.kt$SubscriptionController$failure: Exception`.
They were inspected at these boundaries:

| Boundary | Occurrences | Cancellation and outcome contract |
| --- | ---: | --- |
| Persisted startup intent and revision loading | 2 | Retry-budget exhaustion is handled before ordinary cancellation. Storage failures record controller failure, stop owned work and rethrow to the caller. |
| Per-room subscription worker | 1 | Ordinary cancellation is rethrown; an activity check precedes the typed `Change.Failed` message, retaining a bounded observable protocol failure. |
| Reducer command processing | 1 | Ordinary cancellation completes the pending caller promise exceptionally and propagates. Budget exhaustion is a domain failure; other failures settle the promise, cancel active work and retain only bounded desired-room failures, or stop the controller for a global failure. |
| Outer reducer and session-source collection | 2 | Explicit cancellation branches precede generic handling. Other failures record controller failure and stop owned work. |

The two additional `InstanceOfCheckForException` IDs belong to the reducer's explicit
ordinary-cancellation versus `RetryLimitExceeded` classification. This subtype is a
domain retry-budget outcome despite inheriting `CancellationException`; treating it
as ordinary cancellation would leave waiters without the modeled terminal result.
No swallowing, finally-throwing or coroutine-correctness rule was baselined. Stale
IDs were removed rather than kept as blanket future exclusions. Final integrated
source and runtime tests remain required after subsequent actor corrections.


The independent JVM compiler audit found seven redundant non-null assertions in
server cleanup, JDBC and metric-tag paths. They were removed using the compiler's
existing non-null proof, preserving suppressed cleanup failures and metric values;
two resulting long lines were wrapped. The server recompilation has zero Kotlin
warnings, and the aggregate ten-module detekt gate passes. The three remaining
LockerClient redundancies belong to its active source owner; four generated
protobuf-descriptor conversion warnings are visible and were not edited in generated
bindings. Raw audit and follow-up logs are retained alongside the ID inventory.
