The combined pass10 at `4a93a9e` timed out the original per-keyspace codec test after its unchanged15-second JUnit bound. This timeout remains unreproduced; no production cause or fixture-readiness loss was established. Parent retains the original pass10 failure XML. Investigation uses isolated source `31aba20` (same production codec behavior).

- Original standalone codec suite:4 tests passed.
- Temporary diagnostic test invoked the exact original HTTP fixture30 times under a15-second outer JUnit bound: passed. Stage markers cover collector readiness, subscriptions, both writes, default payload, other payload, and close.
- Whole SDK JVM task with that temporary test passed (original197 cases plus1 diagnostic case). This is a diagnostic whole-suite run, not the unchanged final gate; its full-task Gradle log is retained. Per-case results were overwritten by the later targeted runs.
- Same diagnostic fixture300 times:1 JUnit case passed,0 failures/errors/skips; XML captures all300 attempts and every stage. The diagnostic patch records the exact temporary source.
- Explicit late-collector gate: hold the OTHER_KEYSPACE collector until after both updates finish;4 original assertions/tests and15-second bounds pass. The patch and XML prove durable journal replay works despite that delayed start.

No source fix is claimed. Both collectors consume `notificationsAfter(0)` from durable accepted-event history; waiting for the second `onStart` would not explain or repair this timeout. All temporary diagnostics/gates were removed after evidence collection. The original test source and timeout are byte-identical to `31aba20`. The parent's unchanged combined pass11 is the final gate.

Every command used the repository wrapper, `:connector:jvmTest`, the immutable `dependencies-status-final.json` manifest, and its isolated `.fh/maven`. Targeted runs used `--tests '*NotificationCodecTests'` or `--tests '*NotificationCodecTests.repeated*'`; the diagnostic full task used `:connector:jvmTest --rerun`. No parent, connector-owner, or publisher sources were edited.
