# F39 final immutable publication and Fullhouse consumer verification

The production library source cut is `00a0990f61a6f655b26cc02913bed90a04dc789b`. The paired dependencies and source cuts are listed in [the release transition](F39-release-transition.md). Local publication used Fullhouse's `./fh deps resolve` and `./fh deps publish --library NAME`, explicit ignored workspace source paths and an isolated Maven repository. No external publication, app release or deployment occurred. Old Locker31/Social31 content versions were preserved. Publication receipts cover Locker176 artifacts and the preserved Social consumer prerequisite1561 artifacts; final `fh deps check` verifies binary hashes and platform metadata. Exact frozen-manifest, generator installer/patch/binary hashes and the clean final consumer commit are [recorded](evidence/final-consumer/immutable-final-hashes.json).

The consumer is `/Users/mikeroberts/workspace/rollgames/.worktrees/fullhouse-lockers-review-build`, branch `fix/lockers-review-consumer`, based on `1d5a23f`, with consumer commits `2ad795d` (listener/server compatibility), `b4c64a0` (joined Core ownership), and `cb1060d` (consumer report). The complete reviewable consumer patch is [retained](evidence/final-consumer/fullhouse-compatibility.patch). Social is `/Users/mikeroberts/workspace/latenighthack/.worktrees/social-review-lockers-consumer`, preservation commit `3f51e90`. Its original dirty source inventory, modes and file hashes were stable across the copy. No original checkout was edited or built by this task. Other user changes occurred during this long session; the evidence does not claim all originals are byte-identical to the initial snapshots.

## Reproduced consumer boundaries

1. On Locker31, actual onboarding reached a profile-source write after account Ready but before its concurrent ROOM lock committed. The captured GetLocker epoch was0, V2 checksum/signature valid at0, and definitive rejection carried matching owned authority at1. The library's finite, fail-closed frozen-body authority repair resolves this race; new immutable Locker publication includes F44. No Social source fix was made.
2. With F44, the actual onboarding write completed but teardown failed with StreamClosedException: Fullhouse closed the SDK before joining application-owned profile watchers. The isolated consumer adapter now owns one retained cleanup Deferred shared by stop/closeAndJoin, joins app producers before SDK shutdown, joins Social/Locker telemetry before releasing transports, and closes only its owned raw RPC/content HTTP clients. Calls from owned descendants reject before NonCancellable, concurrent/repeated callers await the same outcome, cleanup attempts all lifecycles and aggregates errors, and the fixture retains an original test error with suppressed cleanup. The real onboarding and owned-callback/concurrent-close regressions pass.
3. Production listener adaptations preserve public client8080/internal authenticated peer+admin8081 separation. Explicit test-only routes are used in trusted fixture servers. Server lifecycle shutdown now awaits database/component cleanup; its initial non-suspend adaptation was a concrete compile failure. No service was deployed.

## Actual final consumer gates

Every command used the frozen `dependencies-lockers-recovery-consumer-final.json` and isolated `.fh/maven`; the reproducible runner is [consumer-gates.sh](evidence/final-consumer/consumer-gates.sh).

| Gate | Result |
| --- | --- |
| `:server:jvmTest` | 11 tests,0 failures/errors/skips |
| `:core:test:jvmTest` | 59 tests,0 failures/errors,1 pre-existing ignored test;58 executed |
| `:server:run:compileKotlin` | passed |
| `:core:compileDebugKotlinAndroid` | passed |
| `:core:jsBrowserProductionLibraryDistribution` | passed; compile/distribution only |
| `:core:linkDebugFrameworkIosSimulatorArm64` | passed; compile/framework link only |

Fullhouse removes its JS/Apple test source sets; these consumer gates are not mislabeled as browser/native tests. Actual Locker browser/Apple/Android persistent storage and transport tests are recorded separately. Existing keepAlive test fixtures retain process-isolated leaked Core instances, and existing unrelated consumer compiler warnings remain; neither is claimed remediated by this library task.

## Separate pre-existing application confidentiality debt

A deliberately negative actual HTTP probe created a profile through the preserved Social manager, then read its ProfileSource locker using a fresh RoomServiceRpc with no session credentials. The returned plaintext private key reconstructed a public key equal to the created profile ID, so the negative assertion failed. The probe source, XML and log are [retained outside the normal test source sets](evidence/final-consumer/application-read-policy-debt/probe.kt). No key or plaintext bytes were printed. The intentionally red probe was removed before the green consumer matrix and is not a passing gate.

Locker explicitly does not provide read ACL/confidentiality; its locks protect mutation authority. The preserved Social source stores this secret in an ordinary plaintext locker while describing the room lock as protection. The test proves the fixture's anonymous public-read boundary, not a bypass of independently enabled production App Check. Production attestation is independently configurable and defaults off; device attestation alone would not establish per-account read authorization. This is application confidentiality debt requiring application encryption or account-authorized read policy, not an additional Locker finding. No Social encryption or Locker read protocol was changed.

Released-mode resolution remains a separate blocked external release gate. The local manifest and successful consumer matrix do not make the default public coordinates available or qualify the preserved Social source for an external release.
