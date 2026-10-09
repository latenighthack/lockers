# Integrated review checkpoint

Checkpoint `4b36827` passed the combined Gradle wrapper check against the frozen `dependencies-output-final.json` (ktstore37cabbe/ktbuf6123444). The task-owned PostgreSQL14 instance was required through `LOCKERS_TEST_PG_URL` and `LOCKERS_TEST_PG_REQUIRED=true`.

| Suite | Executed | Failed | Skipped |
|---|---:|---:|---:|
| API JVM | 12 | 0 | 0 |
| Connector JVM | 143 | 0 | 0 |
| Server | 333 | 0 | 2 |
| Sharding | 29 | 0 | 0 |

The two skipped server tests are the optional in-process and external claim load drivers. Both had successful dedicated runs recorded in F39-load-validation.md; those were earlier checkpoints, not final-pin load certification. All required PostgreSQL tests ran, including the corrected namespace gate. Host compilation and verifyRuntimeSecurity passed. Raw output: evidence/integration-pass6.log.

The exact merged PublicForwardingRecoveryTest has six actual HTTP cases; InboxByteAdmissionTest has six, ResponseBoundaryTest two, and ReviewExpiredRatchetTests three, all passed without skips (evidence/integration-expired-forward-green.log). After `21ce1b8` and `6628762`, terminal protocol, fresh batch creation, expired receipt recovery and bounded bulk memory cases also passed (evidence/integration-terminal-bulk-green.log). These focused runs verify preserved version0 inherited authority after the bulk-fetch merge.

All findings remain partial while the final status-metadata/ring-address fixes, combined platform/storage and Fullhouse consumer checks, reviewed static gate and independent review remain pending. External release is a separate gate; see F39-release-transition.md. This document is intermediate evidence, not a declaration of completion.

## Updated status dependency checkpoint

Checkpoint `274e7de` passed with `dependencies-status-final.json` (ktstore37cabbe/ktbuf780bcc0): API12, connector147, server340 executed/2 optional load skips, sharding29, zero failures/errors, plus host compile and resolved security verification. Raw output: evidence/integration-pass7.log. The merged status/destroy focused check passed19 server and6 connector cases with no skips (evidence/integration-status-destroy-green.log).

At `dab26ea`, the actual SDK ring reconnect case, four address-book contract cases and nine HTTP forwarding cases pass along with host compilation (evidence/integration-ring-public-green.log). The ring's actual baseline failing HTTP output is preserved as evidence/F36-ring-public-red.log. At `40b705d`, CLI2, observability-api6 JVM/6 Node/6 Apple, observability-connector4 JVM/4 Node/4 Apple, and observability-server6 all pass with no skips (evidence/integration-monitoring-keymaster-green.log).

The available independent verifier reviewed `274e7de` and independently executed18 F06/F36 cases (nine public forwarding, three server revocation, one proof boundary, two schema adoption, three SDK destroy), zero failures/errors/skips. Generator installation and binding generation actually ran. The two-handle PostgreSQL destruction test actually executed. Its report covers F06/F36, not the still-pending F11/actor/final consumer gates; see evidence/verifier-interim-F06-F36.txt. The earlier sandbox-blocked first pass is not counted as test execution.

All rows remain partial while those remaining source fixes and final verification are pending. Released-mode negative evidence is recorded separately in F39-released-mode-gate.md.

## IndexedDB dependency and ordered intent checkpoint

Checkpoint `e202cc9` passed the combined wrapper gate with immutable `dependencies-indexeddb-final.json` (ktstoref5e9eaa/ktbuf780bcc0): API12, SDK174, server345 executed/2 optional load skips, sharding29, keymaster2, observability-api6, observability-connector4, observability-server6; zero failures/errors, required PostgreSQL, host/security checks passed. Raw counts/output are `evidence/integration-pass8-counts.json` and `evidence/lockers-integration-combined-pass8.log`. The copied manifest is evidence of this dependency checkpoint, not a promise that its absolute repository paths exist on another machine.

The available verifier independently passed nine focused F11 cases at4e66a0d; raw report/XML/output are retained under evidence. Archive canonical adoption then passed23 JVM cases and15 actual Chrome/Android/Apple storage cases. Extending the historical V3/V4/V5/V6 paths through V7 and exercising persistent intent/removal/reopen/rollback passed34 cases (13JVM and7 on each real backend), without failures or skips; see F43-storage-platforms.md. These are scoped/intermediate checkpoints.

F43's server ordering, capability, signed identity and desired-fence/page checks are integrated. The SDK still needs durable confirmation CAS, atomic startup restoration against a stale snapshot, and exact successful revision echo. Final source/static/platform/Fullhouse consumer/independent gates remain open. Default external released dependency resolution remains a separate pending owner release, described in F39-release-transition.md.
