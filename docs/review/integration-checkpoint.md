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
