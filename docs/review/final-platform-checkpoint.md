# Final integrated platform checkpoint

A fresh isolated worktree at `4a93a9e` includes the final signed-subscription transaction inversion fix, SDK durable confirmation/startup guards, exact revision echoes, cancellation fixes, and conservative legacy archive recovery. No production source or generated binding was edited during verification.

| Module | Node | Apple simulator | Chrome IndexedDB | Android SQLite |
|---|---:|---:|---:|---:|
| API | 10 | 10 | — | — |
| Connector | 4 | 7 | 7 | 7 |
| Observability API | 6 | 6 | — | — |
| Observability connector | 4 | 4 | — | — |

All 65 platform test executions passed, with zero failures, errors or skips. API, connector, observability API and observability connector Android production compilation also passed. Real persistent connector tests include historical V3–V6→V7 migration, private key/unknown-byte preservation, revision tombstone close/reopen, and atomic admission rollback on Chrome IndexedDB, Apple SQLite and Android SQLite. Android instrumentation used owned emulator 5590; 5580 was untouched and 5590 remains booted.

The two sequential builds used the immutable `dependencies-indexeddb-final.json` manifest and isolated Fullhouse Maven repository: ktstore `0.2.0-fh.d53a47cbc15434b3ce1e`, ktbuf `1.1.10-fh.9c3962ea021c76107b84`. [Exact counts and manifest digest](evidence/final-platform-checkpoint/test-counts.json), [commands](evidence/final-platform-checkpoint/commands.txt), raw build logs and every platform result XML are retained in `evidence/final-platform-checkpoint`. Generated descriptor conversion warnings and Gradle/Node tooling deprecations remain visible in the logs; no warning suppression or generated edit was used.

The root agent owns the final whole JVM and Fullhouse consumer gates. These platform checks performed no publication or deployment and did not modify the original checkout.

## Authority-repair follow-up

F44 changed connector common source after this checkpoint. Its committed repair separately passed actual Node6, Apple9, Chrome9 and Android9 tests (33 executions), plus Android production compilation, using the same immutable upstreams. Evidence is in F44-sdk-authority-repair.md and evidence/F44-sdk-green. API and both observability modules are unchanged from the checkpoint above; their40 actual platform executions remain applicable. This gives73 applicable platform executions across these source checkpoints, not73 tests from one invocation. JVM-only watcher-fixture changes add no platform production changes.
