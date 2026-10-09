# F39: concrete release transition and remaining external gate

Local paired dependencies are complete, immutable and isolated from global Maven Local:

| Library | Source checkpoint | Locally published version | Artifacts |
| --- | --- | --- | --- |
| ktstore | 37cabbeb2ab292ecea404a1a88b0ebe05bf159d8 | 0.2.0-fh.92be63aec7145a5f4f07 | 67 |
| ktbuf | 780bcc0bad28f59be37edc7d9ddb4c3c72bd8705 | 1.1.10-fh.9c3962ea021c76107b84 | 178 |

The frozen manifest is `.fh/dependencies-status-final.json` in the isolated Fullhouse review consumer; its repository is `.fh/maven`. Publication used `./fh deps publish --library ktstore` and `--library ktbuf`, with explicit source paths. Every target's metadata and binary artifact was present. The final ktbuf checkpoint includes the shared pinned generator source patches, output budgets, terminal transport statuses and numeric status round trips and validated exact HTTP status metadata. Logs are `/tmp/ktbuf-status-final-publication.log` and the earlier ktstore publication evidence.

Released-mode resolution remains blocked because the current catalog does not identify publicly released artifacts containing these new APIs. Local validation is not public release evidence. Neither different bytes under an existing coordinate nor an external release has been performed.

A concrete coordinated release is:

1. Stage **ktstore 0.2.1** from checkpoint37cabbe, preserving the frozen historical codecs and all paired JDBC/indexed owner behavior. Build/publish its JVM, Android, JS and Apple artifacts to the external repository through the owner's normal release workflow. Verify Maven metadata, checksums and all67 artifacts before updating dependents.
2. Stage **ktbuf 1.1.11** from checkpoint780bcc0 with the pinned generator installer/patch manifest. Verify all178 multiplatform artifacts, including RPC/server/test/conformance support and preserved old constructor/function ABI.
3. Update lockers' catalog to these public coordinates in a separate release commit. Remove the workspace override and run portable API/connector/storage tests, JVM service/actual PostgreSQL tests, Android instrumented storage/transport checks, Node and Apple tests, and the resolved security artifact check. Any released-mode resolution failure is a blocker.
4. Stage **lockers 0.1.17** from the final integrated remediation checkpoint, publishing every module (including observability and server-test) for every declared target. Record its final commit, source hashes and public artifact checksums in the release evidence.
5. Update Fullhouse's released catalog and run its JVM/server and shared-core tests plus Android, JS and Apple consumer verification without `-PfhWorkspace` or the isolated Maven repository. Its public listener mounts client routes; internal8081 mounts token-protected peers/admin. Claim nodes require a shared `LOCKERS_PEER_TOKEN` and internal advertised addresses. No deployment is included in this validation.

The proposed versions avoid reusing an ambiguous existing coordinate. Before external staging, the library owners must confirm those coordinates remain unused; if a coordinate has acquired bytes, choose the next unused patch version rather than overwriting it. External publication and application/service release remain separate actions under the repository instructions.
