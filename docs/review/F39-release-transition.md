# F39: concrete release transition and remaining external gate

Local paired dependencies are complete, immutable and isolated from global Maven Local:

| Library | Source checkpoint | Locally published version | Artifacts |
| --- | --- | --- | --- |
| ktstore | f5e9eaa801ff90c7c81538025a9d545a6c75056a | 0.2.0-fh.d53a47cbc15434b3ce1e | 67 |
| ktbuf | 780bcc0bad28f59be37edc7d9ddb4c3c72bd8705 | 1.1.10-fh.9c3962ea021c76107b84 | 178 |
| Locker | 00a0990f61a6f655b26cc02913bed90a04dc789b | 0.1.16-fh.4e06269cd00da3235cb0 | 176 |
| Social consumer prerequisite | 3f51e905cf16f30b1da8f47b28dcff16c9a38934 | 0.1.11-fh.4b1be6049821d9db7f78 | 1561 |

The upstream-only frozen manifest is `.fh/dependencies-indexeddb-final.json` in the isolated Fullhouse review consumer; its repository is `.fh/maven`. Publication used `./fh deps publish --library ktstore` and `--library ktbuf`, with explicit source paths. Every target's metadata and binary artifact was present. The final ktbuf checkpoint includes the shared pinned generator source patches, output budgets, terminal transport statuses and numeric status round trips and validated exact HTTP status metadata. Logs are `/tmp/ktbuf-status-final-publication.log` and `/tmp/ktstore-logical-owner-publication.log`. The actual browser regression verifies cross-handle logical writers and rollback; the Lockers Chrome storage gate now exercises event/cache/journal/ACK, ratchet and push mutations rather than only cache reopen.

Released-mode resolution remains blocked because the current catalog does not identify publicly released artifacts containing these new APIs. Local validation is not public release evidence. Neither different bytes under an existing coordinate nor an external release has been performed.

A concrete coordinated release is:

1. Stage **ktstore 0.2.1** from checkpointf5e9eaa, preserving the frozen historical codecs and all paired JDBC/indexed owner behavior, including native IndexedDB logical-owner serialization and rollback. Build/publish its JVM, Android, JS and Apple artifacts to the external repository through the owner's normal release workflow. Verify Maven metadata, checksums and all67 artifacts before updating dependents.
2. Stage **ktbuf 1.1.11** from checkpoint780bcc0 with the pinned generator installer/patch manifest. Verify all178 multiplatform artifacts, including RPC/server/test/conformance support and preserved old constructor/function ABI.
3. Update lockers' catalog to these public coordinates in a separate release commit. Remove the workspace override and run portable API/connector/storage tests, JVM service/actual PostgreSQL tests, Android instrumented storage/transport checks, Node and Apple tests, and the resolved security artifact check. Any released-mode resolution failure is a blocker.
4. Stage **lockers 0.1.17** from the final integrated remediation checkpoint, publishing every module (including observability and server-test) for every declared target. Record its final commit, source hashes and public artifact checksums in the release evidence.
5. Update Fullhouse's released catalog and run its JVM/server and shared-core tests plus Android, JS and Apple consumer verification without `-PfhWorkspace` or the isolated Maven repository. Its public listener mounts client routes; internal8081 mounts token-protected peers/admin. Claim nodes require a shared `LOCKERS_PEER_TOKEN` and internal advertised addresses. No deployment is included in this validation.

The proposed versions avoid reusing an ambiguous existing coordinate. Before external staging, the library owners must confirm those coordinates remain unused; if a coordinate has acquired bytes, choose the next unused patch version rather than overwriting it. External publication and application/service release remain separate actions under the repository instructions.

The final consumer manifest is `.fh/dependencies-lockers-recovery-consumer-final.json`, with all four explicit source paths and verified immutable publication receipts. The Locker source cut includes F44 authority-race repair and its reviewed static baseline. Earlier Locker31/Social31 local versions were retained without replacing bytes. Social is an isolated preservation snapshot of pre-existing dirty user work, needed because the referenced observability artifact was unavailable publicly; it received no source remediation and is not externally release-qualified by this local publication. A released Fullhouse build also needs its Social modules released through their normal owner workflow. The actual consumer read-policy probe records separate application confidentiality debt; it does not change Locker public reads or expand this library release into Social security remediation. See [final consumer verification](F39-final-consumer.md) for provenance, reproduced failures, actual target results and the application boundary.
