# F41 — patched resolved JDBC and aligned direct Netty runtime

Status: fixed in source; final integrated consumer verification still required.

Reproduction: the review's resolved library/app graphs selected PostgreSQL 42.3.1 /
42.7.3 and mixed direct Netty 4.1 modules. Forcing those historical artifacts through
an isolated init script makes the new resolved-artifact guard fail. The storage
boundary's driver regression also fails when forcing 42.3.1, then passes without
that override. Evidence retains both red/green output.

Fix: pgJDBC 42.7.14 at the owning ktstore boundary (isolated commit bf391ea) and the
lockers library/application runtime; direct Netty artifacts use the published
4.1.139.Final BOM. Both were current patched supported releases at verification.
The library's check now includes verifyRuntimeSecurity over actual artifacts;
consumer metadata includes the BOM. A JVM test verifies actual loaded JDBC and
unshaded codec/transport implementations, and an EmbeddedChannel regression writes
and releases a valid HTTP/2 client preface. Shaded gRPC retains unrelocated version
resource names, so Version.identify alone is not an accurate artifact/loaded-class
check; shaded dependencies require their own advisory assessment.

Validation: storage-driver regression passes; lockers security guard and two
network contract tests pass; all 78 connector JVM tests pass. Recorded library and
application runtime graphs both select pgJDBC 42.7.14 and consistent direct Netty
4.1.139.Final modules. The patched storage library was published only to the private
Fullhouse worktree Maven repository with ./fh deps resolve/publish --library ktstore.
No external repository or global Maven Local was used.

Primary evidence: [pgJDBC security](https://jdbc.postgresql.org/security/),
[42.7.14 release](https://jdbc.postgresql.org/changelogs/2026-10-07-42.7.14-release/),
[Netty 4.1.139 release](https://netty.io/news/2026/10/06/4-1-139-Final.html).
The PostgreSQL SCRAM CPU issue requires a malicious/impersonated endpoint; simple
query injection requires the non-default simple mode and a vulnerable query shape.
The public listener is CIO, so a Netty server HTTP/2 advisory does not establish an
exposed listener here. 42.7.14 also covers the maintainer's latest requireAuth and
buffer-padding fixes; this change does not claim those conditions occur locally.

Replay commands: ./gradlew :server:verifyRuntimeSecurity :server:test --tests
'*NetworkDependencyContractTest' :connector:jvmTest, with the explicit generated
-PfhWorkspace manifest. Raw logs and resolved graphs are in evidence/F41.
