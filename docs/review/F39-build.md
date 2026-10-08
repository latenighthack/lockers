# F39 — reproducible build and platform coverage

Status: partial; release publication and final integrated Fullhouse consumers remain required.

Reproduction: a clean released-mode `./gradlew :connector:compileKotlinJvm --no-daemon`
failed with missing `com.latenighthack.ktstore:ktstore-library:0.2.0`.
The initial API JVM test task reported NO-SOURCE. Existing connector portable tests
were restricted to JVM; codegen selected an arbitrary PATH generator and Docker
provided no generator compiler. The preserved baseline had already added explicit
Node/Yarn repositories; workspace Node tests now resolve those successfully.

Fix: Gradle installs one versioned protobuf plugin into its private build directory
using a pinned Go toolchain; all proto, plugin, compiler artifact, classifier and
version inputs participate in up-to-date checks. Local, CI and container provisioning
use Go 1.26.1 and the plugin property in gradle.properties. Generated bindings are
untouched. Docker now supplies the compiler. Portable protocol/codec/indexed storage
contracts live in commonTest. Real JVM SQLite, Apple SQLite, browser IndexedDB and
Android SQLite contracts verify persistence across close/reopen and indexed deletion.
Node excludes the browser-only IndexedDB contract explicitly. Android instrumentation
also exposed the actual transitive platform floor: ktcrypto requires API 26, so the
connector now publishes minSdk 26 instead of promising unsupported API 24.

Validation: API portable tests pass on JVM, Node and iOS Simulator Arm64. Connector
portable contracts pass on JVM, Node, Chrome Headless and iOS Simulator Arm64; the
real persistent driver contract passes on JVM, Chrome Headless, iOS Simulator Arm64
and the dedicated API34 LockersReview emulator (`connectedDebugAndroidTest`, three
instrumentation tests). JVM storage adoption tests remain part of the final suite.
The browser test uses Dispatchers.Default so virtual test time cannot race real
IndexedDB callbacks. Container generation independently installed the pinned plugin;
`go version -m` confirms both module and compiler. Raw logs and XML are in evidence/F39.

Commands use the wrapper with the generated isolated Fullhouse manifest:
`-PfhWorkspace=/Users/mikeroberts/workspace/rollgames/.worktrees/fullhouse-lockers-review-build/.fh/dependencies.json`.
The patched ktstore publication was prepared by `./fh deps resolve` and
`./fh deps publish --library ktstore`, never global Maven Local.

Remaining release gate: ktstore 0.2.0 must exist in the configured released repository
before a clean released dependency resolution, CI build or full released Docker build
can succeed. Workspace builds do not prove this gate. External publication is a
separate release operation and was not performed. Fullhouse verification must run
again after all remediation commits are integrated and locally published immutably.
