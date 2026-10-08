# F42 — consistent publication contract and enforced handwritten-source analysis

Status: fixed; final integrated static-analysis and consumer checks remain required.

Reproduction: a temporary naming violation in API commonMain produced detekt
NO-SOURCE and a successful exit in the original configuration. README promised JVM
only and authorization pending, while builds supplied additional variants and lock
authority. LICENSE was MIT but generated POM configuration requested Apache-2.0.
LockerSigning prose documented four-byte prefixes although its Writer used eight.

Fix: README documents the published targets, Android26 connector floor, write-lock
hierarchy/open reads, trusted server extensions, state/event Flow contracts,
source/agent outcomes, and pure optimistic transforms with frozen transport retries.
POM metadata now follows the existing MIT LICENSE; generated JVM POM was inspected.
Canonical V1 bytes are unchanged. Four exact domain preimages are published as
cross-language JSON fixtures and portable tests, including omitted/zero keyspace
identity equivalence. Protocol upgrades must use a new domain/version.

Detekt explicitly scans handwritten Kotlin across actual KMP and JVM source sets,
uses a reviewed existing-debt baseline, and fails on new findings. Coroutine rules
are enabled; no GlobalCoroutineUsage or swallowed-cancellation rule is baselined.
Type-aware coroutine findings require type-aware tasks and do not replace runtime
regressions. This is an adoption gate, not a claim that historical style debt has
been eliminated. Existing exception findings remain tied to the correctness fixes.

Validation: the commonMain naming probe fails after the scanner fix. After baseline
generation, a fresh GlobalScope.launch probe fails with GlobalCoroutineUsage.
Removing the probes restores a successful aggregate detekt across all ten code
modules. The two signing tests pass on JVM, Node and iOS Simulator Arm64; the
published POM says MIT License and links the MIT license. Evidence/F42 contains
raw red/green logs, POM, portable test XML and reviewed rule counts.

Commands: ./gradlew detekt; ./gradlew :api:jvmTest :api:jsNodeTest
:api:iosSimulatorArm64Test :api:generatePomFileForJvmPublication with the isolated
Fullhouse workspace manifest. Baseline updates for subsequent remediation commits
must be reviewed in the integrated branch; new correctness suppressions are not
an acceptable way to pass this gate.
