# Static-analysis adoption baseline

The baseline covers handwritten Kotlin in every source set, including commonMain
and platform implementations. Build-generated protobuf and DI files are excluded.
Generation does not imply approval to add suppressions. The aggregate detekt task
is enforced; newly introduced findings fail the build. Typed coroutine rules also
require the corresponding type-aware task; syntax-only analysis does not prove
cancellation safety, so runtime cancellation/lifecycle regressions remain required.

The captured review baseline records existing formatting/import/complexity debt,
plus exact reviewed generic boundary catches. The integrated review, cancellation
contracts and additions/removals are documented in
docs/review/F42-integrated-static-audit.md and its JSON inventory. Initial rule
counts are in docs/review/evidence/F42/baseline-rule-counts.json.
SwallowedException and ThrowingExceptionFromFinally findings are repaired in source
instead of baselined.
No GlobalCoroutineUsage or SuspendFunSwallowedCancellation entry is suppressed.
Do not regenerate a baseline merely to make a new correctness defect disappear.
When incorporating this remediation branch's new source, review any additional
baseline entries explicitly and remove obsolete entries after repairs.

The gate was tested with a temporary commonMain GlobalScope.launch probe after
baseline generation: detekt failed with GlobalCoroutineUsage. The probe was removed,
then the full aggregate task passed. A naming violation in commonMain was previously
ignored as NO-SOURCE, and now fails too. Neither probe is in source or the baseline.
