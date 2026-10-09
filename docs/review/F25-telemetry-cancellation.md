# F25: telemetry preserves operation cancellation

Two failing regressions demonstrated that a cancellation thrown by suspend span startup was swallowed, and a span returning `NonCancellable` as its context prevented cancellation of the library operation. `observe` now rethrows startup cancellation and removes the span context's `Job` before installing tracing context.

Both regressions pass along with the observability API suite on JVM, Node JS and Apple simulator; Android compilation also passes. Evidence: `/tmp/lockers-F25-telemetry-red.log` (2 tests, 2 failures) and `/tmp/lockers-F25-telemetry-green.log`.
