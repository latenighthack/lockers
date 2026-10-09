# F26: host and fixture lifetime

The `monolith(core)` convenience overload now starts the component in an application-owned coroutine and joins shutdown in non-cancellable cleanup. Its routes remain public. Test service helpers explicitly start workers, parent services to the host, join component cleanup before closing storage, and roll back configuration or setup failures without replacing the original exception. Subscription tests explicitly start their directly constructed services.

Targeted component shutdown, cancellation, routing, freshness, outcome and proof tests pass in `/tmp/lockers-F26-integration-green.log`.
