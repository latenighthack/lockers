# F21: transient session admission preserves identity

The regression injects an `IOException` into the atomic session admission operation. Previously the server incorrectly returned `SESSION_EXISTS`, which directs the SDK to abandon that identity. It now returns `UNKNOWN_ERROR` for transient failures and `UPGRADE_REQUIRED` when a custom store lacks the required atomic admission contract. Cancellation still propagates unchanged.

Evidence: `/tmp/lockers-F21-session-create-red.log` and `/tmp/lockers-F26-integration-green.log`. The latter includes the new session test, runtime cancellation, component shutdown, gateway boundaries, subscription freshness, outcome metadata, and proof quotas.
