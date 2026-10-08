# F22 — revisioned desired push state

Reproduction: hold an uncancellable register RPC, unregister its backend, then release the old success. The old code recreates and confirms the removed credential (`ReviewPushControllerTests.late register response cannot restore an unregistered credential`, `/tmp/connector-F22-repro.log`).

Fix: a bounded reducer persists additive push intent records with monotonic revision and durable revoke tombstones. All desired state, session incarnation, confirmations, failures and closure share one StateFlow. Exactly three backend effect lanes reconcile independently and cancel/join obsolete work before its successor. Confirmations require matching session, connection epoch, revision and credential bytes; storage acknowledgement also compares the persisted revision. Requests carry credentialRevision and optionally obtain a fresh session proof for each retry. The server's matching revision CAS is tracked by runtime worker.

Public status Flow and awaitUnregistered expose removal outcomes. Register/unregister complete after durable intent changes, including while offline; removal is retained until acknowledged. Caller-owned custom PushRegistrationStore implementations must implement durable intent methods.

Verification: four controller tests pass: transient retry, stale register after removal, stale register after rotation, and offline removal recovered/retried by a replacement controller using identical revision. `/tmp/connector-F22-fix.log`. Public proof wiring and schema upgrade are F06/F33 gates.
