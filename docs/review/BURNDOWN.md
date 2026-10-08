# Review remediation burndown

Each row is complete only after its reproduction, regression, fix, commit and integration verification are recorded. Partial rows remain open.

| Finding | Priority | Issue | Status | Commits |
|---|---|---|---|---|
| F01 | P1 | An unsigned child lock can override an existing parent authority | partial | 0ea9f0cc |
| F02 | P1 | A valid sealed signature can carry an unauthenticated open payload | partial | 13a6d1fa, 8e0d9d86 |
| F03 | P1 | Old unlock and delegation signatures remain reusable | partial | 189eee14, 4ca4d43b, c1d6f215 |
| F04 | P1 | Internal event and push gateways are exposed without peer authentication | partial | 670ecd59 |
| F05 | P1 | Web Push endpoints allow server requests to private HTTP destinations | partial | cf38a3d9 |
| F06 | P1 | Session proof does not protect subscription or push credential mutations | partial | 01c53db9, 77c07c69, c0593e78 |
| F07 | P1 | A stale negative lock cache can disable signature enforcement | partial | 4c0b7dcd |
| F08 | P1 | The default mutation path has no database version CAS or commit fence | open |  |
| F09 | P1 | The default example agent can overwrite an independently locked keyspace | partial | f28f0afc |
| F10 | P1 | A legacy ratchet can commit before its associated content write fails | open |  |
| F11 | P1 | The client can discard a successfully committed ratchet key | partial | f41db2c4 |
| F12 | P2 | Session creation and challenge rotation are not atomic | partial | b7768ab3 |
| F13 | P2 | Creating a locker does not advance its version | partial | 987e85bc |
| F14 | P2 | Null and zero keyspace aliases bypass duplicate and serialization checks | partial | a15c0ac4, 14b4a323 |
| F15 | P2 | Bulk reads represent a tombstone as a live empty locker | partial | 99379f6c |
| F16 | P2 | Legacy revalidation cannot remove cached deletions | open |  |
| F17 | P2 | Subscription changes on one node do not refresh another node's fanout cache | open |  |
| F18 | P2 | The default delivery mode can acknowledge a write whose event has no recovery path | open |  |
| F19 | P2 | Persisted push work can become invisible to a running worker | open |  |
| F20 | P2 | Push drain can duplicate in-flight sends and does not bound queued coroutines | open |  |
| F21 | P2 | Push registration failures are not retried on ordinary reconnects | open |  |
| F22 | P2 | Late push acknowledgements can restore obsolete or removed credentials | partial | 4374c3c0 |
| F23 | P2 | A watcher publishes partial history as its initial snapshot | partial | fe7b93a8 |
| F24 | P2 | Application Flow collectors can block acceptance and ACKs for unrelated rooms | partial | a321be6b |
| F25 | P2 | Cancellation is retried or converted into application failure | partial | ea56134d |
| F26 | P2 | Background ownership and shutdown are detached from the caller's lifecycle | open |  |
| F27 | P2 | Connection state can remain true during failure and awaiters can hang after closure | open |  |
| F28 | P2 | The session ping branch never emits its pong and there is no heartbeat deadline | open |  |
| F29 | P2 | Failure during session publication leaks local state and can block the next open | open |  |
| F30 | P2 | A denied ring lease is never retried while the shard map stays unchanged | open |  |
| F31 | P2 | Ring mode does not maintain authority for nonzero keyspaces | open |  |
| F32 | P2 | Shutdown releases authority before requests and workers have drained | open |  |
| F33 | P2 | Retention and queue discovery grow without a bound | open |  |
| F34 | P2 | Notification codec context does not survive delivery | open |  |
| F35 | P2 | The high-level client ACKs broadcasts without surfacing their payload | open |  |
| F36 | P2 | The documented JVM routing factory receives incompatible addresses | open |  |
| F37 | P2 | An agentPending receipt can remain pending permanently | open |  |
| F38 | P2 | Malformed identity and authority data are persisted and resource limits are incomplete | open |  |
| F39 | P2 | Build reproducibility and target tests do not cover the published contract | open |  |
| F40 | P2 | A topology priming error terminates polling while readiness can later report success | open |  |
| F41 | P2 | Resolved JDBC and network dependencies need security patching and version alignment | open |  |
| F42 | P3 | Documentation and quality gates disagree with the implementation | open |  |
