# Review remediation burndown

Each row is complete only after its reproduction, regression, fix, commit and integration verification are recorded. Partial rows remain open.

| Finding | Priority | Issue | Status | Commits |
|---|---|---|---|---|
| F01 | P1 | An unsigned child lock can override an existing parent authority | partial | 0ea9f0cc |
| F02 | P1 | A valid sealed signature can carry an unauthenticated open payload | partial | 13a6d1fa, 8e0d9d86, 9e7fa716, c967d1d4 |
| F03 | P1 | Old unlock and delegation signatures remain reusable | partial | 189eee14, 4ca4d43b, c1d6f215, 333ab38b |
| F04 | P1 | Internal event and push gateways are exposed without peer authentication | partial | 670ecd59, e2cdeafd, 4a49f969, dab26ea7 |
| F05 | P1 | Web Push endpoints allow server requests to private HTTP destinations | partial | cf38a3d9 |
| F06 | P1 | Session proof does not protect subscription or push credential mutations | partial | 01c53db9, 77c07c69, c0593e78, 9c1d34e3, 4d02049d, 3f0bb0fb, f8b18ae1, 274e7deb |
| F07 | P1 | A stale negative lock cache can disable signature enforcement | partial | 4c0b7dcd |
| F08 | P1 | The default mutation path has no database version CAS or commit fence | partial | ca6e060c, 56ba7047 |
| F09 | P1 | The default example agent can overwrite an independently locked keyspace | partial | f28f0afc |
| F10 | P1 | A legacy ratchet can commit before its associated content write fails | partial | 984e2f03, 238cc852, 55f8733e, 50f5a42f, a6d21566, abe4311f |
| F11 | P1 | The client can discard a successfully committed ratchet key | partial | f41db2c4, f1fe0fe1, 81372047, 076f5a14, d0a45c15, 8bdf808e, 4e66a0d0 |
| F12 | P2 | Session creation and challenge rotation are not atomic | partial | b7768ab3 |
| F13 | P2 | Creating a locker does not advance its version | partial | 987e85bc, 1b749f20 |
| F14 | P2 | Null and zero keyspace aliases bypass duplicate and serialization checks | partial | a15c0ac4, 14b4a323, c268b2a7, 3aa145b4, 1cc3f671 |
| F15 | P2 | Bulk reads represent a tombstone as a live empty locker | partial | 99379f6c |
| F16 | P2 | Legacy revalidation cannot remove cached deletions | partial | 9b77ef23 |
| F17 | P2 | Subscription changes on one node do not refresh another node's fanout cache | partial | 70e6bfc5 |
| F18 | P2 | The default delivery mode can acknowledge a write whose event has no recovery path | partial | d103c0c7, fab169a5, 83650fb0 |
| F19 | P2 | Persisted push work can become invisible to a running worker | partial | 8786ba85 |
| F20 | P2 | Push drain can duplicate in-flight sends and does not bound queued coroutines | partial | ca1cfd5e, b4258738 |
| F21 | P2 | Push registration failures are not retried on ordinary reconnects | partial | 7dc75fbc, 7c880d08, 47ef0852, 7f7bfff6 |
| F22 | P2 | Late push acknowledgements can restore obsolete or removed credentials | partial | 4374c3c0, 9c8c7741, 6d041947, f8f4442e |
| F23 | P2 | A watcher publishes partial history as its initial snapshot | partial | fe7b93a8, 1e2ff0c9 |
| F24 | P2 | Application Flow collectors can block acceptance and ACKs for unrelated rooms | partial | a321be6b, 1e2ff0c9 |
| F25 | P2 | Cancellation is retried or converted into application failure | partial | ea56134d, 3555f92a, 565a39fe, 2f59f54c, c862e868, 845fbfd7 |
| F26 | P2 | Background ownership and shutdown are detached from the caller's lifecycle | partial | 0d339e5a, b87f34b2, 1f3593f4, 70351cf4, 5cb73ce3, c6cca713, eb14e47b, 40b705dd, 7f7bfff6, 46775599, 845fbfd7, 82a262a4 |
| F27 | P2 | Connection state can remain true during failure and awaiters can hang after closure | partial | 0750a296, 7f7bfff6, 845fbfd7, 82a262a4 |
| F28 | P2 | The session ping branch never emits its pong and there is no heartbeat deadline | partial | d2eea55e, b11e9f2c, 811d2cf4 |
| F29 | P2 | Failure during session publication leaks local state and can block the next open | partial | 8865d82d, 4c85efc8 |
| F30 | P2 | A denied ring lease is never retried while the shard map stays unchanged | partial | 2e3a03db |
| F31 | P2 | Ring mode does not maintain authority for nonzero keyspaces | partial | bb99a0c3 |
| F32 | P2 | Shutdown releases authority before requests and workers have drained | partial | 5c2755b1 |
| F33 | P2 | Retention and queue discovery grow without a bound | partial | 0457e0d8, abc27633, 1ea959e8, d0b64125, 8b895ba0, 26434f43 |
| F34 | P2 | Notification codec context does not survive delivery | partial | 85533853, 4b368276 |
| F35 | P2 | The high-level client ACKs broadcasts without surfacing their payload | partial | 60533196, a5131b73 |
| F36 | P2 | The documented JVM routing factory receives incompatible addresses | partial | d771d62f, 8c4b7c52, 93804aba, 21ce1b8e, bcde47b3, dab26ea7 |
| F37 | P2 | An agentPending receipt can remain pending permanently | partial | 4f437d68, 45a96a6d, 9f854ad6 |
| F38 | P2 | Malformed identity and authority data are persisted and resource limits are incomplete | partial | 4cc7cea7, c8e89e10, cf64d932, 1510d954, 43a239b6, 2ef40806, 3b6e8db2, 6fd246a7, 1c3efeb6, f70aa723, c1bf97a4, 93acbe6e, 1fc5e4a9, 08c2d665, eb5f1c44, 5fa731a2, 03b11b55, 6628762c, b37de936, 7f7bfff6, 82a262a4 |
| F39 | P2 | Build reproducibility and target tests do not cover the published contract | partial | 608fb377, 69941b4e, d6ef56d1, e5664adb, 3b5f8cb1, f6a533ee, 58d72268, 95c839d1, 9a82beea |
| F40 | P2 | A topology priming error terminates polling while readiness can later report success | partial | 1ff4c46b |
| F41 | P2 | Resolved JDBC and network dependencies need security patching and version alignment | partial | c2236968, a9d78e30 |
| F42 | P3 | Documentation and quality gates disagree with the implementation | partial | 008d5d30, 91e672c2, fce28011, 7a370dd1, 46775599, 2765fbde, 3c7de149 |
| F43 | P2 | An older signed subscription can commit after a newer unsubscribe | partial |  |
