# lockers

A standalone **locker sync primitive**: versioned, subscribable per-room key/value
blobs ("lockers") with real-time, per-session fan-out over gRPC (unary over HTTP,
streaming over WebSockets). A locker is an opaque payload identified by
`(room, keyspace, lockerId)` and carrying a monotonic version for optimistic
concurrency. Clients subscribe to a room and receive live events as lockers change.

> Scope: this repo is the primitive only — durable storage, sessions, subscriptions,
> and push fan-out. Higher-level features (chat, profiles, games, …) are add-ons
> layered on top by consumers and are intentionally out of scope.

## Modules

| Module | What it is |
| --- | --- |
| `api` | All protobuf definitions and the generated Kotlin (one codegen tree). KMP: JVM, Android, JavaScript, iOS. |
| `connector` | Client SDK: `LockerClient` / `TypedLockerClient` and the `Stream` reconnect loop. KMP: JVM, Android, JavaScript, iOS. |
| `server` | JVM service host: session, session-gateway, room, and push services wired via kotlin-inject. |
| `server:test` | Test fixtures (`attachTestServices`) that boot the real monolith over an in-memory store. |
| `server:run` | Application entrypoint (`Main.kt`): config, persistence selection, HTTP endpoints, graceful shutdown. |

## Build & test

Proto generation installs the pinned `protoc-gen-kt` into `api/build/tools`.
Install Go 1.21 or later; the build selects the exact Go toolchain and plugin
versions from `gradle.properties` using `GOTOOLCHAIN`. It never selects an
unversioned generator from your PATH. CI and Docker provide Go 1.26.1.

Then:

```bash
./gradlew build                 # compile everything + run all tests
./gradlew :connector:jvmTest    # acceptance gate: integration tests vs. an in-process server
./gradlew :server:test          # server-side unit tests (persistence, config, rate limiter)
./gradlew detekt                # static analysis
```

## Running the server

```bash
./gradlew :server:run:run       # boots on :8080
```

Configuration is read from the environment at startup (12-factor). Every value has a
safe local-dev default; production must set at least `LOCKERS_DB_URL`.

| Env var | Default | Meaning |
| --- | --- | --- |
| `LOCKERS_HTTP_PORT` | `8080` | HTTP port. |
| `LOCKERS_DB_URL` | _(unset)_ | Postgres JDBC URL. **Unset ⇒ in-memory store (NOT durable).** |
| `LOCKERS_MAX_LOCKER_BYTES` | `1048576` | Max serialized locker payload; larger writes are rejected. |
| `LOCKERS_ROOM_WRITES_PER_SEC` | `50` | Per-room sustained write rate (`<=0` disables limiting). |
| `LOCKERS_ROOM_WRITE_BURST` | `100` | Per-room burst allowance. |
| `LOCKERS_SESSION_CACHE_SIZE` | `1000` | Room→sessions cache entries. |
| `LOCKERS_SHARD_MULTIPLIER` | `4` | Shard count = CPU cores × this. |
| `APNS_TEAM_ID` / `APNS_KEY_ID` / `APNS_KEY_PATH` | _(unset)_ | APNS credentials; absent ⇒ push no-ops. |
| `APNS_ENVIRONMENT` | `development` | `development` or `production`. |
| `APNS_TOPIC` | `com.latenighthack.lockers` | APNS push topic. |
| `LOG_LEVEL` | `INFO` | Root log level. |

### Persistence

Durable storage uses ktstore's configured `Database` via `ServerStorage.postgres`,
with all server and extension definitions composed before opening. Set `LOCKERS_DB_URL` to a
Postgres JDBC URL, e.g.:

```bash
export LOCKERS_DB_URL="jdbc:postgresql://localhost:5432/lockers?user=lockers&password=secret"
```

With no `LOCKERS_DB_URL`, the server falls back to an in-memory store and prints a
warning — convenient for local dev, **not durable across restarts**. The
`server:test` `PersistenceTest` proves data survives a store "restart" through the
SQL delegate.

### Observability

Independent Grafana packs, optional Prometheus/OTel adapters and connector telemetry are documented in [monitoring/README.md](monitoring/README.md). Dashboards select existing data sources and need no recording rules.

- **Metrics**: Prometheus exposition at `GET /metrics` (Micrometer). All metrics are
  namespaced `lockers.*`.
- **Health**: `GET /healthz` (liveness) and `GET /readyz` (readiness).
- **Logs**: structured JSON to stdout (logback + logstash encoder), directly
  ingestible by Loki/Promtail, ELK, or Datadog.

## Connector usage

Compose `ConnectorStorage.definitions` with all application/social definitions before
creating the shared handle. Pass it as `database` to `LockersClient.create`; opening
is idempotent. `ConnectorStorage.inMemory()` is a configured test factory.
`ServerExtensionFactory.storeDefinitions` declares extension stores before server
startup, and the factory receives the shared database.

V1 definitions preserve historical persisted names and encodings. The configured
version 3 adoption migrates legacy browser version 1, Android version 2 and
unversioned SQL stores: it derives keys from records, preserves original payload
bytes and creates previously unregistered stores. Historical definitions are
immutable migration contracts; future formats need a new definition and explicit
consecutive migration. Adoption, duplicate-key failure and reopen are covered by
frozen protobuf fixtures, including Android's legacy text blob bindings.


The client wraps lockers as typed values. See
`connector/src/jvmTest/.../LockerClientTests.kt` for complete, runnable examples.

```kotlin
val typed = TypedLockerClient(
    lockerClient,
    keyspace = LockerKeyspace { value = MY_KEYSPACE },
    writer = MyValue::toByteArray,
    reader = MyValue.Companion::fromByteArray,
)

// optimistic, versioned update
typed.updateLocker(roomId, lockerId) { current -> current.mutate() }

// live, per-room view that folds change events into a map
typed.watchAll(roomId).collect { lockers -> render(lockers) }
```

The `Stream` reconnect loop retries transient session-open failures with backoff and
surfaces terminal failures (rejected key, rejected session id, upgrade required) via
`Stream.fatalError` instead of crashing.

## Local dependency verification

Released builds resolve published artifacts only and never use global Maven Local.
The definition-backed ktstore 0.2.0 transition must be published and verified before
releasing this library; a workspace verification does not establish released resolution.
For paired development, configure explicit library paths in Fullhouse's ignored
`.fh/workspace.json`, then run `./fh deps resolve` and
`./fh deps publish --library lockers`. Use its generated `-PfhWorkspace` manifest
and isolated Maven repository for library tests and consumer verification.

Portable protocol, codec and storage contracts run in `commonTest`. JVM and Apple
SQLite, browser IndexedDB and Android instrumentation exercise persistent drivers:

```bash
./gradlew :api:jvmTest :api:jsNodeTest :api:iosSimulatorArm64Test
./gradlew :connector:jvmTest :connector:jsNodeTest :connector:jsBrowserTest
./gradlew :connector:iosSimulatorArm64Test :connector:connectedDebugAndroidTest
```

Node intentionally excludes the browser IndexedDB test. Android requires an app-owned
test emulator; Apple tests require macOS and an installed simulator runtime.

## Published contract

`api` and `connector` publish JVM, Android, JavaScript (browser and Node) and iOS
(arm64, x64 and Simulator arm64) variants. Connector Android consumers require
API 26 because its crypto dependency requires that floor. The server runs on JVM.
Local validation uses the explicit Fullhouse workspace procedure above; Maven
Central publication and application/service releases are separate operations.

Session streams authenticate their session key. Room writes are authorized by
room, keyspace and locker locks; public-key room IDs establish room authority,
while opaque unclaimed rooms use first-writer establishment. Locks authorize
mutation, not reads: subscribe/read access is open unless an application supplies
a separate read-access policy. Session capabilities are identifiers, not secrets.
Signed `sealed` envelopes provide integrity; their contents remain readable.
Encrypt sensitive payloads before writing them and keep decryption keys outside
those records, or enforce an application read-access policy on every read and
subscription route. A room described as private by an application needs that
additional confidentiality protection.
Server agents and private peer gateways are trusted extensions; applications must
install intentional agents rather than treating untrusted callbacks as sandboxed.

Optimistic update transforms, notification builders and codecs must be pure and
repeatable. A conflict may run them again against a newer source value. Ambiguous
transport retries reuse the exact frozen request, including encoded notification,
request identity and signature; retrying does not create a new logical mutation.
A committed source and agent completion are separate outcomes. Consumers must
retain the committed result when reporting an agent failure or indeterminate work.

Collect state and notification Flows in an owned CoroutineScope and close clients
when that scope ends. Locker snapshot streams represent current state and may
conflate superseded values; notification/event streams have separate delivery
semantics. Durable outbox and inbox records bridge temporary disconnection; replay
may duplicate an event, so stable identities and durable acceptance precede ACKs.

Canonical V1 signing preimages use eight-byte big-endian byte-array lengths and
signed eight-byte big-endian scalar values. Protocol-domain changes require a new
version rather than changing V1 bytes. Exact cross-language vectors are published
in [signing-v1.json](docs/protocol/signing-v1.json) and run in portable API tests.

Detekt enforces handwritten Kotlin across all source sets with the reviewed
baseline in `config/detekt`. Newly introduced findings fail `build`/CI. Baselines
capture existing complexity/style debt and do not replace correctness tests;
coroutine cancellation and transport behavior require the runtime regressions.

## License

MIT — see [LICENSE](LICENSE). Published POM metadata uses the same license.
