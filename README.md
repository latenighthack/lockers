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
| `api` | All protobuf definitions and the generated Kotlin (one codegen tree). KMP, JVM target. |
| `connector` | Client SDK: `LockerClient` / `TypedLockerClient` and the `Stream` reconnect loop. KMP, JVM target. |
| `server` | JVM service host: session, session-gateway, room, and push services wired via kotlin-inject. |
| `server:test` | Test fixtures (`attachTestServices`) that boot the real monolith over an in-memory store. |
| `server:run` | Application entrypoint (`Main.kt`): config, persistence selection, HTTP endpoints, graceful shutdown. |

## Build & test

Proto codegen shells out to `protoc-gen-kt` (the ktbuf Kotlin protoc plugin, a Go
binary) which must be on your `PATH`. Then:

```bash
./gradlew build                 # compile everything + run all tests
./gradlew :connector:jvmTest    # acceptance gate: integration tests vs. an in-process server
./gradlew :server:test          # server-side unit tests (persistence, config, rate limiter)
./gradlew detekt                # static analysis (advisory)
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

Durable storage uses ktstore's Postgres-backed `StoreDelegate`
(`SqlStoreDelegate` over a JDBC driver, `BYTEA` blobs). Set `LOCKERS_DB_URL` to a
Postgres JDBC URL, e.g.:

```bash
export LOCKERS_DB_URL="jdbc:postgresql://localhost:5432/lockers?user=lockers&password=secret"
```

With no `LOCKERS_DB_URL`, the server falls back to an in-memory store and prints a
warning — convenient for local dev, **not durable across restarts**. The
`server:test` `PersistenceTest` proves data survives a store "restart" through the
SQL delegate.

### Observability

- **Metrics**: Prometheus exposition at `GET /metrics` (Micrometer). All metrics are
  namespaced `lockers.*`.
- **Health**: `GET /healthz` (liveness) and `GET /readyz` (readiness).
- **Logs**: structured JSON to stdout (logback + logstash encoder), directly
  ingestible by Loki/Promtail, ELK, or Datadog.

## Connector usage

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

## Publishing

`api` and `connector` publish via `maven-publish`:

```bash
./gradlew :api:publishToMavenLocal :connector:publishToMavenLocal
```

## Roadmap / not yet done

- **Authorization & tenant isolation** — session open is signature-verified, but room
  and locker operations are not yet authorized per-identity. Design pending.
- **iOS/JS targets** — `api` and `connector` are structured for Kotlin Multiplatform
  but currently build the JVM target only. Upstream libraries have iOS/JS variants, so
  adding targets is a follow-up, not a rewrite.
- **Distributed tracing** — structured logs and Prometheus metrics are in place;
  OpenTelemetry span instrumentation (or attaching the OTel Java agent) is the next
  step for end-to-end traces.

## License

MIT — see [LICENSE](LICENSE). (Chosen as a permissive default; change if your project
requires otherwise.)
