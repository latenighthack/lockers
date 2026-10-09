# Lockers monitoring

Import any JSON file in `dashboards/`, choose the existing Prometheus, Loki and Tempo data sources, and select the deployment. Each dashboard works independently. The overview links to the other packs. No recording rules, plugins, fixed data-source UIDs or application-specific queries are required. Grafana 11.4.0 is the compatibility baseline.

To provision files, mount `dashboards/` at `/etc/grafana/lockers` and install `provisioning/dashboards.yml` in Grafana's dashboard provisioning directory. Do not replace existing data-source configuration. `smoke/datasources.yml` illustrates the optional Loki derived field and Tempo-to-Loki mapping; use existing data-source UIDs in your own provisioning configuration. Use the same deployment value for Prometheus `environment`, Loki `deployment_environment` and the OpenTelemetry resource `deployment.environment`. Match `service_name` to the parent's trusted service resource, and keep trace IDs in JSON fields rather than Loki stream labels. Relayed connector events are logged by the parent server; client traces use the separately configured `client_service_name`.

## Embedded JVM host

Add `com.latenighthack.lockers:lockers-observability-server` at the same version as `lockers-server`. The parent owns its registry, SDK, scrape endpoint and authentication.

```kotlin
val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
val diagnostics = LockersMonitoring.diagnostics(parentOpenTelemetry)
val database = ServerStorage.postgres(jdbcUrl, extensionDefinitions, registry, diagnostics)
val core = ServerCore::class.create(config, database)
val monitoring = LockersMonitoring.attach(
    core, registry, registry, parentOpenTelemetry,
    MonitoringOptions(serviceName = "my-server", clientServiceName = "my-client",
        environment = "production", storageId = "primary"),
    spanExporter = parentClientSpanExporter,
)
core.setup()
```

For a `CompositeMeterRegistry`, pass it as the second argument and its Prometheus child as the third. Attach before setup, registering Lockers meters or constructing services. Pass the registry and diagnostics to the measured storage factory to include background/read operations outside write tracing. Custom databases can use `MeasuredStoreDelegate(delegate, registry, diagnostics)`. Extension stores are excluded from Lockers storage metrics; extensions retain their own instrumentation and shared registry seam.

Scrape the parent's existing internal endpoint. Backlog COUNT queries run every 15 seconds outside scraping, and read no payloads. `storageId` is a stable logical database name, shared across replicas of that database; never use connection URLs. Shared queue totals use `max` across replicas of the same storage. Per-backend push depths remain local estimates. Snapshot timestamp/health must accompany interpretation of cached totals. Polling failure leaves the last successful values visible; unavailable initial counts are -1.

`close()` cancels monitoring work and unregisters its owned gauges/collector. Stop the core and call this during parent shutdown; it does not close the parent registry or OpenTelemetry SDK. The standalone runner uses the adapter automatically and enables OTLP tracing when an OTLP endpoint is configured. Parent JVM binders remain parent-owned.

The metric catalog is `catalog.json`. New uniform RPC `error` outcomes represent thrown failures; returned protobuf rejection codes retain their legacy counters and normalize to rejected/conflict/redirect. Cancellation is separate. Histograms use the fixed contract buckets and aggregate across nodes; do not average per-node percentiles. Labels are bounded operations/outcomes/components/backends/platforms plus configured resource/storage names. Never add device, room, session, locker or trace IDs as labels.

## Multiplatform connector

Add `com.latenighthack.lockers:lockers-observability-connector` at the connector's version. JVM, Android, JS and Apple implementations select their platform automatically.

```kotlin
val telemetry = ConnectorTelemetry(
    transport = ClientTelemetryTransport { batch -> parentAuthenticatedUpload(batch) },
    traceBridge = parentTraceBridge,
)
val client = LockersClient.create(/* existing arguments */, telemetry = telemetry)
```

The parent transport must make exactly one attempt, without automatic retries or instrumentation of its own upload. Encode with `TelemetryContract.encode`; the receiving application validates with `TelemetryContract.decode` and calls `monitoring.ingester.accept`. Mount the application-owned route behind the application's authentication policy. The library installs no public endpoint. Fullhouse's `/api/telemetry/lockers` is a reference implementation alongside its unchanged client-span route.

Metrics aggregate every observation without sampling, up to a bounded one million observations per operation/outcome per window, and flush every 30 seconds. Durations above 24 hours are clamped. Diagnostics buffer 256 records and flush up to 32 per second. Each upload is at most 64 KiB, times out after five seconds and is discarded after one attempt. Overflow/upload/oversize losses are reported on a later successful upload; loss during outages or process death cannot be reported immediately. Upload loss counts batches; overflow counts observations/diagnostic records. These are fleet observations from reporting devices, not exact device counts or authoritative request totals.

The trace bridge provides context and completes library spans using the existing parent trace IDs and sampling policy. Its context must contain tracing elements rather than dispatcher/job ownership. Client-owned shared reads inherit this context without inheriting waiter cancellation. Parent transport spans remain the only transport spans. Reconnect spans describe connection lifetime; other operation spans describe their operation. Acknowledgement/open/terminal/conflict events are bounded counters, with instantaneous event observations. No trace context is stored in delivery intents, and a later delivery/receipt is not represented as a causal child of a write.

The parent supplies `ClientSpanExporter` to forward original span IDs/timestamps as INTERNAL OTLP spans, with trusted client service/deployment resources. Server diagnostic events use bounded fields and exclude payloads, credentials and exception messages. An absent client span exporter increments `lockers_connector_diagnostics_dropped_total`.

For trace-to-log links, search the trace ID across services: uploaded client traces have a client service resource, while their operational events are logged by the server. The example data-source configuration matches either the log body or structured trace metadata and allows two minutes around the span for batching delays. Trace IDs remain log fields rather than indexed labels.

The parent owns collector shutdown: call `telemetry.close()` and optionally await `awaitClosed()`. Draining is capped at two seconds; monitoring never delays library operations or adds persisted stores.

## Alerts and validation

Load `alerts/lockers.yml` separately if desired. It configures no notification destinations. Thresholds are starting points; see `RUNBOOKS.md`. `alerts/tests.yml` is a promtool test input, not a rule file.

Generate/check shipped files with `python3 monitoring/tools/generate.py --check`. Build the versioned ZIP with `./gradlew :observability-server:monitoringBundle`. Publishing the Maven artifacts or ZIP externally remains a separate release step.

For a disposable stack, run `docker compose -f monitoring/smoke/compose.yml up -d`, then `python3 monitoring/smoke/verify.py`; stop it with the matching `down` command. Images are pinned. Its synthetic fixtures validate dashboard queries/aggregation and correlation; Kotlin tests separately verify real exposition, library behavior, imported histogram merging and observer failure isolation. The fixture supports healthy, idle, disabled and failing states. Local ports are allocated dynamically and printed by verification.

Fullhouse's default Mimir path scrapes Lockers/runtime meters through its collector and keeps its application metrics on OTLP, avoiding duplicate Lockers series. DNS discovery collects per-replica counters independently; a single load-balanced scrape target would mix counter lifetimes. Docker uses server/A discovery; Railway uses the configured private server domain/AAAA discovery. See [Railway private networking](https://docs.railway.com/networking/private-networking) and the [OpenTelemetry Prometheus receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/prometheusreceiver).

## Runtime operations reporting

The server also emits `fullhouse.rpc.*`, `fullhouse.dispatcher.*`, `fullhouse.database.*`, and `fullhouse.queue.*` series. RPC outcomes reflect generated protocol result symbols, including application failures carried by HTTP 200. Labels contain service/method names, bounded shard groups, pool purposes and fixed queue names; they contain no user or room identities. Existing `lockers.*` series remain available.

Queue depth, recipient counts, oldest age and eligibility are collected in owned background jobs with a four-second deadline, never in the scrape path. Shared database snapshots carry `scope=shared`: use `max` across replicas rather than summing them. Unknown or unavailable ages are NaN; historical rows retain an unknown-age count. Check the collection timestamp and failure counter before treating a cached snapshot as current. Worker enablement, capacity, active work, heartbeat and progress are local to each process. Inbox presence uses a separate bounded JDBC reporting pool so it cannot consume coordination pool slots.

Server schema version 6 adds reporting indexes while retaining historical definitions and stored bytes. Enqueue time and W3C trace context are operational metadata outside signed application envelopes. Background delivery, agent and provider spans use the host SDK supplied by `LockersMonitoring.attach`, or `ServerCore.workTelemetry` for custom hosts; the library does not close that SDK.

Connector hosts may pass `SyncObserver` to `LockersClient.create`. Callbacks must enqueue without blocking. Stages describe hydration, persistence, acceptance, replay, ACKs, duplicate delivery, rebasing, shared reads, mutation waits and interactive/background network waits. Observer failures cannot replace operation results, and operation cancellation remains cancellation. `TypedLockerClient.watchAllVersioned` exposes the existing complete snapshot with locker versions for consumer correlation.
