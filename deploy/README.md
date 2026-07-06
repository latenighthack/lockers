# Deployment blueprints

`lockers` is one binary with three modes (sharding is opt-in):

1. **Monolith** — a single process owns every shard of both rings; all routing is in-process. This
   is the default with **zero** sharding config and is byte-for-byte the pre-sharding behavior. Just
   run the image with `LOCKERS_DB_URL` set (or unset for an ephemeral dev store).
2. **Homogeneous cluster** — N identical processes over one Postgres; the room and session rings span
   the same N nodes. This is what the blueprints below deploy.
3. **Tiered cluster** — a separate pool of session/gateway (WebSocket) servers with its own
   membership. Same interfaces, distinct `ShardRouter.create(...)` wiring (not templated here).

The **only** technology-coupled layer is the set of `:sharding-core` SPI implementations. Everything
above them (partition function, ring, `ShardMap`, `ShardRouter`, ownership gating) is
technology-agnostic and unit-tested with zero infrastructure.

## SPI → technology mapping (the whole coupling surface)

Each `com.latenighthack.lockers.sharding.spi` interface has exactly one blueprint implementation,
all in `server/src/main/kotlin/com/latenighthack/lockers/server/cluster/`:

| `:sharding-core` SPI | VM/bare-metal + Postgres impl | Where |
| --- | --- | --- |
| `Membership` | `StaticMembership` — fixed roster from `LOCKERS_PEERS` | `StaticMembership.kt` |
| `PeerLocator` | `StaticPeerLocator` — `NodeId → PeerAddress` from the same roster | `StaticMembership.kt` |
| `ShardMapSource` | `PostgresShardMapSource` — polls the `shard_map` table (LISTEN/NOTIFY upgrade documented) | `PostgresShardMapSource.kt` |
| `OwnershipCoordinator` / `ShardLease` | `AdvisoryLockCoordinator` — `pg_try_advisory_lock(key)`; lost DB session ⇒ `isValid=false` | `AdvisoryLockCoordinator.kt` |
| east-west transport (`RemoteGateway`) | `PeerConnectionPool` + `HttpSessionGateways` / `HttpPushGateways` (ktbuf HTTP/WS) | `RemoteGateway.kt` |
| north-south routing | smart client (connector) + server reject-with-redirect (`RingRoomOwnership`) | `RingRoomOwnership.kt` |

`BlueprintV.wire(...)` (`BlueprintV.kt`) assembles these into a `ClusterContext`, which
`MonolithComponent` consumes to swap the in-process `Local*Discovery` for `Ring*Discovery`. When no
peers are configured, `BlueprintV.wire` returns `null` and the monolith path is taken unchanged.

A **Kubernetes** alternative implements the *same* SPI with different tech (a ConfigMap/CRD
`ShardMapSource`, a k8s `Lease`/etcd coordinator) behind an analogous factory — see `k8s/`.

## Environment variables (cluster mode)

Cluster mode turns on only when **both** `LOCKERS_PEERS` and `LOCKERS_NODE_ID` are set (a single
stray var can never silently promote a monolith to a mis-configured cluster).

| Var | Meaning | Default |
| --- | --- | --- |
| `LOCKERS_NODE_ID` | this node's logical ring identity (must appear in `LOCKERS_PEERS`) | unset ⇒ monolith |
| `LOCKERS_ADVERTISE_ADDR` | this node's peer-reachable `host:port` (must agree with its peer entry) | unset |
| `LOCKERS_PEERS` | full roster: `id=host:port,...` or bare `host:port,...` (host = id) | unset ⇒ monolith |
| `LOCKERS_DB_URL` | Postgres JDBC URL — reused for storage, `shard_map`, and advisory locks | unset ⇒ in-memory (dev) |
| `LOCKERS_REQUIRE_DB` | fail fast at boot if `LOCKERS_DB_URL` is unset | `false` |
| `LOCKERS_SHARD_COUNT_DEFAULT` | room-ring global shard count | `256` |
| `LOCKERS_KEYSPACE_SHARD_COUNTS` | per-keyspace overrides, e.g. `1=512,30=128` | none |
| `LOCKERS_RING_VNODES` | consistent-hash virtual nodes per node | `128` |
| `LOCKERS_SESSION_SHARD_COUNT` | session/gateway-ring shard count | `256` |

Ports: `8080` public (client/peer + `/healthz` `/readyz` `/metrics`), `8081` internal admin
(bind cluster-internal; set `LOCKERS_ADMIN_TOKEN`).

## The `shard_map` control table

The epoch-stamped source of truth for shard→node assignment, read by every node
(`PostgresShardMapSource`) and written by a control plane / reshard tool. Create it once on the
shared Postgres (`JdbcShardMapGateway.TABLE_DDL`):

```sql
CREATE TABLE IF NOT EXISTS shard_map (
    epoch    BIGINT  NOT NULL,
    keyspace BIGINT  NOT NULL,
    shard    INTEGER NOT NULL,
    node_id  TEXT    NOT NULL,
    PRIMARY KEY (epoch, keyspace, shard)
);
```

Every node reads the **highest** epoch present; any `(keyspace, shard)` not listed at that epoch
falls back to the consistent-hash ring computed from `LOCKERS_PEERS`. So an **empty** table is
valid — nodes route purely off static membership until a control plane starts writing rows. The
session ring is keyed under the reserved keyspace `-1`.

LISTEN/NOTIFY (latency upgrade, optional): a control plane can `NOTIFY lockers_shard_map` on epoch
change; a node holding `LISTEN lockers_shard_map` refreshes immediately instead of on the poll timer.
Polling is the correctness floor.

Advisory locks share the same database — no extra store. `AdvisoryLockCoordinator` derives one
`bigint` key per `(keyspace, shard)` and holds `pg_try_advisory_lock` on a dedicated connection; if
that connection dies (partition, restart) Postgres releases the lock and the lease reports
`isValid=false`, so a partitioned owner stops coordinating without any heartbeat protocol.

## Building the image

```
docker build -t lockers:local .
```

Multi-stage: `gradle:8-jdk17` runs `:server:run:installDist`; the runtime is
`eclipse-temurin:17-jre-jammy`, non-root `USER 10001`, container-aware heap
(`-XX:MaxRAMPercentage=75 -XX:+UseContainerSupport`), `EXPOSE 8080 8081`, entrypoint `/app/bin/run`.

See `railway/` and `k8s/` for the two concrete topologies.
