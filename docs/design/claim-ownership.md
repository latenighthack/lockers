# Claim-Based Room Ownership (replaces ring sharding for multi-node)

Status: PROPOSED — 2026-08-29
Target: `lockers-server` (kitkit). No client/connector changes. No proto changes (reuses `NOT_OWNER` + `ShardRedirect`).

## Motivation

The monolith deployment is correct and is what production runs today. The clustered path (Blueprint V: consistent-hash rings + Postgres advisory locks + `shard_map` control table) is dormant and, on inspection, cannot be safely enabled:

1. **Connection blowup.** One *dedicated, pinned* Postgres connection per held shard lease (`JdbcAdvisoryLockGateway.tryLock`, `cluster/AdvisoryLockCoordinator.kt:107`). Keyspace 0 defaults to 256 shards (`LOCKERS_SHARD_COUNT_DEFAULT`, `LockersConfig.kt:160`) ⇒ **256 pinned connections cluster-wide, regardless of node count or load**, vs Postgres default `max_connections=100`. A 2-node cluster fails at boot. Session-pinned locks also forbid PgBouncer.
2. **Fencing is not real in production.** `ShardLease.fencingToken` is the constant advisory key, not monotonic (`AdvisoryLockCoordinator.kt:31`), and no write path reads it (log-only). `pg_try_advisory_lock` cannot preempt a live holder, yet `OwnerLifecycle` doc/comments claim higher-epoch preemption (`OwnerLifecycle.kt:27-29,102`) — true only of `InMemoryOwnershipCoordinator`, which is what the tests exercise. Green tests over an invariant production doesn't have.
3. **Orphaned shards + dead-end redirects.** Lease loss is detected only lazily (`isValid` on the write path) and re-acquire happens only on a new `ShardMap` emission — a dropped lock with no epoch change is never retried. Worse, a route-local node without a lease returns `Remote(address = route.address ?: "")` (`RingRoomOwnership.kt:34-37`) and local routes have `address = null` (`ShardRouter.kt:119`) ⇒ clients receive `ShardRedirect{ownerAddress=""}` and cannot fail over. Writes wedge.
4. **No control plane.** Nothing writes `shard_map` (reads + DDL only); keymaster has no reshard/topology commands (`IntrospectCommands.kt:15-18` — scaffolds that exit non-zero); LISTEN/NOTIFY is a comment (`PostgresShardMapSource.kt:153-155`). Resharding is manual SQL.
5. **Static membership, no failure detector.** A dead node stays in the ring; its locks release but routing still points at it until an operator intervenes.
6. **Session ring is unfenced** (`RingSessionOwnership` gates on route-local only).

Root cause: a Postgres *session* advisory lock is the wrong lease primitive — it ties connections to shard count, cannot express preemption/fencing, and requires a control plane + failure detector that were never built.

**This design replaces shard-granularity ring ownership with room-granularity claims** on the existing shared Postgres: one TTL-renewed row per active room is both the router and the fence. O(nodes) connections, real monotonic fencing epochs, automatic TTL failover, no `shard_map`, no reshard tool, no static ring. At our cardinality (thousands of ephemeral rooms, not millions) per-room state is trivial, and O(shards)-stateless routing — the ring's one real advantage — is not worth its apparatus.

Why single-writer-per-room matters at all here: rooms run server-side **game agents** (locker write → agent → driver). The locker-version CAS already guarantees data integrity with N writers, but without ownership two nodes can both run the agent for one move (double compute, double side effects e.g. push). Ownership makes the agent single-writer; the CAS remains the data backstop.

## Goals / non-goals

Goals:
- 2..~16 node operation on existing Postgres only; Redis optional later, behind an interface.
- O(nodes) coordination connections; PgBouncer-compatible (no session-pinned state).
- Real fencing: monotonic per-room epoch, consulted on the write path.
- Automatic failover via TTL; no operator action on node death.
- Monolith behavior byte-identical when the feature is off (default off).

Non-goals:
- Splitting one room across nodes (single-writer per room is the point).
- Deleting the ring code (quarantined behind config, removable later).
- Client changes (existing `NOT_OWNER`/`ShardRedirect` handling is reused).

## Design

Two claim-style registries, same pattern (row + TTL + in-memory cache), replacing the two rings:

| Concern | Ring today | Claim design |
|---|---|---|
| Room write coordination | room ring + advisory lock/shard | `room_claim` table, row per active room |
| Fan-out delivery discovery | session ring (`RingSessionGatewayDiscovery`) | `session_gateway` table, row per live session |

Fan-out itself is already per-session directed RPC via `SessionGatewayDiscovery.findServer(sessionId)` (`RoomServiceImpl.kt:377,439,544`) over the existing east-west transport (`PeerConnectionPool`/`HttpSessionGateways`) — reused unchanged. **No pub/sub bus is required at any node count in this design** (no broadcast amplification exists to begin with).

### Schema

```sql
CREATE TABLE IF NOT EXISTS room_claim (
    room_id     BYTEA PRIMARY KEY,
    node_id     TEXT        NOT NULL,
    node_addr   TEXT        NOT NULL,          -- host:port for ShardRedirect; never empty
    epoch       BIGINT      NOT NULL DEFAULT 1, -- monotonic fencing token
    expires_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS room_claim_node ON room_claim (node_id);

CREATE TABLE IF NOT EXISTS session_gateway (
    session_id  BYTEA PRIMARY KEY,
    node_id     TEXT        NOT NULL,
    node_addr   TEXT        NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS session_gateway_node ON session_gateway (node_id);
```

All times are DB-server time (`now()`), so there is a **single clock authority**; node clock skew is irrelevant everywhere except the renew *interval* choice.

### Core SQL (RoomClaimStore, plain JDBC on the ktstore Hikari pool)

ktstore's KV model can't express conditional atomic SQL; this is a small dedicated DAO in `lockers-server` using the same `DataSource`. (Requires ktstore ≥0.0.9, which pools; exposing the `DataSource` off `SqlStoreDelegate` or passing the JDBC url separately is an implementation detail — prefer a `LOCKERS_DB_URL`-constructed pool of its own, max 2, to avoid coupling.)

Claim (insert-or-steal-if-expired-or-renew-own; single round trip, atomic):

```sql
INSERT INTO room_claim (room_id, node_id, node_addr, epoch, expires_at)
VALUES (:room, :me, :addr, 1, now() + :ttl)
ON CONFLICT (room_id) DO UPDATE
   SET node_id   = EXCLUDED.node_id,
       node_addr = EXCLUDED.node_addr,
       epoch     = room_claim.epoch + 1,
       expires_at = EXCLUDED.expires_at
 WHERE room_claim.expires_at < now()      -- steal only if expired
    OR room_claim.node_id = :me           -- or re-claim own (extends, bumps epoch on takeover only — see note)
RETURNING node_id, node_addr, epoch;
```

- Returned row with `node_id = :me` ⇒ we own it at `epoch` (cache it).
- No row returned (conflict + WHERE false) ⇒ someone else validly owns it ⇒ `SELECT node_id, node_addr, epoch FROM room_claim WHERE room_id = :room` and redirect there.
- Note: the `node_id = :me` arm exists so claim is idempotent on the owner; epoch bump on self-reclaim is harmless (monotonicity is all that matters) but MAY be avoided with a `CASE` if churn in the token bothers metrics.

Renew (one statement per node per interval for ALL owned rooms — this is the whole coordination cost):

```sql
UPDATE room_claim SET expires_at = now() + :ttl
 WHERE node_id = :me AND expires_at >= now()
RETURNING room_id;   -- rows NOT returned vs local cache = lost claims → demote locally
```

Release (graceful drain / room idle eviction):

```sql
DELETE FROM room_claim WHERE room_id = :room AND node_id = :me;
```

Lookup (cold cache miss on a non-owner):

```sql
SELECT node_id, node_addr, epoch, expires_at FROM room_claim WHERE room_id = :room;
```

`session_gateway` is the same shape: upsert on WS connect (unconditional — a reconnect legitimately moves the session), batched renew by `node_id`, delete on disconnect, lookup with short-TTL cache (5s) for delivery.

### ClaimRoomOwnership

New `ClaimRoomOwnership : RoomOwnership` (`services/room/v1/RoomOwnership.kt:11-13` interface, unchanged):

```
resolve(keyspace, roomId):
  c = cache[roomId]
  if c != null && c.owner == self && c.freshEnough  -> Local(epoch=c.epoch)
  row = store.claim(roomId, self, advertiseAddr, ttl)     // tries to become owner
  cache[roomId] = row
  return row.node_id == self ? Local : Remote(row.node_addr, row.epoch)
```

- **Keyspace is deliberately ignored for ownership**: a room is claimed whole. The ring sharded `(keyspace, roomId)` so keyspaces of one room could land on different nodes — which defeats single-writer-per-room for the game agent and buys nothing at our scale. `resolve`'s signature keeps `keyspace` for interface compat.
- Owner-side cache entries are trusted for `min(ttl/3, renewInterval)`; non-owner (redirect) entries for ≤2s so takeovers propagate quickly. Cache is bounded (reuse cache4k, already a dep).
- `Remote.address` is always the claimant's `node_addr` from the row — **the dead-end `""` redirect cannot occur by construction.**
- First contact wins: an unclaimed/expired room is claimed by whichever node receives the first write. No placement policy needed; LB spreads sessions, sessions' first writes spread rooms.

### Fencing on the write path (what the ring never did)

`RoomOwner.Local` gains the claim epoch (additive: `data object Local` → `data class Local(val epoch: Long = 0)`; monolith `LocalRoomOwnership` returns epoch 0). On the owner:

1. `redirectIfNotOwner` (`RoomServiceImpl.kt:89-96`) resolves Local(epoch=E) and threads E into the write.
2. Locker save proceeds under the existing version CAS (unchanged; final data backstop).
3. **Demotion:** when a renew round's `RETURNING` set is missing a cached room (or `claim` returns another node), the node drops the cache entry, stops treating itself as owner, and evicts that room's caches (reuse the `evictRoomCaches` hook, `MonolithComponent.kt:70`). In-flight writes racing a steal are caught by the CAS; the *next* write redirects.
4. Steal is only possible after TTL expiry, and expiry only happens if the owner failed to renew for a full TTL — i.e. it is partitioned from Postgres, at which point it also cannot write lockers (same DB). **The store being the same substrate as the lease is what makes lazy fencing sufficient**: an owner that can still write data can still renew; one that can't renew can't write either. A GC-pause straddling expiry is covered by the CAS + demote-on-next-renew.

### Wiring (MonolithComponent / Main)

- `LockersConfig`: add `LOCKERS_ROOM_OWNERSHIP = local | ring | claim` (default `local`), `LOCKERS_CLAIM_TTL_MS` (default 15000), `LOCKERS_CLAIM_RENEW_MS` (default 5000, must be < ttl/2), reuse `LOCKERS_ADVERTISE_ADDR` (required for `claim`; boot-fail if absent), `LOCKERS_NODE_ID` (required; boot-fail if absent). `claim` + no `LOCKERS_DB_URL` ⇒ boot-fail (`LOCKERS_REQUIRE_DB` semantics).
- `MonolithComponent` ownership selection (`MonolithComponent.kt:76-77`) becomes three-way; `claim` mode instantiates `RoomClaimStore`, `ClaimRoomOwnership`, `ClaimRenewalService` (started in `start()`, `releaseAll`+stop in `stop()` — mirror `ownerLifecycle` lifecycle at `MonolithComponent.kt:119-145`).
- `sessionGatewayDiscovery` (`MonolithComponent.kt:52-54`) gains `RegistrySessionGatewayDiscovery(sessionGatewayStore, pool, selfAddr)`: local session ⇒ local server (as `LocalSessionGatewayDiscovery`, `SessionServiceImpl.kt:67-71`); else lookup `session_gateway` ⇒ `HttpSessionGateways` peer RPC; row absent/expired ⇒ null (caller already treats null as offline ⇒ push queue path).
- Session WS connect/disconnect hooks upsert/delete `session_gateway` (gateway server already knows attach/detach).
- **Ring quarantine:** `ring` mode keeps existing `BlueprintV` wiring but boot-logs a deprecation warning + fails fast if `roomShardCount > LOCKERS_RING_MAX_CONNECTIONS` (default 64) — the guard that prevents enabling the 256-connection footgun as-is.
- Push worker (`LOCKERS_PUSH_WORKER_ENABLED`) unchanged: still exactly one drainer node by config.

### Failure matrix

| Event | Behavior |
|---|---|
| Owner crashes | Sessions reconnect via LB to any node; claims expire after ≤TTL (15s); next write to each room claims on the new node; agent resumes from durable store. Session rows expire likewise; delivery to dead rows fails fast → push queue. |
| Node partitioned from Postgres | Cannot renew ⇒ loses claims; also cannot write lockers (same DB) ⇒ no split-brain writes. Demotes on failed renew. `/readyz` already pings DB. |
| Postgres down | Total outage, same as monolith today (store is down). No new failure mode. |
| Two nodes claim same new room concurrently | Single atomic upsert; exactly one row wins; loser gets the winner's row back and redirects. |
| Steal during owner GC pause | Stale owner's next renew returns without that room ⇒ demote; interleaved write caught by locker-version CAS; epoch is monotonic across takeovers. |
| Rolling deploy | Drain: stop accepting, `releaseAll` (DELETE by node_id), close. Rooms re-claim on next write elsewhere. Zero-op. |

Consequences to accept: ≤TTL of write-unavailability per room after an owner crash (writes to that room error/retry until re-claim; 15s default, tunable); one PK lookup per cold room/session miss (amortized by caches); `room_claim` churn ~1 batched UPDATE per node per 5s (trivial).

### Observability

Meters (micrometer, follow `lockers.shard.*` naming): `lockers.claim.rooms.owned` (gauge), `lockers.claim.acquires`, `lockers.claim.steals`, `lockers.claim.lost` (renew shortfall), `lockers.claim.redirects`, `lockers.claim.renew.duration`, `lockers.gateway.registry.misses`. Log acquire/steal/demote at INFO with roomId+epoch+node (mirrors `OwnerLifecycle` logging style).

## Implementation plan (phased, each lands green independently)

1. **P1 — stores + ownership (dark).** `RoomClaimStore`, `SessionGatewayStore` (+DDL in existing create-tables path), `ClaimRoomOwnership`, `ClaimRenewalService`, config plumbing, metrics. Default `local`: zero behavior change. Unit + concurrency tests.
2. **P2 — wiring + east-west.** Three-way ownership select, `RegistrySessionGatewayDiscovery`, WS attach/detach registry hooks, drain on stop, ring boot-guard + deprecation warn. Two-node in-process harness tests (below).
3. **P3 — release + adopt.** kitkit `vNEXT` (0.1.9) → Central; fullhouse bumps pin; staging 2-replica soak; prod stays `local` until wanted. Fullhouse ADR update (deployment-infrastructure + lockers migration docs) at adoption time.

## Test plan

Existing suites must stay green throughout (`server/src/test`: `PersistenceTest`, `RoomOwnershipTest`, `SessionOwnershipTest`, `ShardedFanoutTest`, `PushServiceTest`, …) — default `local` guarantees P1/P2 are additive.

**Unit — claim SQL semantics** (Postgres via testcontainers if available in kitkit CI, else H2 is NOT acceptable — the ON CONFLICT/WHERE semantics are the product; gate these tests on a `LOCKERS_TEST_PG_URL` env like other infra-gated tests):
- fresh claim inserts epoch=1, returns self
- second node claim while valid ⇒ no steal, returns owner row, epoch unchanged
- claim after expiry ⇒ steal succeeds, epoch strictly increases
- self re-claim extends expiry
- renew returns exactly the still-owned set; renew after steal excludes stolen room
- release deletes only own row (release-after-steal is a no-op)
- **concurrency:** 32 coroutines × 2 simulated nodes hammering claim on one expired room ⇒ exactly one winner per epoch, epochs strictly monotonic, no lost updates (assert final epoch == steal count + 1)
- clock authority: claims/renews from a node with skewed local clock behave identically (all SQL uses `now()`)

**Unit — ClaimRoomOwnership:** cache hit returns Local without store call; non-owner cache expiry ≤2s; demote on renew shortfall flips resolve to Remote with populated address; `Remote.address` never empty (property test over store states).

**Unit — RegistrySessionGatewayDiscovery:** local session short-circuits; remote resolves addr; expired row ⇒ null (offline path); cache TTL respected.

**Integration — two-node harness** (in-process: two `MonolithComponent(config=claim)` sharing one Postgres + one `PeerConnectionPool` over loopback HTTP, pattern of existing `ShardedFanoutTest`/`SessionOwnershipTest`):
1. *Redirect correctness:* client A on node1 creates room, client B writes via node2 ⇒ node2 answers `NOT_OWNER` + redirect to node1's addr; retry against node1 succeeds. Assert redirect address non-empty.
2. *Single agent:* register counting agent; two clients on two nodes post interleaved lobby writes ⇒ agent invocations == writes on owner node only; zero on non-owner.
3. *Cross-node fan-out:* subscriber on node2, writer on node1 ⇒ change delivered over registry-discovered gateway; assert `session_gateway` row used (miss meter == expected).
4. *Failover:* kill node1 (cancel scope, no release), fast-forward TTL (test TTL 500ms) ⇒ write via node2 steals with epoch+1; subscriber reconnected to node2 receives subsequent changes; total re-claim latency < 2×TTL.
5. *Graceful drain:* stop node1 ⇒ claims deleted immediately; next write on node2 claims with no TTL wait.
6. *Postgres partition (fencing):* proxy node1's JDBC through a toggleable failing DataSource; break it ⇒ node1 demotes after next renew, its locker write attempt fails (store unreachable), node2 steals; restore ⇒ node1 redirects (does not resurrect ownership).
7. *Monolith regression:* full existing suite under `LOCKERS_ROOM_OWNERSHIP=local` and a smoke pass under `claim` with a single node (claims always local, no redirects ever emitted).

**Fullhouse E2E (post-adoption, staging):** existing `runCoreTest` multi-client ViewModel flows (invite/join/game start) against a 2-replica deployment; verify game completion, chat delivery, push, and `lockers.claim.*` meters in Grafana. Soak ≥24h; alert on `claim.lost > 0` steady-state.

**Load:** 1k active rooms × 2 nodes, 50 writes/s aggregate for 10 min: p99 write latency delta vs monolith < 5ms; `room_claim` table remains ≤ active-room cardinality; renew round < 50ms.

## Optional Redis (deferred — and now smaller than previously discussed)

Because fan-out is per-session directed (no broadcast), Redis is NOT needed for delivery at any planned scale. Remaining optional uses, all drop-in behind existing seams:
- `RoomClaimStore`/`SessionGatewayStore` on Redis (`SET NX PX` + Lua renew) if claim/registry read QPS ever pressures Postgres — same interface, one impl file each.
- Pub/sub invalidation of non-owner caches to shave the ≤2s redirect staleness — cosmetic.

Decision rule: adopt only on measured Postgres pressure from `lockers.claim.*`/registry meters, not preemptively. Postgres-only is the plan of record through ~16 nodes.

## Alternatives considered

- **Fix the ring in place** (lease table per shard, keep rings): solves connections + fencing but retains shard_map control plane, reshard tooling, static membership, fixed-S repartition problem, and (keyspace,room)-split ownership that breaks single-agent-per-room. More machinery for the same outcome at our cardinality; rejected. The `:sharding-core` module stays for reference/reuse.
- **Cloudflare Durable Objects:** rejected — off-stack.
- **Postgres LISTEN/NOTIFY bus:** unnecessary given directed per-session fan-out; NOTIFY adds a dedicated connection per node and N× filtering for no gain here.
