# Railway blueprint (a concrete instance of VM/bare-metal + Postgres)

Railway specialization of the Postgres blueprint:

- **Managed Postgres** backs `LOCKERS_DB_URL` **and** the `shard_map` / advisory-lock coordination
  (one database, no extra coordination store).
- **Private networking** (`*.railway.internal`) carries east-west peer RPC.
- **Env vars** carry all config (12-factor).

## The addressability constraint (why one service per shard-owner)

Railway load-balances **replicas of a single service** behind **one** internal DNS name, so an
individual replica is **not** directly addressable. The ring needs to reach a *specific* owner node,
so replicas-behind-one-name do not work for the room/session rings.

**Deploy each shard-owner as its own Railway service** — `lockers-0`, `lockers-1`, … — each with a
distinct `*.railway.internal` host that is its `LOCKERS_ADVERTISE_ADDR`. The services share one
Postgres and form the ring. This mirrors Kubernetes StatefulSet ordinals without k8s. Keep each
service at **replica count 1** (scaling = adding another `lockers-N` service, not raising replicas).

## Per-service configuration

Every `lockers-N` service:

1. Deploys this repo's `Dockerfile` (Railway autodetects it; `service.railway.json` pins the
   builder and the readiness healthcheck).
2. References the shared Postgres plugin's `DATABASE_URL` as `LOCKERS_DB_URL`.
3. Sets its own identity and the **same** full roster in `LOCKERS_PEERS`.

`shared.env` holds the values identical across services; `lockers-0.env` / `lockers-1.env` hold the
per-service identity. Set them as Railway service variables (do **not** commit real secrets — these
files are templates; `DATABASE_URL` is injected by the Postgres plugin reference).

Example for a 2-node cluster (internal hostnames are `<service>.railway.internal`, default port 8080):

```
# shared across every lockers-N service
LOCKERS_PEERS=lockers-0=lockers-0.railway.internal:8080,lockers-1=lockers-1.railway.internal:8080
LOCKERS_REQUIRE_DB=true
LOCKERS_SHARD_COUNT_DEFAULT=256
LOCKERS_SESSION_SHARD_COUNT=256
LOCKERS_ADMIN_TOKEN=<generate-a-strong-token>

# lockers-0 only
LOCKERS_NODE_ID=lockers-0
LOCKERS_ADVERTISE_ADDR=lockers-0.railway.internal:8080

# lockers-1 only
LOCKERS_NODE_ID=lockers-1
LOCKERS_ADVERTISE_ADDR=lockers-1.railway.internal:8080
```

`LOCKERS_DB_URL` is set on each service as a reference to the Postgres plugin's `DATABASE_URL`
(Railway: `${{Postgres.DATABASE_URL}}`).

## One-time database setup

Create the `shard_map` table on the shared Postgres (see `../README.md` for the DDL). An empty table
is valid — nodes route off static membership (the `LOCKERS_PEERS` ring) until a control plane writes
rows, so the cluster is functional immediately after the table exists.

## Public ingress

Expose **one** service (or a thin router) publicly for clients; the smart client then follows
ownership redirects to the right `lockers-N` over the same public edge. The admin port (8081) stays
on the private network only — reach it with `keymaster` from within the Railway project, seeding all
nodes:

```
keymaster --node lockers-0.railway.internal:8081 --node lockers-1.railway.internal:8081 push stats
```

## Smoke test

Build the image, deploy `lockers-0` + `lockers-1` on one managed Postgres, connect a client, write on
the non-owning node → observe live delivery; stop one service → observe redirect + continued
delivery; `curl http://<service>.railway.internal:8080/readyz` reflects ring + DB state.
