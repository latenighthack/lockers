# F31 — whole-room ring authority

Repro: WholeRoomOwnershipTest acquires the production default authority lease for keyspace 0, then resolves locker keyspaces 1,30,31,999 in the same room. Baseline returned a self/remote redirect because those scopes had no lease.

Fix: ring ownership resolves one keyspace-0 authority shard for every locker scope in a room. Room locks, child locks, writes and cross-keyspace trusted agent outputs therefore share the same owner. ClusterContext rejects lifecycle configurations that shard room authority independently across keyspaces; Blueprint rejects nonzero per-keyspace room count overrides and tells operators to configure authority count under keyspace 0. Session sharding remains independent.

Migration: ring clients should route rooms under authority keyspace 0; locker keyspace values remain opaque content scopes. LOCKERS_KEYSPACE_SHARD_COUNTS room override now uses `0=COUNT`, or set LOCKERS_SHARD_COUNT_DEFAULT. Claim mode already owns whole rooms and requires no routing migration.

Validation: WholeRoomOwnershipTest red then green; RoomOwnershipTest, OwnerLifecycleTest and ShardCountReshardTest passed together (11 tests). Ring lifecycle fixtures now model the whole-room authority dimension. Commit-time fencing remains F08's independent data-store guard.
