# F35 — typed durable session broadcasts

Reproduction: an accepted lockerless Event with a notification had no typed connector path; the stubbed broadcast flow produced no value and the new regression failed (`/tmp/connector-F35-repro.log`). Existing locker notification decoding requires a locker id.

Fix: `IncomingBroadcast` carries a durable cursor, room/event identity and actual wire push metadata. `BroadcastCodecs` has its own context without inventing a locker/keyspace. `LockerClient` and `LockersClient` expose `broadcasts` and `broadcastsAfter(cursor)`. Decode runs in the consumer after atomic raw Event acceptance/ACK, so a slow, failed or absent decoder cannot prevent transport acceptance. The raw journal supports replacement and codec recovery; applications persist cursors only after handling events.

Verification: a bodyless broadcast accepted without any collector is replayed through a replacement client and its context-bound codec sees canonical empty title/body-only metadata. Log: `/tmp/connector-F35-fix.log`.
