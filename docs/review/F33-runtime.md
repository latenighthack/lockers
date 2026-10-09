# F33 — Bounded durable work and explicit retention

Outbox reproductions failed for sequence exhaustion (wrapped Long.MAX_VALUE and committed source state) and a delegate rejecting full-table outbox reads during claim discovery.

Outbox fix: additive per-room head and ordered entry stores preserve frozen V1 records. Discovery queries bounded room heads and at most64 ordered events, without whole-table materialization/grouping or a global claim scan. Historical rows adopt in bounded128-record transactions before claims, preserving legacy room prefixes. Source callback, sequencing, admission, receipts and intents commit together; source/settle paths use Room→OutboxQuota nested physical locks. Counts enforce4096 pending/parked records per room and1,000,000 retained records per queue namespace. Source events generated inside a mutation callback retain existing semantics. Sequence and attempt counters cannot wrap.

Automatic delivery retries expire after30days into durable operator-visible parked records. Parking advances the active room prefix; records never silently age-delete. Trusted reconciliation marks a parked record resolved; reissuing requires a fresh event identity and sequence. Resolved audit records retain31days; accepted active metadata is removed alongside its canonical intent because receiver receipt stores own the replay horizon. Receiver inbox/push completion records retain31days, exceeding automatic upstream retry. Old adoption records conservatively start their deadline at adoption, because V1 lacks creation time.

Validation:12 discovered targeted tests passed, including two actual PostgreSQL tests with distinct database handles and unique schemas (zero skipped). The new PG quota test gates the first source mutation and proves a different room's transaction cannot pass the nested global lock; rejection rolls back its sequence. Additional tests cover260+ bounded discovery records, expired parking with a newer prefix, durable parked payload/recipients, global capacity rollback, lease fencing, replay acceptance and recipient progress. Test PG handles now close before schema cleanup. Push/admin bounded scans and completion pruning continue in the next scoped part of F33; inbox receipt retention is coordinated with the build worker.


### Push retention and administrative discovery

A store delegate rejecting whole-table `getAll("push")`/`getAll("push_deadletter")` reproduced the statistics failure (`/tmp/runtime-F33-push-red.log`). An APNs-only replica consumed queued FCM work before the FCM replica started (`/tmp/runtime-F33-provider-red.log`). Both now pass.

Push administration uses indexed authoritative counts and pages capped at256, with bounded pages for bulk operator operations. Shared depth gauges refresh from those counts. Configured replicas claim only their provider lanes; unknown backends park with their payload. Cooperative provider calls have a30s deadline and Firebase exposes explicit10s connect/30s read/write bounds. Retry attempt overflow cannot wrap.

Additive retention and dead-letter indexes preserve all existing schema declarations. Pushes stop automatic delivery after30days and park durably. Completion identities retain31days. Pending/parked work never silently ages out; retained queue and dead-letter namespaces have finite1M limits. Admin retry after expiry requires a fresh identity. Retry/purge recheck and update canonical dead letters and queue metadata atomically; purge resolves retained parked metadata to completion. Session destruction also removes indexed canonical dead letters. Historical adoption completes before counts/session cleanup and compares canonical snapshots.

Verification:28focused tests, zero failures/skips, including two independent real PostgreSQL handles racing retained capacity and fenced completion (`PushQuotaPgTest`), provider deadline/budget, expiry/dedup retention, capped list and cross-replica backend preservation. Runnable host compilation passes. Manifest `dependencies-final-upstream.json`; log `/tmp/runtime-F33-push-green.log`. Inbox receipt work is separately owned by the paging/build worker; its31day receiver horizon pairs with automatic outbox retry ending30days.


### Complete delivery frames and claimed byte bounds

Two red regressions showed an accepted near-limit event repeatedly failing gateway admission after recipient IDs enlarged its request, and a64-item claim loading more than16MiB (`DeliveryFrameBoundsTest`, `/tmp/runtime-F33-frame-red.log`). Sender routing now splits every intent's recipient group by both1024-recipient count and complete protobuf frame bytes, before event-group batching. Acknowledgement removes only each accepted subset; a failure in the second frame preserves remaining recipients for retry. Claimed canonical intents stay within16MiB in addition to room/item limits; oversized historical intents park with their original payload for reconciliation.

The API common `InboxAdmission` contract also caps conservative durable expansion at64MiB. A1MiB event fanout to1024 recipients reproduced a small-wire/large-storage admission failure (`/tmp/runtime-F33-expansion-red.log`). Sender chunks and complete PostEvents batches now bound the sum of per-recipient expansion, matching the receiver before it clones or encodes paired rows.

Verification:14focused tests, zero failures/skips, including two actual PostgreSQL tests, sender subset acceptance/failure/retry, large event/request bounds, ordered prefix byte limits, existing outbox rollback/quota/lease/dedup tests. Runnable host compiles; `/tmp/runtime-F33-frame-green.log` with frozen `dependencies-final-upstream.json`.
