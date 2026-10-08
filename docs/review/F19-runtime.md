# F19 — durable push discovery

Repro: PushServiceTest starts a worker against an empty queue, then enqueues through an independent API-only service instance sharing storage. Baseline timed out because only local SharedFlow emissions discovered new work.

Fix: worker scans authoritative durable state periodically and retries scan failures. A conflated channel is only a latency hint; no hint race can lose persisted work. Local in-flight identity guard and permits acquired before launch keep periodic scans/drains from duplicating local work or creating an unbounded backlog of suspended coroutines. F20 adds distributed durable leases and persisted retry scheduling.

Validation: new API-only replica repro red then green; all PushServiceTest cases pass (17 tests), including bounded provider concurrency, retry, dead-letter and admin behavior.
