# F34: consistent mutable-array isolation in the in-memory inbox

The configured ktstore in-memory delegate stores typed object values, whereas SQL stores encoded bytes. Returning an accepted inbox row or a legacy row lookup therefore exposed stored mutable payload and identity arrays to the caller. A baseline test mutates the returned payload to9 and observes9 in durable state instead of the original7.

The built-in inbox now detaches accepted row IDs/payloads, lookup results and legacy replay payloads. Full Events returned by admission are detached once per shared fanout object. The private persisted metadata remains unchanged, and receipt deduplication still compares the original encoded Event digest. No upstream dependency or historical format changed.

The red regression fails on b2f0890; the new regression and SQLite receipt persistence test pass with the final paired transport/storage pin. Logs: evidence/F34-inbox-isolation-red.log and evidence/F34-inbox-isolation-green.log.
