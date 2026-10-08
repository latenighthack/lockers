# F24 — durable acceptance independent from application consumers

Reproduction: a blocked change collector exhausted SharedFlow's buffer and timed out 70 unrelated room commits (`ReviewConnectorTests.slow change subscriber cannot block unrelated room commits`, `/tmp/connector-F24-repro.log`).

Fix: accepted locker changes and raw session events live in an additive indexed connector journal. Reads use 64-row pages and persisted application cursor APIs (`lockerChangesAfter`, `eventsAfter`). Application Flow collectors and notification codecs execute outside cache acceptance/ACK persistence. Cache changes and their journal entries commit together; session event cache acceptance, raw event journal and transport ACK intent share a transaction with consistent client mutex → database order. Snapshot wakeups are conflated hints that reload cache state. Envelope decoding uses the common supported-envelope validator; malformed incoming bodies cannot be accepted or ACKed.

Live-only `changes`/`events` remain available. Consumers needing recovery save a cursor after handling its item and resume explicitly. Notification delivery reads accepted raw events so absent/slow collectors do not erase its source.

Verification: all eight review tests pass, including 70 unrelated commits with a blocked collector, rollback after cache save before ACK, and 70 no-collector events replayed after store-wrapper replacement. `/tmp/connector-F24-fix.log`. Journal retention and schema migration are final F33/storage gates.
