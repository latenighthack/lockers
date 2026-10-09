# F38 bounded finite inbox replay

The previous watch open materialized every pending event into one response. A 12-event inbox carrying 1 MiB each exceeds the transport envelope; a large count likewise yields an unbounded open frame. Recovery also materialized the entire inbox and retained a database dispatcher while building the response.

The current inbox declaration adds a sortable binary primary key (session prefix, room sequence, event identity), retaining the frozen V1 payload/declaration. Version 3→4 rebuilds keys from exact original bytes, preserving unknown protobuf fields. Replay freezes the highest matching key, reads at most 64 records at a time, splits chunks below the 8 MiB envelope, and releases the database owner before each Flow emission. Initial watch uses Open for the first chunk and Events for remaining chunks; catch-up uses the same finite replay. The live subscription precedes the snapshot and bounded identity tracking avoids repeating queued events.

Validation: InboxReplayPagingTest checks 150 records in three complete ordered pages, distinct-session exclusion, ACK during emission, late insertion beyond the frozen upper bound, and 12 MiB split into two frames. StorageAdoptionTest migrates every historical SQLite table and verifies exact bytes/unknown fields/reopen. Receipt, gateway admission, and metadata stream regressions pass together.
