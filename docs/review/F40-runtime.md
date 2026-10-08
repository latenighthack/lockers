# F40 — shard-map startup recovery and readiness

Repro: PostgresShardMapSourceTest injects an initial database failure, then a valid epoch-7 table. The baseline polling job died before entering its guarded loop, so the map never loaded; regression failed.

Fix: initial and later reads share one cancellation-aware retry loop. A StateFlow health snapshot records whether the map has actually loaded and its last successful refresh. Blueprint readiness waits for a loaded map and rejects stale refreshes after three poll intervals, in addition to checking DB reachability. Duplicate bind calls fail explicitly.

Validation: PostgresShardMapSourceTest red then green (6 tests), including initial failure, subsequent recovery, pre-load readiness rejection and refresh staleness.
