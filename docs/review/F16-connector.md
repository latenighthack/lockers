# F16 — authoritative cache repair and complete snapshots

Reproduction: `ReviewSnapshotRepairTests` reproduced stale cache surviving a successful missing GetLocker and a paged snapshot exposing only page one. Both assertions failed before the fix (`/tmp/connector-F16-repro.log`).

Fix: single and bulk reads treat successful authoritative absence as cache repair. They compare the captured pre-request record before removing it, preserving any newer acceptance during the request. Physical removal journals a deletion at the observed version without inventing a future version. Versioned server tombstones use the normal monotonic acceptance path. Missing records in legacy whole-room snapshots are individually confirmed, and non-OK reads never imply deletion.

When snapshot paging is advertised, hydration requests 64 records, signs each subscription/snapshot page, verifies one stable watermark and non-repeating tokens, drains all pages, then accepts the whole snapshot in one database transaction and acceptance boundary. Cache watchers cannot observe partially accepted pages. Legacy whole-response behavior remains supported.

Verification: four tests pass for absence repair, complete page draining, concurrent newer cache preservation, and inconsistent watermark rejection. Existing connector and signed session proof tests also passed with the new hydration path. Log: `/tmp/connector-F16-fix.log`. Server paging implementation and platform verification are owned by the integration/build workers.
