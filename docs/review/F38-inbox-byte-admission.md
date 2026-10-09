# F38: durable inbox bytes and bounded fanout

Reproduction on b2f0890: a two-group request with two 1MiB Events and 40 recipients bypassed any aggregate expansion check. The new GatewayAdmissionTest expected OUT_OF_RANGE, but baseline processed both groups. Count quotas also allowed payload bytes to grow to many terabytes despite finite row counts.

The receiver now shares the sender's conservative 64MiB expansion formula, checking the entire PostEvents request before processing any group and checking direct inbox batches before row cloning. Each shared Event is frozen, encoded and hashed once rather than once per SID. Raw SID aliases fan out once.

A transactionally maintained additive V2 ledger caps the exact encoded legacy inbox plus full Event sidecar bytes: 1GiB globally and 64MiB per session by default, configurable with positive Long environment values. ACK and session erasure release charges atomically; delivery receipts retain their 31-day identities. Complete historical rows are charged through four-row indexed batches with a durable primary cursor, releasing ownership and yielding between batches. ACK may run during backfill without double charging or underflow. Existing historical overages reject new admission while permitting cleanup. One delivery that cannot fit its configured cap fails permanently OUT_OF_RANGE; occupied capacity is retryable RESOURCE_EXHAUSTED. Ledger records retain SID identity history bounded by the reserved session namespace.

Validation: baseline aggregate test failed as expected. New tests verify exact bytes including unknown fields and signed varints, quota rollback, ACK/destroy/reopen, interrupted legacy backfill and concurrent ACK, 1MiB fanout expansion rejection before writes, canonical SID deduplication, and two independent real PostgreSQL handles racing a three-event byte budget. Existing metadata, receipts, 1,030-event ACK regression and frozen historical migration tests remain green. PostgreSQL tests ran against a private distinct schema without skips.

Logs: evidence/F38-inbox-byte-red.log and evidence/F38-inbox-byte-green.log. No generated bindings or frozen historical declarations were edited.
