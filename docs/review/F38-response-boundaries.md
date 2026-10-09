# F38 complete response and identity admission boundaries

Red repro:12stored1MiB lockers produced bulk reads and stale-write conflict replies above the8MiB envelope. Room claim history exhaustion escaped as an untyped IllegalStateException. Unknown protobuf fields on the same raw RoomId bypassed room read/write rate buckets; concurrent first writes could create independent buckets.

Fix: bound complete read, lock, delete, successful batch and conflict response bytes. Oversized responses fail RESOURCE_EXHAUSTED without partial data; rejected conflict batches leave content/outbox unchanged. Snapshot paging remains complete within envelope chunks. RoomClaimCapacityExceeded maps precisely to RESOURCE_EXHAUSTED during routing admission and mutation fencing. Room rate caches use copied raw identity on both lookup and insertion, and write bucket initialization is atomic.

Validation: ResponseBoundaryTest covers bulk/legacy/conflict envelope exhaustion, no mutation/outbox on rejection, complete paged recovery and typed claim exhaustion before persistence. RoomIdentityAdmissionTest covers unknown-field aliases and simultaneous bucket creation. Each original defect reproduced on the isolated baseline; final targeted suite, snapshot and invalid-data repair tests pass.
