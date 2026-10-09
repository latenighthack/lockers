# F38: bound raw storage fetches before replay/snapshot aggregation

Sixty-four-row storage pages could retain512MiB of valid near8MiB bodies before the snapshot capture ceiling or inbox frame splitter ran. Historical full-event sidecars could double that replay footprint. Finite emitted frames alone did not bound eager storage retrieval.

Built-in locker snapshot iteration and inbox replay now fetch at most four rows per indexed call. Inbox replay accumulates across these private fetches to preserve external requested page counts and byte ceilings, releases database ownership before downstream emission, and retains its finite high-water cursor/order. Existing150-event replay still emits64,64,22; oversized individual historical events fail permanently OUT_OF_RANGE.

A guarded real in-memory indexed delegate rejects any locker/inbox fetch capable of retaining more than32MiB of valid bodies. The baseline fails; corrected snapshot iteration retrieves every150 unique lockers, and inbox replay returns every150 ordered events with the same external page contract. Existing ACK-during-replay, encoded frame splitting and snapshot paging/binding/capacity/reopen tests pass.

Logs: evidence/F38-storage-fetch-red.log and evidence/F38-storage-fetch-green.log. No protocol, historical codec or dependency change.
