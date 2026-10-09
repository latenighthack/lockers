# F38: stop reading oversized bulk replies incrementally

The complete-response output ceiling alone did not bound eager row retrieval: GetLockers fetched and decoded all64 requested bodies before validating the reply. Valid near8MiB bodies could transiently retain about1GiB even though the reply was rejected.

The service now retrieves four identities at a time and appends each result to one bounded8MiB validation writer. As soon as the complete reply cannot fit, it fails OUT_OF_RANGE before retrieving subsequent chunks. It returns the complete ordered response or fails; no partial response or silent omission is introduced. Storage keys use raw locker identity plus normalized keyspace.

The baseline regression uses a guarded store that fails if identities beyond8 are fetched; the old64-ID lookup triggers that guard. The corrected service fetches exactly two four-ID chunks and rejects while encoding the eighth1MiB result. Existing20MiB response/conflict/complete paging and invalid-envelope read tests remain green on the final paired dependency pin.

Logs: evidence/F38-bulk-memory-red.log and evidence/F38-bulk-memory-green.log.
