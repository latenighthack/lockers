# F10: immutable delete receipts

Three regression tests failed: replaying a lost delete reply after recreation returned a conflict, a reused request identity was accepted with different contents, and deleting an existing tombstone advanced the version and delivery watermark again.

Delete requests now optionally carry an immutable 16–64 byte identity. The source mutation, common write receipt, and outbox intent commit in one room-fenced storage transaction. Exact retries return the original version and lock state; different packets under the same identity are rejected. A receipt replay does not delete recreated content. Existing tombstones with the expected version are authenticated no-ops. Empty identity remains compatible with legacy callers.

Retained delete outcomes use the common receipt format and include exact source locker identity/version. The SDK treats a delete CAS conflict as an explicit decision for its caller; it never retries by rebasing onto newer content.

Evidence: `/tmp/lockers-F10-delete-red.log` (3 tests, 3 failed), `/tmp/lockers-F10-delete-green.log` (delete and historical outcome tests pass).
