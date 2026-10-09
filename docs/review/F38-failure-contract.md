# F38 permanent versus recoverable admission failures

The previous RESOURCE_EXHAUSTED mapping treated deterministic oversize and permanent identity history limits like temporary queues. ktbuf marks that code retriable, so unchanged requests could retry indefinitely.

The final contract is:

| Failure | Result |
| --- | --- |
| Deterministic input/output bytes, count, depth or work | OUT_OF_RANGE (malformed shape: INVALID_ARGUMENT) |
| Permanent locker/lock/room-claim identity histories | FAILED_PRECONDITION |
| Reserved session identity namespace full | Open.NAMESPACE_EXHAUSTED (12), terminal |
| Active sessions, ACK/expiry-recoverable queues, leases, proof quota, refill budget | RESOURCE_EXHAUSTED; handshake code9 |
| Handshake work cannot ever fit configured burst | Open.INVALID_REQUEST (11), terminal |

SessionAdmission distinguishes active capacity from permanent active+revoked namespace reservations. Destroying an active session can free active capacity but never its reserved identity slot. Byte/depth/work caps are terminal even when configured small; temporary verification token depletion still retries after refill.

Validation: SessionAdmissionFailureContractTest creates at active capacity, observes temporary9, destroys/recreates until permanent reservation capacity, then observes terminal12. CPU, response/legacy snapshot, namespace and actual two-handle PostgreSQL regressions verify their respective terminal/recoverable codes. These final mappings supersede earlier intermediate RESOURCE_EXHAUSTED evidence in the linked issue reports.
