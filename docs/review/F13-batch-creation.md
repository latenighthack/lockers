# F13: version-zero bulk authority discovery for fresh lockers

Independent review found that `LockerClient.updateLockers` without an initial key requires bulk authority discovery, but bulk lookup returned UNKNOWN_ERROR for missing lockers. Single lookup already returned the requested identity/version0/current lock authority. The actual HTTP red tests prove both the inconsistent bulk response and the ordinary high-level atomic batch timing out before submission.

Bulk lookup now returns the same OK/version0/authority shape for absent identities as single lookup. The ID remains the requested ID and the inherited authority remains current; a missing locker is not a live empty body. Stored tombstones and corrupted bodies keep their established distinct paths. No wire fields or authorization checks changed.

`ReviewBatchCreationTests.bulkAbsentLookupMatchesSingleVersionZeroAuthority` and `.ordinaryAtomicBatchCreatesFreshUnclaimedLockers` pass, along with the signed batch/lost-single-reply integration regression (3 tests). The created rows are version1 with their submitted payloads. Red/green logs and actual baseline test XML are attached. Final integrated/platform/consumer gates remain separate.
