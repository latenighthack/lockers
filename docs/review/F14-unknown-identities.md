# F14: unknown identity fields cannot create aliases

Two regressions failed: unknown fields on a locker ID bypassed duplicate batch admission, and equivalent raw identities failed bulk-read/prefetch lookup. The latter also prevented subsequent versioned updates from finding the stored row.

Server batch admission, prefetch and bulk reads now use raw locker identity plus numeric keyspace to construct logical keys. Unknown message fields remain compatible on the wire; stored body bytes are unchanged. This also removes dependence on the generator's previously inconsistent unknown-field equality.

Evidence: `/tmp/lockers-F14-unknown-alias-red.log` (2 tests, 2 failed), `/tmp/lockers-F14-unknown-alias-green.log` (alias and canonical batch tests pass).
