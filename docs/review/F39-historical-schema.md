# F39 — preserve independently captured storage history

The integrated V2 push sidecars were accidentally inserted into the historical
V3 declaration during cherry-pick. A regression comparing that declaration with
the fourteen independently captured historical store fixtures fails on this
change. Move both sidecars into additionsV4 so adoption creates them through the
configured V3-to-V4 migration. The frozen V1 schemas and fixtures remain intact.
The historical guard, actual SQLite unknown-field adoption/reopen, push claims
and session revocation pass together in /tmp/lockers-F39-historical-green.log;
the failing guard is /tmp/lockers-F39-historical-red.log.
