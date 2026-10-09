# F02 conflict reply envelope validation

The read endpoints rejected ambiguous stored Locker envelopes, but stale write/delete conflicts still decoded and returned the same ambiguous bytes. The isolated baseline accepted the conflict without a DATA_LOSS error.

Conflict replies now validate both structural wire shape and supported envelope before exposing an existing body. Invalid stored bytes fail DATA_LOSS with the stored version, allowing owner repair through the existing read metadata contract. The bytes are neither rewritten nor deleted, and no representation is chosen from an ambiguous body.

Validation: ConflictEnvelopeBoundaryTest covers stale source writes and deletes against contradictory stored envelopes, confirms version7 in the repair error and exact unchanged stored bytes. Existing persisted-envelope read/agent regressions also pass. Baseline evidence is included in F38-response-red.log; final targeted green log is included here.
