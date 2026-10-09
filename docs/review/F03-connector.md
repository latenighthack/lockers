# F03 — negotiate epoch-bound authority signatures

Reproduction: the client emits signatureVersion 0 and grant targetVersion 0 against a server advertising authorityV2 with known scope history (`ReviewAuthorityV2Tests`, `/tmp/connector-F03-repro.log`; both regressions failed).

Fix: capable-server writes/deletes/ratchets bind authoritative lock incarnation and final notification/shared-key metadata using the V2 domains; grants discover scope and parent histories and bind both. Unlock signs its explicit expected authority version. Atomic initial-lock batches bind the discovered scope history and sign writes against its new incarnation; ordinary batches discover effective authority versions in a bounded bulk read. Old-server V1 signing bytes are preserved behind capability negotiation. Authority discovery has room routing metadata.

An already submitted request is reused before transformation, notification encoding or signing; ambiguous retries no longer recompute discarded work. Notification builders see a provisional payload envelope before the final V2 signature binds the encoded notification. Committed batch source state is accepted before reporting agent completion status.

Verification: 21 targeted/existing fastpath tests pass (`/tmp/connector-F03-fix.log`), including V2 write/ratchet verification, grant history, unlock version binding, and immutable transform/codec execution across a lost response. Root owns API/domain/server replay tests.
