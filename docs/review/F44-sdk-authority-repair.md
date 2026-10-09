# F44 — Bounded repair after definitive authority drift

The valid baseline at 31aba20 executed eight JVM tests with three failures: a ROOM grant race, a KEYSPACE grant race, and the expected finite repair budget. The two actual HTTP steady-state signed writes passed. Raw baseline XML and build output are retained in evidence/F44-sdk-red.

LockerClient now permits at most three authority-only re-signs after a definitive SIGNATURE_INVALID response. It verifies the original V2 signature locally, checks the reported authority covers the target and uses the same owned key, and reuses the already encoded payload, notification and parent version. A changed request receives a fresh write identity. Scope counters are compared only within the same normalized scope: ROOM epoch five to KEYSPACE epoch one is a valid authority change.

An ambiguous attempt, a ratchet, a different public key, an unchanged same-scope epoch, an invalid scope or exhausted repair budget remains terminal. Caller transforms and notification codecs execute once for an authority-only repair. A valid unlocked authority tombstone is retained as the previous epoch even when it has no public key. Server verification, public APIs and wire schemas are unchanged.

Validation on the fixed source executed 29 focused JVM tests, six Node tests, nine Apple simulator tests, nine real Chrome tests and nine owned Android emulator 5590 tests. Every execution passed with zero failures, errors or skips. Android production compilation also passed. The common repair tests perform actual platform P256 sign/verify and assert frozen body/notification, fresh identities and exactly one caller transform/codec. Raw logs, all result XML and exact counts are in evidence/F44-sdk-green.

Both builds used the repository wrapper, the explicit dependencies-indexeddb-final.json manifest and isolated Fullhouse Maven repository. The immutable inputs are ktstore 0.2.0-fh.d53a47cbc15434b3ce1e and ktbuf 1.1.10-fh.9c3962ea021c76107b84. Fullhouse consumer verification requires a new immutable Locker publication and is tracked separately; no consumer pass or external release is claimed here.
