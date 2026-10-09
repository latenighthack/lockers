# F43 — real storage verification for ordered subscription intents

The shared persistent SDK mutation contract now verifies an atomic subscribe intent
at revision1, removal intent/tombstone at revision2, physical membership removal while
the sidecar remains, close/reopen preserving revision2, and re-subscribe at revision3.
With a one-history budget it also attempts a second room: membership insertion occurs
before history admission fails, and the regression confirms both membership and
revision changes rolled back. This is performed through the actual SDK store methods,
including nested logical owners, rather than a synthetic sidecar transaction.

Historical migration fixtures now cover V3/V4/V5/V6→V7 on real SQLite and IndexedDB.
The V6 fixture completes canonical private archive adoption before its upgrade;
old private pending/archive bytes, unknown scope/keyspace fields, legacy epoch-zero
metadata, terminal authority epochs, existing uncertainty acknowledgements and cache
bytes remain intact across two current-schema reopens.

Validation on source8e86d3e plus SDK af00180 and these test additions:13 JVM guards,
7 Chrome IndexedDB tests,7 Apple simulator SQLite tests,7 Android SQLite tests on the
owned emulator5590. Every test executed; zero failures, errors or skips. The frozen
`dependencies-indexeddb-final.json` manifest selects ktstoref5e9eaa
(`0.2.0-fh.d53a47cbc15434b3ce1e`) and ktbuf780bcc0
(`1.1.10-fh.9c3962ea021c76107b84`) in the isolated Fullhouse Maven repository.
Raw output and exact counts are retained in `evidence/F43-storage`.

Final combined/static and Fullhouse consumer gates still require the complete root
checkpoint. No external release or deployment was performed.
