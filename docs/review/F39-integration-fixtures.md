# F39: integrated ownership and delivery fixtures

The combined server run exercised 262 tests and found 15 failures. Older fixtures submitted invalid empty locker envelopes, assumed first version zero and deleted claim epochs, omitted session admission and worker startup, asserted asynchronous delivery immediately, or combined JDBC claims with unfenced in-memory content storage. One failure also exposed an actual watch-producer exception and cleanup defect, fixed separately under F26.

Ownership, resharding, latency and claim fixtures now submit supported envelopes, await durable delivery, explicitly start and join owned services, admit active session identities, and preserve retained authority epochs. PostgreSQL cluster tests use two actual fenced database handles and a unique schema, and the pool drain test verifies an actual PostgreSQL connection closes after a null slot. The original routing, CAS, handoff and event delivery assertions remain exercised.

Targeted integration verification: 38 tests, zero failures, zero skips in `/tmp/lockers-integration-fixtures-pass1.log`. The full integrated suite remains a separate final gate.
