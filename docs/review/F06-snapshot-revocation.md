# F06: revoke durable snapshot leases with session authority

Reproduction: `SessionRevocationTest.authenticatedDestroyRemovesAuthorityAndReservesTheId` creates a leased two-page snapshot bound to a session, destroys that session using its signed proof, then tries the retained page token. Before the fix, the page remained readable. The baseline test failed; after the fix it passes.

Session destruction now removes all snapshot rows for the session inside the existing session-authority transaction, alongside subscriptions, inbox and push work. Snapshot deletion uses indexed batches of 256 and checks cancellation between batches. A failure rolls back the entire revocation.

Validation: repository wrapper `:server:test --tests '*SessionRevocationTest'`, frozen paired dependency manifest, JVM. Red and green logs are attached; broader integration remains a separate final gate.
