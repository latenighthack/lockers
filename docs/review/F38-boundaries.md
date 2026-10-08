# F38 boundary and resource validation

Authority boundary regression reproduced malformed replacement keys being persisted, a malformed public-key room identifier falling back to first-writer authority, and unknown scope enum values reaching storage. The four authority regressions and existing LockVerifier tests now pass. Public keys must be canonical compressed P-256 encodings; structurally public room identifiers cannot fall back to another authority model; scopes and epochs are validated before storage. Crypto validation preserves coroutine cancellation.

Validation: `:server:test --tests *BoundaryAuthorityTest --tests *LockVerifierTest` (red and green logs in evidence/F38). This is the authority portion of F38; bounded session/notification admission and complete stable snapshot/inbox paging remain in progress. The additive paging API commit deliberately leaves the advertised capability false until implementation is complete.

Session create now requires canonical, decodable compressed P-256 keys and nonempty bounded IDs. Open validates stored key bytes before decoding and reports INVALID_PUBLIC_KEY instead of throwing or poisoning a stream. Coroutine cancellation is preserved in create/open catch blocks. Two additional red regressions now pass together with concurrent create/open and notification replay tests. Resource admission and paging remain in progress.
