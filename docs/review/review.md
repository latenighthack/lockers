# Lockers library correctness and coroutine review

The current working tree has authority bypasses that should be fixed before an externally accessible release, and several delivery and lifecycle gaps that weaken its Flow and coroutine design. The highest priorities are lock hierarchy enforcement, authenticated and unambiguous mutation envelopes, private peer gateways, session mutation authentication, and commit-time ownership/version checks. There are 11 P1 findings, 30 P2 findings, and one P3 documentation/quality finding below.

The main design correction is to make committed state and durable work authoritative, then expose them through Flow with explicit lifecycle ownership. Several current paths instead depend on transient emissions, detached jobs, or application collectors completing before correctness-critical work can progress.

## Scope and validation

This review covers the API/protocol definitions, connector, server, claim and ring ownership, delivery and push workers/providers, storage definitions, extension seams, keymaster, build/container/CI configuration, and relevant tests. It applies to a snapshot of the current working tree, including the uncommitted storage migration, based on HEAD 64e397f20fc92edbe18f967c54b4aabe3cd3edf7. Library source in the original checkout was left unchanged. Builds and diagnostic tests ran in a separate worktree.

| Validation | Outcome |
| --- | --- |
| Existing JVM suites with a disposable PostgreSQL database | 268 passed, two optional load tests skipped |
| Additional diagnostic probes | 20 passed by asserting the observed defects |
| JVM, JS, Android release, iOS Arm64, iOS simulator Arm64, iOS x64 compilation | Passed |
| iOS simulator test task | No test sources; skipped |
| JS Node test task in workspace mode | Blocked by Node distribution repository resolution |
| Default released dependency mode | Blocked by unresolved ktstore-library 0.2.0 |
| Fullhouse consumer builds and external releases | Not performed during this review |

The passing diagnostic tests are characterizations of faulty behavior, not tests demonstrating fixes. The existing suites total 270 cases; enabling PostgreSQL exercised 21 cases that had previously been skipped. The final run totals 290 cases including diagnostics, with 288 passing and two skipped.

Validation used the repository wrapper and an explicit isolated Fullhouse ktstore pin, 0.2.0-fh.fc082ff74cf9904be6cd, from its generated workspace manifest. Global Maven Local was not used, and no dependency was republished. This confirms the reviewed source with that workspace dependency; it does not demonstrate that the unresolved released 0.2.0 coordinate is usable.

[Final results](/Users/mikeroberts/workspace/latenighthack/kitkit-review-2026-10-08/evidence/final-summary.json), [Gradle validation log](/Users/mikeroberts/workspace/latenighthack/kitkit-review-2026-10-08/evidence/final-validation.log), [diagnostic sources](/Users/mikeroberts/workspace/latenighthack/kitkit-review-2026-10-08/evidence/diagnostics), [test XML](/Users/mikeroberts/workspace/latenighthack/kitkit-review-2026-10-08/evidence/test-results), and [source archive](/Users/mikeroberts/workspace/latenighthack/kitkit-review-2026-10-08/evidence/source-snapshot.zip) are included beside this report. [The source comparison](/Users/mikeroberts/workspace/latenighthack/kitkit-review-2026-10-08/evidence/source-comparison.json) found no differences between original source and the snapshot, excluding the two diagnostic files. The temporary PostgreSQL instance and local HTTP probe server were stopped.

P1 means an authority/security failure or a risk of losing usable authority. P2 means a correctness, recovery, lifecycle, scalability, or build gap that should be resolved in the supported design. P3 means a lower-priority quality inconsistency. Conditions and evidence are stated per finding; code-established cases are distinguished from diagnostic reproductions.

## Findings

### F01 An unsigned child lock can override an existing parent authority

**P1.** [LockVerifier.kt:90](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/LockVerifier.kt:90), [LockVerifier.kt:104](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/LockVerifier.kt:104), [LockVerifier.kt:49](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/LockVerifier.kt:49)

In an opaque room, applyLock resolves the parent key but the unsigned TOFU branch checks only whether the room ID embeds a public key. It does not reject an existing room or keyspace parent. Anyone who knows the room and locker IDs can establish their own more-specific key; resolveEffective then selects that key ahead of the legitimate parent. A room lock therefore does not secure its descendants.

**Evidence.** Reproduced by unsignedChildOverridesAnExistingRoomAuthority: establish a room lock with the owner's key, establish an unsigned locker lock with an unrelated key, and observe the attacker's key becoming the effective authority.

**Correction.** Permit unsigned establishment only when the relevant authority hierarchy is genuinely unclaimed. Require a valid parent grant whenever an ancestor governs the scope, and perform establishment atomically with that ancestor state. Cover room-to-keyspace, room-to-locker, and keyspace-to-locker delegation in both TOFU and public-keyed rooms.

### F02 A valid sealed signature can carry an unauthenticated open payload

**P1.** [model.proto:128](/Users/mikeroberts/workspace/latenighthack/kitkit/proto/common/v1/model.proto:128), [RoomServiceImpl.kt:548](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:548), [LockerClient.kt:32](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:32)

Locker.open and Locker.sealed are independent protobuf fields. The server hashes and verifies only sealed.payload.enclosure.innerPayload, then stores the entire Locker. The client extracts open.encodedPayload first. Appending or replacing an open field does not change the signed context, yet it changes the bytes applications consume. The same ambiguity reaches agents that read open.

**Evidence.** Reproduced by unsignedOpenFieldSurvivesValidationOfSignedSealedPayload: a signature for payload 10 is accepted with an additional open payload 99, and both representations survive the subsequent read. The normal client accessor selects 99.

**Correction.** Reject every invalid or ambiguous envelope before signature verification and persistence; require exactly one supported representation. Share that validator across single writes, batches, reads, and extension outputs. Define which metadata is authenticated: notifications and ratchet.newSharedKeys are also outside the current signing contexts. Introduce any changed signing preimage through an explicit protocol version, while retaining the existing wire compatibility path.

### F03 Old unlock and delegation signatures remain reusable

**P1.** [LockerSigning.kt:47](/Users/mikeroberts/workspace/latenighthack/kitkit/api/src/commonMain/kotlin/com/latenighthack/lockers/common/LockerSigning.kt:47), [LockerSigning.kt:56](/Users/mikeroberts/workspace/latenighthack/kitkit/api/src/commonMain/kotlin/com/latenighthack/lockers/common/LockerSigning.kt:56), [LockVerifier.kt:110](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/LockVerifier.kt:110), [LockVerifier.kt:143](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/LockVerifier.kt:143)

unlockContext contains no lock version or incarnation. Unlock deletes the row, and re-establishment resets its version to 1. An old unlock signed by a key reused for a later lock therefore removes that later lock as well. Changing the unsigned parentLockVersion to the current version also allows reuse after a key returns. Grant contexts likewise lack parent epoch/version, and applyLock ignores parentLockVersion.

**Evidence.** Reproduced by oldUnlockSignatureRemovesAReestablishedLock: lock, signed unlock, recreate with the same key, then replay the original signature and version. The second unlock succeeds.

**Correction.** Retain a monotonic authority incarnation even when a scope becomes unlocked. Bind that incarnation and the expected authority version into grants, unlocks, and ratchets, and compare them atomically. Decide explicitly whether ancestor revocation invalidates existing descendants. Migrate the signing domain rather than silently changing V1 bytes.

### F04 Internal event and push gateways are exposed without peer authentication

**P1.** [MonolithComponent.kt:135](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/MonolithComponent.kt:135), [Server.kt:19](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/Server.kt:19), [SessionServiceImpl.kt:380](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:380), [PushServiceImpl.kt:374](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/push/v1/PushServiceImpl.kt:374)

clientServices deliberately mounts SessionGateway and PushGateway on the public listener. Their implementations accept caller-selected session IDs and events or pushes without authenticating a peer or checking room write authority or subscriptions. A caller with a target session ID can inject locker bodies and arbitrary versions directly into its inbox, bypassing RoomService signature checks; the client trusts those events. Arbitrary push traffic and durable queue growth are also possible.

**Evidence.** Confirmed by the public route composition and gateway implementations. This requires access to the public listener and the relevant session identifier; no session private key or locker signing key is required.

**Correction.** Give trusted peer RPCs their own transport boundary and authenticate peer identity. Keep in-process discovery usable without exposing equivalent public endpoints. Validate sender authority and event identity at that boundary; an internal port alone is insufficient if callers can reach it. Preserve the extension API while requiring an explicit decision about which of its routes are public.

### F05 Web Push endpoints allow server requests to private HTTP destinations

**P1.** [PushServiceImpl.kt:301](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/push/v1/PushServiceImpl.kt:301), [WebPushProvider.kt:48](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/push/v1/providers/WebPushProvider.kt:48)

A Web Push registration accepts an arbitrary endpoint. The provider checks only that it is nonempty before handing it to an HTTP client. With Web Push configured, the public registration and send gateway can be combined to make the server POST to loopback, private network, or other unintended destinations. The encryption of the request body does not prevent the outbound connection.

**Evidence.** Reproduced by webPushAllowsLoopbackHttpEndpoint: a valid synthetic registration reached a temporary HTTP endpoint on 127.0.0.1 and the provider treated its 201 response as Accepted. The diagnostic contacted only a local test server.

**Correction.** Validate endpoints at registration and at send time. Require HTTPS, validate host and port against the supported push-service policy, reject private and special IP destinations after DNS resolution, and constrain redirects and outbound network access. Authenticate registration ownership independently. Apply length and public-key/auth-secret validation before persisting credentials.

### F06 Session proof does not protect subscription or push credential mutations

**P1.** [RoomServiceImpl.kt:360](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:360), [RoomServiceImpl.kt:225](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:225), [PushServiceImpl.kt:301](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/push/v1/PushServiceImpl.kt:301), [PushServiceImpl.kt:339](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/push/v1/PushServiceImpl.kt:339), [SessionServiceImpl.kt:147](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:147)

Session opening verifies a private-key signature, but the unary subscription and push registration RPCs use a bare caller-supplied sessionId. Knowing an ID is sufficient to add or remove its subscriptions, replace its push destination, or unregister a backend. This can suppress delivery or redirect push information to another device. destroySession additionally returns OK without revoking anything.

**Evidence.** Confirmed by direct request handling: these methods never derive an authenticated session principal from GrpcRequestContext. README's intentionally unfinished room read authorization does not justify bypassing ownership of an established session.

**Correction.** Bind unary mutations to an authenticated session context or a signed, scoped session capability with expiry and replay protection. Define whether identifiers are public names or secret capabilities; the current API mixes those models. Implement authenticated revocation that closes live sockets and removes or expires session routing, subscription, inbox, and push state, with a documented deletion policy.

### F07 A stale negative lock cache can disable signature enforcement

**P1.** [RoomServiceImpl.kt:75](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:75), [RoomServiceImpl.kt:408](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:408), [RoomServiceImpl.kt:790](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:790), [ClaimRoomOwnership.kt:46](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/claim/ClaimRoomOwnership.kt:46)

roomHasLocksCache stores false without an expiry or authority epoch. Reads on a non-owner can populate it. A different owner may subsequently establish a lock, after which the reader can acquire the room with its old negative cache intact. effectiveLockOrNull then skips all lock resolution. This affects the default legacy write/delete path and paths that choose cached resolution rather than batch lock prefetch. Reads can also report incorrect lock state.

**Evidence.** Reproduced by staleNegativeReadCacheBypassesNewRoomLock using two services over one store: one caches an unlocked read, the other locks the room, and the first accepts an unsigned write. Claim acquisition has no callback that invalidates that cache; demotion-only eviction does not cover a previously non-owning reader.

**Correction.** Make authorization reads authoritative inside the mutation transaction. If caching is retained, key it by a proven ownership/authority epoch and invalidate before acquiring or resuming authority, with distributed lock-change invalidation. A TTL alone cannot establish the security invariant.

### F08 The default mutation path has no database version CAS or commit fence

**P1.** [RoomServiceImpl.kt:485](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:485), [RoomServiceImpl.kt:534](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:534), [LockerStore.kt:50](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/LockerStore.kt:50), [RoomServiceImpl.kt:710](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:710), [AdvisoryLockCoordinator.kt:28](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/cluster/AdvisoryLockCoordinator.kt:28)

Legacy writes compare a fetched version and then perform an unconditional save under a process-local room mutex. Two coordinators can both read version N and both commit N+1. Ownership is checked before queueing and database work, with no owner epoch checked at commit, so a paused or draining former owner can continue after takeover. Deletes validate before their outbox transaction; standalone lock operations also lack a shared mutation transaction. The advisory lease's so-called fencing token is the stable shard hash, not a monotonically increasing token enforced by storage.

**Evidence.** Reproduced by twoCoordinatorsAcceptTheSameVersionWithoutStorageCAS: two services read version 3 and both return successful version 4. The fast batch source path does serialize its read/validation/write transaction, which improves version races, but still lacks a commit-time ownership fence.

**Correction.** Require the storage mutation to atomically compare both expected locker/authority version and the current owner incarnation. Include content, lock changes, receipts, and delivery intents in the same transaction. Apply that invariant to writes, deletes, lock/unlock, and derived writes. Remove comments and metrics claiming that an ordinary read-and-save is a database CAS.

### F09 The default example agent can overwrite an independently locked keyspace

**P1.** [ServerCore.kt:87](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/ServerCore.kt:87), [ExampleLockerAgent.kt:26](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/agents/ExampleLockerAgent.kt:26), [RoomServiceImpl.kt:633](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:633)

ServerCore installs ExampleLockerAgent whenever the embedder supplies no override. That agent mirrors a write in keyspace 31 into keyspace 30, and derived writes bypass the target's lock verification. An unsigned write to an open input keyspace can therefore overwrite a locked output keyspace. Shipping this demo as the default also assigns implicit meaning to otherwise opaque keyspaces.

**Evidence.** Reproduced by defaultExampleAgentOverwritesAnIndependentlyLockedKeyspace using the actual ServerCore default. A direct unsigned write to keyspace 30 is rejected, while the same bytes written to keyspace 31 replace its locked output.

**Correction.** Use a no-op registry by default and require explicit installation of trusted authoritative agents. Preserve their ability to produce privileged derived state, but declare and enforce their permitted input/output scopes and validation policy. Put the demonstration behind an explicit sample configuration.

### F10 A legacy ratchet can commit before its associated content write fails

**P1.** [RoomServiceImpl.kt:578](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:578), [LockVerifier.kt:171](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/LockVerifier.kt:171), [RoomServiceImpl.kt:618](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:618)

In the legacy path, applyRatchet saves the new lock key before the locker is saved or delivery is accepted. A storage exception or cancellation between those operations leaves the new authority committed with no successful content write. A client retry using the old key then fails verification, despite never receiving a successful ratchet response.

**Evidence.** Reproduced by legacyRatchetCommitsEvenWhenLockerPersistenceFails: inject a locker-save failure, submit a correctly signed ratchet write, and observe the new public key persisted while the locker remains absent. Fast batch source transactions avoid this particular partial commit.

**Correction.** Commit ratchet, locker, receipt, and delivery intent in one transaction on every supported path. Make success and ambiguous-response recovery refer to the complete transaction outcome. Do not advertise atomic ratcheting on a path that can persist only its authority half.

### F11 The client can discard a successfully committed ratchet key

**P1.** [LockerClient.kt:754](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:754), [LockerClient.kt:788](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:788), [LockerClient.kt:836](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:836), [LockerClient.kt:436](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:436)

A new ratchet private key lives only in a local variable until onRatcheted runs. The client throws on agentFailed or agentPending before adopting that key or accepting the committed source. The server may already have committed both source and ratchet. A response lost across process death has the same problem: a write receipt preserves the server outcome, but it cannot reconstruct the client's discarded private key. Subsequent writes may be permanently locked out.

**Evidence.** Confirmed by the ordering of the success, agent-status, and key-adoption branches. This is distinct from F10: even a fully atomic successful server commit can leave the client holding the previous authority.

**Correction.** Persist a pending ratchet key and immutable request identity before submission. Resolve a committed outcome and adopt that key even when derived agent work is pending or failed. Return a typed outcome separating source commit from agent completion; reserve an ordinary write-failed exception for a source mutation that did not commit. Recover pending key transitions on startup.

### F12 Session creation and challenge rotation are not atomic

**P2.** [SessionServiceImpl.kt:597](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:597), [SessionServiceImpl.kt:611](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:611), [SessionServiceImpl.kt:521](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:521), [SessionServiceImpl.kt:559](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:559), [SessionStore.kt:23](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionStore.kt:23)

Create checks for absence and then unconditionally saves the session. Concurrent creates for the same identifier can both succeed with different authorization keys, leaving one persisted authority. Open similarly reads a challenge, verifies it, and unconditionally rotates it; concurrent uses of the same valid challenge can both succeed. The create path does not use the session owner gate or a database put-if-absent.

**Evidence.** Reproduced by concurrentSessionCreatesBothSucceedAndOverwriteAuthority: two services both observe an absent session, both return OK with different public keys, and only one authority row remains. The equivalent open race follows the same read/verify/save structure.

**Correction.** Add atomic create-if-absent and challenge compare-and-rotate operations. Tie the winning socket/registry attachment to the committed session incarnation, and reject competing admissions before publishing live state. Test both same-node and cross-node races; serialization of event ACKs does not serialize session admission.

### F13 Creating a locker does not advance its version

**P2.** [RoomServiceImpl.kt:534](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:534), [RoomServiceImpl.kt:719](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:719)

When no stored locker exists, the server assigns request.parentVersion as the new version. With the usual parent 0, creation returns 0; a second stale parent-0 request is then accepted and returns 1. The first existence transition does not invalidate optimistic concurrency. The same initial rule applies to deletion of an absent locker.

**Evidence.** Reproduced by sameInitialParentVersionCommitsTwice: identical requests using parent 0 both succeed, first at version 0 and then at version 1. This also interacts with client watchers that ignore equal-version state changes.

**Correction.** Define one initial parent sentinel, reject inappropriate parents for absence, and advance the version on every committed state transition. Include first-create races and deletion/recreation in the protocol's version tests. Guard negative values and overflow, and plan how legacy version-0 rows are interpreted during migration.

### F14 Null and zero keyspace aliases bypass duplicate and serialization checks

**P2.** [RoomServiceImpl.kt:255](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:255), [RoomServiceImpl.kt:294](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:294), [LockerClient.kt:386](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:386), [LockerClient.kt:740](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:740)

Duplicate detection uses the raw LockerId, while storage and signing canonicalize a missing keyspace to zero. A batch can contain both representations of the same locker. Both changes validate against the same prefetched version, two conflicting events are created, and one row wins persistence. Client mutation locks likewise use unnormalized IDs, allowing nominally serialized writes to the same stored locker to take different locks.

**Evidence.** Reproduced by nullAndZeroKeyspaceAliasesPassAtomicBatchDistinctness: the atomic batch succeeds with two version-0 outcomes, one stored locker, and two delivery intents.

**Correction.** Define a canonical identity type for room, keyspace, and locker bytes. Normalize at API entry before distinctness checks, mutation-lock acquisition, receipt construction, storage lookup, and routing. Keep the existing signed equivalence of omitted and zero keyspace; enforce it consistently rather than changing its meaning.

### F15 Bulk reads represent a tombstone as a live empty locker

**P2.** [RoomServiceImpl.kt:212](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:212), [RoomServiceImpl.kt:235](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:235), [RoomServiceImpl.kt:449](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:449), [LockerClient.kt:645](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:645)

getLockers decodes stored.locker regardless of stored.deleted. Deleted rows contain an empty byte array, which decodes to a non-null empty Locker and is returned as OK. getLocker hides the deleted row, getAllLockers filters it out, and subscribeAndSnapshot correctly returns a versioned identified tombstone. The client bulk-read path treats any non-null locker body as present.

**Evidence.** Reproduced by bulkReadReturnsDeletedLockerAsPresent: getLocker returns null after deletion, while getLockers returns a non-null Locker for the same ID.

**Correction.** Use one versioned tombstone representation across every read, snapshot, event, and write-conflict response. Teach clients to preserve the tombstone version while removing the visible value. Add parity tests for all read APIs, including an empty but genuinely present payload.

### F16 Legacy revalidation cannot remove cached deletions

**P2.** [LockerClient.kt:349](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:349), [LockerClient.kt:363](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:363), [LockerClient.kt:651](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:651), [RoomServiceImpl.kt:449](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:449)

Legacy hydration merges only lockers returned by getAllLockers, which omits deleted rows. fetchLocker accepts only a non-null response. Neither operation removes a previously cached locker when the server reports absence. A disconnected client that misses a delete, or creates a replacement session after an inbox gap, can keep the deleted value indefinitely despite repeated successful revalidation.

**Evidence.** Confirmed by the absence-only branches and the default capabilities path. The fast subscribeAndSnapshot path includes tombstones and can repair this specific gap; its behavior is not available when deliveryOutboxEnabled is false.

**Correction.** Return versioned tombstones on legacy-compatible reads, or reconcile a complete authoritative snapshot with its boundary/watermark. Do not infer an unversioned delete from absence while a newer live event may race hydration. Make reconciliation repair deletions as well as additions.

### F17 Subscription changes on one node do not refresh another node's fanout cache

**P2.** [RoomServiceImpl.kt:188](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:188), [RoomServiceImpl.kt:360](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:360), [RoomServiceImpl.kt:388](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:388), [RoomServiceImpl.kt:96](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:96)

Subscriptions are deliberately allowed on non-owner nodes, but each RoomService caches its own room-to-session set without expiry or distributed invalidation. A writing owner that has already cached recipients does not learn a subscription added elsewhere, and can continue delivering to a session removed elsewhere. The legacy path can therefore miss live and offline inbox delivery until eviction or takeover.

**Evidence.** Confirmed by the shared subscription store, process-local cache, and local-only cache updates. Fast outbox commits query durable recipients and avoid this cached-recipient path.

**Correction.** Resolve recipients from authoritative storage at the event's commit boundary, or maintain an observed subscription revision tied to a coherent distributed cache. Keep subscribe-and-snapshot atomicity and unsubscribe semantics explicit. Do not rely on requests happening to land on the writer.

### F18 The default delivery mode can acknowledge a write whose event has no recovery path

**P2.** [LockersConfig.kt:125](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/LockersConfig.kt:125), [RoomServiceImpl.kt:615](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:615), [RoomServiceImpl.kt:666](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:666)

deliveryOutboxEnabled defaults to false. The legacy path persists a locker and then sends events directly to gateways. Gateway lookup, RPC failure, or a crash between persistence and inbox acceptance can leave no durable delivery intent. deliver records failures and the write still returns OK. A durable session inbox helps only after the gateway has accepted an event.

**Evidence.** Confirmed by the default configuration and post-persistence direct delivery. The optional outbox path provides the missing durable intent, so the reliability contract currently changes materially with configuration.

**Correction.** Make durable delivery the supported invariant for production writes, deletes, and derived state, after resolving its scaling issues in F33. If a weaker legacy mode remains, expose its guarantee explicitly and provide snapshot repair that includes deletions. Treat local Flow wakeups as hints to recoverable work.

### F19 Persisted push work can become invisible to a running worker

**P2.** [PushServiceImpl.kt:107](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/push/v1/PushServiceImpl.kt:107), [PushServiceImpl.kt:139](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/push/v1/PushServiceImpl.kt:139), [PushServiceImpl.kt:150](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/push/v1/PushServiceImpl.kt:150), [PushServiceImpl.kt:401](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/push/v1/PushServiceImpl.kt:401)

The push worker reads the durable queue once in onStart, then consumes only this instance's replay-zero SharedFlow. Work enqueued by an API-only replica emits into a different instance and is never discovered by the running worker. An enqueue between the startup query and actual subscription can also be missed. A startup storage failure terminates the collector with no recovery loop.

**Evidence.** Reproduced by APIReplicaEnqueueIsInvisibleToRunningWorkerReplica: the worker sends its startup row, another instance enqueues a new row into the same store, and the row remains pending with no second send. Structurally, there is no polling or distributed observation that can discover it later.

**Correction.** Use durable queue claims with a bounded worker pool and a store-driven observation or notification seam, plus periodic recovery scanning. Subscribe to wakeups before scanning, and rescan after errors or restarts. The queue row must determine whether work exists; an in-process emission cannot be its only discovery mechanism.

### F20 Push drain can duplicate in-flight sends and does not bound queued coroutines

**P2.** [PushServiceImpl.kt:157](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/push/v1/PushServiceImpl.kt:157), [PushServiceImpl.kt:208](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/push/v1/PushServiceImpl.kt:208), [PushServiceImpl.kt:446](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/push/v1/PushServiceImpl.kt:446)

Each emitted queue row creates another coroutine. drainQueue re-emits all rows, including those already being sent, without a row lease or in-flight guard. A semaphore bounds vendor calls but not the number of jobs waiting behind it. Multiple worker replicas have the same duplicate-send risk, while a large backlog can create an arbitrarily large coroutine set.

**Evidence.** Reproduced by drainingQueueDuplicatesAnInFlightSend: hold the first provider send open, invoke drainQueue, and observe a second concurrent send for the same row.

**Correction.** Claim rows atomically with lease identity, expected state, and a persisted next-attempt time. Use a fixed number of consumers over bounded claims. Make drain a request to accelerate discovery rather than a second delivery stream. Preserve at-least-once semantics for ambiguous provider acceptance and make duplicates observable.

### F21 Push registration failures are not retried on ordinary reconnects

**P2.** [PushRegistration.kt:103](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/PushRegistration.kt:103), [PushRegistration.kt:108](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/PushRegistration.kt:108), [PushRegistration.kt:165](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/PushRegistration.kt:165), [Stream.kt:273](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/Stream.kt:273)

sendRegister catches transport failures and returns while leaving the row pending. Reconciliation is triggered by a distinct session ID, not a successful connection epoch. Stream keeps the same session ID through an ordinary reconnect, so no retry occurs. This contradicts the controller and facade documentation, and awaitRegistered can wait forever after a transient outage.

**Evidence.** Reproduced by registrationTransportFailureIsNotRetriedForSameSession: one failed registration remains pending; publishing an equal session ID makes no second RPC. The source has no independent retry loop.

**Correction.** Reconcile persisted desired registrations against a session state containing connection incarnation and readiness. Retry recoverable failures with bounded backoff while that desired state remains current. Represent terminal failure and closed state explicitly so callers can await a real outcome.

### F22 Late push acknowledgements can restore obsolete or removed credentials

**P2.** [PushRegistration.kt:118](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/PushRegistration.kt:118), [PushRegistration.kt:141](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/PushRegistration.kt:141), [PushRegistration.kt:173](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/PushRegistration.kt:173)

Two controller jobs and direct unregister calls mutate the same registration state without a serialized generation check. An older register response can overwrite a newer credential or recreate a row after unregister. It can also confirm a backend for a superseded session. Read-modify-write updates of reconciled.value can lose concurrent changes. Unregister removes local intent even if the server call failed, leaving no durable revoke to retry.

**Evidence.** Reproduced by lateRegistrationResponseResurrectsUnregisteredCredential: block register, complete unregister and local deletion, then release the register response; the credential is recreated and reported registered.

**Correction.** Use a serialized reducer with desired credential revision, session incarnation, and durable pending removal. Apply acknowledgements only when all three still match. SubscriptionController's generation-checked confirmations provide a useful local pattern, with bounded commands and explicit lifecycle outcomes.

### F23 A watcher publishes partial history as its initial snapshot

**P2.** [LockerClient.kt:469](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:469), [LockerClient.kt:479](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:479), [LockerClient.kt:489](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:489)

watchSnapshot calls merge for each cached locker, and merge emits immediately. Its first value therefore represents only the first historical item rather than the room/keyspace snapshot. Live events may interleave those partial histories. SharingStarted.WhileSubscribed also retains replay across idle periods while a restarted producer rebuilds its local map. The state.isEmpty check reads the mutable map outside stateMutex.

**Evidence.** Reproduced by cachedWatchFirstEmissionContainsOnlyOneOfThreeLockers: three stored lockers yield a first list of size one. The higher-level watchAll documentation describes initial history as one snapshot.

**Correction.** Load and reduce a complete snapshot before one initial emission, buffering and version-ordering live events around a snapshot boundary. Expose an immutable state with loading/freshness metadata when cached state is intentionally emitted early. Keep all state access serialized, and define replay reset and watcher eviction.

### F24 Application Flow collectors can block acceptance and ACKs for unrelated rooms

**P2.** [LockerClient.kt:318](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:318), [LockerClient.kt:459](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:459), [LockerClient.kt:531](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:531), [Stream.kt:522](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/Stream.kt:522)

accept holds a client-wide mutex while saving and emitting to internalChanges. Once its 64-item buffer fills, one slow collector blocks every room's acceptance and writers waiting on that mutex. Notification emission and raw stream-event emission also happen before ACK persistence. Cancellation after save but before emission can leave an active watcher behind the store; a replay of equal content is suppressed by the stored-version guard. With no collectors, replay-zero change and notification emissions are instead ephemeral.

**Evidence.** Confirmed by the save/emit/mutex ordering and Flow buffers. SharedFlow buffering does not persist values without subscribers, and suspending overflow can make producers wait for collectors. These are documented contracts, rather than unusual runtime failures. [Kotlin SharedFlow API](https://kotlinlang.org/api/kotlinx.coroutines/kotlinx-coroutines-core/kotlinx.coroutines.flow/-shared-flow/)

**Correction.** Commit accepted state and dedup/ACK intent independently of application multicast. Observe committed state through a transaction-aware storage Flow or a reducer that reloads from storage after conflated wakeups. Give state snapshots conflation and give events requiring delivery a persisted cursor or inbox. Move emission outside correctness mutexes; blindly dropping changes would introduce another delivery gap.

### F25 Cancellation is retried or converted into application failure

**P2.** [LockerClient.kt:255](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:255), [LockerClient.kt:832](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:832), [PushRegistration.kt:166](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/PushRegistration.kt:166), [PushServiceImpl.kt:210](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/push/v1/PushServiceImpl.kt:210), [LockVerifier.kt:205](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/LockVerifier.kt:205)

WRITE_EXCEPTION_HANDLER treats CancellationException as retryable through its catch-all branch. The retry helper eventually produces RetryLimitExceeded, which the client wraps as LockerWriteException. Push registration runCatching and several provider/server catch(Exception) branches also catch coroutine cancellation and classify it as an ordinary failure or retry. This weakens cancellation propagation and can run failure-side persistence while a component is stopping.

**Evidence.** Reproduced by cancelledWriteIsConvertedToLockerWriteFailure: cancel an in-flight update and observe LockerWriteException instead of the cancellation signal. Other identified catch sites were established by source inspection.

**Correction.** Rethrow CancellationException before transient-error classification. Use narrowly scoped NonCancellable cleanup only for necessary resource release or completion of an already-defined transaction boundary. Review the pinned retry helper's exception contract, including its use of CancellationException for retry exhaustion, and distinguish exhaustion from actual caller cancellation.

### F26 Background ownership and shutdown are detached from the caller's lifecycle

**P2.** [LockerClient.kt:282](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:282), [Stream.kt:106](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/Stream.kt:106), [Stream.kt:271](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/Stream.kt:271), [PushRegistration.kt:96](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/PushRegistration.kt:96), [LockersClient.kt:81](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockersClient.kt:81), [ServerExtension.kt:22](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/ServerExtension.kt:22)

The connector components construct separate root SupervisorJobs; LockerClient uses GlobalScope plus such a job. Server workers also own independent root scopes. Parent cancellation cannot propagate into them, dispatchers are fixed, and most stop/close methods cancel without joining. Several start methods can create duplicate loops, and partial factory startup has no rollback. A supervisor isolates a failed child but does not restart a failed collector or expose its failure to callers.

**Evidence.** Confirmed by constructors and lifecycle methods. SupervisorJob supports a parent job, and its failure isolation requires an explicit child-failure policy. [Kotlin SupervisorJob API](https://kotlinlang.org/api/kotlinx.coroutines/kotlinx-coroutines-core/kotlinx.coroutines/-supervisor-job.html)

**Correction.** Give each client/server component one explicit lifecycle scope with a supplied parent, owned child jobs, and injected execution contexts. Keep deliberately shared reads owned by that client scope so one waiter cannot cancel everyone. Make start idempotent or reject duplicate starts, clean up partial initialization, and provide suspending close/stop that cancels and joins. Add an equivalent awaitable lifecycle seam for extensions.

### F27 Connection state can remain true during failure and awaiters can hang after closure

**P2.** [Stream.kt:293](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/Stream.kt:293), [Stream.kt:315](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/Stream.kt:315), [Stream.kt:343](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/Stream.kt:343), [Stream.kt:553](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/Stream.kt:553), [LockersClient.kt:43](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockersClient.kt:43)

onConnected is cleared around the outer connection loop and normal completion, but an exception retried inside repeatWithBackoff can leave it true throughout retries. Fatal failure and stop do not consistently clear it. sessionIdSource also remains populated through disconnection or abandoned sessions. awaitConnected waits only for true and never observes fatal or closed state; subscription and push waiters have similar missing lifecycle outcomes.

**Evidence.** Confirmed by the distinct updates to isConnected, fatalError, sessionId, and stop. A previously connected client can report a stale true value, while a never-connected terminal client can leave awaitConnected suspended indefinitely unless its caller supplies cancellation.

**Correction.** Publish one StateFlow describing Connecting, Connected(session, connectionEpoch), Retrying, Failed, and Closed. Derive compatibility properties from it. Transition in connection-level try/finally and make await APIs complete with a typed terminal outcome. Use its connection epoch for registration/subscription reconciliation.

### F28 The session ping branch never emits its pong and there is no heartbeat deadline

**P2.** [SessionServiceImpl.kt:219](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:219), [Stream.kt:513](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/Stream.kt:513)

The server constructs StreamControlEvent.Message(Pong) in the ping branch but never emits it. The client ignores pong responses and has no matching receive deadline. Sending periodic pings alone therefore cannot detect an unresponsive application session or provide a working ping/pong contract.

**Evidence.** Reproduced by sessionPingDoesNotEmitPong: open a valid test stream, send ping, and receive no response. The stream remains otherwise active. This probe also showed that an empty session public key is accepted, covered in F38.

**Correction.** Emit the pong, track heartbeat identity or a monotonic deadline, and reconnect when the expected response does not arrive. Distinguish transport liveness from slow application processing so an observer cannot monopolize the heartbeat path. Test half-open connections, clean closes, and cancellation of heartbeat jobs.

### F29 Failure during session publication leaks local state and can block the next open

**P2.** [SessionServiceImpl.kt:204](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:204), [SessionServiceImpl.kt:212](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:212), [SessionServiceImpl.kt:356](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:356), [ClaimSessionRegistry.kt:48](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/claim/ClaimSessionRegistry.kt:48)

A new socket is placed in openStreamCancellationChannels and activeStreamsCount is incremented before attachBeforeSnapshot completes. If that database operation fails, openState is never completed, so onCompletion skips cleanup. The next open can replace the stale channel and then suspend forever sending cancellation to a channel whose collector has already disappeared. Separately, ClaimSessionRegistry's asynchronous node-only delete can remove a newer attachment on the same node if it runs after reattach.

**Evidence.** Confirmed by publication and cleanup ordering. The registration failure case needs a deterministic fault-injection regression test; the resulting map/count leak and unmatched channel send follow directly from the current branches.

**Correction.** Use explicit registration ownership and try/finally from the first local mutation. Cancel the prior socket by job or a nonblocking close signal. Attach and detach registry rows using a socket/attachment incarnation so cleanup cannot erase a successor; monitor and await required publication before announcing connection success.

### F30 A denied ring lease is never retried while the shard map stays unchanged

**P2.** [OwnerLifecycle.kt:71](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/cluster/OwnerLifecycle.kt:71), [OwnerLifecycle.kt:83](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/cluster/OwnerLifecycle.kt:83), [OwnerLifecycle.kt:108](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/cluster/OwnerLifecycle.kt:108), [AdvisoryLockCoordinator.kt:28](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/cluster/AdvisoryLockCoordinator.kt:28)

OwnerLifecycle acquires only initially or for shards whose map owner changed, then records the map even when acquire returned null. Reconciliation of the same map cannot acquire a missing lease, and a failed/expired lease is not restored independently. PostgreSQL advisory locks cannot preempt a live old holder merely because the desired epoch is higher, so a normal handoff race can leave the new owner unavailable indefinitely.

**Evidence.** Reproduced by deniedLeaseIsNeverRetriedAtTheSameMap: deny the first acquire, later allow it, reconcile the same map, and observe one attempt and zero held leases.

**Correction.** Continuously reconcile desired shards against actual valid leases, with bounded retry and cancellation on topology change. Make ownership state and retry failures observable. Test the real advisory-lock handoff protocol rather than relying only on the in-memory coordinator's stronger preemption behavior.

### F31 Ring mode does not maintain authority for nonzero keyspaces

**P2.** [ClusterContext.kt:28](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/cluster/ClusterContext.kt:28), [BlueprintV.kt:88](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/cluster/BlueprintV.kt:88), [RoomServiceImpl.kt:779](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:779)

ClusterContext defaults roomKeyspaces to only zero, and BlueprintV does not override that list. With a real ownership coordinator, writes to nonzero keyspaces have no maintained lease and can be redirected back toward the same node indefinitely. Even if more leases are configured, room-wide locks and agents span keyspaces while ring ownership splits them; room-wide authority changes and atomic batches can require coordination across different owners.

**Evidence.** Confirmed by production wiring and RingRoomOwnership's leaseFor(keyspace, shard) gate. This applies to the retained ring mode; claim mode intentionally owns an entire room and avoids the keyspace split.

**Correction.** Make the supported ownership unit match the authority unit. Prefer whole-room claim ownership for room locks, batches, and agents. If ring mode remains supported, wire its actual keyspace set and define how room-wide operations are serialized across those shards, or reject unsupported combinations explicitly.

### F32 Shutdown releases authority before requests and workers have drained

**P2.** [Main.kt:256](/Users/mikeroberts/workspace/latenighthack/kitkit/server/run/src/main/kotlin/com/latenighthack/lockers/server/Main.kt:256), [MonolithComponent.kt:189](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/MonolithComponent.kt:189), [ClaimRenewalService.kt:98](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/claim/ClaimRenewalService.kt:98), [DeliveryWorker.kt:102](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/DeliveryWorker.kt:102), [ClaimJdbcPool.kt:57](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/claim/ClaimJdbcPool.kt:57)

The shutdown hook closes coordination and calls component.stop before stopping the public listener. component.stop releases shard leases or claim rows first, then cancels workers and closes dispatchers without awaiting all work. A successor can acquire authority while an existing handler continues on this node; the listener can still admit requests during part of that interval. This amplifies the missing commit fence in F08. ClaimJdbcPool.close also treats a successfully received null slot as an empty channel and stops draining: a normal partially initialized queue such as [null, openConnection] leaves the connection unclosed.

**Evidence.** Confirmed by the shutdown order. stopAndRelease also cancels its renewal job without joining before deleting rows, permitting an in-flight renewal or publication to overlap release.

**Correction.** First reject new traffic and mark the component draining; then stop claiming work, cancel or finish handlers and join worker/renewal jobs. Release ownership only after authoritative work has stopped, then close providers, dispatchers, stores, and transports. Use one suspending shutdown path with bounded grace and preserve commit fencing as the final defense. Drain nullable pool slots by checking ChannelResult success separately from its nullable value, and await or close borrowed connections under a defined policy.

### F33 Retention and queue discovery grow without a bound

**P2.** [Stream.kt:60](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/Stream.kt:60), [Stream.kt:69](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/Stream.kt:69), [LockerSyncCoordinator.kt:11](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerSyncCoordinator.kt:11), [LockerClient.kt:286](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:286), [DeliveryOutboxStore.kt:45](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/DeliveryOutboxStore.kt:45), [DeliveryOutboxStore.kt:90](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/DeliveryOutboxStore.kt:90)

Confirmed ACK rows are retained forever, while getPendingAcks scans all rows before filtering. Write receipts, locker tombstones, and sessions have no defined reclamation policy. Watcher and mutation-lock maps retain every key encountered. SubscriptionController's command channel is unlimited. DeliveryOutbox.claim reads and groups the entire backlog inside one database lock shared with all room commits; four lanes repeat that scan, so independent room lanes still contend at storage.

**Evidence.** Confirmed by storage queries, map insertion without removal, and the single outbox transaction key. This is a workload-dependent scaling risk rather than a measured performance claim.

**Correction.** Define deduplication and idempotency horizons before pruning ACKs, receipts, or tombstones. Add indexed due-work/head-of-room queries with bounded results and safe row claims instead of full scans under a global lock. Use observer reference counts and lock entry lifetimes, bound or coalesce persisted-intent wakeups, and measure backlog age, query size, lock wait, active jobs, and storage growth.

### F34 Notification codec context does not survive delivery

**P2.** [NotificationCodec.kt:11](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/NotificationCodec.kt:11), [SessionServiceImpl.kt:453](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:453), [SessionServiceImpl.kt:340](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:340), [LockerClient.kt:523](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:523)

Encoding supplies notification title/body to NotificationContext, but the session inbox serializes only notification.payload and the locker. Both live responses and replay reconstruct the notification without push title/body, so decoding sees null values. A consumer codec that binds its payload to those context fields cannot reliably round-trip.

**Evidence.** Confirmed by the inbox event shape and reconstruction. Existing simple codec tests do not exercise a context-dependent codec through persistence and session replay.

**Correction.** Persist and replay the metadata that is part of the codec contract, or explicitly narrow encode/decode context to values guaranteed on both sides. Version storage/wire changes as needed. Test context-dependent authentication and transformation through both live and offline delivery.

### F35 The high-level client ACKs broadcasts without surfacing their payload

**P2.** [SessionServiceImpl.kt:428](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:428), [LockerClient.kt:518](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:518), [LockersClient.kt:58](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockersClient.kt:58)

Broadcast creates an event with an empty room ID and no locker ID. LockerClient emits notifications only when lockerId is non-null, so broadcast payloads never reach its notification Flow. Stream still accepts and ACKs the event. The facade does not expose raw Stream.events as an alternative broadcast channel.

**Evidence.** Confirmed by the broadcast event shape and the notification branch. A caller using Stream directly can observe raw events; the standard LockersClient API cannot consume these broadcast notifications.

**Correction.** Expose a session-level event/notification Flow with explicit room notification and broadcast variants. Decode and acknowledge each variant under a documented delivery policy. Keep locker-scoped codecs scoped, and provide a separate broadcast codec context rather than inventing a locker identity.

### F36 The documented JVM routing factory receives incompatible addresses

**P2.** [RoutingRpcClient.kt:28](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/RoutingRpcClient.kt:28), [RoutingRpcClient.kt:115](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/RoutingRpcClient.kt:115), [RoutingRpcClient.kt:82](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/RoutingRpcClient.kt:82), [RoutingRpcClient.kt:109](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/RoutingRpcClient.kt:109)

RoutingRpcClient's default normalizer prefixes bare owners with http://, while the pinned ktbuf JVM HttpRpcClient prepends http:// itself for non-HTTPS input. The documented factory therefore builds a double-scheme HTTP URL after a normal redirect. The server peer pool explicitly works around this same transport requirement. The owner cache also conflates room and session key namespaces, and concurrent client creation can allocate a losing transport that is never closed.

**Evidence.** Confirmed by routing code and inspection of the pinned JVM transport; server RemoteGateway.kt documents and avoids the double-prefix issue. Fake routing tests do not exercise a real HttpRpcClient destination.

**Correction.** Make address adaptation an explicit platform transport contract and add real transport tests for bare, HTTP, HTTPS, and redirected endpoints. Namespace room/session routing keys, including keyspace if ring mode requires it. Serialize client creation or close losing instances, and provide bounded cache/resource disposal.

### F37 An agentPending receipt can remain pending permanently

**P2.** [RoomServiceImpl.kt:310](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:310), [RoomServiceImpl.kt:321](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:321), [RoomServiceImpl.kt:348](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:348), [LockerClient.kt:436](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/src/commonMain/kotlin/com/latenighthack/lockers/connector/LockerClient.kt:436)

The batch source transaction saves an agentPending receipt, then runs the agent outside that transaction. A crash or cancellation in between leaves that receipt pending. Retries correctly avoid executing the agent again, but there is no recovery worker or completion protocol to resume or resolve the pending task. Applications receive a committed source with indefinitely missing derived state.

**Evidence.** Confirmed by the source receipt, replay branch, and synchronous post-commit hook. Avoiding blind replay is intentional and protects an at-most-once extension; the unresolved gap is how pending work reaches a final state.

**Correction.** Define agent completion as a first-class durable workflow with a completion/status Flow. Give recoverable agents immutable request identity and an idempotent effect/derived-write contract before permitting replay. For arbitrary non-idempotent extensions, expose an explicit indeterminate outcome and reconciliation hook. Always preserve the committed source result, as required by F11.

### F38 Malformed identity and authority data are persisted and resource limits are incomplete

**P2.** [SessionServiceImpl.kt:581](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:581), [SessionServiceImpl.kt:546](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:546), [LockVerifier.kt:76](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/LockVerifier.kt:76), [LockVerifier.kt:196](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/LockVerifier.kt:196), [RoomServiceImpl.kt:501](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/room/v1/RoomServiceImpl.kt:501), [SessionServiceImpl.kt:401](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/services/session/v1/SessionServiceImpl.kt:401)

Session create checks only null key/ID values and accepts empty or undecodable public keys; open decodes the stored key outside its verification try block. Lock establishment does not validate the new key or scope enum. A structurally public-keyed room with invalid key bytes falls back to TOFU rather than rejection. Initial versions accept arbitrary signed values and later increments can overflow. Legacy writes cap the locker body but not the complete request/notification; single gateway posts, aggregate room snapshots, and inbox-open responses lack a consistently bounded or paged envelope.

**Evidence.** The ping diagnostic created a session with an empty public key and received OK. The other malformed-input and unbounded aggregate paths were established by inspection. No particular transport frame ceiling or dependency CVE is assumed here.

**Correction.** Validate canonical IDs, key encodings, known scopes, supported envelopes, versions, and complete encoded sizes at the trust boundary. Reject invalid public-keyed room encodings explicitly. Bound recipients, sessions, notification bytes, and global admission/storage consumption, and paginate snapshots/inbox replay with stable cursor semantics. Return precise protocol errors rather than poisoning persisted state or throwing decoder exceptions.

### F39 Build reproducibility and target tests do not cover the published contract

**P2.** [libs.versions.toml:1](/Users/mikeroberts/workspace/latenighthack/kitkit/gradle/libs.versions.toml:1), [build.gradle.kts:30](/Users/mikeroberts/workspace/latenighthack/kitkit/api/build.gradle.kts:30), [fh-workspace.settings.gradle:17](/Users/mikeroberts/workspace/latenighthack/kitkit/gradle/fh-workspace.settings.gradle:17), [Dockerfile:6](/Users/mikeroberts/workspace/latenighthack/kitkit/Dockerfile:6), [build.gradle.kts:55](/Users/mikeroberts/workspace/latenighthack/kitkit/connector/build.gradle.kts:55), [ci.yml:50](/Users/mikeroberts/workspace/latenighthack/kitkit/.github/workflows/ci.yml:50)

The reviewed working tree cannot resolve ktstore-library:0.2.0 from its configured released repositories. Explicit Fullhouse workspace substitution made validation possible, but is a different dependency coordinate. In workspace mode, PREFER_SETTINGS suppresses Kotlin's Node distribution Ivy repository and jsNodeTest cannot resolve org.nodejs:node:24.9.0. The Docker build installs neither Go nor protoc-gen-kt even though a clean build needs that generator. Local generation uses whichever executable is on PATH, whereas CI pins a version. Connector test dependencies and cases live only in jvmTest; the Apple test task has no sources, and fake transport/storage tests do not validate every published platform.

**Evidence.** Recorded dependency and Node resolution failures, followed by successful JVM/PostgreSQL tests and JS/Android/Apple compilation using the explicit pinned ktstore workspace dependency. Docker generator absence and platform test coverage were established by inspection.

**Correction.** Complete the dependency publication/version transition and verify released-mode resolution before release. Preserve isolated workspace repositories while allowing toolchain distribution resolution. Pin and provision the same generator in local, CI, and container builds; include all generator/compiler inputs in caching. Move portable protocol/reducer tests to commonTest and add real platform storage/transport tests plus Fullhouse consumer checks for affected variants.

### F40 A topology priming error terminates polling while readiness can later report success

**P2.** [PostgresShardMapSource.kt:120](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/cluster/PostgresShardMapSource.kt:120), [BlueprintV.kt:57](/Users/mikeroberts/workspace/latenighthack/kitkit/server/src/main/kotlin/com/latenighthack/lockers/server/cluster/BlueprintV.kt:57)

PostgresShardMapSource.bind calls its first refresh outside the retrying loop. A transient database or table-read failure on that call ends the job permanently. Later database recovery cannot restart polling. BlueprintV readiness checks only database connectivity, so it can return ready while the router stays on its synthetic bootstrap map and never adopts control-plane updates.

**Evidence.** Confirmed by the unguarded first refresh, subsequent loop, and database-only readiness predicate. This concerns ring mode; claim ownership has a different coordination source.

**Correction.** Put initial loading and later refreshes in the same cancellation-aware recovery pipeline. Publish Loaded, Retrying, and Failed/stale source state alongside the map, and derive readiness from the required initial load and its freshness policy. Test an initial read failure followed by recovery without a membership change.

### F41 Resolved JDBC and network dependencies need security patching and version alignment

**P2.** [libs.versions.toml:19](/Users/mikeroberts/workspace/latenighthack/kitkit/gradle/libs.versions.toml:19), [Library runtime dependencies](/Users/mikeroberts/workspace/latenighthack/kitkit-review-2026-10-08/evidence/server-runtime-dependencies.log), [Application runtime dependencies](/Users/mikeroberts/workspace/latenighthack/kitkit-review-2026-10-08/evidence/server-app-runtime-dependencies.log)

The server application resolves PostgreSQL JDBC 42.7.3, while the library graph alone resolves 42.3.1 through the workspace ktstore dependency. Both are in the affected range for the SCRAM authentication CPU-exhaustion advisory fixed in 42.7.11. Exploitation requires SCRAM and a malicious or impersonated database endpoint; loginTimeout does not necessarily stop the worker's CPU work. [Maintainer SCRAM advisory](https://github.com/pgjdbc/pgjdbc/security/advisories/GHSA-98qh-xjc8-98pq)

The library's 42.3.1 also predates the fix for CVE-2024-1597. That SQL injection requires non-default simple query mode and a specific user-controlled negated numeric/string query pattern. The application override to 42.7.3 contains that fix, and this review does not establish an exploitable default SQL path. [Maintainer SQL injection advisory](https://github.com/pgjdbc/pgjdbc/security/advisories/GHSA-24rp-q3w6-vc56)

Netty resolves a mix of 4.1.104.Final modules, including HTTP/2, and 4.1.116.Final core/handler modules. The HTTP/2 version falls in the range for a maintained server frame-flood advisory; this service uses CIO for its public listener and Netty for outgoing dependencies, so the server-side advisory is not evidence of an exposed Netty HTTP/2 listener here. [Maintainer HTTP/2 advisory](https://github.com/netty/netty/security/advisories/GHSA-w9fj-cfpg-grvv)

**Evidence.** Established from both resolved Gradle runtime graphs and the maintainers' affected-version/condition statements. The graph also resolves coroutines core 1.10.1 despite the catalog's 1.9.0 request; review and test the resolved versions rather than assuming catalog pins win.

**Correction.** Update the JDBC dependency at its owning library boundary and retain an appropriate application constraint, using a currently patched supported release that covers applicable advisories. Align related Netty modules and update their parent dependencies with protocol tests. Add resolved-artifact advisory checks with documented reachability conditions; do not treat every transitive advisory as an exploitable application defect.

### F42 Documentation and quality gates disagree with the implementation

**P3.** [README.md:137](/Users/mikeroberts/workspace/latenighthack/kitkit/README.md:137), [README.md:148](/Users/mikeroberts/workspace/latenighthack/kitkit/README.md:148), [gradle.properties:15](/Users/mikeroberts/workspace/latenighthack/kitkit/gradle.properties:15), [build.gradle.kts:26](/Users/mikeroberts/workspace/latenighthack/kitkit/build.gradle.kts:26), [LockerSigning.kt:10](/Users/mikeroberts/workspace/latenighthack/kitkit/api/src/commonMain/kotlin/com/latenighthack/lockers/common/LockerSigning.kt:10)

README describes room/locker authorization as entirely pending and the connector/API as currently JVM-only, despite lock authority and additional targets being implemented. The repository license is MIT, while publication metadata says Apache 2.0 and retains a comment claiming no LICENSE file exists. Detekt is explicitly advisory and can never fail the build. These inconsistencies make the real supported/security contract difficult for consumers to determine. LockerSigning's documentation specifies four-byte length prefixes, while Writer.bytes actually uses an eight-byte long, which can mislead external protocol implementations.

**Evidence.** Confirmed against current build configuration, LICENSE, README, and POM properties. The licensing discrepancy is a factual metadata inconsistency; this review does not choose a license on the owner's behalf.

**Correction.** Align README and publication metadata with the intended current contract, including lock semantics, read access, delivery modes, supported targets, and extension trust. Adopt a reviewed static-analysis baseline and enforce important checks over actual KMP source sets. Document transform purity/retry behavior; the single-write retry path currently reevaluates transform and codecs even when reusing a frozen submitted request. Publish exact canonical signing vectors and correct the length-prefix specification.

## Flow and coroutine design changes

The fast source/outbox transaction, generation-checked SubscriptionController, stable event identities, and version-aware cache acceptance are useful foundations. The storage definitions also provide an explicit composition boundary before opening a database. Extend those patterns consistently across every supported path.

| Design boundary | Current gap | Target invariant |
| --- | --- | --- |
| Component lifecycle | Independent root jobs and fire-and-forget close | One owned scope with an explicit parent, observable child failures, and cancel-and-join shutdown |
| Authoritative mutation | Process-local serialization and separately persisted authority/content | One transaction validates owner incarnation and canonical expected version, then commits authority, content, receipt, and delivery intent |
| State observation | Save followed by a transient change emission | A Flow of committed immutable state can recover from a missed wakeup |
| Work discovery | Local SharedFlow emission after queue save | Durable due rows determine work; bounded claims, notifications, and recovery polling discover it |
| Registration and subscriptions | Different confirmation/retry models | Persisted desired state reduced by revision and connection incarnation |
| UI snapshots | Partial history and observer backpressure in acceptance | Complete snapshots with version/watermark ordering, conflated state, and independent observer lifetimes |
| Notifications and broadcasts | Ephemeral payload events mixed into ACK acceptance | Explicit variants, preserved codec context, and a defined persisted delivery/cursor contract |
| Agent completion | Synchronous hook after source commit | Observable source-commit and derived-work outcomes with an explicit recovery contract |

**Own asynchronous work at the component boundary.** Accept a parent CoroutineScope and execution contexts, create an owned child job, and retain handles for long-lived loops. Use supervision where independent work is intended, with a real failure policy. A deliberate shared read may outlive one waiter, but must not outlive the client. Make Closed and Failed observable to every await API. Await required teardown before callers close stores or dispatchers.

**Reduce persisted intent serially.** Use the subscription reducer's generation checks for push registration as well. Treat register, rotate, unregister, session changes, RPC acknowledgements, and retry ticks as inputs to one reducer. Persist intent before acknowledging an accepted API command. Capture desired revision and connection epoch in each network job, and apply only acknowledgements that still match. A bounded mailbox can carry commands; a conflated wakeup can carry “re-read durable intent.” Channels and Flow are both appropriate when their ownership, buffering, and delivery semantics are explicit.

**Expose committed state through a storage observation seam.** The current store interfaces are suspend CRUD, so this requires an actual observation contract rather than a cosmetic Flow wrapper. Define transaction-aware room/locker snapshots and versioned updates, including tombstones. Emit a complete initial state and reduce newer events around a snapshot watermark. Make subscriber absence and cancellation harmless to correctness. UI consumers can conflate state; durable notifications need a cursor or inbox whose progress is independent of rendering.

**Use durable workers as recoverable Flow pipelines.** Subscribe to change hints before an initial scan, claim a bounded set of due rows, process with a fixed concurrency limit, and persist outcome/retry time. Always include a recovery scan for lost hints, worker restarts, and other replicas. Catch recoverable errors inside the worker iteration and expose terminal failures. A Flow of “work is available” is useful only when the database remains able to rediscover the work.

**Separate transport acceptance from application observation.** Define precisely when a session event is accepted: locker state and event dedup/ACK intent must be committed before acknowledging the server. Persist notification work if its delivery is promised. Slow observers, absent observers, or a codec failure must have a stated policy and must not implicitly decide whether all rooms can progress. Define replay and deduplication per public Flow; a replay-zero notification stream is an ephemeral API.

**Preserve authority and compatibility during the redesign.** Validate ambiguous envelopes immediately without requiring a wire change. New authority incarnations, authenticated metadata, and signing domains need negotiated/versioned protocol support, golden signing vectors, and migration coverage. Retain frozen storage definitions and introduce explicit migrations for new authority, inbox, and workflow state. Keep privileged agents and server extensions explicit, with lifecycle and scope boundaries; do not remove their legitimate ability to derive authoritative state. Change source schemas or upstream generators when needed, never generated bindings.

## Recommended implementation order

1. Close the authority and trust-boundary gaps in F01–F09: hierarchy, envelope validation, replay prevention, peer gateways, Web Push destinations, session proof, coherent authorization reads, commit fencing, and the demo default.
2. Make ratchets and source outcomes recoverable in F10–F16. Resolve version creation, canonical identity, and tombstone parity alongside transaction work. A committed source must remain committed in the client's result even if an agent is unresolved.
3. Unify durable delivery, push queue claims, and controller reconciliation in F17–F22. Make persisted state sufficient to recover after a missed local signal or another replica's enqueue.
4. Rework observation and lifecycle in F23–F32: complete state snapshots, independent acceptance/ACKs, cancellation propagation, connection state, heartbeat, attachment cleanup, lease recovery, supported ownership units, ordered shutdown, and the topology source recovery in F40.
5. Bound storage/work discovery, complete notification/agent semantics, and close validation/build gaps in F33–F42. Verify release-mode dependencies and Fullhouse consumers before publication.

Protocol and schema changes should have focused fixtures for old and new clients. More Flow operators alone will not repair the authority or transaction invariants.

## Test gaps to close

The existing passing suites should be retained. Add tests that assert the desired opposite of each diagnostic, then extend them around failure boundaries:

- Authority hierarchy: every parent/child scope, unsigned establishment under a parent, ambiguous envelopes, malformed keys, stale grants/unlocks, key reuse, and scope revocation policy.
- Distributed mutation: pause an owner between resolve, queue entry, validation, and commit; expire or release its claim; let a successor write; then resume it. Assert one valid owner and one successful expected-version transition, including delete and ratchet.
- Client acceptance: cancel between storage commit and observer wakeup; pause or remove collectors; restart watchers; hydrate a complete snapshot concurrently with newer events; repair offline deletes. Compare visible state against committed storage.
- Controller reconciliation: late responses after unregister or key rotation, reconnect with the same session ID, replacement session, failure before confirmation, and stop with pending intent. Use a virtual clock and injected contexts where possible.
- Delivery workers: enqueue from another replica, enqueue during startup scanning, duplicate wakeups/drain, lease loss, process death after provider acceptance, large backlogs, and startup database failure.
- Session lifecycle: concurrent create/open, attach failure, reattach before old cleanup, nullable JDBC pool slot draining, signed revocation, actual ping/pong deadlines, and an awaiter observing terminal failure or close.
- Extension outcomes: crash after source commit, cancellation during derivation, indeterminate non-idempotent effects, replay-safe completion, and committed ratchet adoption after agent failure.
- Target parity: portable commonTest cases plus real JVM/Android/JS/Apple storage and transport tests, generated protocol fixtures, and Fullhouse consumer validation using immutable isolated dependency pins.

Keep source-commit correctness tests separate from performance claims. The scan/retention findings predict growth from code; load and profiling work should measure realistic backlog, subscriber count, and lock contention.

## Trust decisions that need an explicit contract

Room and locker reads currently remain broadly available by identifier. The signed “sealed” representation carries a cleartext enclosure; encrypted payload mode is marked deferred in the protocol. State whether confidentiality and read authorization belong to this library or to consumer encryption/access layers.

State whether session IDs are public names or bearer capabilities. The signature-based session open design points toward public names with key proof; unary methods must follow the same model.

State what a parent rotation or revocation means for already-established child grants, how long receipts and ACK deduplication remain valid, and how an unresolved agent result is reconciled. These decisions determine safe storage retention, protocol migration, and public Flow replay semantics.
