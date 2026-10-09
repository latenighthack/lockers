# F32 — Coordination resources and shutdown ordering

Two failing regressions reproduced an idle connection leaking behind an unallocated null pool slot, and graceful drain returning before cancellation cleanup of an active renewal finished.

Fix: pool closure marks/ closes admission first and drains ChannelResult values without treating null as end-of-channel. Borrowed connections are closed on return after shutdown, and closure is idempotent. Claim drain cancels and joins renewal before demotion/release; cancellation during release propagates. Runnable shutdown stops both listeners before service drain and leaves coordination pools available until services finish. F26 adds joined service/worker drain before ownership release.

Validation: ClaimResourceDrainTest, ClaimRenewalServiceTest, InMemorySessionGatewayStoreTest, RegistryIncarnationTest all passed (15 discovered tests); :server:run:compileKotlin passed. Pool test uses an actual DriverManager-registered driver and Connection proxy to exercise the concrete slot order; renewal test gates NonCancellable cleanup to prove the drain waits.
