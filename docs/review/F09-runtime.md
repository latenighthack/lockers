# F09 — explicit trusted agents

Regression: `DefaultAgentAuthorityTest` submits an untrusted keyspace 31 locker to the default graph and requires zero derived writes, while proving explicit ExampleLockerAgent installation remains supported. Baseline failed (one derived keyspace 30 write); fixed regression passed.

Fix: the composition root uses `LockerAgentRegistry.None`; embedders explicitly install trusted server agents through the existing override seam. Agents remain privileged server extensions.

Validation: `./gradlew :server:test --tests '*DefaultAgentAuthorityTest'` with the review's explicit ktstore workspace manifest (red then green, 1 test).
