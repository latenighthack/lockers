# F04 — internal authenticated gateways

Repro: public service inventory contained privileged SessionGateway and PushGateway. `GatewayBoundaryTest` failed on baseline. The added real CIO HTTP test caught a second defect: ktbuf-server 1.1.10 `serveAll` discards contextProcessor for unary methods. Explicit `serveUnary` registration now rejects missing/wrong peer credentials with HTTP 401 before handler dispatch. All three regressions pass; server runnable compiles.

Fix: public routes include client services and extensions only. Gateways and forwarded room methods use `monolithPeer(component, token)` on the internal admin listener. Local discovery and the all-services test harness remain available. Production multi-node startup requires LOCKERS_PEER_TOKEN; peer transports supply it. Constant-time byte comparison checks the credential.

Deployment migration: LOCKERS_ADVERTISE_ADDR must identify the internal LOCKERS_ADMIN_PORT listener, and all participating replicas must share LOCKERS_PEER_TOKEN. Keep that listener private; use TLS through the private service mesh when peers cross an untrusted network. User/client domains continue using LOCKERS_HTTP_PORT. Embedder hosts explicitly mount monolithPeer on their internal authenticated listener.

Validation: `./gradlew :server:test --tests '*GatewayBoundaryTest' :server:run:compileKotlin`, explicit review fhWorkspace/isolated Maven repository. Public inventory red; HTTP callback bypass red; final 3 tests green.
