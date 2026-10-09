# F26 / F33 / F36 — server peer and forwarding transport ownership

Baseline `PeerConnectionOwnershipTest` proved eviction/close discarded an owned AutoCloseable transport without disposal (`/tmp/server-peer-repro.log`). Both gateway and room-forwarding caches also retained unbounded addresses and hid HTTP ownership behind authentication wrappers.

PeerConnectionPool now bounds independently owned transports (64 by default) and active calls (1024). Returned lightweight stubs acquire a transport lease per operation. Concurrent factories serialize; active eviction retires a lease without interrupting its request, counts against capacity, and disposes on completion. Idle LRU eviction finishes disposal before replacement. Underlying ownership survives the peer-token decorator. Shutdown closes admission, cancels calls, closes transports, and suspend closeAndJoin joins calls and disposal once, including concurrent shutdown and cleanup failure aggregation.

Room forwarding uses the same bounded pool and evicts failed endpoints. Room service joins it at shutdown. ClaimContext and runnable claim/ring host shutdown await disposal before releasing coordination resources. Custom transports have explicit close/join hooks; AutoCloseable works by default. Canonical full HTTP endpoints replace the obsolete schemeless workaround.

Four ownership regressions cover concurrent authenticated reuse, explicit disposal, active-eviction capacity, and cancellation/EOF of a real held HTTP socket. Eight real two-node claim integration tests cover forwarding, offline delivery and failover; all passed with run-host compilation (`/tmp/server-peer-final.log`). An existing asynchronous metric assertion now waits for the posted counter after inbox persistence becomes observable.
