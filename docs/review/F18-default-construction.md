# F18 — durable delivery for direct service construction

Full integration reproduced a null outbox failure in direct RoomServiceImpl
construction: Kotlin resolved the constructor parameter in a worker property
initializer, bypassing the effective built-in outbox fallback. Explicitly select
the initialized property there. The existing two-replica subscription regression
now waits for durable worker delivery, proving the default constructor delivers
without an explicit outbox parameter and that replica subscription changes are
observed. The first broad run failed at construction; targeted rerun recorded in
/tmp/lockers-F18-default-green.log. Final integration remains pending.
