# F17 — authoritative recipients across replicas

Repro: SubscriptionFreshnessTest primes writer A's empty cache, subscribes through B, then writes through A. Baseline omitted the new subscriber. It also checks that a later unsubscribe through B prevents delivery from A.

Fix: legacy recipient lookup always reads the durable subscription registry; local cache entries are observational snapshots. Durable batch commits already snapshot recipients transactionally; the root's F08 unified source transaction keeps that contract for every mutation mode.

Validation: SubscriptionFreshnessTest red then green (subscribe and unsubscribe across two independent services over shared storage).
