# F33 retain unacknowledged stream delivery identities

Red reproduction: keep one old event unacknowledged while receiving/ACKing1030newer events with a two-event inbox budget. The stream LRU evicted the old identity and a matching delivery retry woke catch-up, emitting that old event again.

Per-watch delivered identities now remain until successful durable ACK deletion. ACK and delivery producers share a concurrent pending set. It is bounded by the configured per-session inbox capacity, and capacity cleanup removes only identities proved no longer pending; an unacknowledged identity is never evicted. Tracking happens before network emission, and ACK pruning happens only after the database deletion succeeds. This preserves gateway migration catch-up without repeating an already delivered pending event.

Validation: the end-to-end1030ACK stress test fails on the isolated LRU baseline and passes with pending tracking. Existing lost-gateway-response, ACK/replay, two-gateway wake, finite inbox paging and notification metadata stream regressions pass together.
