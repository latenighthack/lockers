# F06 — authenticated destruction and permanent session revocation

A new discovered regression first failed because an authenticated destroy request
returned OK while the session and its state survived. The fixed implementation
reserves the identity permanently and erases session authority, subscriptions,
inbox plus full-event metadata, push registration and queued push work inside the
same session-authority transaction. Signed retries after revocation cannot
mutate state, and session creation cannot reuse the destroyed identity. Permanent
reservations prevent delayed delivery intents from reaching a replacement user.

Local streams receive a nonblocking close hint. The owned stream Flow also polls
shared authority at one-second intervals so destruction on a different gateway
closes the connection. F29 separately proves remote shared-store revocation and
attachment cleanup. F38 accounts for reserved identities in admission limits.

The authenticated destroy regression, proof replay/substitution, concurrent
session challenge consumption, stream setup cleanup, notification metadata,
push claim cleanup, malformed authority, and four actual PostgreSQL room fences
pass together with PostgreSQL required. Evidence logs:
/tmp/lockers-F06-revoke-red.log and /tmp/lockers-F06-integrated-green.log.
Final full-suite, platform and consumer verification remains pending.
