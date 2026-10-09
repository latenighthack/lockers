# F04 — preserve authentication in public convenience routing

A real HTTP regression reproduced privileged SessionGateway routes on the default
Routing.monolith helper despite the runnable's split listener fix. The Fullhouse
public listener uses that helper. Default monolith routing now mounts public
client routes and extensions only; peers require the authenticated internal
listener and admin services require explicit internal mounting.

All-service fixtures move to an explicitly trusted helper in the server:test
artifact. Local discovery remains unchanged. Production consumers must mount
monolithAdmin and monolithPeer on their private listener, with a shared peer
credential and a private advertise address. A real default-router HTTP test first
fails (200) then passes (404); private bad credentials remain 401. Logs:
/tmp/lockers-F04-convenience-red.log and
/tmp/lockers-F04-convenience-green.log. Consumer migration remains a final gate.
