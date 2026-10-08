# Lockers operational alerts

Install only `lockers.yml` as rules. Supply notification routing in your existing Alertmanager/Grafana configuration. Select the deployment in the corresponding dashboard before investigating. Missing monitoring data is unavailable data, not a healthy zero.

| Alert | Starting condition | Investigation |
|---|---|---|
| Unexpected RPC errors | Above 1% with at least 1 request/s, sustained 10 minutes | Inspect service/operation outcomes and matching logs/traces. Cancellation and protobuf rejection are separate. |
| Delivery stalled | Pending durable intents and no accepted gateway groups for 5 minutes, sustained another 5 minutes, applicable worker enabled | Confirm fresh backlog samples, gateway discovery and worker activity; check timeouts/renewals. Recovery must preserve room ordering and existing leases. |
| Push stalled | Pending pushes and no successful sends for 10 minutes, sustained another 10 minutes, push worker enabled | Check configured APNS/FCM/Web Push providers, rejection classes, concurrency and retry policy. Preserve the existing single push drainer requirement. |
| New dead letters | Dead-letter arrivals in the last 5 minutes | Diagnose provider errors. Inspect/redrive using existing protected management APIs; do not modify queue records manually. |
| Agent failures | Agent exceptions in the last 5 minutes | Inspect agent latency and traces. A source write may already be durable; an agent failure does not mean the source write failed. |
| Claim renewal overdue | Last successful renewal older than twice the configured interval, sustained 10 seconds, claim mode enabled | Check database connectivity and renew failures before approaching the claim TTL; preserve demotion/fencing behavior. |
| Backlog telemetry stale | Last snapshot older than 60 seconds or unhealthy, sustained 1 minute | Inspect refresh failures/database availability. Cached depths are not current. |

Tune these durations/thresholds to measured traffic and operation budgets. Sparse deployments may need a lower RPC traffic gate. Queue stall alerts deliberately allow transient backoff; they do not declare every nonempty queue unhealthy. Disabled components do not trigger their gated alerts. Connector metrics are best effort; use its telemetry loss and upload panels before drawing fleet conclusions. Correlate trace IDs through the existing Loki/Tempo data-source settings.
