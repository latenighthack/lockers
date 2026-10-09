# F05 — Web Push destination policy

Repro: WebPushEndpointTest supplies loopback, metadata, arbitrary-host, lookalike-host, credential and nonstandard-port endpoints. Before the fix these reached sender initialization and returned Retryable; the regression expected rejection before any sender/network work and failed.

Fix: only HTTPS:443 endpoints on explicit operator-trusted push provider domains are admitted. Credentials/fragments are rejected; every resolved DNS address must be public. Default trusted hosts cover Google, Mozilla, Apple, and Windows notification providers. WEBPUSH_ENDPOINT_HOSTS explicitly configures other trusted operators. Browser supplied attacker domains cannot enter the allowlist. The pinned webpush sender creates a JDK HttpClient with default NEVER redirect behavior (verified vendor bytecode), so redirects are not followed outside the policy.

Validation: WebPushEndpointTest red then green, two tests including injected DNS mixed public/private addresses and exact wildcard domain boundaries. No external network requests were needed.
