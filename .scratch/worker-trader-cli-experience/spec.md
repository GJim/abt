# Worker and Trader CLI Experience

Status: ready-for-agent

## Problem Statement

`abt-worker` currently requires repeated command-line arguments for enrollment and reconciliation, so an approved account worker cannot simply start reconciliation from its previously established identity. The user needs an interactive enrollment flow that persists non-secret identity settings and makes `reconcile` directly runnable.

The system also needs a native Windows `abt-trader` CLI that follows the same device-identity model while serving the distinct Trader role: enrollment, certificate retrieval, authenticated WSS connection, heartbeat, event delivery, acknowledgement, reconnection, and certificate rotation. The current Trader control-plane contract requires an attestation issuer that the intended Windows CNG deployment cannot supply; it must be aligned with the existing Worker CNG proof boundary.

## Solution

Provide native Windows Worker and Trader CLIs backed by non-exportable Windows CNG ECDSA P-256 device keys. Enrollment interactively collects absent non-secret fields, preserves flag-based automation, and writes only non-secret identity data next to the launched executable. The CLI never persists MT5 passwords, registration invites, private keys, or Trader certificates.

Worker `reconcile` and Trader `connect` read their saved identity configuration. Pending enrollment is reported clearly until an administrator approves it. Trader `connect` is a foreground WSS client that prints lifecycle events as JSON Lines, persists only a local acknowledgement mirror, and relies on the control plane as the authoritative replay cursor.

Unify the control plane’s Worker and Trader device-identity requirements on Windows CNG proof. Remove Trader attestation JWT and attestation trust-root requirements without retaining a compatibility branch. Both roles use 30-day certificates, automatically attempt rotation after half their validity period, and retain a one-hour old-certificate overlap before retiring old CNG keys.

## User Stories

1. As a Worker operator, I want `abt-worker enroll` to prompt for missing enrollment values, so that I can enroll without memorising a long command line.
2. As a Worker operator, I want supplied enrollment flags to take precedence over prompts, so that automated deployment remains possible.
3. As a Worker operator, I want the MT5 password prompt to use the same visible input behavior as the other enrollment fields while remaining absent from application output and files.
4. As a Worker operator, I want successful enrollment to save my controller URL, account binding, enrollment ID, and CNG key name, so that I can subsequently run `abt-worker reconcile` without repeating them.
5. As a Worker operator, I want `reconcile` to report that my enrollment is awaiting approval, so that I know not to re-enroll or reuse an invite prematurely.
6. As a Worker operator, I want my enrollment invite excluded from saved files, so that a one-time admission credential cannot be recovered locally.
7. As a Worker operator, I want existing identity configuration protected from accidental replacement, so that an active worker is not displaced by an unintended new enrollment.
8. As a Worker operator, I want explicit `--replace-config` acknowledgement before a successful new enrollment replaces saved identity, so that replacement is deliberate.
9. As a Worker operator, I want to select a non-default configuration through `--config`, so that multiple independent deployments can share a Windows user safely.
10. As a Worker operator, I want a clear failure when the executable directory cannot hold the default configuration, so that the CLI never silently writes identity data somewhere unexpected.
11. As a Worker operator, I want the default configuration and runtime state files beside the launched executable, so that a self-contained deployment has predictable local files.
12. As a Worker operator, I want configuration writes to be atomic and versioned, so that interruption never yields a partly accepted identity.
13. As a Worker operator, I want corrupted or missing identity configuration to fail clearly, so that reconciliation cannot connect under guessed account details.
14. As a Worker operator, I want a persistent, non-exportable Windows CNG key, so that the same device can prove its approved identity without exposing its private key.
15. As a Worker operator, I want a role-specific default CNG key name, so that Worker and Trader identities cannot accidentally share a key.
16. As a Worker operator, I want re-enrollment to reuse my role’s existing key unless I explicitly supply a new key name, so that ordinary enrollment retries do not create unmanaged keys.
17. As a Worker operator, I want certificate rotation to happen automatically after half of a certificate’s 30-day lifetime, so that reconciliation continues without routine manual renewal.
18. As a Worker operator, I want rotation attempted on connection and then checked every 24 hours while the certificate is past half-life, so that transient control-plane outages have ample recovery time.
19. As a Worker operator, I want the old certificate to remain usable for one hour after successful rotation, so that the new configuration can take effect without an avoidable availability gap.
20. As a Worker operator, I want retired CNG keys removed only after the overlap ends, so that a key is never deleted while its certificate remains valid.
21. As a Worker operator, I want failed retired-key cleanup to warn and retry daily, so that key cleanup is durable without disrupting a valid new identity.
22. As a Trader operator, I want `abt-trader enroll` to interactively collect controller URL, invite, strategy name, and public-IP declaration, so that I can establish a Trader identity without constructing protocol requests.
23. As a Trader operator, I want the CLI to detect a public IP through the configured default HTTPS echo service and ask me to confirm or replace it, so that the submitted address remains my declaration.
24. As a Trader operator, I want detection failure to fall back to manual public-IP input, so that enrollment is not blocked by an external echo service outage.
25. As a Trader operator, I want `--public-ip` to bypass public-IP detection, so that managed deployments do not make the echo-service request.
26. As a Trader operator, I want the Trader’s strategy name and declared public IP saved as non-secret enrollment identity data, so that intentional re-enrollment can preserve its declared context.
27. As a Trader operator, I want Trader enrollment to use the same Windows CNG P-256 device-key proof as a Worker, so that the supported local trust boundary is consistent.
28. As a Trader operator, I want no attestation JWT or external attestation issuer dependency, so that native Windows CNG enrollment can complete using the approved device-proof model.
29. As a Trader operator, I want `abt-trader connect` to report pending approval clearly, so that I know when an administrator must act before a WSS session can start.
30. As a Trader operator, I want `connect` to run in the foreground until I stop it, so that it can maintain my authenticated Trader WSS session.
31. As a Trader operator, I want each connection to retrieve the current approved certificate into memory, so that a certificate is not persisted in local configuration.
32. As a Trader operator, I want 30-second bidirectional heartbeats, so that the control plane can track Trader session health.
33. As a Trader operator, I want interrupted WSS connections to reconnect and receive replayed lifecycle events, so that temporary network failures do not lose my event stream.
34. As a Trader operator, I want every received lifecycle event emitted as one JSON Lines record on standard output, so that a strategy process can consume it reliably.
35. As a Trader operator, I want diagnostics written to standard error, so that they do not corrupt the event stream.
36. As a Trader operator, I want an event acknowledgement sent only after its JSON Lines record is written successfully, so that an interruption produces an allowed duplicate rather than a lost event.
37. As a Trader operator, I want the local state file to mirror only control-plane-acknowledged cursors, so that the local cursor never claims an event that the control plane has not accepted.
38. As a Trader operator, I want the control plane to remain the replay-cursor authority, so that a missing or corrupted local state file cannot cause skipped events.
39. As a Trader operator, I want event replay duplicates to retain immutable event IDs, so that my downstream consumer can de-duplicate at-least-once delivery.
40. As a Trader operator, I want the same automatic certificate-rotation, one-hour overlap, and retired-key cleanup lifecycle as a Worker, so that both device roles have consistent operational safety.
41. As a control-plane administrator, I want Worker and Trader enrollment to retain administrator approval, role-bound single-use invites, certificate binding, challenge-response, and revocation, so that simplifying the local CNG contract does not remove the existing trust controls.
42. As a control-plane administrator, I want a valid unrevoked old certificate and proof of its private key to authorize rotation without a second approval, so that normal 30-day renewal does not introduce operational downtime.
43. As a control-plane administrator, I want revoked or expired device certificates to be unable to rotate or open new sessions, so that revocation remains authoritative.
44. As a maintainer, I want the updated identity model recorded as an architectural decision, so that future work does not reintroduce an unsupported Trader attestation issuer requirement.

## Implementation Decisions

- Add an `abt-trader` native Windows command-line entry point. Its first release contains `enroll` and foreground `connect`; submitting common trading intents and cancelling them from the CLI are out of scope.
- Keep Worker commands as `enroll` and `reconcile`. Enrollment prompts for every missing required enrollment field while preserving existing command-line flags for non-interactive use.
- Treat `enrollment_id` as the shared external CLI and configuration identifier for both roles. It is not a secret; it identifies a pending or approved enrollment.
- Store each role’s default identity configuration beside the launched executable as `worker.json` or `trader.json`. `--config` selects an alternate configuration path. If the default directory is not writable, fail and require `--config`; do not fall back to another directory.
- Do not add extra ACL protection to configuration or state files. Consequently, neither may contain passwords, invites, private keys, attestation tokens, or certificates.
- Identity configuration is versioned JSON written through an atomic replacement. It contains only the controller origin, enrollment identity and role-specific non-secret binding data, and the current CNG key name.
- Enrollment refuses to overwrite an existing configuration unless `--replace-config` is explicitly supplied. A failed or pending replacement enrollment does not change the old configuration.
- Use distinct default CNG key names for Worker and Trader. Re-enrollment reuses the role’s saved key unless enrollment explicitly receives a replacement key name. The CLI never silently deletes an existing CNG key.
- Give each identity configuration a colocated mutable state file derived from its configuration path. Worker state holds retired-key cleanup work. Trader state holds the acknowledged-cursor mirror plus retired-key cleanup work.
- Trader state is not an event-replay authority. After a successful connection, the control plane’s persisted Trader cursor determines automatic replay. A missing or corrupt local Trader state file emits a warning but must not prevent a safe connection.
- Trader public-IP discovery uses `https://api.ipify.org`, validates a single textual IP response, and requires user confirmation or replacement before enrollment. `--public-ip` bypasses discovery; a discovery failure prompts for manual input.
- Trader certificates are retrieved through the approved enrollment and CNG proof flow for each connection and are held only in memory. The same current certificate is used immediately to authenticate the WSS session.
- Standardize Worker and Trader device identity on non-exportable native Windows CNG ECDSA P-256 keys. Remove the Trader `attestation_jwt` request field, issuer trust-root validation, and compatibility handling from enrollment and rotation contracts.
- Preserve administrator approval, role-bound single-use invites, public-key challenge-response, certificate binding, expiry, rotation, and revocation as the control-plane trust controls.
- Issue both Worker and Trader certificates for 30 days. Once a certificate passes half its lifetime, a successful connection attempts rotation immediately and a long-lived connection rechecks every 24 hours. The final 24 hours retain the daily cadence.
- Rotation requires both the valid, unrevoked old key and the replacement CNG key to prove possession. It does not require administrator approval. The control plane records the rotation as an immutable audit event.
- A successful rotation atomically makes the new key current and leaves the old certificate valid for one hour. The state file records the retired key and its earliest cleanup time. Once eligible, cleanup is attempted; failures are reported and retried every 24 hours.
- Expired certificates cannot establish or resume sessions. The CLI reports the condition explicitly and never uses an expired certificate as a fallback.

## Testing Decisions

- Primary acceptance tests exercise public CLI commands and public control-plane REST/WSS contracts, not private helpers or internal storage details.
- Worker CLI tests inject the existing MT5, CNG keystore, enrollment transport, output, clock, filesystem, and sleep seams. They verify interactive prompting, flag precedence, secret exclusion, configuration replacement protection, direct reconciliation from saved identity, pending-approval output, and safe configuration failures.
- Trader CLI tests use injected CNG, HTTP/WebSocket, output, clock, filesystem, and timer seams. They verify interactive enrollment, confirmed/overridden public-IP declaration, public-IP failure fallback, certificate retrieval, foreground session behavior, heartbeat, reconnection, JSON Lines event emission, acknowledgement ordering, and safe local-state recovery.
- Certificate lifecycle tests verify both Worker and Trader through their public rotation contracts: half-life triggering, daily checks, rejected/revoked/expired old certificates, proof of old and replacement keys, atomic identity switch, one-hour overlap, and durable old-key cleanup retry.
- Control-plane API tests verify that Trader enrollment and rotation accept Windows CNG public-key proof without `attestation_jwt`, reject malformed or mismatched proofs, and preserve approval, invite, expiry, revocation, audit, and certificate-binding guarantees.
- Existing Worker enrollment and reconciliation tests are the prior art for native CLI dependency injection, password-safe output, MT5 evidence checks, and reconnect behavior. Existing control-plane Trader enrollment, certificate, WSS session, event ACK, and replay tests are the prior art for public REST/WSS contract coverage.
- Tests must prove observable behavior: no secret persistence or output, correct command outcomes, correct public request and response contracts, and safe replay/rotation behavior. They must not assert private implementation call order except where the externally observable safety property depends on it.

## Out of Scope

- Trader intent submission and Trader cancellation commands.
- Linux, TPM 2.0, PKCS#11, or non-Windows Trader and Worker key providers.
- A third-party or self-hosted Trader attestation issuer.
- Persisting MT5 passwords, registration invites, private keys, Trader certificates, or any secret in CLI files.
- Automatic fallback from an unwritable executable directory to a user-profile directory.
- Additional ACL hardening for CLI configuration and state files.
- Manual CLI rotation commands; rotation is connection-driven and automatic.
- Replacing the existing administrator approval, registration-invite, certificate revocation, or WSS authorization models.

## Further Notes

The control plane’s persisted Trader acknowledgement cursor is authoritative. Local state is intentionally only an acknowledgement mirror and retired-key cleanup ledger; it must never be used to skip control-plane replay.

This specification records the Windows CNG identity decision made in ADR-0006. The planned feature has not been implemented by publishing this specification.
