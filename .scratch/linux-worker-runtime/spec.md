# Linux Worker Runtime

Status: ready-for-human

## Problem Statement

`abt-worker` currently runs only on native Windows. Its MT5 adapter depends on
the Windows-only MetaTrader5 Python package, and its device identity depends on
Windows CNG. Operators need to run an account Worker on a Linux x86_64 host
without weakening the existing hardware-bound device identity, centralized MT5
credential handling, enrollment approval, certificate rotation, or broker
safety model.

MetaTrader 5 has no native Linux terminal or Python integration. Linux support
therefore needs a deliberate boundary between the native Linux Worker and a
same-host MT5 terminal running under Wine. The Linux host must also provide a
TPM 2.0 device; a software private-key fallback would not provide the
non-exportable device identity required of an approved Worker.

## Solution

Run the authoritative `abt-worker` process natively on Linux. It owns
enrollment, controller communication, Worker identity and state, reconciliation,
certificate rotation, and a non-exportable ECDSA P-256 device key held by TPM
2.0. Access the TPM-backed key directly through the TPM 2.0 TSS so the Linux
adapter matches the existing CNG keystore's public-key and signing contract
without introducing a token PIN or a second keystore abstraction.

Run the MetaTrader 5 terminal and the Windows MetaTrader5 Python package in the
same Linux host's 64-bit Wine environment. A dedicated Wine MT5 bridge exposes
only the MT5 operations required by the existing Worker contracts to the native
Worker through inherited stdin/stdout pipes. The bridge is not a Worker identity
authority and cannot connect to the controller on the Worker's behalf.

Preserve the public `abt-worker enroll` and `abt-worker reconcile` commands and
their controller contracts across Windows and Linux. Select the platform
keystore and MT5 adapter internally. Linux startup fails clearly when Wine, the
MT5 terminal, the bridge, TPM 2.0 TSS, TPM device, or configured key is
unavailable; it never falls back to a software key or a remote MT5 bridge.

## User Stories

1. As a Worker operator, I want to run `abt-worker enroll` and `abt-worker reconcile` natively on a Linux x86_64 host, so that the Worker host does not require Windows.
2. As a Worker operator, I want the Linux commands and configuration schema to match the Windows Worker experience, so that platform choice does not change the operational workflow.
3. As a Worker operator, I want the same-host MT5 terminal to run under 64-bit Wine, so that the Worker can use the supported Windows MetaTrader5 integration without requiring a separate Windows machine.
4. As a Worker operator, I want startup diagnostics to identify missing or unhealthy Wine, MT5 terminal, bridge, TPM 2.0 TSS, or TPM dependencies, so that deployment failures are actionable.
5. As a Worker operator, I want the Linux device private key generated and used inside TPM 2.0, so that approval remains bound to a non-exportable device identity.
6. As a Worker operator, I want the TPM-backed key to require no interactive PIN, so that the foreground Linux command matches the current Windows CNG experience.
7. As a control-plane administrator, I want Linux and Windows Workers to use the same ECDSA P-256 enrollment, certificate, challenge-response, rotation, revocation, and audit contracts, so that Linux support does not create a second controller identity path.
8. As a control-plane administrator, I want the Linux Worker to reject a missing TPM and any software-key fallback, so that it cannot silently enroll under a weaker local provider.
9. As a Worker operator, I want MT5 passwords received or entered by the native Worker to cross only the protected same-host bridge channel and remain memory-only, so that Linux support does not add credential persistence.
10. As a Worker operator, I want a bridge or Wine failure to fail the affected MT5 operation explicitly and trigger the existing safe reconnect behavior, so that infrastructure failure cannot appear as successful reconciliation or execution.
11. As a maintainer, I want Worker domain logic to depend on platform-neutral keystore and MT5 interfaces, so that Windows CNG, Linux TPM 2.0 TSS, native Windows MT5, and Wine MT5 remain replaceable adapters.
12. As a release owner, I want Linux acceptance exercised on a real TPM 2.0 host and a real Wine MT5 terminal, so that mocks alone cannot qualify Linux support.
13. As a maintainer, I want a blocking feasibility prototype to prove the pinned Wine and MT5 matrix, inherited-pipe bridge, and demo-account market-order roundtrip before production implementation begins, so that unsupported platform assumptions fail early.

## Implementation Decisions

- Support Ubuntu 24.04 LTS x86_64 only in the first release. ARM Linux, other distributions, macOS, WSL, and containers are not supported.
- Keep `abt-worker` as a native Linux Python process. Do not run the authoritative Worker process inside Wine.
- Run the MT5 terminal and a minimal Windows Python bridge inside one 64-bit Wine prefix on the same host. Remote bridge endpoints are forbidden.
- The native Worker directly launches and owns the Wine bridge child process. Use inherited stdin/stdout pipes for a versioned, length-prefixed, bounded request/response protocol; reserve bridge stderr for secret-safe diagnostics. Do not create a socket or TCP listener.
- Reject unknown protocol versions, operations, fields, oversized messages, malformed responses, and mismatched response IDs. Permit only one MT5 operation in flight per bridge.
- Pass MT5 passwords to the bridge only for the operation that requires them. Neither side writes passwords to configuration, state, command-line arguments, environment variables, logs, crash messages, or protocol diagnostics.
- Treat bridge EOF, timeout, malformed output, Wine process exit, and MT5 initialization failure as explicit operation failures. Never synthesize empty MT5 evidence or success-shaped responses.
- Keep controller HTTPS/WSS communication, device signing, certificate handling, reconciliation policy, effect journaling, and Worker state in the native Linux process. The Wine bridge receives no registration invite, device private key, device certificate, or controller credential.
- Add a Linux TPM keystore implementing the existing hardware-keystore contract directly through `tpm2-pytss` ESAPI. Generate an ECDSA P-256 signing-only key with `fixedTPM`, `fixedParent`, and `sensitiveDataOrigin`; never expose or export its private scalar.
- Use empty TPM object authorization to provide the same no-PIN experience as the current Windows CNG provider. Restrict key use through one dedicated Unix account, private state permissions, and access to `/dev/tpmrm0`.
- Persist the TPM public blob and TPM-wrapped private blob as opaque Worker state. These blobs are bound to the originating TPM and parent and are not a portable private-key backup.
- Preserve Windows CNG as the Windows Worker provider. Platform selection is explicit and fail-closed; an unsupported platform or unavailable provider produces a clear error.
- Preserve the existing controller enrollment and certificate contracts exactly. Like the current Windows path, the controller verifies possession of the enrolled public key but does not remotely attest the local key provider. Correct TPM deployment is a trusted local-host responsibility.
- Support exactly one Worker identity per Linux host. Run it in the foreground under a dedicated Unix account and give no unrelated process access to its state directory or TPM device.
- Keep the default Worker identity, mutable state, TPM-bound blobs, and Wine prefix beside the launched executable, matching the current portable Windows layout. Explicit `--config` continues to select an alternate identity path.
- Do not provide a systemd service in this feature. Foreground shutdown must close controller sessions, terminate the owned bridge cleanly, and leave the Wine prefix and MT5 terminal in a recoverable state.
- The final Linux release must support every MT5 read and write operation exposed by the current Worker and preserve its effect journal, unknown-outcome handling, reconnect behavior, certificate lifecycle, and safety semantics.
- Before production implementation, complete the blocking Wine/MT5 feasibility prototype. Use a dedicated disposable demo account that starts with no orders or positions, place a broker-minimum-volume market order, close it immediately, and verify zero orders and positions at the end. Any uncertain or uncleared outcome is a failed prototype requiring manual cleanup.
- Pin the exact Ubuntu, Wine, Windows Python, MetaTrader5 wheel, and MT5 terminal build matrix proven by the prototype. Any component upgrade requires the complete conformance gate to pass again.

## Testing Decisions

- Platform-neutral Worker contract tests run unchanged against Windows and Linux adapter fakes. They cover enrollment, approval, reconciliation, read and write operations, reconnect, certificate rotation, revocation, effect journaling, unknown outcomes, and secret exclusion.
- Linux TPM integration tests may use a TPM 2.0 simulator for development feedback. Release acceptance additionally runs against a real TPM 2.0 device and proves that the private key cannot be exported.
- Wine bridge contract tests cover every supported MT5 operation, protocol-version mismatch, unknown operations, malformed and oversized messages, timeout, bridge exit, Wine failure, and MT5 initialization failure.
- Secret-safety tests prove that MT5 passwords are absent from arguments, environment variables, configuration, state, logs, diagnostics, journal entries, and persisted bridge artifacts.
- Process-boundary tests prove that the bridge accepts requests only through inherited handles, emits no protocol data on stderr, and cannot outlive an intentional Worker shutdown.
- End-to-end Linux acceptance uses the pinned same-host 64-bit Wine MT5 matrix and a dedicated disposable demo account. It enrolls through the public controller API, receives administrator approval, authenticates with the TPM-backed key, reconciles actual MT5 evidence, performs and closes a minimum-volume market order, proves zero remaining exposure, reconnects after bridge restart, and rotates the device certificate.
- Compatibility tests prove that adding Linux metadata and adapters does not change existing Windows Worker configuration, CNG behavior, controller contracts, or approval state.

## Out of Scope

- Native Linux MetaTrader 5 support.
- Running the MT5 terminal or bridge on another host.
- Software TPMs, vTPMs, PKCS#11 providers, and software-key providers.
- Falling back to a file-based or otherwise exportable device private key.
- Linux support for `abt-trader`; this feature applies only to `abt-worker`.
- ARM Linux, non-Ubuntu distributions, macOS, WSL, systemd services, Kubernetes, and general container orchestration.
- Automatically installing Wine, MetaTrader 5, broker profiles, or TPM middleware.
- Controller-side TPM attestation, EK/AK trust roots, PCR-bound key policy, and user-entered TPM or token PINs.
- Changing MT5 trading, reconciliation, controller approval, credential authority, or certificate security semantics.

## Further Notes

This feature extends Worker platform support without replacing ADR-0006's
Windows CNG decision. Windows Workers continue to use CNG; Linux Workers provide
an equivalent local non-exportable ECDSA P-256 identity through the TPM 2.0
TSS. Neither platform currently supplies controller-verifiable hardware
attestation. A separate ADR is required after the feasibility prototype passes
because the native Linux/Wine process boundary and TPM trust boundary are
costly to reverse and introduce a new platform security architecture.
