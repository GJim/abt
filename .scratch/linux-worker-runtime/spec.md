# Linux Worker Runtime

Status: ready-for-human

## Problem Statement

`abt-worker` currently runs only on native Windows. Its MT5 adapter depends on
the Windows-only MetaTrader5 Python package, and its device identity depends on
Windows CNG. Operators need to run an account Worker on a Linux x86_64 host
while preserving centralized MT5 credential handling, enrollment approval,
certificate rotation, controller proof contracts, and the broker safety model.

MetaTrader 5 has no native Linux terminal or Python integration. Linux support
therefore needs a deliberate boundary between the native Linux Worker and a
same-host MT5 terminal running under Wine. The target Linux hosts do not
reliably expose TPM 2.0 devices, so Linux identity uses an explicitly selected
SoftHSM2 PKCS#11 software token protected by the dedicated Unix account and
private filesystem permissions. Unlike Windows CNG, this Linux provider does
not claim hardware binding or resistance to a privileged host administrator.

## Solution

Run the authoritative `abt-worker` process natively on Linux. It owns
enrollment, controller communication, Worker identity and state, reconciliation,
and certificate rotation. Add a Linux PKCS#11 keystore backed by a private
SoftHSM2 token and an ECDSA P-256 signing key. The adapter matches the existing
CNG keystore's public-key and signing contract without exposing a second
controller identity protocol or requiring the operator to enter a token PIN.
SoftHSM's software-backed identity is an accepted Linux security trade-off, not
a hardware-backed equivalent to CNG.

Run the MetaTrader 5 terminal and the Windows MetaTrader5 Python package in the
same Linux host's 64-bit Wine environment. A dedicated Wine MT5 bridge exposes
only the MT5 operations required by the existing Worker contracts to the native
Worker through inherited stdin/stdout pipes. The bridge is not a Worker identity
authority and cannot connect to the controller on the Worker's behalf.

Preserve the public `abt-worker enroll` and `abt-worker reconcile` commands and
their controller contracts across Windows and Linux. Select the platform
keystore and MT5 adapter internally. The Linux package declares SoftHSM2 as a
runtime dependency; operators do not install, initialize, or maintain tokens
manually. The first Linux enrollment provisions the token and key atomically.
Linux startup fails clearly when Wine, the MT5 terminal, the bridge, the PKCS#11
module, token state, PIN credential, or configured key is unavailable. It never
silently regenerates identity or falls back to a raw PEM key or remote MT5
bridge.

## User Stories

1. As a Worker operator, I want to run `abt-worker enroll` and `abt-worker reconcile` natively on a Linux x86_64 host, so that the Worker host does not require Windows.
2. As a Worker operator, I want the Linux commands and configuration schema to match the Windows Worker experience, so that platform choice does not change the operational workflow.
3. As a Worker operator, I want the same-host MT5 terminal to run under 64-bit Wine, so that the Worker can use the supported Windows MetaTrader5 integration without requiring a separate Windows machine.
4. As a Worker operator, I want startup diagnostics to identify missing or unhealthy Wine, MT5 terminal, bridge, PKCS#11 module, token state, credential, or key dependencies, so that deployment failures are actionable.
5. As a Worker operator, I want Linux enrollment to create and use an ECDSA P-256 identity in a dedicated SoftHSM2 token, so that Linux keeps the same controller proof contract without requiring TPM hardware.
6. As a Worker operator, I want SoftHSM provisioning and login to require no PIN entry or separate token administration, so that the foreground Linux commands match the current Windows CNG interaction model.
7. As a control-plane administrator, I want Linux and Windows Workers to use the same ECDSA P-256 enrollment, certificate, challenge-response, rotation, revocation, and audit contracts, so that Linux support does not create a second controller identity path.
8. As a control-plane administrator, I want Linux enrollment metadata to identify the provider as software-backed PKCS#11 rather than hardware-backed, so that the accepted Linux security trade-off is explicit and auditable.
9. As a Worker operator, I want MT5 passwords received or entered by the native Worker to cross only the protected same-host bridge channel and remain memory-only, so that Linux support does not add credential persistence.
10. As a Worker operator, I want a bridge or Wine failure to fail the affected MT5 operation explicitly and trigger the existing safe reconnect behavior, so that infrastructure failure cannot appear as successful reconciliation or execution.
11. As a maintainer, I want Worker domain logic to depend on platform-neutral keystore and MT5 interfaces, so that Windows CNG, Linux PKCS#11, native Windows MT5, and Wine MT5 remain replaceable adapters.
12. As a release owner, I want Linux acceptance exercised with the packaged SoftHSM2 module and a real Wine MT5 terminal, so that fakes alone cannot qualify Linux support.
13. As a maintainer, I want a blocking feasibility prototype to prove the pinned Wine and MT5 matrix, inherited-pipe bridge, and demo-account market-order roundtrip before production implementation begins, so that unsupported platform assumptions fail early.

## Implementation Decisions

- Support Ubuntu 24.04 LTS x86_64 only in the first release. ARM Linux, other distributions, macOS, WSL, and containers are not supported.
- Ship the Linux Worker as a distribution package that declares the Ubuntu SoftHSM2 runtime library as a dependency. SoftHSM is an in-process PKCS#11 library, not a daemon; no separate service lifecycle or manual token administration is part of normal operation. The Worker must not invoke `apt` or another system package manager itself.
- Keep `abt-worker` as a native Linux Python process. Do not run the authoritative Worker process inside Wine.
- Run the MT5 terminal and a minimal Windows Python bridge inside one 64-bit Wine prefix on the same host. Remote bridge endpoints are forbidden.
- The native Worker directly launches and owns the Wine bridge child process. Use inherited stdin/stdout pipes for a versioned, length-prefixed, bounded request/response protocol; reserve bridge stderr for secret-safe diagnostics. Do not create a socket or TCP listener.
- Reject unknown protocol versions, operations, fields, oversized messages, malformed responses, and mismatched response IDs. Permit only one MT5 operation in flight per bridge.
- Pass MT5 passwords to the bridge only for the operation that requires them. Neither side writes passwords to configuration, state, command-line arguments, environment variables, logs, crash messages, or protocol diagnostics.
- Treat bridge EOF, timeout, malformed output, Wine process exit, and MT5 initialization failure as explicit operation failures. Never synthesize empty MT5 evidence or success-shaped responses.
- Keep controller HTTPS/WSS communication, device signing, certificate handling, reconciliation policy, effect journaling, and Worker state in the native Linux process. The Wine bridge receives no registration invite, device private key, device certificate, or controller credential.
- Add a Linux PKCS#11 keystore implementing the existing hardware-keystore method contract through `python-pkcs11` and the packaged SoftHSM2 module. Generate an ECDSA P-256 signing key with `CKA_SIGN=true`, `CKA_SENSITIVE=true`, and `CKA_EXTRACTABLE=false`. These attributes constrain normal PKCS#11 operations but do not make a software token hardware-backed or resistant to a privileged host administrator.
- During the first Linux enrollment, provision the SoftHSM token in a staging directory, generate cryptographically random SO and user PINs, create and self-test the key, then publish token state and credentials atomically. Discard the SO PIN after successful provisioning; persist only the user PIN needed for automatic Worker login.
- Store the SoftHSM configuration without credentials, the token directory with mode `0700`, and the user-PIN credential file with mode `0600`, all owned by the dedicated Worker account. Never place the PIN in command-line arguments, environment variables, logs, diagnostics, controller messages, or general Worker configuration.
- Automatically open and log in to the token on Worker startup. The operator is never prompted for a PIN and never needs `softhsm2-util` or `pkcs11-tool` for normal installation, enrollment, reconciliation, rotation, or deprovisioning.
- Preserve Windows CNG as the Windows Worker provider. Platform selection is explicit and fail-closed; an unsupported platform, missing PKCS#11 module, missing or corrupt token, missing credential, or missing key produces a clear error and never creates a replacement identity during ordinary startup.
- Preserve the existing controller enrollment and certificate proof contracts. Record Linux enrollment provider metadata as `PKCS11-SOFTWARE`, not CNG, TPM, or hardware-backed. The controller verifies possession of the enrolled public key but does not attest local token implementation or filesystem protections.
- Support exactly one Worker identity per Linux host. Run it in the foreground under a dedicated Unix account and give no unrelated account access to its identity state. A copied token directory plus credential can clone the Linux identity; this is an accepted consequence of the software-backed provider and must be documented operationally.
- Keep the default Worker identity, mutable state, SoftHSM configuration, token directory, PIN credential, and Wine prefix beside the launched executable, matching the current portable Windows layout. Explicit `--config` continues to select an alternate identity path.
- Do not provide a systemd service in this feature. Foreground shutdown must close controller sessions, terminate the owned bridge cleanly, and leave the Wine prefix and MT5 terminal in a recoverable state.
- The final Linux release must support every MT5 read and write operation exposed by the current Worker and preserve its effect journal, unknown-outcome handling, reconnect behavior, certificate lifecycle, and safety semantics.
- Before production implementation, complete the blocking Wine/MT5 feasibility prototype. Use a dedicated disposable demo account that starts with no orders or positions, place a broker-minimum-volume market order, close it immediately, and verify zero orders and positions at the end. Any uncertain or uncleared outcome is a failed prototype requiring manual cleanup.
- Pin the exact Ubuntu, Wine, Windows Python, MetaTrader5 wheel, and MT5 terminal build matrix proven by the prototype. Any component upgrade requires the complete conformance gate to pass again.

## Testing Decisions

- Platform-neutral Worker contract tests run unchanged against Windows and Linux adapter fakes. They cover enrollment, approval, reconciliation, read and write operations, reconnect, certificate rotation, revocation, effect journaling, unknown outcomes, and secret exclusion.
- Linux PKCS#11 integration tests run against the exact packaged SoftHSM2 version. They prove atomic first-enrollment provisioning, ECDSA P-256 generation, public PEM stability after process restart, DER ECDSA-SHA256 signatures accepted by the existing controller verifier, automatic login without operator input, key rotation, and explicit deprovisioning.
- Keystore failure tests cover missing module, missing or corrupt configuration, token, credential, and key; wrong PIN; duplicate token or key labels; interrupted staging; permissions broader than specified; and startup after identity state is copied. None of these paths may silently regenerate or replace identity.
- Attribute tests prove the signing key reports `CKA_SIGN=true`, `CKA_SENSITIVE=true`, and `CKA_EXTRACTABLE=false`, and that standard PKCS#11 export operations reject private-key extraction. Documentation and acceptance evidence must still state that copying the software token directory and credential can clone the identity.
- Wine bridge contract tests cover every supported MT5 operation, protocol-version mismatch, unknown operations, malformed and oversized messages, timeout, bridge exit, Wine failure, and MT5 initialization failure.
- Secret-safety tests prove that MT5 passwords are absent from arguments, environment variables, configuration, state, logs, diagnostics, journal entries, and persisted bridge artifacts. Separate SoftHSM tests prove that the user PIN exists only in the mode-`0600` credential file and process memory and is absent from arguments, environment variables, logs, diagnostics, controller messages, and general Worker configuration.
- Process-boundary tests prove that the bridge accepts requests only through inherited handles, emits no protocol data on stderr, and cannot outlive an intentional Worker shutdown.
- End-to-end Linux acceptance uses the packaged SoftHSM2 module, pinned same-host 64-bit Wine MT5 matrix, and a dedicated disposable demo account. It enrolls through the public controller API, records provider `PKCS11-SOFTWARE`, receives administrator approval, authenticates with the PKCS#11 key, reconciles actual MT5 evidence, performs and closes a minimum-volume market order, proves zero remaining exposure, reconnects after bridge restart, and rotates the device certificate.
- Compatibility tests prove that adding Linux metadata and adapters does not change existing Windows Worker configuration, CNG behavior, controller proof contracts, or approval state.

## Out of Scope

- Native Linux MetaTrader 5 support.
- Running the MT5 terminal or bridge on another host.
- Hardware TPM, software TPM, vTPM, external hardware HSM, remote signer, and raw file-based PEM providers in the first Linux release.
- Claiming that the SoftHSM identity is hardware-bound, non-clonable, or protected from a privileged host administrator.
- Linux support for `abt-trader`; this feature applies only to `abt-worker`.
- ARM Linux, non-Ubuntu distributions, macOS, WSL, systemd services, Kubernetes, and general container orchestration.
- Automatically installing Wine, MetaTrader 5, or broker profiles from inside the Worker. SoftHSM2 is installed by the Linux distribution package dependency, not by runtime shell commands.
- Controller-side hardware attestation, TPM EK/AK trust roots, PCR policy, and user-entered token PINs.
- Changing MT5 trading, reconciliation, controller approval, credential authority, or certificate security semantics.

## Further Notes

This feature extends Worker platform support without replacing ADR-0006's
Windows CNG decision. Windows Workers continue to use CNG. Linux Workers use an
explicitly software-backed PKCS#11 identity through SoftHSM2 while preserving
the same controller-visible ECDSA P-256 public-key and signature contracts.
Neither platform supplies controller-verifiable provider attestation. The
Linux provider metadata and operational documentation must disclose that a
privileged host administrator who obtains both token state and its credential
can clone the identity. A separate ADR is required after the feasibility work
because the native Linux/Wine process boundary and the accepted SoftHSM trust
boundary are costly to reverse and introduce a new platform security
architecture.
