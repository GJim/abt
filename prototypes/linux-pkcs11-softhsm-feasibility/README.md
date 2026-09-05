# Linux SoftHSM2 / PKCS#11 feasibility prototype

Status: **VALIDATED for the keystore feasibility gate**. This is not yet wired into the production `abt-worker` CLI.

## Blocking question

Can Linux preserve the existing Worker identity contract—`public_key_pem()` and DER ECDSA-SHA256 `sign(payload)`—with a zero-prompt, software-backed PKCS#11 provider that can later be replaced by a hardware PKCS#11 module?

The prototype answers **yes** for SoftHSM2. It does not claim hardware binding or root-resistant non-exportability.

## Public seams

- `provision_identity(identity_path, module_path=...)`
- `PKCS11KeyStore.open(identity_path, module_path=...)`
- `PKCS11KeyStore.public_key_pem()`
- `PKCS11KeyStore.sign(payload)`
- `PKCS11KeyStore.close()`
- `delete_identity(identity_path)`

## Provisioning and storage

Provisioning uses the PKCS#11 C API directly for token and PIN initialization and `python-pkcs11` for key generation and normal signing. It does not invoke `softhsm2-util`, place PINs in argv, or require operator interaction.

Before publishing the identity directory it:

1. creates a private staging directory;
2. generates random SO and user PINs;
3. initializes a dedicated SoftHSM token;
4. creates an ECDSA P-256 key with `CKA_SIGN=true`, `CKA_SENSITIVE=true`, `CKA_EXTRACTABLE=false`, and `CKA_PRIVATE=true`;
5. performs an in-token sign and controller-compatible verification self-test;
6. writes the future final config and metadata; and
7. atomically renames the staging directory into place.

The SO PIN is not persisted. The user PIN is stored only in `user-pin` with mode `0600`. The identity and token directories use mode `0700`. Ordinary open failures never create a replacement identity. As with the existing Python Worker credential paths, immutable Python `bytes`/`str` objects and values passed through `python-pkcs11` cannot be guaranteed to be zeroed immediately in process memory; the zero-prompt design prevents persistence and observation outside the credential file but does not claim memory-forensic resistance against a privileged host administrator.

## Signature compatibility

SoftHSM2 2.6.1 does not expose `CKM_ECDSA_SHA256` in the tested configuration. The adapter therefore hashes the payload with SHA-256, asks PKCS#11 for raw ECDSA `r || s`, and converts that result to DER. The existing ABT controller verifier accepts the result unchanged.

## Executed gates

The integration suite exercises real SoftHSM state and covers:

- provision, P-256 public PEM, DER signature, and existing controller verification;
- close/reopen and stable identity after restart;
- atomic publish, refusal to overwrite an existing identity, and isolation of stale interrupted-staging artifacts;
- missing token, missing credential, wrong PIN, and broad-permission failures;
- signing attributes and rejection of standard private-value export;
- two simultaneous sessions for one identity and serialized concurrent signing;
- distinct replacement identity and old/new rotation proofs;
- explicit deletion without silent recreation;
- absence of the PIN from metadata, config, argv, and environment; and
- the accepted clone risk: copying both token state and credential reproduces the software identity.

## Reproduce

From the repository root:

```bash
bash prototypes/linux-pkcs11-softhsm-feasibility/run-tests.sh
```

The script runs both target-distro and target-Python matrices:

- Ubuntu 24.04 packages: SoftHSM2 2.6.1, `python3-pkcs11` 0.7.0, Python 3.12 (distribution integration check).
- Python 3.13 container: `python-pkcs11` 0.9.5 and `cryptography` 50.0.1 with distro SoftHSM2 (ABT Python compatibility check).

## Remaining production work

- add `python-pkcs11` to ABT's Linux dependency set;
- move the adapter from `prototypes/` into `abt.worker`;
- implement platform provider selection and the Wine MT5 adapter in the public CLI;
- package SoftHSM2 as a Linux distribution dependency;
- connect replacement identity staging to the existing certificate-rotation transaction; and
- run the complete Linux CLI E2E: enroll, approval, proof, reconcile, trade, restart, and certificate rotation.

The overall dual-platform CLI verdict remains **PARTIAL** until those production wiring and E2E gates pass.
