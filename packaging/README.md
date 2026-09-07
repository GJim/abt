# Linux Worker Packaging

`abt-worker` on Linux runs natively on Ubuntu 24.04 LTS x86_64.
Its device identity provider is backed by SoftHSM2 PKCS#11 (`libsofthsm2`).

## Distribution Dependencies

The Linux Worker distribution package depends on:
- `libsofthsm2` (>= 2.6.1): in-process SoftHSM2 runtime library for PKCS#11 device identity.
- `python3` (>= 3.13)

SoftHSM2 is an in-process library, not a system daemon; operators do not manually initialize tokens.
Token provisioning and lifecycle maintenance are handled automatically by `abt-worker enroll` and `abt-worker reconcile`.
