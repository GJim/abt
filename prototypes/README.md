# Prototypes

The MT5 read-only smoke prototype was removed after validating the Python-to-MT5
connection. Its supported replacement is the `mt5` CLI.

The throwaway [Linux → Wine → MT5 feasibility spike](linux-wine-mt5-feasibility/README.md)
validates the proposed inherited-pipe process boundary before production Linux
Worker implementation. It is not a supported runtime.

The [Linux SoftHSM2 / PKCS#11 feasibility prototype](linux-pkcs11-softhsm-feasibility/README.md)
validates zero-prompt software-token provisioning, the existing Worker signing
contract, restart, rotation, fail-closed behavior, and the accepted clone risk.
It is not yet wired into the production `abt-worker` CLI.
