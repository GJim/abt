# Unify Worker and Trader on Windows CNG device identity

Workers and Traders use the same native Windows CNG, non-exportable ECDSA P-256 device-key boundary and prove possession of that key to enroll, obtain certificates, establish sessions, and rotate certificates. The controller does not require Trader attestation JWTs: introducing a separate attestation issuer would add an unimplemented trust dependency, while a self-issued assertion would add no security; administrator approval, certificate binding, challenge-response, and revocation remain the trust controls.
