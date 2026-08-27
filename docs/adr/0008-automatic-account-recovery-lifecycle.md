# Automatic Account Recovery Lifecycle

Supersedes ADR-0007's routine Worker isolation model.

Broker observation is the authoritative account fact; the control ledger's
desired account state is the recovery target. A Worker writes a durable
SQLite/WAL journal record before every journaled MT5 write, records the
irreversible `send_started` boundary immediately before the call, and never
re-sends that effect ID after the boundary. Lost receipts are resolved by a
fresh broker observation and, when required, a new compensating effect.

Routine uncertainty transitions the account into the Account Recovery
Lifecycle instead of a permanent worker freeze. `CONVERGING_EMPTY` cancels
observed orders, observes again, closes observed positions, and observes a
broker-verified empty account before reaching `READY`. Any state other than
`READY` rejects new entry admission. `NEEDS_HUMAN` remains reserved for
identity, journal-integrity, or persistently unobservable-broker failures;
revocation remains terminal and cannot automatically return to `READY`.

ADR-0007's terminal exclusivity, authenticated commands, broker verification,
and account-wide empty convergence remain in force. The legacy freeze,
cleanup, and release controls and their persisted records are removed.
