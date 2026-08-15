# Superseded by ADR 0003

**Status: superseded by ADR 0003**

# Use controller-authoritative SQLite with worker outboxes

The cross-host MT5 controller uses the SQLite database on the controller host as the sole authority for paired-order state, strategy-command idempotency, and immutable events. Each account worker keeps a local SQLite command inbox and reporting outbox, but cannot accept new entries while disconnected; this preserves a single lifecycle authority without operating a central PostgreSQL service.
