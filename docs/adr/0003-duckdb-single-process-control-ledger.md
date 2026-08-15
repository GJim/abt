# Use a single-process DuckDB control ledger

The controller uses DuckDB as its sole command, state, and immutable-event authority, replacing the SQLite decision in ADR 0001. DuckDB was selected for its analytical read capabilities, but its single-process read-write model is an explicit deployment boundary: one ASGI process owns all database access, and every REST, WSS, and local CLI write is serialized through one transaction writer; no external process or tool may write the database file.
