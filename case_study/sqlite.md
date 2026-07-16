# SQLite vs Acta

SQLite is an embedded transactional database stored in a single file.

## SQLite advantages

- Mature SQL engine with transactions, indexes, constraints, updates, and recovery.
- Strong consistency and well-defined concurrent-reader behavior.
- Excellent choice when records must be queried and mutated flexibly.

## Acta advantages

- Immutable columnar blocks are tailored to compression and scans.
- Ingestion performs sequential block appends rather than page updates.
- Time bounds and column statistics provide coarse range pruning with little index machinery.
- Producers can encode and compress blocks concurrently.

## Trade-off

SQLite should be preferred when database semantics matter. Acta deliberately gives up transactions, mutation, and general-purpose indexing for simpler append-heavy time-series storage.
