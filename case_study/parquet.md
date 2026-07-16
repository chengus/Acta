# Parquet vs Acta

Parquet is the natural comparison for Acta's typed, compressed columnar blocks.

## Parquet advantages

- Mature readers, writers, schemas, encodings, and query-engine integrations.
- Excellent compression and column pruning for sealed analytical datasets.
- Row-group statistics support efficient predicate pushdown.

## Acta advantages

- Designed around continuously appending blocks to one file.
- Readers can include newly completed blocks without rebuilding a dataset manifest.
- Concurrent producers can prepare and compress independent blocks before a short append step.

## Trade-off

Parquet is preferable for interoperable, immutable analytical datasets. Acta targets live time-series capture where append behavior is part of the format rather than a convention built around multiple files.
