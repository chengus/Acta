# Changelog

## Acta file format v0.2 RC1 — 2026-07-24

Second release candidate of the Acta binary format.

- Made the primary timestamp column optional while retaining time-series
  indexing as the normal optimized path.
- Assigned primary timestamp column ID `0` to mean that no primary time column
  is present.
- Required blocks without a primary time column to store zero timestamp bounds
  and leave `TS_SORTED` clear.
- Defined writer-facing primary selection as an explicit column name,
  `"auto"` for the first timestamp or date column in schema order, or `None`
  to omit native time indexing.
- Defined readers without a primary time column to scan in file order without
  timestamp-range filtering, timestamp pruning, or timestamp ordering.
- Added v0.2 compatibility fixtures for all existing cases and a new
  no-primary fixture with semantic, corruption, and interrupted-append tests.
- Preserved the complete v0.1 specification and fixture set unchanged.

Files use format version `(0, 2)` in the prologue. Readers that only implement
v0.1 must reject them as unsupported.

## Acta file format v0.1 RC1 — 2026-07-20

First release candidate of the Acta v0.1 binary format.

- Frozen the 64-byte file prologue and generic frame layout.
- Defined fixed-schema columnar data blocks and per-block encoding selection.
- Defined checksummed append, discovery, projected reads, and recovery semantics.
- Defined explicit file-format version and compatibility behavior.
- Froze compatibility fixtures covering minimal framing, representative typed
  columns, implicit row IDs, `TS_SORTED`, and a `date32` primary column.

Files use format version `(0, 1)` in the prologue. Any incompatible change to
the binary representation or its required interpretation will use a new format
version.
