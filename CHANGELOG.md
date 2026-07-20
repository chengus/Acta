# Changelog

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
