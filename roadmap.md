# Pre-alpha roadmap

## 1. Define the format — v0.1 RC1 published

- [x] Iterate on the experimental [v0.1 binary design](spec/v0.1/format_v0.1.md) using the
  checked-in framing probe, fixture, and encoding study.
- [x] Specify the file header, schema, block metadata, column layout, and versioning rules.
- [x] Define supported logical types, nullability, encodings, checksums, and incomplete-block handling.
- [x] Publish small binary fixtures for compatibility testing.
- [x] Define a data-block flag declaring that primary timestamps are
  monotonically nondecreasing within the block, including validation rules and
  a compatibility fixture.
- [x] Define a `date32` logical type for calendar dates, including its use as
  the primary time column and a compatibility fixture.
- [x] Freeze and publish the binary layout as v0.1 RC1 on 2026-07-20.
- [ ] Promote RC1 to final v0.1 after independent implementations can read the
  compatibility fixtures and interchange files without a format change.

## 2. Build the Python reference implementation

Grow the framing probe into a complete, readable reference implementation.
Optimize for spec fidelity and iteration speed, not throughput; it doubles as
the correctness oracle for the later C++ core and survives as a pure-Python
fallback package.

- Implement typed column buffers, schema validation, and block encoding with
  NumPy-backed encoders and Zstandard compression.
- Implement block serialization, commit trailers, and sequential append.
- Implement file scanning, time-range pruning, column projection, and decoding.
- Implement recovery: checksum validation and truncation to the last complete frame.
- Exercise the concurrency model with multi-process tests: concurrent tailing
  readers against a live writer, and interrupted-append recovery.
- Round-trip all compatibility fixtures and expand them as the spec evolves.
- Feed implementation findings back as compatible clarifications. Any
  incompatible binary change after RC1 uses a new format version.

## 3. Build the C++ core

Start once the schema and block layout have stopped moving.

- Implement typed column buffers and schema validation.
- Implement block encoding, compression, serialization, and sequential append.
- Implement file scanning, time-range pruning, column projection, and decoding.
- Support concurrent readers and parallel block preparation with serialized appends.

## 4. Add Python bindings

- Expose file creation, append, scan, and schema APIs matching the reference
  implementation, with the C++ core as a drop-in backend.
- Accept Python sequences and NumPy arrays with minimal copying.
- Map errors and data types consistently between C++ and Python.
- Package wheels for the initially supported platforms.

## 5. Verify correctness

- Add unit, round-trip, malformed-file, concurrency, and cross-version fixture tests.
- Differentially test the C++ core against the Python reference implementation
  on generated and real datasets.
- Run sanitizers and fuzz the binary parser.
- Verify files written in Python and C++ are interchangeable.

## 6. Measure and release

- Benchmark format-level properties (compression ratio, bytes read per range
  query, blocks pruned) from the reference implementation.
- Benchmark C++ ingestion, compression, range scans, and concurrent reads
  against CSV and Parquet baselines.
- Document the public API, format limitations, and compatibility policy.
- Publish an experimental pre-alpha release with sample datasets and examples.
