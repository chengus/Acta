# Pre-alpha roadmap

## 1. Define the format — v0.2 RC1 published

- [x] Use the experimental [v0.1 binary design](spec/v0.1/format_v0.1.md) as a
  design test for framing, fixtures, and encoding choices.
- [x] Specify the file header, schema, block metadata, column layout, and versioning rules.
- [x] Define supported logical types, nullability, encodings, checksums, and incomplete-block handling.
- [x] Publish small binary fixtures for compatibility testing.
- [x] Define a data-block flag declaring that primary timestamps are
  monotonically nondecreasing within the block, including validation rules and
  a compatibility fixture.
- [x] Define a `date32` logical type for calendar dates, including its use as
  the primary time column and a compatibility fixture.
- [x] Preserve v0.1 as a historical design-test snapshot. It is not a supported
  compatibility baseline.
- [x] Publish [v0.2 RC1](spec/v0.2/format_v0.2.md) with an optional primary time
  column and explicit writer selection by column name, `"auto"`, or `None`.
- [x] Publish v0.2 compatibility fixtures, including a no-primary file with
  zero time bounds and `TS_SORTED` clear.
- [ ] Promote RC1 to final v0.2 after independent implementations can read the
  v0.2 compatibility fixtures and interchange files without a format change.

Supported Acta compatibility starts at v0.2. Implementations may retain v0.1
fixtures as design regression tests, but are not required to read or write
v0.1 files.

## 2. Build the Python reference implementation — in progress

Grow the v0.2 framing probe into a complete, readable reference implementation.
Optimize for spec fidelity and iteration speed, not throughput; it doubles as
the correctness oracle for the later C++ core and survives as a pure-Python
fallback package. The package lives in [python/](python/) as `acta-format`.

- [x] Implement typed column buffers, schema validation, and block encoding with
  NumPy-backed encoders and Zstandard compression.
- [x] Implement block serialization, commit trailers, and sequential append.
- [x] Implement file scanning, time-range pruning, column projection, and decoding.
- [x] Implement recovery: checksum validation and truncation to the last complete frame.
- [x] Exercise the concurrency model with multi-process tests: concurrent tailing
  readers against a live writer, and interrupted-append recovery.
- [x] Round-trip the v0.1 design fixtures during initial format exploration,
  including byte-exact regeneration through the public writer.
- [ ] Update schema construction, encoding, decoding, and reader behavior for
  the v0.2 optional-primary rules.
- [ ] Round-trip every v0.2 compatibility fixture, including byte-exact
  regeneration through the public writer.
- [ ] Feed findings back into v0.2; finalize the format only once real datasets
  stop forcing revisions.

## 3. Build the C++ core

Start once the v0.2 schema and block layout have stopped moving. The first C++
implementation targets v0.2 and does not need a v0.1 compatibility path.

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

- Add unit, round-trip, malformed-file, and concurrency tests against the v0.2
  compatibility fixtures.
- Add cross-version fixture tests when a supported version after v0.2 exists;
  v0.1 fixtures may remain design regression tests only.
- Differentially test the C++ core against the Python reference implementation
  on generated and real datasets.
- Run sanitizers and fuzz the binary parser.
- Verify files written in Python and C++ are interchangeable.

## 6. Measure and release

- Benchmark format-level properties (compression ratio, bytes read per range
  query, blocks pruned) from the reference implementation.
- Benchmark C++ ingestion, compression, range scans, and concurrent reads
  against CSV and Parquet baselines.
- Document the v0.2 public API, format limitations, and compatibility policy.
- Publish an experimental pre-alpha release supporting v0.2 with sample
  datasets and examples.
