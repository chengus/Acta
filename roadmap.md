# Pre-alpha roadmap

## 1. Define the format — complete

- [x] Iterate on the experimental [v0.1 binary design](spec/v0.1/format_v0.md) using the
  checked-in framing probe, fixture, and encoding study.
- [x] Specify the file header, schema, block metadata, column layout, and versioning rules.
- [x] Define supported logical types, nullability, encodings, checksums, and incomplete-block handling.
- [x] Publish small binary fixtures for compatibility testing.

## 2. Build the C++ core

- Implement typed column buffers and schema validation.
- Implement block encoding, compression, serialization, and sequential append.
- Implement file scanning, time-range pruning, column projection, and decoding.
- Support concurrent readers and parallel block preparation with serialized appends.

## 3. Add Python bindings

- Expose file creation, append, scan, and schema APIs.
- Accept Python sequences and NumPy arrays with minimal copying.
- Map errors and data types consistently between C++ and Python.
- Package wheels for the initially supported platforms.

## 4. Verify correctness

- Add unit, round-trip, malformed-file, concurrency, and cross-version fixture tests.
- Run sanitizers and fuzz the binary parser.
- Verify files written in Python and C++ are interchangeable.

## 5. Measure and release

- Benchmark ingestion, compression, range scans, and concurrent reads against CSV and Parquet baselines.
- Document the public API, format limitations, and compatibility policy.
- Publish an experimental pre-alpha release with sample datasets and examples.
