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
  zero time bounds and `TS_SORTED` clear, and an IANA-timezone file pinning the
  padding of the variable-length type-parameter record.
- [ ] Promote RC1 to final v0.2 after independent implementations can read the
  v0.2 compatibility fixtures and interchange files without a format change.

Supported Acta compatibility starts at v0.2. Implementations may retain v0.1
fixtures as design regression tests, but are not required to read or write
v0.1 files.

## 2. Build the Rust core — structural validator complete

The crate at the repository root is the canonical native implementation. It
targets v0.2 and intentionally has no v0.1 compatibility path or parallel C++
core. An earlier exploratory reader was removed; the implementation now follows
the staged plan in [Acta_Crate_API_Plan_Revised.md](Acta_Crate_API_Plan_Revised.md),
which builds trustworthy parsing and validation before reading and writing.

### 2.1 Stage 1 — checked wire parser and structural validator — complete

- [x] Implement a checked byte cursor, CRC32C, alignment rules, prologue
  parsing, frame envelopes, and bounded positional reads.
- [x] Validate the prologue, frame prefixes, frame types, sequence numbers,
  commit trailers, and every frame CRC against the v0.2 specification.
- [x] Classify an interrupted append distinctly from corruption, and report the
  offset a writer resumes from.
- [x] Apply configurable resource limits to declared sizes before reading.
- [x] Expose `acta::validate`, `acta::validate_with_limits`, `Limits`, and
  `ValidationReport`; keep every wire-format module `pub(crate)`.
- [x] Cover the specification with fixture, corruption, and mutation tests, and
  add a `cargo fuzz` target over the top-level validator.
- [x] Keep the production dependency surface empty for this stage.

### 2.2 Stage 2 — schema and data-frame metadata validation — in progress

- [x] Parse the schema frame: column descriptors, logical types, type
  parameters, column IDs, names, nullability, and primary selection.
- [x] Parse data-frame headers: block header fields and the geometry of the
  column table, stream table, and statistics area.
- [x] Parse the column and stream descriptor tables themselves, and validate
  stream alignment, ranges, non-overlap, and stored CRCs. These run on the
  decode path; `acta::validate` still stops at the block header, so a file can
  validate and still fail to decode.
- [x] Enforce the section 8 invariants for row counts, `ROW_IDS`, timestamp
  bounds, and `TS_SORTED` declarations that metadata alone can settle. Null
  counts live in the column table and follow with it.
- [x] Expose public schema and descriptive block metadata without decoding
  column values, through `Reader::open` and a metadata-first snapshot.
- [x] Walk the file once, through `format::scan`, so `acta::validate` and
  `Reader::open` cannot disagree about which files are well formed.
- [x] Publish a compatibility fixture for the IANA timezone type-parameter
  record, the only variable-length parameter record in v0.2.

### 2.3 Stage 3 — stream decoding and full validation — complete

- [x] Implement every v0.2 transform: raw, bit packing, frame of reference,
  delta, delta-of-delta, byte-stream split, and boolean RLE.
- [x] Add independent Zstandard stream decompression, behind the default `zstd`
  feature so the crate still builds with no dependencies at all.
- [x] Reconstruct dense nulls, validate dictionary and run-length layouts,
  UTF-8, and `TS_SORTED`.
- [x] Bound one block decode by a configurable total of decoded bytes, so a
  small file cannot declare a large decode.
- [x] Verify optional min/max statistics against decoded values.
- [x] Add `ValidationLevel::Full` alongside the structural path.

### 2.4 Stages 4 and 5 — snapshot reader, projection, and ranges

- [x] Native arrays, immutable schemas, and `RecordBatch`, decoded one block at
  a time through `Reader::read_block`.
- [x] A lazy file-order scan over committed frames.
- [x] Column projection, block pruning by primary bounds, and half-open
  timestamp and `date32` ranges.

### 2.5 Stage 6 and later — writer

- [x] Begin with deterministic plain/raw output and require every generated file
  to round-trip through the validator.
- [x] Add `Writer::create`, raw stream serialization, frame/schema/data CRCs,
  commit trailers, `flush`, `sync`, and `finish` with exclusive file creation.
- [x] Support every v0.2 logical type, nullable representation, optional
  primary column, and optional implicit row IDs in generated files.
- [x] Stage 7 target: add bounded buffered block preparation and configurable
  block-size thresholds for sustained ingestion.
- [x] Add explicit writer codec selection with raw output as the baseline and
  Zstandard compression as the first optional codec.
- [x] Ensure compressed and uncompressed files both round-trip through the
  validator and reader, with compression tests.
- [x] Add initial throughput/ratio benchmarks. The reproducible harness and its
  baseline results are in [benchmarks/stage7/](benchmarks/stage7/README.md).

#### Stage 7b — writer transforms and adaptive encoding

- [x] Add writer support for bit packing, frame of reference, delta,
  delta-of-delta, byte-stream split, Boolean RLE, dictionary, and run-length
  layouts where valid for the logical type.
- [x] Profile transform candidates once per stream, include descriptor and
  compressed-size costs, and select a specialized encoding only when it beats
  the raw baseline by a measured margin.
- [x] Fall back deterministically to raw encoding when a transform is invalid,
  too expensive, or does not provide a material win. This describes
  `WriterEncoding::Adaptive`, which is the only policy that selects.
- [x] Add `WriterEncoding::Fixed`, which applies one requested transform to
  every column it is offered for and returns an error instead of falling back,
  so that a benchmark can separate the cost of a transform from the cost of
  choosing it. Its offered set is the same candidate table adaptive prices.
- [x] Add transform-specific malformed-output, round-trip, determinism, and
  benchmark coverage. The reproducible harness and its recorded sample are in
  [benchmarks/stage7b/](benchmarks/stage7b/README.md).

Stage 7b precedes append reopening. The existing Stage 3 transform work is
reader-side decoding; this milestone adds the corresponding writer-side
serialization and selection logic. `WriterEncoding::Raw` remains the default,
so a file written without asking for adaptive encoding is byte-for-byte what
the Stage 6/7 writer produced.

#### Stage 7c — statistics generation and selection — complete

- [x] Generate deterministic min/max statistics for supported logical types.
- [x] Add explicit writer controls and a documented size heuristic for when
  statistics justify their encoded size and ingestion cost.
- [x] Round-trip files with and without statistics through structural and full
  validation, the reader, and Stage 5 block pruning.

The public `WriterStatistics` policy keeps `None` as the default, writes exact
kind-one pairs for fixed-width types under `MinMax`, and uses the reproducible
`Automatic` rule documented on the type itself and in the crate README. The
Stage 7c benchmark is in [benchmarks/stage7c/](benchmarks/stage7c/README.md).

Statistics verification remains part of Stage 3 full validation; Stage 7c is
only writer-side generation and selection. Nothing reads optional statistics
for pruning yet — Stage 5 prunes on the mandatory primary bounds alone — so the
benchmark measures the cost of writing them and confirms they leave pruning
unchanged. Pricing them against a measured pruning benefit belongs to future
statistics-based pruning work.

#### Stage 8 — append reopening

- [x] Add `Writer::open`, `Writer::open_with_schema`, and
  `Writer::open_with_limits` with schema reconstruction exposed by
  `Writer::schema`, an optional exact schema guard, and a row-ID feature guard.
- [x] Continue frame sequences and implicit row IDs without overwriting any
  committed data.
- [x] Hold a cooperative exclusive writer lock that never blocks readers, and
  refuse incomplete tails until explicit recovery is requested.

#### Stage 8b — explicit recovery and tail repair

- [x] Add read-only recovery inspection reporting the last committed offset
  and proposed repair.
- [x] Add an explicit, exclusive repair operation that truncates only a
  verified incomplete tail, synchronizes, and validates the result.
- [x] Refuse complete-frame corruption and test recovery boundaries and
  injected I/O failures.

Stage 8b keeps planning separate from mutation. Inspection is a read-only
snapshot; repair acquires the shared writer lock before discovery, rescans the
same handle, rechecks its length, and post-validates after synchronization.
Only a physically incomplete final data frame is repairable. Complete
corruption, incomplete schema frames, salvage, and corrupt-final-frame approval
remain refused or unimplemented.

#### Stage 9 — reader refresh and tailing

- [x] Add an explicit `Reader` refresh operation that extends a snapshot only
  with newly committed frames while preserving schema, sequence, row-ID, CRC,
  resource-limit, and incomplete-tail checks.
- [x] Add synchronous tail-following behavior with explicit polling,
  cancellation, and incomplete-frame retry semantics; keep async runtimes out
  of the core crate.
- [x] Preserve projection, primary-range filtering, file order, snapshot
  isolation, and reader/writer coexistence across refresh boundaries.
- [x] Cover append, interrupted-tail, repair, replacement, truncation,
  corruption, and multi-process concurrency boundaries.

Stage 9 intentionally implements only refresh and tailing. Merge-by-primary,
global sorting, public row IDs, optional-statistics pruning, and general query
planning remain future features.

#### Stage 10 — API ergonomics and reader benchmark baseline

- [ ] Audit and stabilize the native Rust API around schema construction,
  arrays and batches, create/open/append workflows, scans, refresh/tailing,
  validation, recovery, options, limits, and structured errors.
- [ ] Add concise end-to-end examples and rustdoc for the common lifecycle, and
  remove avoidable ceremony without adding integration-specific dependencies
  to the core crate.
- [ ] Add a reproducible Rust reader benchmark harness and representative
  multi-block v0.2 datasets covering cold and warm open, structural and full
  validation, full scans, sparse projections, selective ranges, refresh,
  tailing, allocations, peak memory, bytes read, and concurrent readers.
- [ ] Record a pre-optimization baseline and define correctness checks and
  measurement methodology before changing hot paths.
- [ ] Run the release-facing correctness gate: documentation tests, fuzzing,
  Miri where supported, and cross-platform locking/recovery/tailing tests.

#### Stage 11 — PyO3 bindings

- [ ] Add a separate PyO3/maturin binding crate; keep PyO3, NumPy, Arrow, and
  Python packaging dependencies out of the core `acta` crate.
- [ ] Expose schemas, validation, create/open writers, append, scans,
  projection, primary ranges, refresh/tailing, and recovery with Pythonic
  ownership and context-manager behavior.
- [ ] Accept Python sequences and NumPy arrays with minimal copying, and keep
  any PyArrow conversion in the binding or an optional adapter.
- [ ] Map logical types, nulls, time metadata, limits, and structured errors
  consistently without hiding corruption or resource-limit failures.
- [ ] Add Rust/Python differential and lifecycle tests, then build wheels for
  the initially supported platforms.

The package in `legacy/python-v0.1/` remains the v0.1 design-history implementation. Stage
11 wraps the authoritative Rust v0.2 core rather than extending that package
into a second v0.2 implementation.

#### Stage 12 — measured optimization

Performance changes must preserve strict verification, resource limits,
projection isolation, stable ordering, malformed-file rejection, and the
public behavior stabilized in Stage 10.

- [x] Decode raw and transformed values directly into typed destinations. Bit
  unpacking yields values one at a time, transforms narrow to the column's type
  as they decode, and a column with no nulls becomes its array without a copy.
- [ ] Use the Stage 10 Rust baseline and Stage 11 Python workloads to identify
  bottlenecks; do not optimize paths without before/after measurements.
- [ ] Optimize bitmap operations, dictionary/RLE expansion, variable-width
  offsets, and byte-stream split where measurements justify it.
- [ ] Evaluate a single payload pass for frame and stream CRC verification,
  reusable checksum/decompression buffers, and adaptive coalescing of required
  adjacent projected streams.
- [ ] Replace or specialize CRC32C only if it is measured as a bottleneck.
- [ ] Evaluate bounded decoded-column caching and bounded parallel block or
  column decoding only after the single-threaded pipeline is measured and
  simplified.
- [ ] Retain benchmark results and regression thresholds for throughput,
  allocations, peak memory, and bytes read.

#### Future stages — advanced reads and integrations

- Merge-by-primary for blocks that declare `TS_SORTED` and bounded-memory
  global primary sorting require separate ordering and memory contracts.
- Public row-ID exposure, richer optional-statistics pruning, and general
  predicate/query planning remain independent features.
- Arrow, DataFusion, DuckDB, C/C++, Serde, Chrono, async, and other integrations
  remain optional companion work after the Rust and Python APIs stabilize.

## 3. Keep Python as compatibility and test tooling

The package in [legacy/python-v0.1/](legacy/python-v0.1/) remains a v0.1 design-history artifact. It is
not a second implementation track or a release blocker. The v0.2
specification, compatibility fixtures, and Rust core are authoritative.

- [x] Preserve the v0.1 implementation and design fixtures for regression tests.
- [ ] Use the existing Python fixture generators to produce reproducible v0.2
  compatibility data where they reduce test maintenance.
- [ ] Add Python/Rust differential checks for generated and real datasets once
  the Rust writer is available.
- [ ] Keep any Python v0.2 reader or pure-Python fallback explicitly optional;
  implement it only if downstream users require it.

## 4. Stage 11 Python binding constraints

Stage 11 owns the binding implementation and release checklist above. This
section records the architectural boundary: the binding is a separate
PyO3/maturin crate over the Rust v0.2 core, while `legacy/python-v0.1/` remains historical
v0.1 compatibility and test tooling rather than a fallback production core.

## 5. Expand correctness coverage

- [x] Add unit, fixture, malformed-file, and structure-aware mutation tests for
  the prologue and generic frame against the v0.2 compatibility fixtures.
- [x] Add fuzz targets over the top-level validator and the metadata reader.
- [x] Extend fixture, malformed-file, and mutation coverage to schema
  descriptors and block metadata.
- [x] Extend fixture, malformed-file, and fuzz coverage to the column and
  stream descriptor tables and to stream decoding. The checked-in fixtures use
  plain layout and raw transforms only, so every other layout, transform, and
  codec is covered by byte sequences written from the specification.
- [ ] Add projection, range, ordering, refresh, and concurrency tests once a
  reader exists.
- [ ] Add writer round-trip tests covering every type, layout, transform,
  nullability pattern, row-ID mode, primary selection, and recovery boundary.
- [ ] Add cross-version fixture tests when a supported version after v0.2 exists;
  v0.1 fixtures may remain design regression tests only.
- [ ] Differentially test the Rust core against the compatibility fixtures and
  Python tooling on generated and real datasets where applicable.
- [ ] Run sustained fuzzing, Miri, and supported sanitizers in CI.
- [ ] Verify Rust-written files against the published fixtures and any
  supported language bindings.
- [ ] Run cross-platform CI on Linux, macOS, and Windows, including ARM64
  compilation where runners are available.

## 6. Measure and release

- [ ] Benchmark reader performance before and after each optimization milestone;
  retain regression thresholds for throughput, allocations, peak memory, and
  bytes read.
- [ ] Benchmark format-level properties (compression ratio, bytes read per range
  query, blocks pruned) from the reference implementation.
- [ ] Benchmark Rust ingestion, compression, range scans, and concurrent reads
  against CSV and Parquet baselines.
- [ ] Document the v0.2 public API, format limitations, performance methodology,
  and compatibility policy.
- [ ] Publish an experimental pre-alpha release supporting v0.2 with sample
  datasets and examples.
