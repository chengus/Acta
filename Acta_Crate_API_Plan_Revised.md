# Acta Crate and Public API Design

## Summary

Design Acta as one synchronous Rust crate:

- `acta`: dependency-light implementation of the Acta v0.2 format, including validation, reading, writing, schema types, and native columnar arrays.

Arrow interoperability is explicitly deferred to a future optional companion crate, likely `acta-arrow`, after the native Acta API and wire implementation are stable.

The implementation is divided into stages. Early stages prioritize trustworthy parsing and validation of the v0.2 wire format before introducing writing, append reopening, higher-level scans, or external integrations.

The API targets application developers first while preserving room for advanced inspection. Existing pre-1.0 Rust APIs are not compatibility constraints.

## Design principles

- One public crate presents a coherent API for validation, reading, and writing.
- Wire-format parsing, serialization, frame descriptors, recovery machinery, and codecs remain internal.
- The validator and reader share the same checked low-level parser.
- The writer uses the same schema and format definitions as the reader, but serializer correctness is tested independently against the validator and compatibility fixtures.
- All arithmetic derived from file data is checked before slicing, seeking, or allocating.
- Core remains synchronous and independent of Arrow, async runtimes, Serde, Chrono, Python, and C ABI concerns.
- Public APIs are introduced only when their behavior and resource requirements are well defined.

## Crate organization

```text
acta/
├── rust/
│   ├── lib.rs
│   ├── error.rs
│   ├── limits.rs
│   ├── schema.rs
│   ├── array.rs
│   ├── batch.rs
│   ├── format/
│   │   ├── mod.rs
│   │   ├── constants.rs
│   │   ├── cursor.rs
│   │   ├── prologue.rs
│   │   ├── frame.rs
│   │   ├── schema_frame.rs
│   │   └── data_frame.rs
│   ├── validate/
│   │   ├── mod.rs
│   │   └── report.rs
│   ├── read/
│   │   ├── mod.rs
│   │   ├── reader.rs
│   │   ├── scan.rs
│   │   └── block.rs
│   ├── write/
│   │   ├── mod.rs
│   │   ├── writer.rs
│   │   ├── options.rs
│   │   └── block_builder.rs
│   └── codec/
│       ├── mod.rs
│       ├── bitpack.rs
│       ├── delta.rs
│       ├── rle.rs
│       └── compression.rs
└── tests/
    ├── fixtures.rs
    ├── validation.rs
    ├── corruption.rs
    ├── reading.rs
    ├── writing.rs
    └── roundtrip.rs
```

This remains one crate. The directories are internal modules, not independently published packages.

## Public API direction

Expose common application-facing types from the crate root while retaining focused `schema`, `array`, `read`, `write`, `validate`, `format`, and `error` modules.

```rust
use acta::{Reader, SchemaBuilder, ValidationOptions, Writer};
```

All public structs use private fields and validated constructors. Extensible public enums use `#[non_exhaustive]` where appropriate.

## Core types

```rust
let schema = SchemaBuilder::new()
    .column(Column::new(
        "timestamp",
        LogicalType::Timestamp { /* unit and timezone */ },
    ))
    .column(Column::new("value", LogicalType::Float64).nullable())
    .build(PrimarySelection::Named("timestamp".into()))?;

let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)?;
```

- `LogicalType` represents every v0.2 logical type and its validated parameters.
- `Column` contains a deterministic nonzero ID, unique name, logical type, and nullability.
- Automatically assigned column IDs follow schema-builder insertion order and remain fixed within the constructed schema and written file.
- `SchemaBuilder::build` always requires `PrimarySelection::Named`, `Auto`, or `None`; there is no implicit default.
- `Schema` is immutable after construction and supports lookup by name and column ID.
- The initial native `Array` representation is a closed enum with private fields and typed constructors/accessors.
- Logical arrays retain one value position per row; dense-null wire encoding remains private.
- `RecordBatch` owns arrays, an `Arc<Schema>`, and an explicit row count so zero-column batches remain valid.
- Construction validates column count, logical types, array lengths, validity, UTF-8, and variable-width offsets.
- Implicit row IDs are preserved and validated internally but are not exposed in the initial public API.

## Validation API

Validation is a first-class capability from the first implementation stage.

```rust
let report = acta::validate("ticks.acta")?;

let report = acta::validate_with_options(
    "ticks.acta",
    ValidationOptions::default().with_level(ValidationLevel::Full),
)?;
```

```rust
pub enum ValidationLevel {
    Structural,
    Full,
}
```

### Structural validation

Structural validation checks enough of the file to establish safe and consistent interpretation without necessarily decoding every logical stream:

- file magic and supported format version;
- feature flags and reserved fields;
- prologue CRC32C;
- frame prefix CRC before trusting lengths;
- checked length arithmetic;
- alignment and range validity;
- header, trailer, and commit-marker consistency;
- sequence numbers and frame-type ordering;
- schema descriptors and data-frame metadata;
- stream ranges, non-overlap, and stored-stream CRCs;
- incomplete final-frame detection.

### Full validation

Full validation additionally:

- decompresses and decodes every stream;
- checks element counts and dense-null counts;
- validates dictionary indices and run-length totals;
- validates UTF-8 data;
- verifies timestamp ordering declarations;
- verifies min/max statistics where required;
- checks all semantic invariants defined by Acta v0.2.

`acta::validate` and `acta::validate_with_limits` default to structural
validation. Full validation is selected with
`ValidationOptions::default().with_level(ValidationLevel::Full)` and has the
cost of decoding every complete block and verifying optional statistics.
`Reader::open` may use a lighter structural path while still enforcing
mandatory safety checks.

## Reader API

The first reader is a snapshot reader over complete committed frames.

```rust
let reader = Reader::open(path, ReaderOptions::default())?;

for batch in reader.scan() {
    let batch = batch?;
}
```

- `Reader::open` discovers a snapshot of complete committed frames.
- An incomplete final frame is reported in metadata and ignored for ordinary snapshot reads.
- Reopening is required to observe later appends.
- `Reader` exposes its schema, file metadata, and read-only block metadata.
- Scans are lazy iterators yielding `Result<RecordBatch>`.
- `ReaderOptions` applies frame, allocation, decompression, and total-scan limits before allocation.
- `Reader` and independent scans are intended to be `Send + Sync`.

### Projection

Projection means selecting which columns to return and decode.

```rust
for batch in reader
    .scan()
    .project(["timestamp", "value"])?
{
    let batch = batch?;
}
```

- Default projection is all columns.
- Requested projection order determines output column order.
- Empty projection is valid and preserves row counts.
- Unprojected columns should not be read, decompressed, or decoded unless needed for filtering or ordering.

Projection is introduced only after ordinary full-column reads work correctly.

### Primary-range filtering

```rust
for batch in reader
    .scan()
    .project(["timestamp", "value"])?
    .primary_range(PrimaryRange::timestamp(start, end))?
{
    let batch = batch?;
}
```

- Primary ranges are half-open: `[start, end)`.
- Timestamp and `date32` ranges are explicitly typed.
- Filtering without a compatible primary column is a schema error.
- Block-level bounds are used for pruning before stream decoding.
- The primary column may be decoded for filtering even when not included in the output projection.

### Scan order

The first reader should expose explicit file order rather than silently promising global timestamp order.

```rust
reader.scan().file_order()
```

- File order means block sequence order and row order within each decoded block.
- Global primary ordering is deferred until its memory behavior and handling of unsorted blocks are fully specified.
- A future merge-by-primary operation may require all selected blocks to declare `TS_SORTED`, or may require a separately designed bounded-memory sort.

## Writer API

Writing begins only after the parser, validator, schema reader, and basic reader are trusted.

```rust
let mut writer = Writer::create(path, schema, WriterOptions::default())?;
writer.append(batch)?;
writer.flush()?;
writer.sync()?;
let summary = writer.finish()?;
```

- `Writer::create` uses exclusive creation and never overwrites an existing file.
- Appends consume batches so buffering, splitting, and coalescing do not require cloning.
- Default target block size is 65,536 rows, with a configurable byte target to bound large variable-width blocks.
- Zstandard level 3, automatic statistics, and automatic `TS_SORTED` detection are reasonable initial defaults.
- Crossing configured thresholds may publish complete blocks automatically.
- `flush` publishes complete frames without guaranteeing durable storage.
- `sync` first flushes and then performs the platform durability operation.
- `finish(self)` flushes, synchronizes, consumes the writer, and returns a `WriteSummary`.
- `Drop` performs no I/O. Dropping a writer may discard only uncommitted buffered rows.
- `Writer` should be marked `#[must_use]`, and debug builds may warn when a writer is dropped with buffered rows.
- A partial I/O failure poisons the writer. Further operations fail, previously committed blocks remain readable, and the incomplete tail remains for a future recovery API.
- Multiple writers are unsupported. `Writer` is `Send` but not `Sync`.

```rust
pub struct WriteSummary {
    pub rows_written: u64,
    pub blocks_written: u64,
    pub bytes_written: u64,
    pub last_sequence: Option<u64>,
}
```

## Append reopening

Append reopening is available after ordinary file creation and round-trip
validation.

Future direction:

```rust
let mut writer = Writer::open(path, WriterOptions::default())?;
```

- The existing file schema is authoritative and is returned by `Writer::schema`.
- `Writer::open_with_schema` supplies an optional exact expected-schema guard.
- Exact matching includes column IDs, logical types, nullability, and primary selection.
- `Writer::open_with_limits` reads the existing file under explicit `Limits`.
- Append reopening refuses incomplete tails, continues sequences and row IDs, and
  holds a cooperative exclusive writer lock that never blocks readers.
- Reopening performs structural and whole-frame validation, not `ValidationLevel::Full`.
- Append reopening must never silently truncate or repair a file.

## Metadata and errors

The initial stable metadata API should remain narrow:

- format version and feature flags;
- `FileId`, `SchemaId`, `ColumnId`, and block sequence identifiers;
- file metadata;
- block row counts;
- block offsets and total lengths;
- primary bounds;
- sortedness declarations;
- incomplete-tail status.

Physical stream descriptors, parser internals, serializers, recovery machinery, and low-level frame construction remain private.

Use one crate-wide structured error:

```rust
pub struct Error {
    kind: ErrorKind,
    offset: Option<u64>,
    context: Vec<ErrorContext>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}
```

Error kinds distinguish:

- I/O;
- invalid schema;
- invalid batch;
- unsupported format or feature;
- corruption;
- incomplete tail;
- resource limits;
- schema mismatch;
- unsupported codec or transform;
- poisoned writer state.

Error context may identify a frame sequence, column ID, stream index, or row in addition to a file offset.

# Development stages

## Stage 0 — API skeleton and repository foundations

Goal: establish boundaries without committing to unimplemented behavior.

Deliverables:

- one `acta` crate;
- module layout;
- crate-wide `Error`, `ErrorKind`, and `Result<T>`;
- resource-limit types;
- public API skeletons that compile;
- documentation examples marked appropriately while implementation is incomplete;
- CI for formatting, Clippy, tests, and supported platforms.

Exit criteria:

- crate compiles on the selected MSRV;
- no wire-format parsing exists in public API code;
- internal visibility boundaries are established with `pub(crate)`.

## Stage 1 — Checked wire parser and structural validator

Goal: determine whether a byte sequence is structurally valid Acta v0.2 and identify the first invalid or incomplete location.

Deliverables:

- checked byte cursor and endian helpers;
- prologue parser and CRC validation;
- generic frame-prefix parser;
- checked frame-length calculation;
- header and trailer validation;
- commit-marker and sequence validation;
- incomplete-tail classification;
- `acta::validate` structural mode;
- validation reports with frame counts and valid byte extent.

Not included:

- stream decompression;
- logical array construction;
- writer;
- projection;
- Arrow.

Exit criteria:

- every checked-in v0.2 compatibility fixture is structurally classified correctly;
- malformed input cannot panic, hang, over-allocate, or perform unchecked slicing;
- fuzzing covers the top-level validator.

## Stage 2 — Schema and data-frame metadata validation

Goal: fully understand frame metadata without yet exposing decoded user data.

Deliverables:

- schema-frame parsing;
- logical-type and parameter validation;
- column-ID, name, nullability, and primary-selection checks;
- data-frame header parsing;
- column and stream descriptor validation;
- statistics-area validation;
- stream alignment, range, non-overlap, and stored CRC checks;
- public schema and descriptive block metadata.

Exit criteria:

- all schema and metadata invariants in v0.2 are covered by targeted tests;
- corrupted descriptor tables are rejected with precise context;
- metadata can be inspected without decoding column values.

## Stage 3 — Stream decoding and full validation

Goal: decode every v0.2 stream representation and provide a complete validator.

Deliverables:

- raw fixed-width and variable-width decoding;
- bit packing;
- frame-of-reference;
- delta;
- delta-of-delta;
- byte-stream split;
- Boolean RLE;
- Zstandard decompression;
- validity handling and dense-null reconstruction;
- dictionary and run-length layout validation;
- UTF-8 validation;
- statistics and `TS_SORTED` verification;
- `ValidationLevel::Full`.

Exit criteria:

- every logical type and transform is covered;
- all-null, all-valid, and mixed-null cases are validated;
- malformed compressed and transformed streams fail safely;
- full validation passes all checked-in valid fixtures and rejects all corruption fixtures.

## Stage 4 — Basic snapshot reader

Goal: open valid files and return native Acta arrays and record batches.

Deliverables:

- native closed-enum arrays;
- explicit validity bitmaps;
- immutable schema objects;
- `RecordBatch` with explicit row count;
- snapshot discovery of committed frames;
- sequential block decoding;
- lazy full-column scan iterator;
- explicit file-order behavior;
- reader resource limits.

Not included:

- projection optimization;
- primary-range filtering;
- global primary ordering;
- append reopening.

Exit criteria:

- every valid fixture can be opened and read;
- values round-trip against known fixture expectations;
- incomplete final frames are ignored by snapshot reads and reported in metadata.

## Stage 5 — Projection and primary-range scans

Goal: make columnar reads efficient.

Deliverables:

- column projection;
- reordered and empty projections;
- block pruning using primary bounds;
- half-open timestamp and `date32` primary ranges;
- row filtering within selected blocks;
- decoding of the primary column when needed but not projected;
- scan-level resource limits.

Exit criteria:

- unprojected streams are not decoded unnecessarily;
- range boundaries are tested exactly;
- empty projections preserve correct row counts;
- filtering without a compatible primary column returns a schema error.

## Stage 6 — Deterministic writer

Goal: create valid Acta v0.2 files that are independently accepted by the validator and reader.

Deliverables:

- deterministic prologue and schema serialization;
- block construction;
- deterministic plain layouts using raw streams and codec `none`;
- per-stream and frame CRC calculation;
- commit-trailer writing;
- `Writer::create`;
- `append`, `flush`, `sync`, and `finish`;
- poisoned-writer state;
- `WriteSummary`;
- no-I/O-on-drop behavior.

Exit criteria:

- every logical type round-trips through writer, validator, and reader;
- generated files pass structural validation and decode through the reader;
- block splitting and coalescing are deterministic under fixed options;
- durability semantics are covered by tests where the platform permits.

## Stage 7 — Buffered ingestion and production writer behavior

Goal: make the writer practical for sustained append workloads while adding
the first optional compressed output path.

Deliverables:

- configurable row and byte block targets;
- bounded buffered data and automatic block publication at thresholds;
- explicit writer codec selection, retaining deterministic raw output as the
  baseline;
- Zstandard stream compression behind the existing optional `zstd` feature;
- compressed and uncompressed writer/validator/reader round-trip tests;
- initial throughput, compression-ratio, and memory-boundedness benchmarks;
- clear accounting of buffered, published, and durable rows.

Not included:

- append reopening;
- explicit recovery or tail repair;
- statistics generation or selection heuristics;
- automatic `TS_SORTED` detection;
- adaptive encoding selection.

Exit criteria:

- memory remains bounded under configured limits;
- raw and Zstandard output are independently accepted by the validator and
  decoded by the reader;
- throughput and compression behavior are benchmarked;
- dropping with buffered rows is observable in tests and documentation.

## Stage 7b — Writer transforms and adaptive encoding

Goal: generate specialized stream encodings when they provide a measured win
over deterministic raw output.

Deliverables:

- writer support for bit packing, frame of reference, delta,
  delta-of-delta, byte-stream split, Boolean RLE, dictionary, and run-length
  layouts where valid for the logical type;
- one profiling pass over each candidate stream, including descriptor cost,
  transformed size, and compressed size where Zstandard is enabled;
- deterministic selection of a specialized encoding only when it beats raw by
  a documented margin;
- deterministic raw fallback for invalid, unsupported, or unprofitable
  candidates;
- transform-specific round-trip, malformed-output, determinism, and benchmark
  coverage.

Not included:

- append reopening;
- explicit recovery or tail repair;
- statistics generation or selection heuristics;
- global ordering or query planning.

Exit criteria:

- every writer-generated transform is decoded by the existing reader;
- every logical type has a safe raw fallback;
- encoded output remains independently accepted by the validator;
- selection is reproducible under fixed options and input;
- benchmarks demonstrate when specialized encodings beat raw plus Zstandard.

## Stage 7c — Statistics generation and selection

Goal: write useful block-local statistics without imposing unjustified storage
or ingestion cost.

Status: complete. The public `WriterStatistics` policy adds `None`, `MinMax`,
and deterministic `Automatic` selection without changing default bytes. The
reference benchmark and its methodology are in `benchmarks/stage7c/`.

Deliverables:

- deterministic min/max statistics generation for supported logical types;
- explicit writer controls for statistics generation;
- a size-and-count heuristic selecting statistics only where their encoded size
  and CPU cost are small against the data they describe;
- writer/validator/reader round-trip coverage for present and absent
  statistics;
- integration with Stage 5 block pruning without exposing physical statistic
  descriptors publicly.

Not included:

- statistics verification, which belongs to Stage 3 full validation;
- append reopening or repair;
- general query planning;
- consuming optional statistics for pruning, which is future advanced-read
  work.

Exit criteria:

- generated statistics are accepted by structural validation and agree with
  decoded values under `ValidationLevel::Full`;
- output and heuristic decisions are reproducible under fixed options and
  input;
- benchmarks quantify statistics overhead, and show that optional statistics
  leave Stage 5 primary-bound pruning unchanged.

The `Automatic` thresholds are deliberately not derived from a measured pruning
benefit, because no reader path consumes optional statistics yet: Stage 5 prunes
on the mandatory primary bounds alone, and statistics-based pruning remains a
future advanced-read feature.
Until a reader consumes them, the writer cannot know a query distribution to
price against, so the rule is stated as a conservative size floor and the
benchmark measures only the cost side. Revisit the thresholds once a reader
actually consumes optional statistics for pruning.

## Stage 8 — Append reopening

Goal: safely continue writing an existing valid Acta file.

Deliverables:

- `Writer::open`, `Writer::open_with_schema`, and `Writer::open_with_limits`;
- schema reconstruction from file, exposed by `Writer::schema`;
- exact expected-schema guard through `Writer::open_with_schema`;
- sequence and implicit-row-ID continuation;
- explicit refusal or policy for incomplete tails;
- writer-exclusion strategy or clear unsupported-concurrency behavior.

Exit criteria:

- reopening never overwrites committed data;
- schema mismatches fail before writing;
- interrupted append scenarios are covered by tests;
- no implicit repair or truncation occurs.

## Stage 8b — Explicit recovery and tail repair

Goal: provide an intentional, auditable way to restore appendability after an
interrupted write without weakening the reader's corruption rules.

Deliverables:

- [x] a read-only recovery inspection that reports the last committed offset and
  proposed action through `RecoveryPlan` and `RecoveryAction`;
- [x] an explicit repair operation that truncates only a verified incomplete
  tail through `RecoverySummary`;
- [x] exclusive-access requirements and refusal to repair complete corruption;
- [x] synchronization and post-repair validation;
- [x] recovery-boundary and injected-I/O-failure tests.

The public entry points are:

```rust
pub fn inspect_recovery<P: AsRef<Path>>(path: P) -> Result<RecoveryPlan>;
pub fn inspect_recovery_with_limits<P: AsRef<Path>>(
    path: P,
    limits: Limits,
) -> Result<RecoveryPlan>;
pub fn repair_incomplete_tail<P: AsRef<Path>>(path: P) -> Result<RecoverySummary>;
pub fn repair_incomplete_tail_with_limits<P: AsRef<Path>>(
    path: P,
    limits: Limits,
) -> Result<RecoverySummary>;
```

Inspection is a read-only point-in-time snapshot and never grants permission
to mutate a later file state. Repair independently opens an existing file
read/write, acquires the same cooperative exclusive writer lock as `Writer`,
rescans the locked handle, rechecks its physical length, truncates exactly the
verified incomplete range, synchronizes, and validates again on the same
handle/inode. It is destructive but narrowly bounded: only a physically
incomplete final data frame after a fully validated committed prefix is
repairable. Complete corruption, checksum-failing final frames, incomplete
schema frames, salvage, and caller-selected offsets are refused or
unimplemented.

Exit criteria:

- [x] no repair occurs implicitly during open or append;
- [x] committed frames are never modified;
- [x] corruption inside a complete frame is refused rather than truncated;
- [x] repaired files validate and can be reopened for append.

## Stage 9 — Reader refresh and tailing

Goal: let a long-lived synchronous reader discover newly committed frames
without weakening snapshot, validation, or recovery semantics.

Deliverables:

- [x] an explicit refresh operation that extends a reader snapshot from its last
  committed boundary;
- [x] synchronous tail following with explicit polling, cancellation, and retry of
  a physically incomplete final frame;
- [x] preservation of schema identity, sequence continuity, implicit row-ID
  continuity, CRC checks, resource limits, projection, primary ranges, and file
  order across refresh boundaries;
- [x] append, repair, replacement, truncation, corruption, and multi-process
  concurrency coverage.

Not included:

- async runtime integration;
- merge-by-primary or global sorting;
- public row-ID exposure;
- optional-statistics predicate pruning;
- general query planning.

Exit criteria:

- [x] a reader sees only complete committed frames and never exposes partial rows;
- [x] refresh adds each committed frame exactly once and rejects incompatible file
  replacement, truncation, sequence breaks, row-ID breaks, and corruption;
- [x] tail following can be stopped without leaking threads or handles;
- [x] projection, range filtering, and stable file order behave identically before
  and after refresh.

## Stage 10 — API ergonomics and reader benchmark baseline

Goal: stabilize the native API and establish trustworthy measurements before
bindings or performance changes amplify its current choices.

Deliverables:

- an ergonomics audit covering schema construction, arrays and batches,
  writer create/open/append, scans, refresh/tailing, validation, recovery,
  options, limits, errors, and common ownership patterns;
- focused rustdoc and end-to-end examples for common workflows;
- a reproducible multi-block reader benchmark covering cold and warm open,
  structural and full validation, full scans, sparse projections, selective
  ranges, refresh, tailing, allocations, peak memory, bytes read, and
  concurrent readers;
- a recorded pre-optimization baseline, methodology, datasets, and correctness
  checks;
- release-facing fuzz, Miri where supported, and cross-platform
  locking/recovery/tailing coverage.

Exit criteria:

- ordinary workflows require no wire-format or internal-layout knowledge;
- the public API is documented as the candidate surface the Python binding may
  wrap;
- benchmark results are reproducible and separate correctness assertions from
  timing noise;
- no optimization milestone begins without a relevant baseline.

## Stage 11 — PyO3 bindings

Goal: expose the stable Rust v0.2 implementation to Python without creating a
second format implementation or adding Python dependencies to the core crate.

Deliverables:

- a separate PyO3/maturin binding crate;
- Python APIs for schemas, validation, writer creation/reopening, append,
  scans, projections, primary ranges, refresh/tailing, and recovery;
- Pythonic ownership, iteration, context-manager, and exception behavior;
- Python sequence and NumPy ingestion with minimal copying, with any PyArrow
  support isolated to the binding or an optional adapter;
- logical-type, null, timestamp/timezone, limit, and structured-error mapping;
- Rust/Python differential tests and wheels for the initially supported
  platforms.

The existing `legacy/python-v0.1/` package remains a v0.1 design-history and fixture tool.
Stage 11 wraps the Rust core and does not evolve that package into a parallel
v0.2 implementation.

Exit criteria:

- Python-created files pass Rust structural and full validation;
- Rust-created files round-trip through the Python API;
- append, refresh/tailing, projection/range, and recovery lifecycle tests pass
  across the binding boundary;
- Python dependencies remain absent from the core crate's dependency graph.

## Stage 12 — Measured optimization

Goal: improve Rust and Python workload performance only where Stage 10 and
Stage 11 measurements identify a material bottleneck.

Deliverables may include, when justified by before/after evidence:

- bitmap, dictionary/RLE, variable-width-offset, and byte-stream-split
  improvements;
- a single payload pass for frame and stream CRC verification;
- reusable checksum and decompression buffers;
- adaptive coalescing of adjacent projected streams versus whole-payload reads;
- CRC32C specialization;
- bounded decoded-column caching;
- bounded parallel block or column decoding.

Requirements:

- preserve strict verification, resource limits, projection isolation, stable
  ordering, malformed-file rejection, and the Stage 10 public behavior;
- retain before/after benchmark results for throughput, allocations, peak
  memory, and bytes read;
- avoid adding concurrency or caching without explicit memory, ordering,
  cancellation, and failure semantics;
- optimize the native core where the bottleneck is shared, and the binding
  layer only where conversion or Python ownership is the measured cost.

Exit criteria:

- every optimization has reproducible evidence and regression coverage;
- correctness and malformed-input suites remain unchanged or stronger;
- performance claims identify datasets, configuration, hardware, and variance.

# Future stages

## Future Stage 1 — Advanced read behavior

Goal: add higher-level behavior only after its cost and semantics are well defined.

Possible deliverables:

- merge-by-primary for blocks that all declare `TS_SORTED`;
- bounded-memory global primary sorting;
- row-ID exposure;
- richer statistics-based pruning.

Each feature requires a separate design decision rather than being implied by the initial scan API.

## Future Stage 2 — Integrations

Arrow is moved here and is not part of the initial implementation plan.

A future optional `acta-arrow` crate may provide explicit conversions:

```rust
from_arrow_schema(...)
to_arrow_schema(...)
from_arrow_batch(...)
to_arrow_batch(...)
```

Requirements:

- Arrow dependencies never enter the core `acta` dependency graph.
- Schema import requires explicit Acta primary selection.
- Only lossless mappings to Acta v0.2 types are accepted.
- Unsupported types or metadata return structured adapter errors.
- Dictionary arrays may map to categorical values where compatible.
- No initial zero-copy guarantee is made.

Other future integrations may include:

- C ABI and C++ wrapper;
- DataFusion or DuckDB adapters;
- an optional query layer for predicate planning beyond Stage 5 primary-range
  pruning;
- Serde helpers;
- Chrono conversions;
- async wrappers.

## Test plan by category

### Parser and validator

- prologue magic, version, feature flags, reserved bytes, and CRC;
- truncated prefix, header, payload, and trailer;
- unaligned or overflowing lengths;
- invalid sequence numbers and frame types;
- invalid schema descriptors and type parameters;
- overlapping stream ranges;
- invalid stored and body CRCs;
- malformed bit-packed, delta, dictionary, and run-length streams;
- decompression limits and malformed Zstandard frames;
- incomplete-tail versus corruption classification;
- fuzz testing with arbitrary bytes.

### Schema, arrays, and batches

- every logical type;
- invalid precision, scale, timezone, and fixed-width parameters;
- duplicate names and IDs;
- invalid primary selection and nullable primary columns;
- all-valid, all-null, and mixed validity;
- UTF-8 and variable-width offset validation;
- zero-column batches with explicit row counts;
- schema and array mismatches.

### Reader

- every checked-in valid fixture;
- full-column scans;
- file-order behavior;
- complete and incomplete final frames;
- full, reordered, empty, and unknown projections;
- timestamp and date range boundaries;
- absent primary indexes;
- resource limits;
- coexistence of a snapshot reader with a writer.

### Writer

- create-without-overwrite;
- deterministic serialization;
- round-trip every logical type;
- block splitting and coalescing;
- `flush`, `sync`, and `finish`;
- drop with buffered data;
- partial-I/O poisoning;
- generated-file validation;
- append reopening and schema mismatch through `Writer::open` and
  `Writer::open_with_schema`.

### Tooling and platforms

- compile and run public documentation examples;
- formatting;
- Clippy with warnings denied;
- unit, integration, documentation, and fuzz tests;
- cross-platform fixture tests on Linux, macOS, and Windows;
- performance benchmarks for parsing, projection, decompression, and writing.

## Assumptions

- Acta v0.2 remains the authoritative wire format; this work designs the Rust implementation and public API, not a new format version.
- Rust 2024 and MSRV 1.85 remain the initial baseline unless compatibility requirements justify an older edition or compiler.
- The crate is pre-1.0, so existing Rust APIs may be replaced without deprecation.
- Full-file validation and explicit incomplete-tail recovery are part of the
  core implementation; salvage and corrupt-final-frame approval remain future
  policy features.
- Advanced encoding controls, parallel block preparation, global primary
  sorting, public row IDs, salvage, corrupt-final-frame approval, and broader
  external integrations are intentionally deferred. Reader refresh/tailing,
  PyO3 bindings, and measured optimization are active Stages 9, 11, and 12.
  The Zstandard compression level remains exposed with the codec selection it
  belongs to; everything else about stream encoding remains policy-selected.
