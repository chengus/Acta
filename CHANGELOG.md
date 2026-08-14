# Changelog

Entries under a format heading describe the binary format itself. Entries
under a crate heading describe this implementation's behavior, which can
tighten without the format changing.

## Rust crate 0.1.0-alpha.1 — 2026-08-14

First public alpha of the Rust implementation. The package is published as
`acta-core`, while its Rust library name remains `acta`.

- Added `Reader::refresh`, `Reader::tail`, `RefreshReport`, and `Tail`.
  `Reader::refresh` extends a snapshot in place with exactly the frames
  committed since the reader opened, resuming from its own last committed
  byte offset, expected sequence, and expected implicit row ID without
  rescanning the committed prefix. Every newly complete frame is validated
  exactly as initial opening validates it — prefix and prefix CRC, bounded
  lengths and checked arithmetic, header, payload, trailer, and body CRC,
  frame type and sequence, schema ID, block metadata, implicit row-ID
  continuity, and resource limits — and the snapshot is mutated only after
  the discovery pass succeeds, so a failed refresh leaves every field and
  block vector unchanged. A no-growth refresh succeeds with zero additions; a
  physically incomplete next frame is an interrupted append that exposes no
  block and is reported through `RefreshReport::incomplete_tail`, becoming a
  complete committed frame on a later refresh; a safe Stage 8b repair that
  removed the uncommitted tail is accepted and clears the tail. Refresh never
  truncates, repairs, or acquires the writer lock, so a reader and a writer
  coexist on the same path. A file that shrank below the committed boundary is
  refused with the new `ErrorKind::FileTruncated`.
- A refresh refuses to extend a snapshot from a file that is not the one it
  came from, and checks that two ways because neither alone is enough. It
  compares the path's current file-system identity — not the v0.2 file ID,
  which the deterministic writer stores as a non-unique zero — against the
  identity captured at open, which catches a replacement that unlinked and
  recreated the path. Because an in-place truncate-and-rewrite keeps that
  identity while replacing every byte, it also re-reads the commit trailer of
  the last frame the snapshot committed — its length, sequence, body CRC, own
  CRC, and commit magic — and refuses unless it still matches, at the cost of
  one read rather than a pass over the file. On growth it compares the
  prologue and schema it re-reads against the ones the snapshot holds, so a
  reader can never end up validating new frames against a schema its own
  `Reader::schema` does not return. Any mismatch is the new
  `ErrorKind::FileReplaced` rather than a silent adoption. A snapshot holding
  no data blocks has only its prologue and schema frame to be recognised by,
  so a replacement whose prologue and schema are byte-identical is accepted —
  but such a file agrees with everything that snapshot has exposed.
- `Reader::tail` returns a `Tail` that polls the path for newly committed
  frames synchronously, one block per poll, starting after the blocks the
  reader already holds. Polling never sleeps, spawns a thread, or owns a
  timer; `Ok(None)` means "pending now", never end-of-stream, so `Tail` is
  not an `Iterator` and cannot be fused, and cancellation is simply dropping
  the tail. A block that prunes on its bounds or filters down to no rows is
  consumed inside the poll that reaches it rather than reported as `None`, so
  waiting on `None` never means waiting for data already on disk.
  `Tail::metrics` reports more bytes than an equivalent `Scan`, because it
  counts the frames its refreshes read to discover the blocks as well as the
  bytes its decodes read; the row and decoded-byte allowances from `Limits`
  still bound decoding only. Projection, reordered and empty projection, timestamp and `date32`
  primary ranges, pruning, file order, per-block decode-error handling, and
  cumulative scan limits all match `Scan`, and a partial frame is never
  exposed. A tail borrows its reader, so the reader cannot be refreshed
  directly while a tail exists — the same snapshot safety boundary `Scan`
  draws.
- Added the `ErrorKind::FileReplaced` and `ErrorKind::FileTruncated` kinds for
  a long-lived reader whose path now names a different file than the one its
  snapshot came from, or still names its own file but has lost committed
  bytes. Neither is corruption of the file the reader holds, and a caller can
  now tell all three apart without matching on a message.
- Added the `acta_append` test-only binary used by the multi-process tail
  coverage to append one frame from a separate process. It is a bin target
  behind the non-default `test-fixtures` feature, so `cargo install` never
  puts a test fixture on a user's PATH; CI enables the feature.
- Added `Reader::refresh` and `Reader::tail` coverage for no-growth refreshes,
  one and several added blocks, exactly-once cycles, buffered versus flushed
  writer rows, reader/writer coexistence, every incomplete-tail cut,
  completion, repair acceptance and continuation, row-ID continuation,
  corruption/sequence/schema/row-ID/resource-limit rollback, committed-prefix
  truncation, same-path replacement with different schemas and identical
  lengths, in-place rewrite that preserves the file identity, pruned and
  emptied blocks that must not hide the block behind them, discovery bytes in
  tail metrics, clone and old-scan isolation, projection and primary ranges across
  refresh boundaries, zero-column row counts, validation agreement, pending
  polls, cancellation by drop, date32 and sorted/unsorted filtering, finite
  cumulative limits, and multi-process tailing.
- Added read-only `inspect_recovery` and explicit `repair_incomplete_tail`
  operations. Inspection is a point-in-time structural snapshot; repair
  independently locks and rescans the current file, then truncates only a
  physically incomplete final data frame before synchronizing and scanning it
  again. Complete corruption, checksum-failing final frames, and incomplete
  schema frames are refused, and repair never accepts a caller-provided offset.
  Every failure before the truncation leaves the file unchanged and says so;
  every failure that can follow one — the synchronization, the rescan, or the
  post-repair validation — instead says the tail was already removed, whatever
  its `ErrorKind`. The two sets of messages never overlap, so the two sides of
  the mutation are always distinguishable.
- Added `WriterOptions::zstd_level` and `WriterOptions::with_zstd_level`,
  selecting the Zstandard compression level the writer applies, alongside the
  new `DEFAULT_ZSTD_LEVEL` constant that names the level the writer has always
  used. This is additive: `WriterOptions` is `#[non_exhaustive]`, the field is
  reachable only through the builders, and the default is unchanged, so a file
  written without asking for a level is byte-for-byte what the previous writer
  produced. `Writer::create` and `Writer::open` refuse a level outside the
  range the linked Zstandard library reports, before the file is created or
  opened; the level is validated only when the selected codec is Zstandard.
- Added the `WriterStatistics` writer policy and `WriterOptions::statistics`,
  selected with `WriterOptions::with_statistics`. This is additive rather than
  breaking: `WriterOptions` is `#[non_exhaustive]` and the field is reachable
  only through the builders, and `WriterStatistics::None` is the default, so a
  file written without asking for statistics is byte-for-byte what the Stage 7b
  writer produced under every encoding, codec, row-ID, and block-target
  combination.
- `WriterStatistics::MinMax` writes a canonical section 11 minimum and maximum
  for every column whose logical type has a fixed canonical width, including
  the primary column, whose bounds section 11 says need not be repeated. Nulls
  and NaNs are ignored, a column left with no value gets no statistic,
  infinities are ordinary bounds, and equal floating-point values keep the
  first bit pattern seen, so `-0.0` and `0.0` resolve deterministically without
  either being claimed as the smaller number. `utf8`, `categorical`, and
  `binary` have no v0.2 min/max encoding and never receive one.
- `WriterStatistics::Automatic` applies one deterministic rule: not the primary
  column, at least 64 non-null non-NaN values, and raw dense value bytes at
  least eight times the pair. The size test binds only on `bool`, where it
  raises the floor to 121 values. The rule depends on nothing outside the
  block's contents and the schema, so equal blocks produce equal bytes whatever
  the append partitioning.
- Statistics are charged against the frame header budget while a block is
  priced, so the writer never assembles a block whose statistics it could not
  publish. A schema whose pairs cannot fit that budget — reachable only through
  very wide `fixed_binary` columns — is now refused by `append` with an error
  naming the statistics policy rather than reporting an oversize row.
- **Breaking:** Added `WriterEncoding::Fixed(WriterTransform)` and the public
  `WriterTransform` enum it carries. `WriterEncoding` is not
  `#[non_exhaustive]`, so a downstream match that covered `Raw` and `Adaptive`
  exhaustively no longer compiles. The default policy and the output of the
  existing policies are unchanged.
- The fixed policy applies one requested transform or layout to every column
  value stream, prices nothing, and never falls back to raw. `Writer::create`
  refuses a transform this writer does not offer for one of the schema's
  logical types, before the file is created; a block whose values the transform
  cannot describe fails as `ErrorKind::InvalidArgument` when it is published,
  which poisons the writer and leaves the blocks before it readable. The
  offered set is exactly the section 12 candidate set `Adaptive` prices, which
  is narrower than the format permits — a dictionary `timestamp64` column is
  legal v0.2 that neither policy writes — so a fixed file is always a shape
  adaptive could also have produced. Validity streams stay raw under `Fixed`.
- Added column projection, primary-range filtering, and block pruning to
  `Reader::scan`. `Scan` gained `project`, `primary_range`, `file_order`, and
  `metrics`; `PrimaryRange` and `ScanMetrics` are new public types. The default
  projection is every column in schema order, so an existing scan is unchanged.
  `Reader::read_block` is unchanged and still decodes every column.
- **Breaking:** `Scan` no longer implements `ExactSizeIterator`. Pruning and
  row filtering mean the number of remaining items cannot be known before the
  blocks are read: a block whose bounds overlap a range may hold no matching
  row and yield nothing. `Scan::size_hint` now reports a lower bound of zero
  and the candidate-block count as its upper bound. The nearest replacement for
  the removed `len` is `Scan::remaining_candidate_blocks`, which is deliberately
  named differently because it is an upper bound rather than a count; code that
  called `len` through the trait will not compile against the new method, which
  is preferred here to the same call silently changing meaning.
- Added cumulative `Limits::max_rows_per_scan` and
  `Limits::max_decoded_scan_bytes`, bounding one scan across all of its blocks.
  Both default to no limit, so existing scans do not acquire a cap. Pruned
  blocks and unprojected columns are never charged, per-block limits still
  apply independently, and `Reader::read_block` does not inherit a scan's
  cumulative state.
- A projected read now defers the unsupported-transform and unsupported-codec
  checks for columns it will not decode. Structural descriptor, stream-range,
  length, and frame-envelope validation still cover the whole block, and every
  read that does decode such a column still fails. This widens the set of files
  a projection can read; it does not widen the set of files that validate.
- Statistics verification, section 8 primary bounds, and `TS_SORTED` are
  checked for exactly the columns a read decodes, which for a range scan
  includes the primary column even when it is not projected.
- Added `ValidationLevel`, `ValidationOptions`, and `validate_with_options`.
  `validate` and `validate_with_limits` keep their existing structural
  behavior; full validation decodes every complete block and checks optional
  min/max statistics against the decoded values.
- Tightened data-frame column-descriptor validation: a column whose statistics
  flag is clear must now store a zero statistics offset and a zero statistics
  length. Section 8.1 requires statistics offsets to lie in the statistics
  area but does not spell out what the fields hold when a column claims no
  statistics, so this rule is stricter than the format text. It rejects a file
  whose descriptor carries stale or arbitrary bytes in fields it also declares
  meaningless, which is a writer defect this crate would rather name than
  ignore. This affects block decoding and full validation only; structural
  validation does not read the column descriptor table. No writer in this
  crate has ever emitted such a descriptor, and every checked-in v0.2 fixture
  satisfies the rule.
- Statistics verification now runs per column, immediately after that column's
  values are reconstructed, instead of after the whole block is assembled. The
  set of accepted files is unchanged; when a block has more than one fault the
  error that surfaces first may differ.
- Data-frame column-descriptor errors now name the offending column.

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
