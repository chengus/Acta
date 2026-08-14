# Acta

Acta is an append-only, strongly typed file format for time-series data. It is designed for fast sequential ingestion, compact storage, concurrent readers, and efficient time-range queries—all in one file. Files that only need simple appendable columnar storage may omit the time index.

The Rust implementation is distributed in the `acta-core` package and imported
as `acta`:

```toml
[dependencies]
acta = { package = "acta-core", version = "0.1.0-alpha.1" }
```

## Design

Producers collect rows in private buffers and encode them as immutable, compressed columnar blocks. Completed blocks are appended to the file with only brief coordination; their physical order does not need to match timestamp order. A file normally selects a primary time column, typed as either a timestamp or a calendar date, and every block records that column's min/max bounds. A block whose primary timestamps are already nondecreasing can declare it with a `TS_SORTED` flag, letting readers binary-search within the block and merge sorted blocks without re-sorting. Readers use per-block metadata such as time bounds, row count, schema, and column statistics to skip irrelevant data, then merge matching blocks when ordered results are required.

In v0.2 the primary time column is optional. Without one, Acta still provides typed, compressed, checksummed appendable blocks, but timestamp-range pruning, timestamp ordering, and `TS_SORTED` are unavailable. Writer APIs can select a named column, use `"auto"` to select the first timestamp or date column in schema order, or use `None` to omit the time index.

There is no mutable file footer. Every frame is self-delimiting and ends in a checksummed commit trailer, so a reader discovers new blocks by continuing from the byte after the last complete frame—concurrent tailing needs no coordination with the writer. After an interrupted append, explicit recovery can inspect the validated boundary and, with user approval, truncate only a physically incomplete final data frame; per-stream CRCs mean the format can check a projected read against only the streams it touches, without a whole-block checksum standing in the way.

## Scanning

`Reader::scan` is a lazy, file-order iterator. The default projection returns
every schema column in schema order. Projection names are validated when the
builder is configured, and the requested order becomes the batch order; an
empty projection is valid and returns zero-column batches with the original
row counts.

```rust,no_run
# use acta::Reader;
# let reader = Reader::open("data.acta")?;
for batch in reader.scan().project(["timestamp", "value"])? {
    let batch = batch?;
    assert_eq!(batch.schema().column_count(), 2);
}
# Ok::<(), acta::Error>(())
```

Primary ranges are explicitly typed and half-open: `[start, end)`. Timestamp
endpoints are raw integer values in the primary timestamp column's declared
storage unit; no unit conversion is performed. A `date32` range uses signed
day values.

```rust,no_run
# use acta::{PrimaryRange, Reader};
# let reader = Reader::open("data.acta")?;
# let (start, end) = (0, 1);
# let (first_day, last_day) = (0, 1);
// A window of raw timestamp values, returning only `value`. The primary
// column is read to filter the rows and then dropped.
let mut scan = reader
    .scan()
    .project(["value"])?
    .primary_range(PrimaryRange::timestamp(start, end))?
    .file_order();
let mut rows = 0;
for batch in scan.by_ref() {
    rows += batch?.row_count();
}
assert_eq!(scan.metrics().rows_returned(), rows as u64);

// A range over a `date32` primary column, counting matching rows without
// decoding a single value column.
let mut matching = 0;
for batch in reader
    .scan()
    .project([] as [&str; 0])?
    .primary_range(PrimaryRange::date32(first_day, last_day))?
{
    matching += batch?.row_count();
}
# Ok::<(), acta::Error>(())
```

Ranges prune blocks using inclusive metadata bounds before decoding. Within a
selected block, sorted primary values use contiguous binary-search boundaries;
unsorted values retain their original row order. The primary column is decoded
internally when filtering requires it, even if it is absent from the output,
and is not returned unless projected. `file_order()` means committed block
order and original row order within each block; scans do not provide global
timestamp ordering.

A failure is yielded as an iterator item and the scan continues with the next
block, so a caller that wants to stop at the first one has to stop itself.
`Scan::remaining_candidate_blocks` reports how many blocks pruning has not
already excluded; it is an upper bound on the batches still to come rather
than a count of them, because a block that overlaps a range may hold no
matching row. For the same reason `Scan` is not an `ExactSizeIterator`.

`Scan::metrics` reports what a scan did: blocks considered and pruned, streams
decoded, rows returned, and two byte counters that answer different questions.
`stream_bytes_decoded` is the stored size of the streams the scan decoded,
which is the work projection and pruning remove. `bytes_read` is every byte
the scan read from the file, and it is larger: reading a block verifies its
whole frame body against the commit trailer before any stream is decoded, so
projection narrows what a scan decodes rather than what it reads. Both come
from the lengths the reader actually reads, not from the file size.

Cumulative `Limits::max_rows_per_scan` and `Limits::max_decoded_scan_bytes`
bound a whole scan; both default to no limit. Pruned blocks and unprojected
columns are never charged, per-block limits still apply on top, and
`Reader::read_block` never inherits a scan's cumulative state.

### Refresh

A `Reader` is a snapshot: it holds the schema and committed blocks discovered
when it opened, and a later append is not visible through it. `Reader::refresh`
extends the snapshot in place with exactly the frames committed since the
reader opened. It resumes from the reader's own last committed byte offset,
expected next frame sequence, and expected next implicit row ID, so it never
rescans the committed prefix, and it validates every newly complete frame
exactly as initial opening validates a whole file: prefix and prefix CRC,
bounded lengths, header, payload, trailer, and body CRC, frame type and
sequence, schema ID, block metadata, implicit row-ID continuity, and the
configured resource limits.

```rust,no_run
# use acta::Reader;
let mut reader = Reader::open("data.acta")?;
let before = reader.blocks().len() as u64;
let report = reader.refresh()?;
assert_eq!(reader.blocks().len() as u64, before + report.blocks_added());
# Ok::<(), acta::Error>(())
```

The snapshot is mutated only after discovery succeeds, so a failed refresh
leaves every field and block vector unchanged, and repeated refreshes never
add the same frame twice. A refresh that finds no growth succeeds with zero
additions. A final frame that is not yet complete is an interrupted append: it
exposes no block, the report's `incomplete_tail` is true, and a later refresh
sees the frame once a writer completes it. A safe Stage 8b repair that removed
only that uncommitted tail is accepted and clears the tail; corrupting a
complete frame is refused, and shrinking below the committed boundary is
`ErrorKind::FileTruncated`.

A refresh will not extend a snapshot from a file that is not the one it came
from, and it checks that two ways because neither alone is enough. It compares
the path's current file-system identity — not the v0.2 file ID, which the
deterministic writer stores as a non-unique zero — against the identity
captured at open, which catches a replacement that unlinked and recreated the
path. And because an in-place truncate-and-rewrite keeps that identity while
replacing every byte, it re-reads the commit trailer of the last frame the
snapshot committed, whose length, sequence, body CRC, own CRC, and commit
magic together identify that exact frame, and refuses unless it still matches.
On growth it also compares the prologue and schema it re-reads against the
ones the snapshot holds. Any mismatch is `ErrorKind::FileReplaced`, never a
silent adoption. One boundary is worth stating plainly: a snapshot holding no
data blocks has only its prologue and schema frame to be recognised by, so a
replacement whose prologue and schema are byte-identical is accepted — but
such a file agrees with everything the snapshot has exposed.

Refresh never truncates, repairs, or acquires the writer lock, so a reader and
a writer coexist on the same path.

### Tail

`Reader::tail` returns a live `Tail` that starts after the blocks the reader
already holds and polls the path for newly committed frames, one block per
poll. Polling is synchronous and non-blocking: it sleeps on nothing, spawns no
thread, and owns no timer, so `Ok(None)` means "pending now" and the caller
decides how long to wait before polling again. `None` is never the end of the
stream, so a `Tail` is not an `Iterator` and cannot be fused — which is why
the loop below is not a `while let`. Cancellation is stopping the polls or
dropping the tail, which performs no I/O and leaks no thread or handle.

`None` means only "pending now", too. A block that prunes on its bounds or
filters down to no rows is consumed inside the poll that reaches it, and the
poll carries on to the next committed block, so waiting on `None` never means
waiting for data already on disk.

```rust,no_run
# use acta::Reader;
# fn keep_following() -> bool { true }
# fn wait_a_while() {}
let mut reader = Reader::open("data.acta")?;
let mut tail = reader.tail().project(["value"])?;
while keep_following() {
    match tail.poll_next()? {
        // one newly committed matching block per poll
        Some(batch) => println!("{} new row(s)", batch.row_count()),
        // nothing committed yet; the caller chooses how long to wait
        None => wait_a_while(),
    }
}
# Ok::<(), acta::Error>(())
```

Projection, reordered and empty projection, timestamp and `date32` primary
ranges, pruning, file order, per-block decode-error handling, and cumulative
scan limits all match `Scan`; `Tail::metrics` reports more bytes than an
equivalent scan, because it counts the frames its refreshes read to discover
the blocks as well as the bytes its decodes read. A partial frame is never
exposed: the tail
leaves it undispatched and reports it through `incomplete_tail`, and a later
poll sees it once a writer completes it. A tail borrows its reader mutably, so
the reader cannot be refreshed directly while a tail exists — the same
snapshot safety boundary a `Scan` draws, kept on purpose rather than bypassed
with interior mutation.

## Data types and compression

Acta files use a fixed schema. The v0.2 type system includes:
- `bool`
- signed and unsigned integers (8, 16, 32, and 64 bit)
- `float32` and `float64`
- `decimal64` scaled decimals
- `timestamp64` with a unit and timezone parameter
- `date32` calendar dates, for daily and end-of-day series
- UTF-8 strings
- categorical strings with block-local dictionaries
- variable- and fixed-length binary data

NULL is not a stored type: nullability is a column property, and a nullable column carries a validity bitmap while its value streams stay dense. Columns are non-nullable by default. Values that do not match the declared type are rejected rather than silently changing the schema.

Logical types describe what values mean, while each block selects the most compact physical encoding for its actual data. Candidate encodings include:

- **Bit packing** for integers with a small observed range
- **Frame of reference** for integers clustered in a narrow band
- **Delta** and **delta-of-delta** for counters and timestamps
- **Run-length** or **constant** encoding for repeated values
- **Boolean RLE** for flags and validity bitmaps with long runs
- **Dictionary encoding** for low-cardinality strings and enums
- **Byte-stream split** for floating-point values before general compression
- **Raw values** for data that does not benefit from a specialized encoding

Encoded columns may then use a general-purpose compressor such as Zstandard. This per-block choice preserves a stable schema without forcing every block to use the same representation.

XOR/Gorilla float encoding was evaluated for the experimental format design but
is deferred until it demonstrates a consistent advantage over raw,
dictionary, and byte-stream-split representations.

## Row IDs

Acta may assign an internal, monotonically increasing `uint64` ID to each row. This is useful when timestamps can repeat, producers can write identical records, or precise tombstones and future updates are needed.

Row IDs are implicit rather than stored as a full column:

```text
row_id = block_base_id + row_offset
```

Each block stores one `base_row_id`. When a completed block is appended, it receives a contiguous global ID range. This provides stable unique IDs with minimal storage overhead and avoids writer-local or composite IDs. Row IDs are part of the format design but remain optional, internal, and not prominent in the initial public API.

### Future mutations

Deletion and updates may be added later as an intentionally expensive copy-on-write operation. Acta would copy unaffected compressed blocks, rewrite only affected blocks into a temporary file, preserve existing row IDs, then atomically replace the original file. Published blocks remain immutable; mutations create a new file generation and require an exclusive writer lock.

## Philosophy

Acta prioritizes:

1. Sequential append performance
2. Compression
3. Concurrent readers
4. Time-range query efficiency
5. Multiple concurrent producers

Acta is not intended to provide transactions, in-place updates, rollback, or database-style recovery. The initial concurrency model is parallel buffering and compression with serialized appends of completed blocks; more advanced extent reservation can be added if benchmarks justify it.

## v0.2 release candidate

The current binary design is published as
[Acta file format v0.2 RC1](spec/v0.2/format_v0.2.md). It is accompanied by an
executable [framing and recovery probe](spec/v0.2/format_probe.py) and
deterministic [binary compatibility fixtures](spec/v0.2/fixtures/README.md)
covering minimal framing, a multi-column real-data block, `TS_SORTED`, a
`date32` primary column, and a schema without a primary time column.
Reproducible [encoding benchmarks](benchmarks/v0.1/README.md) remain available
from the initial format evaluation, the
[Stage 7 writer benchmark](benchmarks/stage7/README.md) measures the writer's
ingestion throughput and raw-versus-Zstandard output size, and the
[Stage 7b writer benchmark](benchmarks/stage7b/README.md) compares raw and
adaptive encoding with and without Zstandard on one deterministic dataset.
The [Stage 7c writer benchmark](benchmarks/stage7c/README.md) measures the
storage and ingestion trade-offs of the three statistics policies.
The [Stage 5 scan benchmark](benchmarks/stage5/README.md) reports projection,
pruning, stream, byte, and row counters for representative reader workloads.
The current [BTS flight Parquet-to-Acta benchmark](benchmarks/v0.2/0_bts_flight/README.md)
records a typed 15.75-million-row conversion, output checksum, throughput,
size, and full-file validation recipe. The
[market deltas benchmark](benchmarks/v0.2/1_market_deltas/README.md) records a
239-million-row fixed-schema conversion that deliberately disables adaptive
encoding and value-selected statistics to prioritize ingest throughput.

### Inspecting a file

The crate installs an `acta` binary. `inspect` prints what the reader
discovered in a file, which is the quickest way to check a file by hand:

```bash
cargo run -- inspect spec/v0.2/fixtures/minimal/minimal.acta
```

```text
Acta v0.2

File
----
file id:   000102030405060708090a0b0c0d0e0f
features:  none
schema id: 1
size:      472 bytes
blocks:    1
rows:      3
primary:   time (column 1)
tail:      complete

Schema
------
ID  Name  Type                  Nullable  Primary
1   time  timestamp64(us, UTC)  no        yes

Blocks
------
Seq  Offset  Bytes  Rows  Base Row  Primary Min  Primary Max  Sorted
1    208     264    3     -         1000000      3000000      no
```

Primary bounds are the stored counts, in the primary column's timestamp unit or
in signed days for a `date32` column. A file this crate cannot read exits
non-zero with a message that distinguishes an unsupported version from
corruption.

`head` prints the first logical rows in schema order. It decodes only the
blocks needed to satisfy the requested row count:

```bash
cargo run -- head spec/v0.2/fixtures/nyc_taxi_3_rows/nyc_taxi_3_rows.acta -n 5
```

Values use deterministic native formatting: timestamps retain their raw unit
suffix, dates retain their signed day count, and binary values use hexadecimal.
A value wider than the table column is elided with `…`.

### Runnable examples

The [`examples/`](examples/) directory contains small programs for the basic
crate workflows:

```bash
cargo run --example writer
cargo run --example append
cargo run --example scan -- spec/v0.2/fixtures/nyc_taxi_3_rows/nyc_taxi_3_rows.acta
cargo run --example nullable
cargo run --example reader -- spec/v0.2/fixtures/minimal/minimal.acta
cargo run --example refresh_tail
cargo run --example validate_acta -- spec/v0.2/fixtures/minimal/minimal.acta
```

See [`examples/README.md`](examples/README.md) for what each program covers.
The larger Rust harnesses and real-data adapters are kept under
[`benchmarks/`](benchmarks/) with their reports.

### Creating and reopening a file

The writer exclusively creates a new path or reopens an existing complete Acta
file, buffers rows into bounded blocks, and writes deterministic plain/raw data
by default:

```rust
use std::sync::Arc;
use acta::{
    Array, Column, LogicalType, PrimitiveArray, RecordBatch, Schema, Writer, WriterOptions,
};

let schema = Schema::new(
    1,
    vec![Column::new(1, "value", LogicalType::Int64, false)],
    None,
);
let batch = RecordBatch::try_new(
    Arc::new(schema.clone()),
    vec![Array::Int64(PrimitiveArray::new(vec![1, 2, 3], None))],
    3,
)?;

let mut writer = Writer::create("ticks.acta", schema, WriterOptions::default())?;
writer.append(batch)?;
writer.flush()?;
writer.sync()?;
let summary = writer.finish()?;
assert_eq!(summary.blocks_written(), 1);
```

To continue an existing file, use `Writer::open`. It reconstructs the schema and
continuation point from the file while holding the same exclusive writer lock
used by `create`, and `Writer::schema` returns that reconstructed schema, which
is what new batches must match:

```rust
let mut writer = Writer::open("ticks.acta", WriterOptions::default())?;
let schema = std::sync::Arc::clone(writer.schema());
writer.append(RecordBatch::try_new(schema, columns, rows)?)?;
let summary = writer.finish()?;
```

`open` requires the path to exist and never creates one; there is no implicit
open-or-create. It refuses an incomplete tail, and no failure path truncates or
repairs a byte. Sequences and implicit row IDs continue from the validated end
of the existing chain, and `rows_written`, `blocks_written`, and `last_sequence`
describe only the reopened session, while `bytes_written` is the whole file.

Codec, encoding, statistics, and block-size options apply to the blocks the new
session writes; existing blocks and file-level features are untouched. The one
option the file decides is `row_ids`: it must agree with the prologue's feature
flag, because reopening can neither enable nor disable it.

Opening validates structure and whole frames — the prologue, the schema frame,
and every complete frame's prefix, header, trailer, and body CRC, plus sequence
and row-ID continuity. It is not `ValidationLevel::Full`: no stream is decoded
and no statistic is verified. Because every committed byte is checksummed, the
cost is proportional to file size. Use `Writer::open_with_schema` to add an
exact schema guard, or `Writer::open_with_limits` to read an existing file under
bounds other than the defaults.

Both entry points hold a cooperative exclusive lock for the life of the writer,
released on drop or `finish`. A second `acta` writer fails at once with
`ErrorKind::WriterLocked` rather than waiting, and readers neither take the lock
nor are blocked by it. Specification section 14 defers writer-locking protocols
to a later format version, so this is a convention among `acta` writers rather
than part of the format: it does not constrain other implementations or a
process that simply opens the path, and it is unreliable on network
filesystems. Targets that are neither Unix nor Windows have no lock primitive
and cannot construct a writer at all.

Both examples above run as doctests on `Writer`, so they are compiled and
checked.

Set `WriterOptions::default().with_row_ids(true)` to include implicit row IDs.
Set `.with_row_block_target(rows)` and `.with_byte_block_target(bytes)` to
control automatic block publication; either target may publish a complete
block while `flush` publishes the final partial buffer without requesting
durability. `sync` makes all published blocks durable, and `finish` performs
that final flush and synchronization. `writer.accounting()` reports buffered,
published, durable, and total rows and estimated raw block bytes.

Appended rows are buffered until a target is reached, so many small appends
accumulate into whole blocks and rows keep their order across the blocks they
are split into. Both targets are bounded by what a reader accepts, and together
they bound how much the writer holds in memory: the buffer never exceeds one
target's worth of rows. A single row too large for the byte target is written
as a block of its own rather than split. Blocks are priced from per-column
counters instead of by serializing a candidate, so ingestion cost is
proportional to the rows appended.

The default codec is raw. Select independent Zstandard compression for the
existing raw streams with
`WriterOptions::default().with_codec(acta::WriterCodec::Zstandard)` when the
default `zstd` feature is enabled. The default Zstandard level is 3; select a
different supported level with
`.with_zstd_level(9)`. The writer refuses to overwrite an existing
path, rejects batches whose schema does not match, and becomes poisoned after
a partial I/O or durability failure. Reaching a target publishes a frame from
inside `append`, so a failure there can leave part of a batch already written
and counted; compare `accounting()` across the call to see how much was
accepted. `Drop` does not flush or synchronize, so `finish` is the only way to
end a writer without losing buffered rows.

Optional block-local min/max statistics are selected independently with
`WriterOptions::default().with_statistics(acta::WriterStatistics::MinMax)`.
`None` is the default and preserves the earlier byte output. `MinMax` writes
canonical statistics for fixed-width numeric, boolean, decimal, timestamp,
date32, and fixed-binary columns, including the primary column; variable-width
strings and binary values have no v0.2 statistics encoding. Both enabled
policies ignore nulls and floating-point NaNs, leave a column with no remaining
value without a statistic, and treat infinities as ordinary bounds.

`Automatic` uses a deterministic conservative rule. A column gets a pair when
it is not the primary column, whose block header already carries the complete
pruning bounds; and it has at least 64 non-null, non-NaN values; and its raw
dense value bytes are at least eight times the pair. The last test binds only
on `bool`, whose values cost one bit each, where it raises the floor to 121
values; for every other supported type the 64-value floor is stricter. The rule
reads only the block's contents and the schema, never elapsed time, iteration
order, or how rows were split across appends, so equal blocks produce equal
bytes.

The option changes the data frame header only: each column's statistics flag,
kind, offset, and length, and the statistics area those offsets point into.
Payloads, stream descriptors, and every other header field are byte-for-byte
what the same encoding and codec policy produced without it. Because a pair
lives in the header, which readers bound at 64 MiB, a `fixed_binary` column
charges twice its byte width against that budget; a schema wide enough to
exhaust it is refused by `append` rather than written, and remains writable
with `None`. Recovery remains deferred.

The default encoding policy is also raw: every column uses plain layout and the
raw transform, so default output is byte-for-byte what earlier versions of this
writer produced. Select
`WriterOptions::default().with_encoding(acta::WriterEncoding::Adaptive)` to let
each block choose among the v0.2 layouts and transforms — constant, dictionary,
run length, bit packing, frame of reference, delta, delta of delta,
byte-stream split, and boolean RLE — where the logical type allows it. Each
column is profiled once, the cheapest estimates are shortlisted, and at most
raw plus the two best candidates are actually materialized and compressed. A
specialized encoding is kept only when the complete stored column — every
stream, its descriptor, its padding, and its compressed size — beats the raw
baseline by at least the larger of 64 bytes or one percent; otherwise the block
falls back to raw. Selection is deterministic and depends only on a block's
values, never on how those rows were divided among `append` calls. Adaptive
profiling holds a block's dense values a second time, so it raises peak writer
memory by a small multiple of the byte target; lower the target to lower the
peak. The codec is an independent choice, so all four combinations are
available.

To measure one transform without the selection around it, choose
`WriterEncoding::Fixed(acta::WriterTransform::Delta)`, or any other transform.
Fixed mode applies the requested transform or layout to every column value
stream, prices nothing, and never falls back: `Writer::create` returns an error
for a transform this writer does not offer for one of the schema's logical
types, and a block whose values the transform cannot describe fails when it is
published. A refused block poisons the writer, so blocks written before it stay
on disk and readable while the rest of the ingest does not proceed.

The offered set is exactly the candidate set adaptive would have priced, which
is narrower than what the format permits: a `timestamp64` column offers raw,
frame of reference, delta, and delta of delta and nothing else, so
`Fixed(WriterTransform::Dictionary)` is refused for one even though a
dictionary `timestamp64` column is legal v0.2. Holding both policies to the
same table means a fixed file is always a shape adaptive could also have
written. Validity streams stay raw under `Fixed`, because they are independent
boolean streams that the requested value transform does not describe.

`cargo run --example writer` ingests 5,000 rows in 100-row appends and prints
the resulting block counts and file sizes for both codecs.

### Explicit recovery

Recovery inspection is read-only and reports a point-in-time plan. It validates
the prologue, complete schema frame, every complete frame envelope and CRC, and
the sequence, schema-ID, and implicit row-ID chains:

```rust,no_run
# let path = std::path::Path::new("ticks.acta");
let plan = acta::inspect_recovery(path)?;
if plan.requires_repair() {
    let summary = acta::repair_incomplete_tail(path)?;
    println!("removed {} bytes", summary.bytes_removed());
}
# Ok::<(), acta::Error>(())
```

That example is the crate-root doctest, so it is compiled and checked.

`repair_incomplete_tail` is destructive but narrowly bounded. It independently
reopens the current file read/write, acquires the same cooperative exclusive
writer lock as `Writer`, rescans the locked handle, rechecks its physical
length, truncates only the incomplete final data-frame range, synchronizes, and
scans it once more. A stale inspection plan is never trusted. Complete files,
complete corrupt frames, checksum-failing final frames, and incomplete schema
frames are refused. Salvage mode and an approved corrupt-final-frame policy are
not implemented; no caller-provided truncation offset is accepted. Both scans
are the structural, whole-frame pass `Writer::open` performs rather than
`ValidationLevel::Full`, so no stream is decoded and no statistic is verified.

The writer lock's limits described above apply here too, and they matter more,
because this operation deletes bytes: the lock does not constrain another
implementation or a process that simply opens the path, and it is unreliable on
network filesystems. The physical length is rechecked immediately before the
truncation and a length that moved is refused, but no portable API makes that
check and the truncation a single atomic step, so a writer that does not
participate in the lock can still commit a frame inside that window and lose
it. Readers never take the lock and remain unaffected.

Every failure before the truncation leaves every byte unchanged, and every one
of them says so. A failure can also follow the truncation — the synchronization,
the rescan, or the post-repair validation — and every one of those instead says
the tail was already removed, whatever its `ErrorKind`. The two sets of messages
never overlap, so a caller can always tell which side of the mutation it is on.
After a post-truncation error the file may be shorter already while its
durability or its structure is unconfirmed.

Decoding a block is bounded before it allocates. `Limits` caps the row count, the
declared stream lengths, and the total bytes one block decode may materialize;
that last bound is what a file cannot escape by declaring many columns, or one
column with an enormous element count, in very few stored bytes:

```rust
let limits = acta::Limits::default().with_max_decoded_block_bytes(64 << 20);
let reader = acta::Reader::open_with_limits("data.acta", limits)?;
let batch = reader.read_block(0)?;
```

Zstandard is the crate's only dependency and sits behind the default `zstd`
feature. Building with `--no-default-features` drops it along with its C
toolchain requirement; files whose streams use codec 1 then report an
unsupported stream rather than failing to build.

The v0.1 specification and fixtures remain preserved as historical design
tests, but supported compatibility begins with v0.2. The v0.2 binary layout is
frozen for compatibility testing; incompatible changes will use a new format
version. The Rust API and implementation are alpha-quality: users should expect
API refinement before 1.0. The file-format release candidate remains subject
to compatibility validation by independent implementations.

See [case studies](case_study/README.md) for comparisons with existing storage formats and databases.


#### Why?

<sup><sub>One day I'm working in this team that requires a plot of noisy time series data that will grow big enough to make me bankrupt from an aws bill. I started explore solutions: parquet file is awkward since it needs to be appended very fast while others are reading, sqlite not storage efficient enough, csv? wtf is wrong with you, i cba to deal with yet ANOTHER databse, let alone pay for one. Thus, I created Acta in agony. (yes, yes, I know [xkcd 927](https://xkcd.com/927/)) </sub></sup>
