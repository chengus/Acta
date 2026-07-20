# Acta

Acta is a append-only, strongly typed file format for time-series data. It is designed for fast sequential ingestion, compact storage, concurrent readers, and efficient time-range queries—all in one file.

## Design

Producers collect rows in private buffers and encode them as immutable, compressed columnar blocks. Completed blocks are appended to the file with only brief coordination; their physical order does not need to match timestamp order. Each file has one required primary time column, typed as either a timestamp or a calendar date, and every block records that column's min/max bounds. A block whose primary timestamps are already nondecreasing can declare it with a `TS_SORTED` flag, letting readers binary-search within the block and merge sorted blocks without re-sorting. Readers use per-block metadata such as time bounds, row count, schema, and column statistics to skip irrelevant data, then merge matching blocks when ordered results are required.

There is no mutable file footer. Every frame is self-delimiting and ends in a checksummed commit trailer, so a reader discovers new blocks by continuing from the byte after the last complete frame—concurrent tailing needs no coordination with the writer. After an interrupted append, recovery validates checksums and truncates to the last complete frame; per-stream CRCs let projected reads verify only the bytes they actually touch.

## Data types and compression

Acta files use a fixed schema. The v0.1 type system includes:
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

XOR/Gorilla float encoding was evaluated for the experimental v0.1 design but
is deferred until it demonstrates a consistent advantage over raw,
dictionary, and byte-stream-split representations.

## Row IDs

Acta may assign an internal, monotonically increasing `uint64` ID to each row. This is useful when timestamps can repeat, producers can write identical records, or precise tombstones and future updates are needed.

Row IDs are implicit rather than stored as a full column:

```text
row_id = block_base_id + row_offset
```

Each block stores one `base_row_id`. When a completed block is appended, it receives a contiguous global ID range. This provides stable unique IDs with minimal storage overhead and avoids writer-local or composite IDs. Row IDs are part of the format design but remain optional, internal, and not prominent in the v1 API.

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

## v0.1 release candidate

The binary design is published as [Acta file format v0.1 RC1](spec/v0.1/format_v0.1.md).
It is accompanied by an executable [framing and recovery probe](spec/v0.1/format_probe.py),
deterministic [binary compatibility fixtures](spec/v0.1/fixtures/README.md) covering minimal
framing, a multi-column real-data block, the `TS_SORTED` flag, and a `date32` primary column,
and reproducible [encoding benchmarks](benchmarks/v0.1/README.md). The v0.1 binary layout is
frozen for compatibility testing: incompatible changes will use a new format version.
The API and implementation remain pre-alpha until the release candidate has been validated
by independent implementations.

See [case studies](case_study/README.md) for comparisons with existing storage formats and databases.


#### Why?

<sup><sub>One day I'm working in this team that requires a plot of noisy time series data that will grow big enough to make me bankrupt from an aws bill. I started explore solutions: parquet file is awkward since it needs to be appended very fast while others are reading, sqlite not storage efficient enough, csv? wtf is wrong with you, i cba to deal with yet ANOTHER databse, let alone pay for one. Thus, I created Acta in agony. (yes, yes, I know [xkcd 927](https://xkcd.com/927/)) </sub></sup>
