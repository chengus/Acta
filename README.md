# Acta

Acta is a proposed append-only, strongly typed file format for time-series data. It is designed for fast sequential ingestion, compact storage, concurrent readers, and efficient time-range queries—all in one file.

## Design

Producers collect rows in private buffers and encode them as immutable, compressed columnar blocks. Completed blocks are appended to the file with only brief coordination; their physical order does not need to match timestamp order. Readers use per-block metadata such as time bounds, row count, schema, and column statistics to skip irrelevant data, then merge matching blocks when ordered results are required.

## Data types and compression

Acta files use a fixed schema. The initial type system includes:
- `bool`
- signed and unsigned integers
- floating-point numbers
- scaled decimals
- timestamps
- UTF-8 strings
- categorical strings
- binary data
- fixed-length binary data
- NULL (for nullable columns)
- etc

Columns are non-nullable by default; nullable columns carry a validity bitmap. Values that do not match the declared type are rejected rather than silently changing the schema.

Logical types describe what values mean, while each block selects the most compact physical encoding for its actual data. Candidate encodings include:

- **Bit packing** for integers with a small observed range
- **Delta** and **delta-of-delta** for counters and timestamps
- **Run-length** or **constant** encoding for repeated values
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

## Experimental specification

The initial binary design is documented in [Acta file format v0.1](spec/v0/format_v0.md).
It is accompanied by an executable [framing and recovery probe](spec/v0/format_probe.py),
a [minimal binary fixture](spec/v0/fixtures/minimal.acta), and reproducible
[encoding benchmarks](benchmarks/README.md). These artifacts are experimental and do
not carry a compatibility promise before v1.

See [case studies](case_study/README.md) for comparisons with existing storage formats and databases.


#### Why?

<sup><sub>One day I'm working in this team that requires a plot of noisy time series data that will grow big enough to make me bankrupt from an aws bill. I started explore solutions: parquet file is awkward since it needs to be appended very fast while others are reading, sqlite not storage efficient enough, csv? wtf is wrong with you, i cba to deal with yet ANOTHER databse, let alone pay for one. Thus, I created Acta in agony. (yes, yes, I know [xkcd 927](https://xkcd.com/927/)) </sub></sup>
