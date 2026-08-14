# Market deltas Parquet → Acta benchmark

This is an end-to-end conversion of a 239,246,024-row ClickHouse Parquet
export into Acta v0.2. It uses the dedicated typed adapter in
[`../../../examples/deltas_parquet_to_acta.rs`](../../../examples/deltas_parquet_to_acta.rs).
The adapter validates a fixed Arrow schema before opening the writer and never
samples values to choose an Acta type, transform, or statistics policy.

The recorded result is one release-mode run on an Apple Silicon Mac. It is a
reproducible reference point, not a cross-machine throughput claim. See the
[recorded result](results/summary.md),
[structured result](results/market_deltas_239246024.json), and
[raw converter statistics](results/conversion.stats.txt).

## 1. Obtain the recorded artifacts

The input and output are stored in the
[Acta benchmark dataset on Hugging Face](https://huggingface.co/datasets/lu-chengass/acta-data):

```bash
hf download lu-chengass/acta-data \
  --repo-type dataset \
  --revision main \
  --include 'market_deltas/v0.2/*' \
  --local-dir /tmp/acta-hf
```

For immutable reproduction, replace `main` with the Hugging Face commit
revision recorded after publication.

| Property | Value |
| --- | ---: |
| Rows | 239,246,024 |
| Columns | 13 |
| Parquet row groups | 238 |
| File bytes | 2,057,215,095 |
| Parquet column-chunk compressed bytes | 1,526,729,920 |
| Parquet column-chunk uncompressed bytes | 4,894,064,162 |
| SHA-256 | `7f2e8dfc86f161737b038c81ce4dbc6b3e0a9fdf4bcc19e36b931490f3218c2a` |

## 2. Fixed schema

The converter rejects any difference in field name, order, Arrow type, or
nullability. The mapping is fixed in code:

| Column | Acta type | Nullable | Role |
| --- | --- | --- | --- |
| received_at | timestamp64(us, UTC) | no | primary |
| source_timestamp | timestamp64(us, UTC) | yes | — |
| sink_handoff_at | timestamp64(us, UTC) | yes | — |
| sequence | uint64 | yes | — |
| worker_id | categorical, unordered | yes | — |
| venue | categorical, unordered | no | — |
| market_key | utf8 | no | — |
| instrument_key | utf8 | no | — |
| outcome_label | categorical, unordered | no | — |
| book_side | categorical, unordered | no | — |
| price | float64 | no | — |
| size | float64 | no | — |
| update_kind | categorical, unordered | no | — |

## 3. Convert

From the repository root, use a fresh output path:

```bash
cargo run --release \
  --features parquet-example \
  --example deltas_parquet_to_acta -- \
  /tmp/acta-hf/market_deltas/v0.2/deltas.parquet \
  /tmp/acta-benchmark/market_deltas/deltas.acta
```

The converter refuses to overwrite an existing output. It writes the metrics
report to `<output>.stats.txt` unless `--stats <path>` is supplied.

The recorded configuration uses 262,144-row Arrow batches, a 262,144-row Acta
block target, a 64 MiB raw byte target, Zstandard level 1, no optional column
statistics, and `WriterEncoding::Fixed(WriterTransform::Raw)`.

Raw is the only single fixed transform offered across this mixed timestamp,
integer, floating-point, and string schema. Fixed mode prices no candidates
and never falls back. Zstandard level 1 favors throughput while still
compressing each raw stream.

The timer begins after input metadata and schema validation. It covers Parquet
reader construction, Arrow decoding, typed conversion, Acta compression and
writing, `Writer::finish`, and final synchronization. It excludes compilation,
download, initial metadata inspection, and post-run validation.

## 4. Recorded result

| Metric | Result |
| --- | ---: |
| Parquet input | 2,057,215,095 bytes (1.916 GiB) |
| Acta output | 1,773,915,800 bytes (1.652 GiB) |
| Acta / Parquet size ratio | 0.862290× |
| Size change | 13.771% smaller |
| Conversion time | 448.845981 s |
| Input throughput | 4.371 MiB/s |
| Row throughput | 533,025 rows/s |
| Parquet batches | 913 |
| Acta blocks | 966 |

The writer accounted for 64,637,623,432 raw logical bytes before stream
compression. The result used Rust/Cargo 1.93.0 on macOS 15.6.1,
`aarch64-apple-darwin`, with 8 logical CPUs.

## 5. Verify

Check the recorded checksums:

```bash
shasum -a 256 \
  /tmp/acta-hf/market_deltas/v0.2/deltas.parquet \
  /tmp/acta-hf/market_deltas/v0.2/deltas.acta
```

Expected:

```text
7f2e8dfc86f161737b038c81ce4dbc6b3e0a9fdf4bcc19e36b931490f3218c2a  deltas.parquet
d5c1d8d1654d8b9373e0225685989e7a49cb65338fd2c84969deb24c75ba8f43  deltas.acta
```

Inspect the complete Acta frame chain and schema:

```bash
cargo run --release --bin acta -- inspect \
  /tmp/acta-hf/market_deltas/v0.2/deltas.acta
```

The recorded file reports Acta v0.2, 966 blocks, 239,246,024 rows, primary
column `received_at`, and a complete tail. A subsequent `acta head --rows 3`
successfully decoded the first block and exercised all represented logical
types and nullable fields.
