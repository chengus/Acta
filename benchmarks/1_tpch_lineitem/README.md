# TPC-H SF1 lineitem benchmark

This benchmark uses the six-million-row TPC-H SF1 `lineitem` relation as a
Parquet-friendly analytical counterpoint to the taxi event stream. It contains
integer keys, exact decimal measures, dates, low-cardinality flags, repeated
instructions and modes, and high-cardinality comments.

The source is DuckDB's official pre-generated TPC-H SF1 database, documented at
<https://duckdb.org/docs/current/core_extensions/tpch>.

```text
https://blobs.duckdb.org/data/tpch-sf1.db
```

- Downloaded database size: 260,845,568 bytes
- SHA-256: `cd7506d90327c180399a4efcf340f0955052ce4e11e6a6ce9f6a92b3fd1f1cf6`
- `lineitem` rows: 6,001,215
- Columns: 16

TPC-H is generated benchmark data. These measurements are not audited or
claimed as official TPC-H benchmark results.

## Acquire

Keep downloaded and generated data outside the repository:

```bash
mkdir -p /tmp/acta-benchmark/tpch_sf1
curl -L --fail \
  -o /tmp/acta-benchmark/tpch_sf1/tpch-sf1.db \
  https://blobs.duckdb.org/data/tpch-sf1.db
```

## Run

```bash
uv run benchmarks/1_tpch_lineitem/benchmark.py \
  /tmp/acta-benchmark/tpch_sf1/tpch-sf1.db \
  --work-dir /tmp/acta-benchmark/tpch_sf1/work \
  --sample-blocks 8 \
  --json-output benchmarks/1_tpch_lineitem/results/tpch_sf1_lineitem.json \
  --markdown-output benchmarks/1_tpch_lineitem/results/summary.md
```

The script creates two complete Parquet files with Zstandard level 1 and
65,536-row groups: one in source order and one ordered by `l_shipdate`. On the
recorded run they were approximately 159 MB and 147 MB respectively.

To match Acta v0.1's logical types, the benchmark stores decimal values as
signed `decimal64(scale=2)` unscaled integers and converts TPC-H dates to
microsecond timestamps at midnight. No values are rounded beyond their existing
two-decimal TPC-H scale.

## What is measured

- Exact canonical raw size for all rows and columns
- Complete Parquet file size in source and ship-date order
- Apples-to-apples Parquet and Acta-stream sizes over eight evenly distributed
  row groups
- Per-column Acta transform selection with exact round-trip validation
- Thirty-day projected aggregation over source-order and ship-date-order files
- Raw fixed-width scan and globally sorted bisection baselines

The Acta full-size result is an extrapolation from sampled encoded streams. It
uses eight row groups selected at evenly spaced indices from the ship-date-
ordered file (including the first and last groups). This sampling is
deterministic but assumes those blocks represent the complete table. It does
not include common frame metadata and is not a written Acta file. Change
`--sample-blocks` to test the sensitivity of that estimate. Parquet sizes and
query times are complete native measurements.

Parquet queries run through DuckDB. The raw baselines use NumPy memory maps,
so their timing ratios are end-to-end implementation comparisons rather than
isolated format costs. Projected Parquet bytes are compressed column-chunk
metadata totals; projected raw bytes are complete uncompressed arrays.

See [results/summary.md](results/summary.md) for the recorded findings.
