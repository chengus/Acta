# Benchmark summary

> THIS IS SPECULATION AT BEST! Purpose of this benchmark is to probe design trade-offs, not to claim production performance. The Acta C++ writer and reader do not exist yet.

Two design probes so far, both against Zstandard level 1 and both run without
a real Acta writer or reader: stream-size ratios are solid design evidence,
throughput and latency numbers are not production projections.

| Dataset | Rows | Columns | Canonical raw | Parquet+Zstd | Acta streams+Zstd | Acta vs. Parquet+Zstd |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [0 — NYC TLC yellow-taxi](0_taxi_data/results/summary.md) | 3,724,889 | 12 | 26,881,642 B | 8,378,042 B (0.312) | 7,647,041 B (0.284) | 8.7% smaller |
| [1 — TPC-H SF1 `lineitem`](1_tpch_lineitem/results/summary.md) | 6,001,215 | 16 | 916,854,302 B | 147,092,375 B (0.160, ship-date order) | 129,740,563 B (0.142) | 11.8% smaller |

Both Acta figures are extrapolated from evenly sampled blocks (65,536-row
groups), assume those blocks represent the full table, and exclude common
frame/schema/CRC metadata.

## What each dataset is for

- **Dataset 0 (taxi)** is a real, substantially out-of-order event stream. It
  drives block-size selection (65,536 rows: ~8% better than 4,096, <1% better
  than 262,144, at a much cheaper recovery/buffering cost) and per-column
  encoding choices across twelve real/real-derived types plus controlled cases
  (regular timestamps, monotonic counters, smooth noisy floats, sparse/bursty
  nulls).
- **Dataset 1 (TPC-H lineitem)** is generated, well-behaved analytical data.
  It stress-tests decimal/date/categorical encodings against a mature Parquet
  baseline and isolates ordering as a pruning variable independent of file
  format.

## Cross-dataset findings

1. **No universal transform.** Both studies confirm encoding must stay a
   per-block, per-column decision — dictionary/FOR win on bounded or
   low-cardinality values, delta/delta-of-delta win on regular or monotonic
   sequences, raw remains necessary for genuinely unordered or high-entropy
   data (taxi pickup timestamps: 0.492 of raw; random trip IDs: 1.000).
2. **Ordering dominates pruning, not format choice.** The TPC-H thirty-day
   query touched 2/92 Parquet row groups when ship-date ordered versus 92/92
   in source order (9.9× faster) — a data-locality effect that would apply
   identically to Acta's mandatory per-block timestamp bounds. Acta's real
   advantage case is data with a natural ordered primary timestamp (taxi-like
   time series), not arbitrarily-ordered fact tables.
3. **Acta's stream-size edge over Parquet+Zstd is consistent but modest**
   (8.7% on taxi, 11.8% on TPC-H) and, on both datasets, excludes metadata a
   real file would carry and reflects exhaustive-candidate selection a real
   writer would shortlist.

## Not yet measured

- Append throughput, interrupted-tail recovery, and concurrent-reader
  behavior — no Acta writer or reader exists yet.
- Any dataset beyond one real event stream and one generated analytical
  table.
- C++-representative encode/decode throughput (current bit packing and
  dictionary construction are Python prototypes).
