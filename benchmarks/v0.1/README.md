# Benchmarks

These benchmarks evaluate Acta's proposed per-block encodings and file-layout
trade-offs against raw representations and established analytical formats.
They are design probes rather than production claims: the Acta C++ writer and
reader do not exist yet, so stream-size results are stronger evidence than
prototype throughput.

| ID | Dataset | Focus |
| --- | --- | --- |
| [0](0_taxi_data/README.md) | NYC TLC yellow-taxi trips | Time-series encodings, block-size selection, recovery granularity, and raw/Parquet comparison |
| [1](1_tpch_lineitem/README.md) | TPC-H SF1 `lineitem` | Parquet-friendly analytical compression, projection, and date-range pruning |

Each dataset directory contains acquisition instructions, a reproducible
benchmark script, generated results, and a summary of decisions.
