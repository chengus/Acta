---
license: other
task_categories:
  - other
language:
  - en
tags:
  - benchmarking
  - parquet
  - compression
  - flight-data
  - market-data
  - time-series
  - tsbs
  - telemetry
size_categories:
  - 100M<n<1B
---

# Acta benchmark data

Canonical large artifacts for reproducing real-data Acta v0.2 benchmarks.
The converters, schema mappings, commands, checksums, validation procedures,
and measured results live in the
[Acta GitHub repository](https://github.com/chengus/Acta/tree/main/benchmarks/v0.2).

## Contents

### `bts_flight/v0.2/`

- `bts_flights_15750000.parquet`: 15,750,000 rows, 51 columns, 381,644,436 bytes;
- `bts_flights_15750000.acta`: adaptive Acta v0.2 output using Zstandard level 6, 325,340,952 bytes;
- `manifest.json`: source archive URLs, source checksums, row counts, and the Parquet checksum;
- `benchmark.json`: the recorded Acta checksum, compression level, and benchmark result.

### `market_deltas/v0.2/`

- `deltas.parquet`: original 239,246,024-row, 13-column ClickHouse Parquet export, 2,057,215,095 bytes;
- `deltas.acta`: fixed-schema Acta v0.2 output using fixed raw encoding and Zstandard level 1, 1,773,915,800 bytes;
- `benchmark.json`: schemas, options, checksums, environment, and measured conversion result;
- `conversion.stats.txt`: the converter's persisted beginning and final statistics.

### `tsbs_iot/v0.2/`

- `tsbs_iot_10m_source.txt`: the pinned-seed TSBS TimescaleDB-format source stream;
- `tsbs_iot_10m.parquet`: the normalized 10,000,000-row, 20-column input used by all target writers;
- `tsbs_iot_10m.acta`: Acta v0.2 output using adaptive encoding and Zstandard level 1;
- `tsbs_iot_10m.parquet.target`: Parquet target-format output using 65,536-row groups and Zstandard level 1;
- `tsbs_iot_10m.csv`: UTF-8 CSV target-format output with the same schema and rows;
- `manifest.json`: TSBS revision, generator parameters, source and normalized-input checksums;
- `benchmark.json` and `benchmark.md`: write/read metrics, output checksums, and environment.

## Checksums

```text
33c7d1fd28faa95d002e05ebf778e6345c4eb1c9d39416224ee08967bad23f27  bts_flights_15750000.parquet
ee5c07e11197665895065d3b998f30d2a3984bd6920e9121d04db67e55821d29  bts_flights_15750000.acta
7f2e8dfc86f161737b038c81ce4dbc6b3e0a9fdf4bcc19e36b931490f3218c2a  deltas.parquet
d5c1d8d1654d8b9373e0225685989e7a49cb65338fd2c84969deb24c75ba8f43  deltas.acta
```

## Provenance and use

The BTS Parquet input is a curated, strongly typed subset of the U.S. Bureau
of Transportation Statistics Reporting Carrier On-Time Performance data. The
original monthly archives are available from the
[BTS PREZIP archive](https://transtats.bts.gov/PREZIP/). This dataset is not an
official BTS publication.

The market deltas input is the original user-supplied Parquet export used for
the benchmark. Its Parquet metadata identifies ClickHouse 26.4.1 as the
writer. It contains event timestamps, worker and venue identifiers, market and
instrument keys, outcome and book-side labels, price, size, and update kind.
No broader source attribution or license was embedded in the file, so users
must establish that their intended use is permitted.

These artifacts are provided for software benchmarking and format validation.
Users should review the source data's terms, attribution requirements, and
suitability before redistribution or use.

## TSBS provenance and reproduction

The TSBS IoT artifact uses the official [`timescale/tsbs`](https://github.com/timescale/tsbs)
source at revision `8323e59c74027b108f4ad5ec5d3e498b0101a02e`, the `iot` use case,
TimescaleDB serialization, seed `123`, scale `550`, a 10-second interval, and
the first 10,000,000 measurement rows from the generated stream. The Acta
repository contains the normalization and benchmark commands in
[`benchmarks/v0.2/2_tsbs_iot_devops/README.md`](https://github.com/chengus/Acta/tree/main/benchmarks/v0.2/2_tsbs_iot_devops).
The generated source stream is retained here so the exact normalized slice can
be independently checked; it is not needed when reproducing from the pinned
generator configuration.

### `clickbench_hits/v0.2/`

- `hits.parquet`: the ClickHouse ClickBench `hits`-compatible source artifact,
  99,997,497 rows and 105 columns;
- `hits.acta`: the recorded Acta v0.2 target produced by the writer-only
  benchmark;
- `hits_1m.csv`: a retained 1,000,000-row CSV sample used to estimate the full
  CSV size;
- `benchmark.json` and `benchmark.md`: Acta write/read metrics, source-size
  baselines, CSV size extrapolation, checksums, and environment;
- `README.md`: exact reproduction instructions and timing scope.

The benchmark writes and reads Acta. The original Parquet is used as the size
baseline, with Parquet throughput left unmeasured. Plain CSV throughput is also
left unmeasured because the estimated complete CSV is approximately 75 GiB;
its size is extrapolated from a 1,000,000-row sample.

Recorded checksums and sizes:

```text
hits.parquet  14,779,976,446 bytes  a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f
hits.acta      8,955,412,072 bytes  44a3aaf872cb6cd0318a6537b104ff26214d2738e8c9e7821a7f1bebd10ebad3
hits_1m.csv      801,653,457 bytes  8355799f72ed09d03af458e4456ad8e2057533c809ceebfbb91f316b2d27bea5
```

The ClickBench source and workload are documented in the official
[`ClickHouse/ClickBench`](https://github.com/ClickHouse/ClickBench) repository.
The published Parquet is retained as the reproduction input; the Acta
repository contains the target writer and CSV-streaming harness.
