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
