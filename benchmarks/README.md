# Encoding design benchmark

This probe evaluates candidate Acta column encodings before the C++ core
exists. It emphasizes encoded size, verifies every candidate by round-trip, and
reports Python throughput only as a relative signal. It is not an Acta-versus-
Parquet product benchmark.

## Dataset

The real-data input is the official NYC Taxi and Limousine Commission January
2026 yellow-taxi trip file:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet
```

- Downloaded size: 64,165,080 bytes
- SHA-256: `8b3933fe6f0d7b6d8826613c0dd724edc680ff7c49e2bd4c7635c05102728637`
- Rows: 3,724,889
- TLC dataset description and data dictionary:
  <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

Download it outside the repository:

```bash
mkdir -p /tmp/acta-benchmark
curl -L --fail \
  -o /tmp/acta-benchmark/yellow_tripdata_2026-01.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet
```

The benchmark samples evenly spaced contiguous blocks rather than only the
start of the file. It uses the source timestamps, integer codes, nullable
passenger count, distances, fares, and store-and-forward flag. It derives:

- a boolean from the Y/N flag;
- a signed zone delta and unsigned pickup-zone ID;
- cents as `decimal64(scale=2)`;
- route UTF-8 strings from real pickup/dropoff zones;
- payment labels as categorical strings;
- variable binary route keys; and
- deterministic 16-byte trip IDs.

Controlled regular timestamps, counters, smooth noisy floats, and sparse/burst
validity bitmaps complement the real event stream. These cases keep the design
from overfitting TLC timestamps, which are substantially out of order.

## Run

The script has PEP 723 dependency metadata, so a recent `uv` is sufficient:

```bash
uv run benchmarks/encoding_study.py \
  /tmp/acta-benchmark/yellow_tripdata_2026-01.parquet \
  --block-rows 65536 \
  --blocks 4
```

To regenerate the checked-in 65K result:

```bash
uv run benchmarks/encoding_study.py \
  /tmp/acta-benchmark/yellow_tripdata_2026-01.parquet \
  --block-rows 65536 \
  --blocks 4 \
  --json-output benchmarks/results/nyc_taxi_65536.json \
  --markdown-output benchmarks/results/nyc_taxi_65536.md
```

Every candidate is decoded and compared bit-for-bit with its input. Float
verification preserves NaN payloads and signed zero rather than using an
approximate comparison.

## Interpretation

The Python bit packers prioritize auditable behavior over implementation
speed. C++ should use vectorized bit packing, hashing, and CRC32C. Consequently:

- stored-size comparisons are meaningful design evidence;
- relative transform behavior is useful;
- absolute MiB/s numbers are not production projections; and
- exhaustive search throughput illustrates why the writer should shortlist
  candidates rather than materialize every transform.

See [results/summary.md](results/summary.md) for the decisions derived from the
three block-size runs.
