# TSBS IoT 10M-row benchmark

This benchmark compares Acta v0.2, Parquet, and CSV on the same normalized
10,000,000-row slice of the official TSBS IoT workload. It is intentionally a
write-path benchmark: the target writer receives Arrow record batches that
were fully decoded and materialized before the timed section. Dataset
generation, TSBS parsing, source Parquet decoding, and target compilation are
excluded from write timing.

The source workload is generated with the TSBS `iot` use case, TimescaleDB
serialization, seed `123`, 550 trucks, a 10-second interval, and a three-day
time window. The first 10,000,000 measurement rows are retained. TSBS IoT has
separate `readings` and `diagnostics` measurements; normalization stores them
in one nullable wide table so every target format receives identical rows and
types.

## Reproduce

Build the official TSBS generator from the pinned source revision
`8323e59c74027b108f4ad5ec5d3e498b0101a02e`:

```bash
curl -L --fail \
  https://codeload.github.com/timescale/tsbs/tar.gz/8323e59c74027b108f4ad5ec5d3e498b0101a02e \
  -o /tmp/tsbs.tar.gz
rm -rf /tmp/tsbs-src
mkdir -p /tmp/tsbs-src
tar -xzf /tmp/tsbs.tar.gz -C /tmp/tsbs-src --strip-components=1
go build -o /tmp/tsbs_generate_data /tmp/tsbs-src/cmd/tsbs_generate_data
```

Generate enough source rows; the normalizer stops at exactly 10,000,000:

```bash
/tmp/tsbs_generate_data \
  --use-case=iot --format=timescaledb --scale=550 --seed=123 \
  --timestamp-start=2016-01-01T00:00:00Z \
  --timestamp-end=2016-01-04T00:00:00Z \
  --log-interval=10s --max-data-points=12000000 \
  > /tmp/tsbs-iot-12m.txt

uv run benchmarks/v0.2/2_tsbs_iot_devops/normalize_tsbs_iot.py \
  /tmp/tsbs-iot-12m.txt \
  /tmp/acta-benchmark/tsbs_iot_10m.parquet \
  /tmp/acta-benchmark/tsbs_iot_10m_manifest.json
```

The benchmark writer is run from the repository root:

```bash
cargo run --release --example tsbs_iot_benchmark --features parquet-example -- \
  --input /tmp/acta-benchmark/tsbs_iot_10m.parquet \
  --output /tmp/acta-benchmark/tsbs_iot_10m.acta \
  --format acta
```

Use the same command with `--format parquet` or `--format csv` and distinct
output paths. The input Parquet is decoded completely before the timer starts;
the timed section only converts preloaded batches to the requested target and
finishes the target writer.

## Schema and options

The normalized table has 20 columns: a UTC microsecond timestamp, eight
nullable truck tags, a measurement name, eight nullable reading/diagnostic
values, and five nullable integer fields. Acta uses the timestamp as its
primary column, 65,536-row blocks, adaptive encoding, Zstandard level 1, and
no optional statistics. Parquet uses 65,536-row groups, dictionary encoding,
and Zstandard level 1. CSV is UTF-8 with a header and empty fields for nulls.

## Recorded metrics

The result records dataset fingerprint, output bytes and bytes per row,
Arrow-buffer baseline bytes, write seconds, rows/s, logical MiB/s, output
MiB/s, finish time, full-scan time, peak RSS, and SHA-256 checksums. Acta is
also validated and its scan counters record blocks, streams, decoded bytes,
and file bytes read. Parquet read timing uses typed Arrow decoding; CSV read
timing is a raw line scan and does not parse fields, so the CSV read number is
only a lower-bound baseline rather than an apples-to-apples typed query result.

The checked-in result is a host-specific engineering reference, not a
portable performance claim. Large generated inputs and target files are kept
outside the repository; their checksums and generation parameters are stored
in the result manifest.

## Hugging Face artifacts

The exact source stream, normalized input, target files, and result manifest
are published under the `tsbs_iot/v0.2/` prefix in the
[Acta benchmark dataset](https://huggingface.co/datasets/lu-chengass/acta-data).
Download them without regenerating the source stream:

```bash
hf download lu-chengass/acta-data \
  --repo-type dataset --revision main \
  --include 'tsbs_iot/v0.2/*' --local-dir /tmp/acta-hf
```

The uploaded `tsbs_iot_10m.parquet` is the normalized input. The uploaded
`tsbs_iot_10m.parquet.target` is the independently written Parquet target.
`manifest.json` and `results/tsbs_iot_10m.json` identify every artifact by
SHA-256.
