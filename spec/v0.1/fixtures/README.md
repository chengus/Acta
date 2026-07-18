# v0.1 fixtures

## Minimal framing fixture

`minimal.acta` is a deterministic 472-byte file containing:

- the 64-byte v0.1 prologue;
- one schema frame with a non-nullable UTC microsecond timestamp column named
  `time`; and
- one uncompressed data frame containing timestamp values 1,000,000,
  2,000,000, and 3,000,000.

See [minimal.md](minimal.md) for a byte-by-byte annotation of the prologue,
schema frame, data frame, stored values, checksums, and recovery behavior.

SHA-256:

```text
1e40278e2f58a597faef56107bd6a31048cabf5b5173619f8112ad787c6f658a
```

Regenerate and validate it from the repository root:

```bash
uv run spec/v0.1/format_probe.py \
  --write-fixture spec/v0.1/fixtures/minimal.acta
```

The generator tests every possible truncation point inside the final frame and
representative corruption in the prologue, frame prefix, header, payload, and
trailer before writing the fixture.

## NYC taxi three-row fixture

`nyc_taxi_3_rows.acta` is a deterministic 2,864-byte file containing the first
three source rows in the first sample recorded by
[`nyc_taxi_65536.json`](../../../benchmarks/0_taxi_data/results/nyc_taxi_65536.json).
It includes all 16 benchmark columns, implicit row IDs 0–2, plain layouts, raw
transforms, and uncompressed streams.

See [nyc_taxi_3_rows.md](nyc_taxi_3_rows.md) for the source values, complete hex
dump, field and descriptor offsets, stored stream bytes, checksums, and recovery
behavior.

SHA-256:

```text
d36315647ea15fff8834e090da1a13b3a6124216de25caac93a066b14a7ba90b
```

Regenerate and semantically validate it from the repository root:

```bash
uv run spec/v0.1/fixtures/nyc_taxi_3_rows.py \
  --output spec/v0.1/fixtures/nyc_taxi_3_rows.acta \
  --markdown-output spec/v0.1/fixtures/nyc_taxi_3_rows.md
```

The generator also checks every possible interrupted-write boundary inside the
data frame while retaining the committed schema frame and its retry offset.
