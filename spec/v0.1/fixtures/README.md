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

## `TS_SORTED` block-flag fixture

`ts_sorted.acta` is a deterministic 480-byte file exercising data-block flag
bit 1, `TS_SORTED`, which declares that a block's primary timestamps are
monotonically nondecreasing in row order. It contains:

- the 64-byte v0.1 prologue with file ID `ACTA-TS-SORTED!!`;
- one schema frame with a non-nullable UTC microsecond timestamp column named
  `time`; and
- one uncompressed four-row data frame with block flags `0x2` and timestamp
  values 1,000,000, 2,000,000, 2,000,000, and 3,000,000 — including an equal
  adjacent pair, which the flag permits.

See [ts_sorted.md](ts_sorted/ts_sorted.md) for the data-frame annotation and
the validation rules the fixture exercises.

SHA-256:

```text
8ddd8bf09ce7d341fba9951ef9bab9a2ee0d0f07c3cfd9d78704089582d0a15b
```

Regenerate and semantically validate it from the repository root:

```bash
uv run spec/v0.1/fixtures/ts_sorted/ts_sorted.py \
  --output spec/v0.1/fixtures/ts_sorted/ts_sorted.acta \
  --markdown-output spec/v0.1/fixtures/ts_sorted/ts_sorted.md
```

The generator's self-test applies the section 8 `TS_SORTED` rules: it rejects
a decreasing block that declares the flag, accepts the same values with the
flag clear, rejects declared bounds that are not the first and last row
values, checks every truncation point inside the data frame, and confirms
that flipping the flag bit or a payload byte fails CRC validation.

## `date32` primary-column fixture

`date32.acta` is a deterministic 592-byte file exercising logical type 18,
`date32`, as the primary timestamp column. It is a minimal end-of-day series
containing:

- the 64-byte v0.1 prologue with file ID `ACTA-DATE32-EOD!`;
- one schema frame with a non-nullable `date32` primary column named `date`
  and a `float64` column named `close`; and
- one uncompressed three-row data frame with `TS_SORTED` set, covering
  2026-01-02, 2026-01-05, and 2026-01-06 — a weekend gap in the calendar —
  with the day counts sign-extended to `int64` in the block-header bounds.

See [date32.md](date32/date32.md) for the schema and data-frame annotation
and the validation rules the fixture exercises.

SHA-256:

```text
fb1ff4fe5b7f6d00c54f3240cada2020061982bbff85010896268a513cba2303
```

Regenerate and semantically validate it from the repository root:

```bash
uv run spec/v0.1/fixtures/date32/date32.py \
  --output spec/v0.1/fixtures/date32/date32.acta \
  --markdown-output spec/v0.1/fixtures/date32/date32.md
```

The generator's self-test applies the `TS_SORTED` rules to the day counts,
verifies the `close` min/max statistics, checks every truncation point inside
the data frame, and confirms that flipping the `date32` type ID, the block
flag bit, or a stored day-count byte fails CRC validation.
