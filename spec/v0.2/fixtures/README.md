# v0.2 fixtures

These files are the binary compatibility anchors for Acta v0.2 RC1. Their
checked-in bytes and SHA-256 hashes are frozen. Implementations claiming v0.2
RC1 support MUST read them; writers may choose different valid encodings except
when a fixture explicitly requires byte-exact regeneration.

## Minimal framing fixture

`minimal/minimal.acta` is a deterministic 472-byte file containing:

- the 64-byte v0.2 prologue;
- one schema frame with a non-nullable UTC microsecond timestamp column named
  `time`; and
- one uncompressed data frame containing timestamp values 1,000,000,
  2,000,000, and 3,000,000.

See [minimal.md](minimal/minimal.md) for a byte-by-byte annotation of the
prologue, schema frame, data frame, stored values, checksums, and recovery
behavior.

SHA-256:

```text
5e24223ca88523440dba179227ae16dfcb1d1f0929365fc54da3a61ab1006d32
```

Regenerate and validate it from the repository root:

```bash
uv run spec/v0.2/format_probe.py \
  --write-fixture spec/v0.2/fixtures/minimal/minimal.acta
```

The generator tests every possible truncation point inside the final frame and
representative corruption in the prologue, frame prefix, header, payload, and
trailer before writing the fixture.

## NYC taxi three-row fixture

`nyc_taxi_3_rows/nyc_taxi_3_rows.acta` is a deterministic 2,864-byte file
containing the first three source rows in the first sample recorded by
[`nyc_taxi_65536.json`](../../../benchmarks/0_taxi_data/results/nyc_taxi_65536.json).
It includes all 16 benchmark columns, implicit row IDs 0–2, plain layouts, raw
transforms, and uncompressed streams.

See [nyc_taxi_3_rows.md](nyc_taxi_3_rows/nyc_taxi_3_rows.md) for the source
values, complete hex dump, field and descriptor offsets, stored stream bytes,
checksums, and recovery behavior.

SHA-256:

```text
1a1c7c0a07c3319a536c7f0ea386c533a0af4c5ec05bd6c4748a8ed1ea3947a9
```

Regenerate and semantically validate it from the repository root:

```bash
uv run spec/v0.2/fixtures/nyc_taxi_3_rows/nyc_taxi_3_rows.py \
  --output spec/v0.2/fixtures/nyc_taxi_3_rows/nyc_taxi_3_rows.acta \
  --markdown-output spec/v0.2/fixtures/nyc_taxi_3_rows/nyc_taxi_3_rows.md
```

The generator also checks every possible interrupted-write boundary inside the
data frame while retaining the committed schema frame and its retry offset.

## `TS_SORTED` block-flag fixture

`ts_sorted.acta` is a deterministic 480-byte file exercising data-block flag
bit 1, `TS_SORTED`, which declares that a block's primary timestamps are
monotonically nondecreasing in row order. It contains:

- the 64-byte v0.2 prologue with file ID `ACTA-TS-SORTED!!`;
- one schema frame with a non-nullable UTC microsecond timestamp column named
  `time`; and
- one uncompressed four-row data frame with block flags `0x2` and timestamp
  values 1,000,000, 2,000,000, 2,000,000, and 3,000,000 — including an equal
  adjacent pair, which the flag permits.

See [ts_sorted.md](ts_sorted/ts_sorted.md) for the data-frame annotation and
the validation rules the fixture exercises.

SHA-256:

```text
6ddee2bc6a7db106c9d8790fde1f21b48ab63ccaf3104b8665d00bb781c73a15
```

Regenerate and semantically validate it from the repository root:

```bash
uv run spec/v0.2/fixtures/ts_sorted/ts_sorted.py \
  --output spec/v0.2/fixtures/ts_sorted/ts_sorted.acta \
  --markdown-output spec/v0.2/fixtures/ts_sorted/ts_sorted.md
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

- the 64-byte v0.2 prologue with file ID `ACTA-DATE32-EOD!`;
- one schema frame with a non-nullable `date32` primary column named `date`
  and a `float64` column named `close`; and
- one uncompressed three-row data frame with `TS_SORTED` set, covering
  2026-01-02, 2026-01-05, and 2026-01-06 — a weekend gap in the calendar —
  with the day counts sign-extended to `int64` in the block-header bounds.

See [date32.md](date32/date32.md) for the schema and data-frame annotation
and the validation rules the fixture exercises.

SHA-256:

```text
110a145ac37a759dfa417f9557de6997909fc26849bc88784fa1e0c003737372
```

Regenerate and semantically validate it from the repository root:

```bash
uv run spec/v0.2/fixtures/date32/date32.py \
  --output spec/v0.2/fixtures/date32/date32.acta \
  --markdown-output spec/v0.2/fixtures/date32/date32.md
```

The generator's self-test applies the `TS_SORTED` rules to the day counts,
verifies the `close` min/max statistics, checks every truncation point inside
the data frame, and confirms that flipping the `date32` type ID, the block
flag bit, or a stored day-count byte fails CRC validation.

## No-primary fixture

`no_primary/no_primary.acta` is a deterministic 640-byte file exercising the
v0.2 option to omit a primary timestamp column. Its schema contains `event`
(`utf8`) and `value` (`int64`) columns, with primary timestamp column ID zero.
The three-row data block has zero timestamp bounds and leaves `TS_SORTED`
clear.

See [no_primary.md](no_primary/no_primary.md) for the complete layout and the
validation rules this fixture exercises.

SHA-256:

```text
2d8832aaa3fb1f13308cde7cc426a6a038e7a4a23cc7b1a9cdccd3d9b86b9310
```

Regenerate and semantically validate it from the repository root:

```bash
uv run spec/v0.2/fixtures/no_primary/no_primary.py \
  --output spec/v0.2/fixtures/no_primary/no_primary.acta \
  --markdown-output spec/v0.2/fixtures/no_primary/no_primary.md
```

The generator accepts the valid absence sentinel and rejects otherwise-valid
blocks with a nonzero timestamp bound or a set `TS_SORTED` flag. It also checks
every truncation point inside the data frame and representative CRC-protected
corruption.

## IANA timezone fixture

`timezone/timezone.acta` is a deterministic 608-byte file pinning the encoding
of the only variable-length type-parameter record in v0.2. Its schema contains a
non-nullable `timestamp64` primary column named `time` with millisecond unit and
IANA timezone `Europe/Berlin`, plus an `int64` column named `value`. The three-row
data block sets `TS_SORTED`.

The timezone name is thirteen bytes, so the parameter record carries three bytes
of padding and its stored length is `8 + 13` rounded up to `24`. A reader that
expected `21` rejects this file, and a writer that stored `21` produces one this
fixture rejects. No other checked-in fixture exercises that rounding.

See [timezone.md](timezone/timezone.md) for the annotated record and the
validation rules this fixture exercises.

SHA-256:

```text
7adca0c16d7dee8ee708481fcb10b97c88117ae5799b371f507d512dfba08941
```

Regenerate and semantically validate it from the repository root:

```bash
uv run spec/v0.2/fixtures/timezone/timezone.py \
  --output spec/v0.2/fixtures/timezone/timezone.acta \
  --markdown-output spec/v0.2/fixtures/timezone/timezone.md
```

The generator rejects an unpadded parameter record, IANA mode with an empty
name, and `TS_SORTED` bounds that are not the first and last stored values. It
also checks every truncation point inside the data frame and CRC-protected
corruption of the timezone name.
