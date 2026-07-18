# `date32` primary-column Acta v0.1 fixture

This document annotates [`date32.acta`](date32.acta), the deterministic
compatibility fixture for logical type 18, `date32`, defined in
[`format_v0.1.md`](../../format_v0.1.md) sections 3, 7, and 8. The file is a
minimal end-of-day series: a non-nullable `date32` primary column named
`date` and a `float64` column named `close`, with one three-row data block.

The dates are a Friday followed by a Monday and Tuesday. The weekend gap
shows a nondecreasing but irregular calendar, and the block declares
`TS_SORTED`, exercising the rule that a `date32` primary column follows
every primary-timestamp rule with its day counts sign-extended to `int64`.

Logical contents:

| Row | `date` (`date32`, days from epoch) | Calendar date | `close` (`float64`) |
| ---: | ---: | --- | ---: |
| 0 | 20,455 | `2026-01-02` (Friday) | `101.25` |
| 1 | 20,458 | `2026-01-05` (Monday) | `102.5` |
| 2 | 20,459 | `2026-01-06` (Tuesday) | `101.75` |

The block header declares row count 3, primary bounds
[20,455, 20,459] as sign-extended `int64` day counts, and block
flags `0x2` (`TS_SORTED` set, row IDs disabled).

## Hex dump

```text
00000000: 4143 5441 0d0a 1a0a 0000 0100 4000 0000  ACTA........@...
00000010: 0000 0000 0000 0000 4143 5441 2d44 4154  ........ACTA-DAT
00000020: 4533 322d 454f 4421 4000 0000 0000 0000  E32-EOD!@.......
00000030: 0000 0000 0000 0000 0000 0000 394b 18bb  ............9K..
00000040: 4143 5441 4652 4d0a 0100 0000 0000 0000  ACTAFRM.........
00000050: 1800 0000 0000 0000 4000 0000 0000 0000  ........@.......
00000060: 0000 0000 0000 0000 5d97 31ee 2b8e 3df0  ........].1.+.=.
00000070: 0100 0000 0000 0000 0200 0000 0100 0000  ................
00000080: 0000 0000 0000 0000 2000 0000 0100 0000  ........ .......
00000090: 1200 0000 0400 0000 0000 0000 0000 0000  ................
000000a0: 6461 7465 0000 0000 2000 0000 0200 0000  date.... .......
000000b0: 0b00 0000 0500 0000 0000 0000 0000 0000  ................
000000c0: 636c 6f73 6500 0000 a800 0000 0000 0000  close...........
000000d0: 0000 0000 0000 0000 420d 1542 3d65 21f8  ........B..B=e!.
000000e0: 4143 5441 454e 440a 4143 5441 4652 4d0a  ACTAEND.ACTAFRM.
000000f0: 0200 0000 0000 0000 f000 0000 0000 0000  ................
00000100: 2800 0000 0000 0000 0100 0000 0000 0000  (...............
00000110: 1085 f8cc 52b3 348d 0100 0000 0000 0000  ....R.4.........
00000120: ffff ffff ffff ffff 0300 0000 0200 0000  ................
00000130: e74f 0000 0000 0000 eb4f 0000 0000 0000  .O.......O......
00000140: 4000 0000 8000 0000 e000 0000 1000 0000  @...............
00000150: 0200 0000 0000 0000 0100 0000 0000 0000  ................
00000160: 0000 0000 0300 0000 0000 0000 0100 0000  ................
00000170: 0000 0000 0000 0000 0200 0000 0000 0200  ................
00000180: 0000 0000 0300 0000 0100 0000 0100 0100  ................
00000190: e000 0000 1000 0000 0200 0000 0000 0000  ................
000001a0: 0000 0000 0000 0000 0c00 0000 0000 0000  ................
000001b0: 0c00 0000 0000 0000 0300 0000 0000 0000  ................
000001c0: f00d c2fc 0000 0000 0200 0000 0000 0000  ................
000001d0: 1000 0000 0000 0000 1800 0000 0000 0000  ................
000001e0: 1800 0000 0000 0000 0300 0000 0000 0000  ................
000001f0: 3db1 ebf7 0000 0000 0000 0000 0050 5940  =............PY@
00000200: 0000 0000 00a0 5940 e74f 0000 ea4f 0000  ......Y@.O...O..
00000210: eb4f 0000 0000 0000 0000 0000 0050 5940  .O...........PY@
00000220: 0000 0000 00a0 5940 0000 0000 0070 5940  ......Y@.....pY@
00000230: 6801 0000 0000 0000 0100 0000 0000 0000  h...............
00000240: b6fe c0fd 80d2 5d40 4143 5441 454e 440a  ......]@ACTAEND.
```

```text
0x000 ┌───────────────────────────┐
      │ 64-byte file prologue     │
0x040 ├───────────────────────────┤
      │ 168-byte schema frame     │
0x0e8 ├───────────────────────────┤
      │ 360-byte data frame       │
0x250 └───────────────────────────┘
```

Total: `0x250 = 592` bytes, with file ID
`ACTA-DATE32-EOD!`. Framing, prologue, and trailer layouts are annotated
byte-by-byte in the [minimal fixture](../minimal/minimal.md); this document
annotates what is new here, the `date32` schema and data.

## Schema frame: 0x040–0x0e7

The schema header declares schema ID 1, two columns, and primary
timestamp column ID 1. Both column schema descriptors have zero
type-parameter length because neither `date32` nor `float64` is
parameterized.

| Payload range | ID | Name | Type ID / logical type | Nullable |
| --- | ---: | --- | --- | --- |
| `0x088–0x0a7` | 1 | `date` | 18 / `date32` | no |
| `0x0a8–0x0c7` | 2 | `close` | 11 / `float64` | no |

Column 1 is the primary timestamp column with type `date32`, the case
sections 7 and 8 define for calendar-dated series.

## Data frame: 0x0e8–0x24f

```text
0x0e8–0x117  frame prefix, 48 bytes
0x118–0x207  data-frame header, 240 bytes
0x208–0x22f  stream payload, 40 bytes
0x230–0x24f  commit trailer, 32 bytes
```

**Block header: 0x118–0x157**

| Offset | Value | Meaning |
| --- | ---: | --- |
| `0x118–0x11f` | `1` | Schema ID |
| `0x120–0x127` | `UINT64_MAX` | Base row ID unavailable; row IDs are disabled |
| `0x128–0x12b` | `3` | Row count |
| `0x12c–0x12f` | `2` | Column count |
| `0x130–0x137` | `20,455` | Primary minimum: day count of row 0 as `int64` |
| `0x138–0x13f` | `20,459` | Primary maximum: day count of row 2 as `int64` |
| `0x140–0x143` | `64` | Column-table offset within this frame header |
| `0x144–0x147` | `128` | Stream-table offset within this frame header |
| `0x148–0x14b` | `224` | Statistics-area offset within this frame header |
| `0x14c–0x14f` | `16` | Statistics-area length |
| `0x150–0x153` | `0x2` | Block flags: bit 1 `TS_SORTED` set |
| `0x154–0x157` | `0` | Reserved |

**Column and stream descriptors: 0x158–0x1f7**

| Descriptor range | Column | Contents |
| --- | --- | --- |
| `0x158–0x177` | `date` | plain layout, 0 nulls, 3 dense values, stream 0, no optional statistics |
| `0x178–0x197` | `close` | plain layout, 0 nulls, 3 dense values, stream 1, kind-one min/max at header offset 224 |
| `0x198–0x1c7` | `date` values | raw, uncompressed, offset 0, 12 stored bytes, CRC32C `0xfcc20df0` |
| `0x1c8–0x1f7` | `close` values | raw, uncompressed, offset 16, 24 stored bytes, CRC32C `0xf7ebb13d` |

The `date` column carries no optional statistics: the mandatory block-header
bounds already cover the primary column. The optional statistics area at
`0x1f8–0x207` stores the `close` minimum
`101.25` and maximum `102.5` as canonical `float64` values.

**Stream payload: 0x208–0x22f**

The `date` stream stores three little-endian `int32` day counts; four zero
padding bytes then align the `close` stream to an eight-byte boundary.

| Offset | Stored bytes | Day count | Calendar date |
| --- | --- | ---: | --- |
| `0x208–0x20b` | `e74f0000` | 20,455 | `2026-01-02` |
| `0x20c–0x20f` | `ea4f0000` | 20,458 | `2026-01-05` |
| `0x210–0x213` | `eb4f0000` | 20,459 | `2026-01-06` |

| Offset | Stored bytes | Decoded `float64` | Logical row |
| --- | --- | ---: | --- |
| `0x218–0x21f` | `0000000000505940` | `101.25` | Row 0 |
| `0x220–0x227` | `0000000000a05940` | `102.5` | Row 1 |
| `0x228–0x22f` | `0000000000705940` | `101.75` | Row 2 |

**Commit trailer: 0x230–0x24f**

| Offset | Value | Meaning |
| --- | ---: | --- |
| `0x230–0x237` | `360` | Total data-frame length |
| `0x238–0x23f` | `1` | Repeated frame sequence number |
| `0x240–0x243` | `0xfdc0feb6` | Body CRC32C |
| `0x244–0x247` | `0x405dd280` | Trailer CRC32C |
| `0x248–0x24f` | `ACTAEND\n` | Commit magic |

## Validation rules exercised

1. A `date32` value is a timezone-free calendar day count from `1970-01-01`;
   this fixture stores days 20,455–20,459, spanning a weekend
   gap without claiming any regular spacing.
2. A `date32` column is a valid primary timestamp column. Its day counts are
   sign-extended to `int64` in the mandatory block-header bounds, so
   time-range pruning works unchanged.
3. `TS_SORTED` applies to the primary day counts exactly as it does to
   timestamps: the bounds equal the first and last rows, and a strict reader
   that decodes the column verifies the nondecreasing order.

The generator's self-test rejects a decreasing block that declares
`TS_SORTED`, accepts the same values with the flag clear, rejects declared
bounds past the last row, checks every truncation point inside the data
frame, and confirms that flipping the schema's `date32` type ID, the block
flag bit, or a stored day-count byte fails CRC validation.

## Regenerating and validating

From the repository root:

```bash
uv run spec/v0.1/fixtures/date32/date32.py \
  --output spec/v0.1/fixtures/date32/date32.acta \
  --markdown-output spec/v0.1/fixtures/date32/date32.md
```

Expected SHA-256:

```text
fb1ff4fe5b7f6d00c54f3240cada2020061982bbff85010896268a513cba2303
```
