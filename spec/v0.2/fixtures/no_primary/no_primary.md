# No-primary Acta v0.2 fixture (utf8 + int64)

This document annotates [`no_primary.acta`](no_primary.acta), a deterministic
compatibility fixture for a schema without a primary timestamp column,
defined in [`format_v0.2.md`](../../format_v0.2.md) sections 3, 7, and 8.
The file has a `utf8` column named `event` and an `int64` column named
`value`, with one three-row data block.  The primary timestamp column ID is
`0`, both timestamp bounds are zero, and `TS_SORTED` is
clear. Row IDs are independently disabled for this fixture.

Logical contents:

| Row | `event` (utf8) | `value` (int64) |
| ---: | ---: | ---: |
| 0 | `alpha` | `10` |
| 1 | `beta` | `20` |
| 2 | `gamma` | `30` |

The block header declares row count 3, base row ID `UINT64_MAX`,
`ts_min=0`, `ts_max=0`, and block flags `0x0` (clear).

## Hex dump

```text
00000000: 4143 5441 0d0a 1a0a 0000 0200 4000 0000  ACTA........@...
00000010: 0000 0000 0000 0000 4143 5441 2d4e 4f2d  ........ACTA-NO-
00000020: 5052 494d 4152 5921 4000 0000 0000 0000  PRIMARY!@.......
00000030: 0000 0000 0000 0000 0000 0000 4523 e26d  ............E#.m
00000040: 4143 5441 4652 4d0a 0100 0000 0000 0000  ACTAFRM.........
00000050: 1800 0000 0000 0000 4000 0000 0000 0000  ........@.......
00000060: 0000 0000 0000 0000 6d43 40df d583 3102  ........mC@...1.
00000070: 0100 0000 0000 0000 0200 0000 0000 0000  ................
00000080: 0000 0000 0000 0000 2000 0000 0100 0000  ........ .......
00000090: 0e00 0000 0500 0000 0000 0000 0000 0000  ................
000000a0: 6576 656e 7400 0000 2000 0000 0200 0000  event... .......
000000b0: 0500 0000 0500 0000 0000 0000 0000 0000  ................
000000c0: 7661 6c75 6500 0000 a800 0000 0000 0000  value...........
000000d0: 0000 0000 0000 0000 7527 d4a8 c831 073d  ........u'...1.=
000000e0: 4143 5441 454e 440a 4143 5441 4652 4d0a  ACTAEND.ACTAFRM.
000000f0: 0200 0000 0000 0000 1001 0000 0000 0000  ................
00000100: 3800 0000 0000 0000 0100 0000 0000 0000  8...............
00000110: 6043 5d6b 5c34 a33d 0100 0000 0000 0000  `C]k\4.=........
00000120: ffff ffff ffff ffff 0300 0000 0200 0000  ................
00000130: 0000 0000 0000 0000 0000 0000 0000 0000  ................
00000140: 4000 0000 8000 0000 1001 0000 0000 0000  @...............
00000150: 0000 0000 0000 0000 0100 0000 0000 0000  ................
00000160: 0000 0000 0300 0000 0000 0000 0200 0000  ................
00000170: 0000 0000 0000 0000 0200 0000 0000 0000  ................
00000180: 0000 0000 0300 0000 0200 0000 0100 0000  ................
00000190: 0000 0000 0000 0000 0200 0000 0000 0000  ................
000001a0: 0000 0000 0000 0000 0e00 0000 0000 0000  ................
000001b0: 0e00 0000 0000 0000 0300 0000 0000 0000  ................
000001c0: bedc de22 0000 0000 0300 0000 0000 0000  ..."............
000001d0: 1000 0000 0000 0000 0c00 0000 0000 0000  ................
000001e0: 0c00 0000 0000 0000 0300 0000 0000 0000  ................
000001f0: 8b28 ac58 0000 0000 0200 0000 0000 0000  .(.X............
00000200: 2000 0000 0000 0000 1800 0000 0000 0000   ...............
00000210: 1800 0000 0000 0000 0300 0000 0000 0000  ................
00000220: 5f57 a190 0000 0000 616c 7068 6162 6574  _W......alphabet
00000230: 6167 616d 6d61 0000 0500 0000 0400 0000  agamma..........
00000240: 0500 0000 0000 0000 0a00 0000 0000 0000  ................
00000250: 1400 0000 0000 0000 1e00 0000 0000 0000  ................
00000260: 9801 0000 0000 0000 0100 0000 0000 0000  ................
00000270: d33f c10e 29dd 66ed 4143 5441 454e 440a  .?..).f.ACTAEND.
```

```text
0x000 ┌───────────────────────────┐
      │ 64-byte file prologue     │
0x040 ├───────────────────────────┤
      │ 168-byte schema frame     │
0x0e8 ├───────────────────────────┤
      │ 408-byte data frame       │
0x280 └───────────────────────────┘
```

Total: `0x280 = 640` bytes, with file ID
`ACTA-NO-PRIMARY!`.  Layout conventions follow the [minimal fixture](../minimal/minimal.md).

## Schema frame: 0x040–0x0e7

The schema header declares schema ID 1, two columns, and primary
timestamp column ID 0.  Both column schema descriptors
have zero type-parameter length.

| Payload range | ID | Name | Type ID / logical type | Nullable |
| --- | ---: | --- | --- | --- |
| `0x088–0x0a7` | 1 | `event` | 14 / `utf8` | no |
| `0x0a8–0x0c7` | 2 | `value` | 5 / `int64` | no |

## Data frame: 0x0e8–0x27f

```text
0x0e8–0x117  frame prefix, 48 bytes
0x118–0x227  data-frame header, 272 bytes
0x228–0x25f  stream payload, 56 bytes
0x260–0x27f  commit trailer, 32 bytes
```

**Block header: 0x118–0x157**

| Offset | Value | Meaning |
| --- | ---: | --- |
| `0x118–0x11f` | `1` | Schema ID |
| `0x120–0x127` | `UINT64_MAX` | Base row ID (unavailable) |
| `0x128–0x12b` | `3` | Row count |
| `0x12c–0x12f` | `2` | Column count |
| `0x130–0x137` | `0` | Primary minimum (`ts_min`, always 0) |
| `0x138–0x13f` | `0` | Primary maximum (`ts_max`, always 0) |
| `0x140–0x143` | `64` | Column-table offset within this frame header |
| `0x144–0x147` | `128` | Stream-table offset within this frame header |
| `0x148–0x14b` | `272` | Statistics-area offset (unused) |
| `0x14c–0x14f` | `0` | Statistics-area length (0) |
| `0x150–0x153` | `0x0` | Block flags (all clear) |
| `0x154–0x157` | `0` | Reserved |

**Column and stream descriptors: 0x158–0x227**

| Descriptor range | Column | Contents |
| --- | --- | --- |
| `0x158–0x177` | `event` | plain layout, 0 nulls, 3 values, streams 0–1 |
| `0x178–0x197` | `value` | plain layout, 0 nulls, 3 values, stream 2 |
| `0x198–0x1c7` | `event` values | raw, uncompressed, offset 0, 14 stored bytes, CRC32C `0x22dedcbe` |
| `0x1c8–0x1f7` | `event` lengths | raw, uncompressed, offset 16, 12 stored bytes, CRC32C `0x58ac288b` |
| `0x1f8–0x227` | `value` data | raw, uncompressed, offset 32, 24 stored bytes, CRC32C `0x90a1575f` |

**Stream payload: 0x228–0x25f**

The `event` column has a concatenated UTF-8 values stream and a separate
`uint32` lengths stream. Every physical stream begins at an eight-byte-aligned
payload offset.

#### Event values

| Offset | Stored bytes | Decoded `utf8` |
| --- | --- | --- |
| `0x228–0x22c` | `616c706861` | `alpha` |
| `0x22d–0x230` | `62657461` | `beta` |
| `0x231–0x235` | `67616d6d61` | `gamma` |

#### Event lengths

| Offset | Stored bytes | Decoded `uint32` |
| --- | --- | ---: |
| `0x238–0x23b` | `05000000` | `5` |
| `0x23c–0x23f` | `04000000` | `4` |
| `0x240–0x243` | `05000000` | `5` |

#### Value data (8‑byte aligned)

| Offset | Stored bytes | Decoded `int64` |
| --- | ---: | ---: |
| `0x248–0x24f` | `0a00000000000000` | `10` |
| `0x250–0x257` | `1400000000000000` | `20` |
| `0x258–0x25f` | `1e00000000000000` | `30` |

**Commit trailer: 0x260–0x27f**

| Offset | Value | Meaning |
| --- | ---: | --- |
| `0x260–0x267` | `408` | Total data-frame length |
| `0x268–0x26f` | `1` | Repeated frame sequence number |
| `0x270–0x273` | `0x0ec13fd3` | Body CRC32C |
| `0x274–0x277` | `0xed66dd29` | Trailer CRC32C |
| `0x278–0x27f` | `ACTAEND\n` | Commit magic |

## Validation rules exercised

1. When the primary timestamp column ID is `0`, the
   block header fields `ts_min` and `ts_max` must be zero, and the `TS_SORTED`
   block flag must be clear.
2. A `utf8` column uses plain layout with separate values and unsigned
   32-bit lengths streams.
3. With the `ROW_IDS` file feature disabled, the base row ID is `UINT64_MAX`.
4. An `int64` column uses a fixed-size plain layout with values packed in
   little‑endian order.
5. Every stream is validated by its CRC32C; the generator’s self‑test
   verifies that corruption of any byte in the schema type ID, block flags,
   or data payload is caught.

The generator’s self‑test also checks that truncation at any point inside
the data frame leaves exactly one complete frame (the schema frame) and that
the three semantic invalid variants (non‑zero `ts_min`, non‑zero `ts_max`,
`TS_SORTED` set) raise `Corruption`.

## Regenerating and validating

From the repository root:

```bash
uv run spec/v0.2/fixtures/no_primary/no_primary.py \
  --output spec/v0.2/fixtures/no_primary/no_primary.acta \
  --markdown-output spec/v0.2/fixtures/no_primary/no_primary.md
```

Expected SHA‑256:

```text
2d8832aaa3fb1f13308cde7cc426a6a038e7a4a23cc7b1a9cdccd3d9b86b9310
```
