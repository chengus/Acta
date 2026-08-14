# IANA timezone Acta v0.2 fixture

This document annotates [`timezone.acta`](timezone.acta), the compatibility
fixture for the only variable-length type-parameter record in v0.2: the
`timestamp64` timezone name defined in [`format_v0.2.md`](../../format_v0.2.md)
section 7.

The file has a non-nullable `timestamp64` primary column named `time` with
millisecond unit and IANA timezone `Europe/Berlin`, an `int64` column
named `value`, and one three-row data block with `TS_SORTED` set. Row IDs are
disabled.

Logical contents:

| Row | `time` (`timestamp64[ms, Europe/Berlin]`) | UTC instant | `value` (int64) |
| ---: | ---: | --- | ---: |
| 0 | `1767340800000` | 2026-01-02T08:00:00+00:00 | `101` |
| 1 | `1767342600000` | 2026-01-02T08:30:00+00:00 | `102` |
| 2 | `1767344400000` | 2026-01-02T09:00:00+00:00 | `103` |

Section 3.1 stores zoned values as UTC epoch counts, so the timezone name is
descriptive schema metadata and does not change the stored count.

## The type-parameter record

The record begins at `0x0a4` and is `24`
bytes long, which is what the descriptor's type-parameter length field stores:

| Offset | Size | Value | Meaning |
| --- | ---: | --- | --- |
| `0x0a4` | 1 | `1` | Unit: millisecond |
| `0x0a5` | 1 | `2` | Timezone mode: IANA name |
| `0x0a6` | 2 | `0` | Reserved |
| `0x0a8` | 4 | `13` | Timezone name length |
| `0x0ac` | 13 | `Europe/Berlin` | Timezone name |
| `0x0b9` | 3 | zero | Padding to eight bytes |

The name is 13 bytes, so the record carries 3 bytes of
padding and the stored type-parameter length is
`8 + 13` rounded up to `24`. A reader that
expected `21` here would reject this file, and a writer
that stored `21` would produce one this fixture rejects.
Descriptor padding is separate and follows the parameters.

## Hex dump

```text
00000000: 4143 5441 0d0a 1a0a 0000 0200 4000 0000  ACTA........@...
00000010: 0000 0000 0000 0000 4143 5441 2d54 494d  ........ACTA-TIM
00000020: 455a 4f4e 4521 2121 4000 0000 0000 0000  EZONE!!!@.......
00000030: 0000 0000 0000 0000 0000 0000 a02b eb39  .............+.9
00000040: 4143 5441 4652 4d0a 0100 0000 0000 0000  ACTAFRM.........
00000050: 1800 0000 0000 0000 5800 0000 0000 0000  ........X.......
00000060: 0000 0000 0000 0000 5d97 31ee f1a5 c535  ........].1....5
00000070: 0100 0000 0000 0000 0200 0000 0100 0000  ................
00000080: 0000 0000 0000 0000 3800 0000 0100 0000  ........8.......
00000090: 0d00 0000 0400 0000 1800 0000 0000 0000  ................
000000a0: 7469 6d65 0102 0000 0d00 0000 4575 726f  time........Euro
000000b0: 7065 2f42 6572 6c69 6e00 0000 0000 0000  pe/Berlin.......
000000c0: 2000 0000 0200 0000 0500 0000 0500 0000   ...............
000000d0: 0000 0000 0000 0000 7661 6c75 6500 0000  ........value...
000000e0: c000 0000 0000 0000 0000 0000 0000 0000  ................
000000f0: d256 7489 bb1c 2936 4143 5441 454e 440a  .Vt...)6ACTAEND.
00000100: 4143 5441 4652 4d0a 0200 0000 0000 0000  ACTAFRM.........
00000110: e000 0000 0000 0000 3000 0000 0000 0000  ........0.......
00000120: 0100 0000 0000 0000 565a 8ad1 5e83 e765  ........VZ..^..e
00000130: 0100 0000 0000 0000 ffff ffff ffff ffff  ................
00000140: 0300 0000 0200 0000 0078 b87d 9b01 0000  .........x.}....
00000150: 8066 ef7d 9b01 0000 4000 0000 8000 0000  .f.}....@.......
00000160: e000 0000 0000 0000 0200 0000 0000 0000  ................
00000170: 0100 0000 0000 0000 0000 0000 0300 0000  ................
00000180: 0000 0000 0100 0000 0000 0000 0000 0000  ................
00000190: 0200 0000 0000 0000 0000 0000 0300 0000  ................
000001a0: 0100 0000 0100 0000 0000 0000 0000 0000  ................
000001b0: 0200 0000 0000 0000 0000 0000 0000 0000  ................
000001c0: 1800 0000 0000 0000 1800 0000 0000 0000  ................
000001d0: 0300 0000 0000 0000 30fb 8187 0000 0000  ........0.......
000001e0: 0200 0000 0000 0000 1800 0000 0000 0000  ................
000001f0: 1800 0000 0000 0000 1800 0000 0000 0000  ................
00000200: 0300 0000 0000 0000 78d2 e071 0000 0000  ........x..q....
00000210: 0078 b87d 9b01 0000 40ef d37d 9b01 0000  .x.}....@..}....
00000220: 8066 ef7d 9b01 0000 6500 0000 0000 0000  .f.}....e.......
00000230: 6600 0000 0000 0000 6700 0000 0000 0000  f.......g.......
00000240: 6001 0000 0000 0000 0100 0000 0000 0000  `...............
00000250: c015 fd8c c290 f412 4143 5441 454e 440a  ........ACTAEND.
```

Total: `0x260 = 608` bytes, with file ID
`ACTA-TIMEZONE!!!`. Layout conventions follow the
[minimal fixture](../minimal/minimal.md).

The block header declares row count 3, base row ID `UINT64_MAX`,
`ts_min=1767340800000`, `ts_max=1767344400000`, and block flags
`0x2` (`TS_SORTED`).

## Validation rules exercised

1. A `timestamp64` type-parameter record is padded to the next eight-byte
   boundary, and the stored type-parameter length includes that padding.
2. Timezone mode 2 requires a nonempty, well-formed UTF-8 IANA name; modes 0
   and 1 require an empty one.
3. Parameter padding bytes are zero.
4. `TS_SORTED` requires the block header bounds to equal the first and last
   stored values.
5. Every stream is validated by its CRC32C, and truncation at any point inside
   the data frame leaves exactly the committed schema frame.

## Regenerating and validating

From the repository root:

```bash
uv run spec/v0.2/fixtures/timezone/timezone.py \
  --output spec/v0.2/fixtures/timezone/timezone.acta \
  --markdown-output spec/v0.2/fixtures/timezone/timezone.md
```

Expected SHA-256:

```text
7adca0c16d7dee8ee708481fcb10b97c88117ae5799b371f507d512dfba08941
```
