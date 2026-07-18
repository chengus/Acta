# `TS_SORTED` block-flag Acta v0.1 fixture

This document annotates [`ts_sorted.acta`](ts_sorted.acta), the deterministic
compatibility fixture for data-block flag bit 1, `TS_SORTED`, defined in
[`format_v0.1.md`](../../format_v0.1.md) section 8. The file contains one
non-nullable UTC microsecond timestamp column named `time` and one data block
of four rows whose primary timestamps are monotonically nondecreasing.

Rows 1 and 2 store the same timestamp. The flag claims nondecreasing order,
not strictly increasing order, and this fixture deliberately exercises the
permitted equal-adjacent-values case.

Logical contents:

| Row | `time` (`timestamp64[us, UTC]`) | UTC instant |
| ---: | ---: | --- |
| 0 | 1,000,000 | `1970-01-01T00:00:01Z` |
| 1 | 2,000,000 | `1970-01-01T00:00:02Z` |
| 2 | 2,000,000 | `1970-01-01T00:00:02Z` |
| 3 | 3,000,000 | `1970-01-01T00:00:03Z` |

The block header declares row count 4, timestamp bounds
[1,000,000, 3,000,000], and block flags `0x2`
(`TS_SORTED` set, row IDs disabled). Because the flag is set, the minimum
equals the first row's value and the maximum equals the last row's value.

## Hex dump

```text
00000000: 4143 5441 0d0a 1a0a 0000 0100 4000 0000  ACTA........@...
00000010: 0000 0000 0000 0000 4143 5441 2d54 532d  ........ACTA-TS-
00000020: 534f 5254 4544 2121 4000 0000 0000 0000  SORTED!!@.......
00000030: 0000 0000 0000 0000 0000 0000 941f fbb6  ................
00000040: 4143 5441 4652 4d0a 0100 0000 0000 0000  ACTAFRM.........
00000050: 1800 0000 0000 0000 2800 0000 0000 0000  ........(.......
00000060: 0000 0000 0000 0000 aef7 c9fd 8e69 de0f  .............i..
00000070: 0100 0000 0000 0000 0100 0000 0100 0000  ................
00000080: 0000 0000 0000 0000 2800 0000 0100 0000  ........(.......
00000090: 0d00 0000 0400 0000 0800 0000 0000 0000  ................
000000a0: 7469 6d65 0201 0000 0000 0000 0000 0000  time............
000000b0: 9000 0000 0000 0000 0000 0000 0000 0000  ................
000000c0: 891b eb10 bd0a e0cf 4143 5441 454e 440a  ........ACTAEND.
000000d0: 4143 5441 4652 4d0a 0200 0000 0000 0000  ACTAFRM.........
000000e0: a000 0000 0000 0000 2000 0000 0000 0000  ........ .......
000000f0: 0100 0000 0000 0000 9425 0cf9 5d53 486c  .........%..]SHl
00000100: 0100 0000 0000 0000 ffff ffff ffff ffff  ................
00000110: 0400 0000 0100 0000 4042 0f00 0000 0000  ........@B......
00000120: c0c6 2d00 0000 0000 4000 0000 6000 0000  ..-.....@...`...
00000130: 9000 0000 1000 0000 0200 0000 0000 0000  ................
00000140: 0100 0000 0000 0300 0000 0000 0400 0000  ................
00000150: 0000 0000 0100 0100 9000 0000 1000 0000  ................
00000160: 0200 0000 0000 0000 0000 0000 0000 0000  ................
00000170: 2000 0000 0000 0000 2000 0000 0000 0000   ....... .......
00000180: 0400 0000 0000 0000 a686 1ffe 0000 0000  ................
00000190: 4042 0f00 0000 0000 c0c6 2d00 0000 0000  @B........-.....
000001a0: 4042 0f00 0000 0000 8084 1e00 0000 0000  @B..............
000001b0: 8084 1e00 0000 0000 c0c6 2d00 0000 0000  ..........-.....
000001c0: 1001 0000 0000 0000 0100 0000 0000 0000  ................
000001d0: a99b d717 3acc 696d 4143 5441 454e 440a  ....:.imACTAEND.
```

```text
0x000 ┌───────────────────────────┐
      │ 64-byte file prologue     │
0x040 ├───────────────────────────┤
      │ 144-byte schema frame     │
0x0d0 ├───────────────────────────┤
      │ 272-byte data frame       │
0x1e0 └───────────────────────────┘
```

Total: `0x1e0 = 480` bytes. The prologue and schema
frame are identical to the [minimal fixture](../minimal/minimal.md) except for
the 16-byte file ID `ACTA-TS-SORTED!!` and the prologue CRC32C that covers
it; see that document for their byte-by-byte annotation. This document
annotates the data frame, where the fixtures differ.

## Data frame: 0x0d0–0x1df

```text
0x0d0–0x0ff  frame prefix, 48 bytes
0x100–0x19f  data-frame header, 160 bytes
0x1a0–0x1bf  stream payload, 32 bytes
0x1c0–0x1df  commit trailer, 32 bytes
```

**Block header: 0x100–0x13f**

| Offset | Value | Meaning |
| --- | ---: | --- |
| `0x100–0x107` | `1` | Schema ID |
| `0x108–0x10f` | `UINT64_MAX` | Base row ID unavailable; row IDs are disabled |
| `0x110–0x113` | `4` | Row count |
| `0x114–0x117` | `1` | Column count |
| `0x118–0x11f` | `1,000,000` | Primary timestamp minimum, equal to row 0 |
| `0x120–0x127` | `3,000,000` | Primary timestamp maximum, equal to row 3 |
| `0x128–0x12b` | `64` | Column-table offset within this frame header |
| `0x12c–0x12f` | `96` | Stream-table offset within this frame header |
| `0x130–0x133` | `144` | Statistics-area offset within this frame header |
| `0x134–0x137` | `16` | Statistics-area length |
| `0x138–0x13b` | `0x2` | Block flags: bit 1 `TS_SORTED` set |
| `0x13c–0x13f` | `0` | Reserved |

The column descriptor at `0x140–0x15f`, the stream
descriptor at `0x160–0x18f`, and the optional min/max
statistics at `0x190–0x19f` have the same shape as the
minimal fixture's, with four elements instead of three. The values stream
stores 32 bytes at payload-relative offset 0 with CRC32C
`0xfe1f86a6`.

**Stream payload: 0x1a0–0x1bf**

| Offset | Stored bytes | Decoded `int64` | Logical row |
| --- | --- | ---: | --- |
| `0x1a0–0x1a7` | `40420f0000000000` | 1,000,000 | Row 0 |
| `0x1a8–0x1af` | `80841e0000000000` | 2,000,000 | Row 1 |
| `0x1b0–0x1b7` | `80841e0000000000` | 2,000,000 | Row 2 |
| `0x1b8–0x1bf` | `c0c62d0000000000` | 3,000,000 | Row 3 |

**Commit trailer: 0x1c0–0x1df**

| Offset | Value | Meaning |
| --- | ---: | --- |
| `0x1c0–0x1c7` | `272` | Total data-frame length |
| `0x1c8–0x1cf` | `1` | Repeated frame sequence number |
| `0x1d0–0x1d3` | `0x17d79ba9` | Body CRC32C |
| `0x1d4–0x1d7` | `0x6d69cc3a` | Trailer CRC32C |
| `0x1d8–0x1df` | `ACTAEND\n` | Commit magic |

## Validation rules exercised

A conforming implementation reading this fixture observes:

1. `TS_SORTED` claims `time[i] ≤ time[i + 1]` for every adjacent row pair
   within this block only; rows 1 and 2 show that equality is permitted.
2. Because the flag is set, the header minimum 1,000,000 equals the first
   row's value and the header maximum 3,000,000 equals the last row's
   value, so a reader may binary-search the decoded column for time-range
   boundaries.
3. A strict reader that decodes the column verifies both properties and
   reports a violation as corruption. The flag word is covered by the frame
   header CRC32C, so a flipped flag bit fails structural validation before
   any semantic check.

The generator's self-test builds and rejects a decreasing block that declares
`TS_SORTED`, accepts the same values with the flag clear, rejects sorted
values whose declared bounds are not the first and last rows, checks every
truncation point inside the data frame, and confirms that flipping either the
flag bit or a payload byte is detected by CRC validation.

## Regenerating and validating

From the repository root:

```bash
uv run spec/v0.1/fixtures/ts_sorted/ts_sorted.py \
  --output spec/v0.1/fixtures/ts_sorted/ts_sorted.acta \
  --markdown-output spec/v0.1/fixtures/ts_sorted/ts_sorted.md
```

Expected SHA-256:

```text
8ddd8bf09ce7d341fba9951ef9bab9a2ee0d0f07c3cfd9d78704089582d0a15b
```
