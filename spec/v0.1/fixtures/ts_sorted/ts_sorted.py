#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["google-crc32c>=1.6"]
# ///
"""Generate and validate the `TS_SORTED` block-flag Acta v0.1 fixture."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import struct
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROBE_PATH = SCRIPT_DIR.parent.parent / "format_probe.py"
PROBE_SPEC = importlib.util.spec_from_file_location("acta_format_probe", PROBE_PATH)
assert PROBE_SPEC and PROBE_SPEC.loader
probe = importlib.util.module_from_spec(PROBE_SPEC)
sys.modules[PROBE_SPEC.name] = probe
PROBE_SPEC.loader.exec_module(probe)

FILE_ID = b"ACTA-TS-SORTED!!"
SCHEMA_ID = 1
TS_SORTED = 0x2
TIMESTAMPS = (1_000_000, 2_000_000, 2_000_000, 3_000_000)


def build_fixture(
    timestamps: tuple[int, ...] = TIMESTAMPS,
    block_flags: int = TS_SORTED,
    header_bounds: tuple[int, int] | None = None,
) -> bytes:
    ts_min, ts_max = header_bounds or (min(timestamps), max(timestamps))
    timestamp_parameters = struct.pack("<BBHI", 2, 1, 0, 0)
    name = b"time"
    unpadded = probe.COLUMN_SCHEMA.size + len(name) + len(timestamp_parameters)
    descriptor_length = unpadded + (-unpadded) % 8
    column = probe.pad8(
        probe.COLUMN_SCHEMA.pack(
            descriptor_length, 1, 13, 0, len(name), len(timestamp_parameters), 0
        )
        + name
        + timestamp_parameters
    )
    schema_header = probe.SCHEMA_HEADER.pack(SCHEMA_ID, 1, 1, 0, 0)
    schema_frame = probe.make_frame(1, 0, schema_header, column)

    values = struct.pack(f"<{len(timestamps)}q", *timestamps)
    statistics = struct.pack("<qq", ts_min, ts_max)
    column_table_offset = probe.BLOCK_HEADER.size
    stream_table_offset = column_table_offset + probe.COLUMN_DESCRIPTOR.size
    statistics_offset = stream_table_offset + probe.STREAM_DESCRIPTOR.size
    block_header = probe.BLOCK_HEADER.pack(
        SCHEMA_ID,
        (1 << 64) - 1,
        len(timestamps),
        1,
        ts_min,
        ts_max,
        column_table_offset,
        stream_table_offset,
        statistics_offset,
        len(statistics),
        block_flags,
        0,
    )
    column_descriptor = probe.COLUMN_DESCRIPTOR.pack(
        1, 0, 3, 0, len(timestamps), 0, 1, 1, statistics_offset, len(statistics)
    )
    stream_descriptor = probe.STREAM_DESCRIPTOR.pack(
        2,
        0,
        0,
        0,
        0,
        len(values),
        len(values),
        len(timestamps),
        probe.crc32c(values),
        0,
    )
    data_header = block_header + column_descriptor + stream_descriptor + statistics
    data_frame = probe.make_frame(2, 1, data_header, values)
    return probe.make_prologue(FILE_ID) + schema_frame + data_frame


def decode_data_block(data: bytes) -> tuple[tuple[object, ...], tuple[int, ...]]:
    """Return the data block-header fields and its decoded timestamp values."""
    result = probe.scan_frames(data)
    if result.incomplete_tail or len(result.frames) != 2:
        raise AssertionError("fixture does not contain two complete frames")
    schema, block = result.frames
    if (schema.frame_type, block.frame_type) != (1, 2):
        raise AssertionError("fixture frame types are wrong")
    if probe.SCHEMA_HEADER.unpack_from(schema.header)[:3] != (SCHEMA_ID, 1, 1):
        raise AssertionError("fixture schema header is wrong")
    fields = probe.BLOCK_HEADER.unpack_from(block.header)
    row_count, stream_table_offset = fields[2], fields[7]
    stream_fields = probe.STREAM_DESCRIPTOR.unpack_from(
        block.header, stream_table_offset
    )
    stored = block.payload[stream_fields[4] : stream_fields[4] + stream_fields[5]]
    if probe.crc32c(stored) != stream_fields[8]:
        raise probe.Corruption("bad timestamp stream CRC32C")
    return fields, struct.unpack(f"<{row_count}q", stored)


def validate_ts_sorted(data: bytes) -> None:
    """Apply the format_v0.1.md section 8 `TS_SORTED` validation rules."""
    fields, decoded = decode_data_block(data)
    ts_min, ts_max, flags = fields[4], fields[5], fields[10]
    if (ts_min, ts_max) != (min(decoded), max(decoded)):
        raise probe.Corruption("block timestamp bounds do not match stored values")
    if flags & TS_SORTED:
        if any(left > right for left, right in zip(decoded, decoded[1:])):
            raise probe.Corruption("TS_SORTED is set but primary timestamps decrease")
        if decoded[0] != ts_min or decoded[-1] != ts_max:
            raise probe.Corruption(
                "TS_SORTED requires bounds equal to the first and last values"
            )


def self_test() -> bytes:
    fixture = build_fixture()
    validate_ts_sorted(fixture)
    fields, decoded = decode_data_block(fixture)
    if decoded != TIMESTAMPS or fields[10] != TS_SORTED:
        raise AssertionError("fixture values or block flags are wrong")

    # A block that decreases mid-stream while declaring TS_SORTED must be
    # rejected, while the same values without the claim remain valid.
    unsorted = (2_000_000, 1_000_000, 2_000_000, 3_000_000)
    try:
        validate_ts_sorted(build_fixture(unsorted, TS_SORTED))
    except probe.Corruption:
        pass
    else:
        raise AssertionError("unsorted TS_SORTED block was not rejected")
    validate_ts_sorted(build_fixture(unsorted, 0))

    # Sorted values whose header maximum is not the last row's value violate
    # the first/last equalities even though the ordering itself holds.
    try:
        validate_ts_sorted(
            build_fixture(TIMESTAMPS, TS_SORTED, header_bounds=(1_000_000, 4_000_000))
        )
    except probe.Corruption:
        pass
    else:
        raise AssertionError("bad TS_SORTED bounds were not rejected")

    # Every interrupted write inside the data frame preserves the schema frame.
    result = probe.scan_frames(fixture)
    data_frame_start = result.frames[-1].offset
    for cut in range(data_frame_start + 1, len(fixture)):
        truncated = probe.scan_frames(fixture[:cut])
        if (
            not truncated.incomplete_tail
            or len(truncated.frames) != 1
            or truncated.last_good_offset != data_frame_start
        ):
            raise AssertionError(f"unexpected truncation result at byte {cut}")

    # The flag is covered by the frame header CRC: flipping the TS_SORTED bit
    # itself, or any timestamp payload byte, fails structural validation
    # before the semantic ordering check runs.
    flag_offset = data_frame_start + probe.PREFIX.size + 56
    payload_offset = data_frame_start + probe.PREFIX.size + 160 + 8
    for corruption_offset in (flag_offset, payload_offset):
        damaged = bytearray(fixture)
        damaged[corruption_offset] ^= 0x02
        try:
            probe.scan_frames(bytes(damaged))
        except probe.Corruption:
            pass
        else:
            raise AssertionError(
                f"corruption at byte {corruption_offset} was not detected"
            )
    return fixture


def hexdump(data: bytes) -> str:
    lines: list[str] = []
    for offset in range(0, len(data), 16):
        chunk = data[offset : offset + 16]
        groups = " ".join(
            chunk[index : index + 2].hex() for index in range(0, len(chunk), 2)
        )
        printable = "".join(chr(value) if 32 <= value < 127 else "." for value in chunk)
        lines.append(f"{offset:08x}: {groups:<39}  {printable}")
    return "\n".join(lines)


def byte_range(start: int, length: int) -> str:
    return f"`0x{start:03x}–0x{start + length - 1:03x}`"


def render_markdown(fixture: bytes, output_name: str) -> str:
    result = probe.scan_frames(fixture)
    schema, block = result.frames
    data_start = block.offset
    header_start = data_start + probe.PREFIX.size
    payload_start = header_start + len(block.header)
    trailer_start = payload_start + len(block.payload)
    fields = probe.BLOCK_HEADER.unpack_from(block.header)
    stream_fields = probe.STREAM_DESCRIPTOR.unpack_from(block.header, fields[7])
    trailer = probe.TRAILER.unpack_from(fixture, trailer_start)
    digest = hashlib.sha256(fixture).hexdigest()

    row_lines = []
    for row, value in enumerate(TIMESTAMPS):
        second = value // 1_000_000
        row_lines.append(
            f"| {row} | {value:,} | `1970-01-01T00:00:0{second}Z` |"
        )
    value_lines = []
    for row, value in enumerate(TIMESTAMPS):
        stored = struct.pack("<q", value)
        value_lines.append(
            f"| {byte_range(payload_start + 8 * row, 8)} | `{stored.hex()}` | "
            f"{value:,} | Row {row} |"
        )

    return f"""# `TS_SORTED` block-flag Acta v0.1 fixture

This document annotates [`{output_name}`]({output_name}), the deterministic
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
{chr(10).join(row_lines)}

The block header declares row count 4, timestamp bounds
[{fields[4]:,}, {fields[5]:,}], and block flags `0x{fields[10]:x}`
(`TS_SORTED` set, row IDs disabled). Because the flag is set, the minimum
equals the first row's value and the maximum equals the last row's value.

## Hex dump

```text
{hexdump(fixture)}
```

```text
0x000 ┌───────────────────────────┐
      │ 64-byte file prologue     │
0x040 ├───────────────────────────┤
      │ 144-byte schema frame     │
0x0d0 ├───────────────────────────┤
      │ {len(fixture) - data_start}-byte data frame       │
0x{len(fixture):03x} └───────────────────────────┘
```

Total: `0x{len(fixture):x} = {len(fixture)}` bytes. The prologue and schema
frame are identical to the [minimal fixture](../minimal/minimal.md) except for
the 16-byte file ID `{FILE_ID.decode()}` and the prologue CRC32C that covers
it; see that document for their byte-by-byte annotation. This document
annotates the data frame, where the fixtures differ.

## Data frame: 0x{data_start:03x}–0x{len(fixture) - 1:03x}

```text
0x{data_start:03x}–0x{header_start - 1:03x}  frame prefix, 48 bytes
0x{header_start:03x}–0x{payload_start - 1:03x}  data-frame header, {len(block.header)} bytes
0x{payload_start:03x}–0x{trailer_start - 1:03x}  stream payload, {len(block.payload)} bytes
0x{trailer_start:03x}–0x{len(fixture) - 1:03x}  commit trailer, 32 bytes
```

**Block header: 0x{header_start:03x}–0x{header_start + 63:03x}**

| Offset | Value | Meaning |
| --- | ---: | --- |
| {byte_range(header_start, 8)} | `{fields[0]}` | Schema ID |
| {byte_range(header_start + 8, 8)} | `UINT64_MAX` | Base row ID unavailable; row IDs are disabled |
| {byte_range(header_start + 16, 4)} | `{fields[2]}` | Row count |
| {byte_range(header_start + 20, 4)} | `{fields[3]}` | Column count |
| {byte_range(header_start + 24, 8)} | `{fields[4]:,}` | Primary timestamp minimum, equal to row 0 |
| {byte_range(header_start + 32, 8)} | `{fields[5]:,}` | Primary timestamp maximum, equal to row 3 |
| {byte_range(header_start + 40, 4)} | `{fields[6]}` | Column-table offset within this frame header |
| {byte_range(header_start + 44, 4)} | `{fields[7]}` | Stream-table offset within this frame header |
| {byte_range(header_start + 48, 4)} | `{fields[8]}` | Statistics-area offset within this frame header |
| {byte_range(header_start + 52, 4)} | `{fields[9]}` | Statistics-area length |
| {byte_range(header_start + 56, 4)} | `0x{fields[10]:x}` | Block flags: bit 1 `TS_SORTED` set |
| {byte_range(header_start + 60, 4)} | `0` | Reserved |

The column descriptor at {byte_range(header_start + 64, 32)}, the stream
descriptor at {byte_range(header_start + 96, 48)}, and the optional min/max
statistics at {byte_range(header_start + 144, 16)} have the same shape as the
minimal fixture's, with four elements instead of three. The values stream
stores {stream_fields[5]} bytes at payload-relative offset 0 with CRC32C
`0x{stream_fields[8]:08x}`.

**Stream payload: 0x{payload_start:03x}–0x{trailer_start - 1:03x}**

| Offset | Stored bytes | Decoded `int64` | Logical row |
| --- | --- | ---: | --- |
{chr(10).join(value_lines)}

**Commit trailer: 0x{trailer_start:03x}–0x{len(fixture) - 1:03x}**

| Offset | Value | Meaning |
| --- | ---: | --- |
| {byte_range(trailer_start, 8)} | `{trailer[0]}` | Total data-frame length |
| {byte_range(trailer_start + 8, 8)} | `{trailer[1]}` | Repeated frame sequence number |
| {byte_range(trailer_start + 16, 4)} | `0x{trailer[2]:08x}` | Body CRC32C |
| {byte_range(trailer_start + 20, 4)} | `0x{trailer[3]:08x}` | Trailer CRC32C |
| {byte_range(trailer_start + 24, 8)} | `ACTAEND\\n` | Commit magic |

## Validation rules exercised

A conforming implementation reading this fixture observes:

1. `TS_SORTED` claims `time[i] ≤ time[i + 1]` for every adjacent row pair
   within this block only; rows 1 and 2 show that equality is permitted.
2. Because the flag is set, the header minimum {fields[4]:,} equals the first
   row's value and the header maximum {fields[5]:,} equals the last row's
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
uv run spec/v0.1/fixtures/ts_sorted/ts_sorted.py \\
  --output spec/v0.1/fixtures/ts_sorted/ts_sorted.acta \\
  --markdown-output spec/v0.1/fixtures/ts_sorted/ts_sorted.md
```

Expected SHA-256:

```text
{digest}
```
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fixture = self_test()
    digest = hashlib.sha256(fixture).hexdigest()
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(fixture)
        print(f"wrote {len(fixture)} bytes to {args.output}")
    if args.markdown_output:
        output_name = args.output.name if args.output else "ts_sorted.acta"
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(render_markdown(fixture, output_name))
        print(f"wrote {args.markdown_output}")
    if not args.output and not args.markdown_output:
        print(
            f"Acta v0.1 TS_SORTED fixture passed: {len(fixture)} bytes, "
            f"sha256={digest}"
        )


if __name__ == "__main__":
    main()
