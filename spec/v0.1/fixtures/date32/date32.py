#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["google-crc32c>=1.6"]
# ///
"""Generate and validate the `date32` primary-column Acta v0.1 fixture."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import struct
import sys
from datetime import date, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROBE_PATH = SCRIPT_DIR.parent.parent / "format_probe.py"
PROBE_SPEC = importlib.util.spec_from_file_location("acta_format_probe", PROBE_PATH)
assert PROBE_SPEC and PROBE_SPEC.loader
probe = importlib.util.module_from_spec(PROBE_SPEC)
sys.modules[PROBE_SPEC.name] = probe
PROBE_SPEC.loader.exec_module(probe)

FILE_ID = b"ACTA-DATE32-EOD!"
SCHEMA_ID = 1
DATE32_TYPE_ID = 18
TS_SORTED = 0x2
EPOCH = date(1970, 1, 1)
# Friday, then Monday and Tuesday: a weekend gap keeps the day counts
# nondecreasing but irregular, like a real trading calendar.
CALENDAR = (date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6))
DATES = tuple((day - EPOCH).days for day in CALENDAR)
CLOSES = (101.25, 102.5, 101.75)
ROW_COUNT = len(DATES)


def build_fixture(
    dates: tuple[int, ...] = DATES,
    block_flags: int = TS_SORTED,
    header_bounds: tuple[int, int] | None = None,
) -> bytes:
    date_min, date_max = header_bounds or (min(dates), max(dates))
    date_name, close_name = b"date", b"close"
    schema_payload = b""
    for column_id, type_id, name in (
        (1, DATE32_TYPE_ID, date_name),
        (2, 11, close_name),
    ):
        unpadded = probe.COLUMN_SCHEMA.size + len(name)
        descriptor_length = unpadded + (-unpadded) % 8
        schema_payload += probe.pad8(
            probe.COLUMN_SCHEMA.pack(
                descriptor_length, column_id, type_id, 0, len(name), 0, 0
            )
            + name
        )
    schema_header = probe.SCHEMA_HEADER.pack(SCHEMA_ID, 2, 1, 0, 0)
    schema_frame = probe.make_frame(1, 0, schema_header, schema_payload)

    date_values = struct.pack(f"<{len(dates)}i", *dates)
    close_values = struct.pack(f"<{len(CLOSES)}d", *CLOSES)
    close_offset = len(date_values) + (-len(date_values)) % 8
    payload = probe.pad8(date_values) + close_values
    statistics = struct.pack("<dd", min(CLOSES), max(CLOSES))

    column_table_offset = probe.BLOCK_HEADER.size
    stream_table_offset = column_table_offset + 2 * probe.COLUMN_DESCRIPTOR.size
    statistics_offset = stream_table_offset + 2 * probe.STREAM_DESCRIPTOR.size
    block_header = probe.BLOCK_HEADER.pack(
        SCHEMA_ID,
        (1 << 64) - 1,
        len(dates),
        2,
        date_min,
        date_max,
        column_table_offset,
        stream_table_offset,
        statistics_offset,
        len(statistics),
        block_flags,
        0,
    )
    # The date column relies on the mandatory header bounds; only the close
    # column carries optional kind-one min/max statistics.
    date_descriptor = probe.COLUMN_DESCRIPTOR.pack(
        1, 0, 0, 0, len(dates), 0, 1, 0, 0, 0
    )
    close_descriptor = probe.COLUMN_DESCRIPTOR.pack(
        2, 0, 2, 0, len(CLOSES), 1, 1, 1, statistics_offset, len(statistics)
    )
    date_stream = probe.STREAM_DESCRIPTOR.pack(
        2, 0, 0, 0, 0, len(date_values), len(date_values), len(dates),
        probe.crc32c(date_values), 0,
    )
    close_stream = probe.STREAM_DESCRIPTOR.pack(
        2, 0, 0, 0, close_offset, len(close_values), len(close_values),
        len(CLOSES), probe.crc32c(close_values), 0,
    )
    data_header = (
        block_header
        + date_descriptor
        + close_descriptor
        + date_stream
        + close_stream
        + statistics
    )
    data_frame = probe.make_frame(2, 1, data_header, payload)
    return probe.make_prologue(FILE_ID) + schema_frame + data_frame


def decode_data_block(
    data: bytes,
) -> tuple[tuple[object, ...], tuple[int, ...], tuple[float, ...]]:
    """Return the block-header fields and the decoded date and close values."""
    result = probe.scan_frames(data)
    if result.incomplete_tail or len(result.frames) != 2:
        raise AssertionError("fixture does not contain two complete frames")
    schema, block = result.frames
    if (schema.frame_type, block.frame_type) != (1, 2):
        raise AssertionError("fixture frame types are wrong")
    if probe.SCHEMA_HEADER.unpack_from(schema.header)[:3] != (SCHEMA_ID, 2, 1):
        raise AssertionError("fixture schema header is wrong")
    primary = probe.COLUMN_SCHEMA.unpack_from(schema.payload)
    if primary[1] != 1 or primary[2] != DATE32_TYPE_ID:
        raise AssertionError("primary column is not date32")
    fields = probe.BLOCK_HEADER.unpack_from(block.header)
    row_count, stream_table_offset = fields[2], fields[7]
    columns: list[tuple[bytes, ...]] = []
    for index, layout in enumerate(("i", "d")):
        stream_fields = probe.STREAM_DESCRIPTOR.unpack_from(
            block.header, stream_table_offset + index * probe.STREAM_DESCRIPTOR.size
        )
        stored = block.payload[stream_fields[4] : stream_fields[4] + stream_fields[5]]
        if probe.crc32c(stored) != stream_fields[8]:
            raise probe.Corruption("bad stream CRC32C")
        columns.append(struct.unpack(f"<{row_count}{layout}", stored))
    return fields, columns[0], columns[1]


def validate_date32_fixture(data: bytes) -> None:
    """Apply the section 8 primary-column and `TS_SORTED` rules to the dates."""
    fields, dates, closes = decode_data_block(data)
    date_min, date_max, flags = fields[4], fields[5], fields[10]
    if (date_min, date_max) != (min(dates), max(dates)):
        raise probe.Corruption("block date bounds do not match stored day counts")
    if flags & TS_SORTED:
        if any(left > right for left, right in zip(dates, dates[1:])):
            raise probe.Corruption("TS_SORTED is set but primary day counts decrease")
        if dates[0] != date_min or dates[-1] != date_max:
            raise probe.Corruption(
                "TS_SORTED requires bounds equal to the first and last values"
            )
    stats_offset, stats_length = fields[8], fields[9]
    result = probe.scan_frames(data)
    stats = result.frames[1].header[stats_offset : stats_offset + stats_length]
    if struct.unpack("<dd", stats) != (min(closes), max(closes)):
        raise probe.Corruption("close statistics do not match stored values")


def self_test() -> bytes:
    fixture = build_fixture()
    validate_date32_fixture(fixture)
    fields, dates, closes = decode_data_block(fixture)
    if dates != DATES or closes != CLOSES or fields[10] != TS_SORTED:
        raise AssertionError("fixture values or block flags are wrong")
    if DATES != (20455, 20458, 20459):
        raise AssertionError("calendar day counts changed unexpectedly")

    # Decreasing day counts with TS_SORTED must be rejected; the same values
    # without the claim remain valid.
    unsorted = (DATES[1], DATES[0], DATES[2])
    try:
        validate_date32_fixture(build_fixture(unsorted, TS_SORTED))
    except probe.Corruption:
        pass
    else:
        raise AssertionError("unsorted TS_SORTED block was not rejected")
    validate_date32_fixture(build_fixture(unsorted, 0))

    # Sorted values with a declared maximum past the last row violate the
    # first/last equalities.
    try:
        validate_date32_fixture(
            build_fixture(DATES, TS_SORTED, header_bounds=(DATES[0], DATES[-1] + 1))
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

    # Flipping the date32 type ID in the schema, the block-flag byte, or a
    # stored day-count byte all fail CRC validation before semantic checks.
    type_id_offset = 64 + probe.PREFIX.size + probe.SCHEMA_HEADER.size + 8
    flag_offset = data_frame_start + probe.PREFIX.size + 56
    payload_offset = data_frame_start + probe.PREFIX.size + len(
        result.frames[-1].header
    )
    for corruption_offset in (type_id_offset, flag_offset, payload_offset):
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
    stream_table = fields[7]
    date_stream = probe.STREAM_DESCRIPTOR.unpack_from(block.header, stream_table)
    close_stream = probe.STREAM_DESCRIPTOR.unpack_from(
        block.header, stream_table + probe.STREAM_DESCRIPTOR.size
    )
    trailer = probe.TRAILER.unpack_from(fixture, trailer_start)
    digest = hashlib.sha256(fixture).hexdigest()

    row_lines = []
    for row, (day_count, close) in enumerate(zip(DATES, CLOSES)):
        calendar_day = EPOCH + timedelta(days=day_count)
        row_lines.append(
            f"| {row} | {day_count:,} | `{calendar_day.isoformat()}` "
            f"({calendar_day.strftime('%A')}) | `{close}` |"
        )
    date_lines = []
    for row, day_count in enumerate(DATES):
        stored = struct.pack("<i", day_count)
        date_lines.append(
            f"| {byte_range(payload_start + 4 * row, 4)} | `{stored.hex()}` | "
            f"{day_count:,} | `{(EPOCH + timedelta(days=day_count)).isoformat()}` |"
        )
    close_lines = []
    for row, close in enumerate(CLOSES):
        stored = struct.pack("<d", close)
        close_lines.append(
            f"| {byte_range(payload_start + close_stream[4] + 8 * row, 8)} | "
            f"`{stored.hex()}` | `{close}` | Row {row} |"
        )

    return f"""# `date32` primary-column Acta v0.1 fixture

This document annotates [`{output_name}`]({output_name}), the deterministic
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
{chr(10).join(row_lines)}

The block header declares row count {fields[2]}, primary bounds
[{fields[4]:,}, {fields[5]:,}] as sign-extended `int64` day counts, and block
flags `0x{fields[10]:x}` (`TS_SORTED` set, row IDs disabled).

## Hex dump

```text
{hexdump(fixture)}
```

```text
0x000 ┌───────────────────────────┐
      │ 64-byte file prologue     │
0x040 ├───────────────────────────┤
      │ {data_start - 64}-byte schema frame     │
0x{data_start:03x} ├───────────────────────────┤
      │ {len(fixture) - data_start}-byte data frame       │
0x{len(fixture):03x} └───────────────────────────┘
```

Total: `0x{len(fixture):x} = {len(fixture)}` bytes, with file ID
`{FILE_ID.decode()}`. Framing, prologue, and trailer layouts are annotated
byte-by-byte in the [minimal fixture](../minimal/minimal.md); this document
annotates what is new here, the `date32` schema and data.

## Schema frame: 0x040–0x{data_start - 1:03x}

The schema header declares schema ID {SCHEMA_ID}, two columns, and primary
timestamp column ID 1. Both column schema descriptors have zero
type-parameter length because neither `date32` nor `float64` is
parameterized.

| Payload range | ID | Name | Type ID / logical type | Nullable |
| --- | ---: | --- | --- | --- |
| {byte_range(64 + probe.PREFIX.size + probe.SCHEMA_HEADER.size, 32)} | 1 | `date` | 18 / `date32` | no |
| {byte_range(64 + probe.PREFIX.size + probe.SCHEMA_HEADER.size + 32, 32)} | 2 | `close` | 11 / `float64` | no |

Column 1 is the primary timestamp column with type `date32`, the case
sections 7 and 8 define for calendar-dated series.

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
| {byte_range(header_start + 24, 8)} | `{fields[4]:,}` | Primary minimum: day count of row 0 as `int64` |
| {byte_range(header_start + 32, 8)} | `{fields[5]:,}` | Primary maximum: day count of row 2 as `int64` |
| {byte_range(header_start + 40, 4)} | `{fields[6]}` | Column-table offset within this frame header |
| {byte_range(header_start + 44, 4)} | `{fields[7]}` | Stream-table offset within this frame header |
| {byte_range(header_start + 48, 4)} | `{fields[8]}` | Statistics-area offset within this frame header |
| {byte_range(header_start + 52, 4)} | `{fields[9]}` | Statistics-area length |
| {byte_range(header_start + 56, 4)} | `0x{fields[10]:x}` | Block flags: bit 1 `TS_SORTED` set |
| {byte_range(header_start + 60, 4)} | `0` | Reserved |

**Column and stream descriptors: 0x{header_start + 64:03x}–0x{header_start + fields[8] - 1:03x}**

| Descriptor range | Column | Contents |
| --- | --- | --- |
| {byte_range(header_start + 64, 32)} | `date` | plain layout, 0 nulls, {fields[2]} dense values, stream 0, no optional statistics |
| {byte_range(header_start + 96, 32)} | `close` | plain layout, 0 nulls, {fields[2]} dense values, stream 1, kind-one min/max at header offset {fields[8]} |
| {byte_range(header_start + 128, 48)} | `date` values | raw, uncompressed, offset 0, {date_stream[5]} stored bytes, CRC32C `0x{date_stream[8]:08x}` |
| {byte_range(header_start + 176, 48)} | `close` values | raw, uncompressed, offset {close_stream[4]}, {close_stream[5]} stored bytes, CRC32C `0x{close_stream[8]:08x}` |

The `date` column carries no optional statistics: the mandatory block-header
bounds already cover the primary column. The optional statistics area at
{byte_range(header_start + fields[8], fields[9])} stores the `close` minimum
`{min(CLOSES)}` and maximum `{max(CLOSES)}` as canonical `float64` values.

**Stream payload: 0x{payload_start:03x}–0x{trailer_start - 1:03x}**

The `date` stream stores three little-endian `int32` day counts; four zero
padding bytes then align the `close` stream to an eight-byte boundary.

| Offset | Stored bytes | Day count | Calendar date |
| --- | --- | ---: | --- |
{chr(10).join(date_lines)}

| Offset | Stored bytes | Decoded `float64` | Logical row |
| --- | --- | ---: | --- |
{chr(10).join(close_lines)}

**Commit trailer: 0x{trailer_start:03x}–0x{len(fixture) - 1:03x}**

| Offset | Value | Meaning |
| --- | ---: | --- |
| {byte_range(trailer_start, 8)} | `{trailer[0]}` | Total data-frame length |
| {byte_range(trailer_start + 8, 8)} | `{trailer[1]}` | Repeated frame sequence number |
| {byte_range(trailer_start + 16, 4)} | `0x{trailer[2]:08x}` | Body CRC32C |
| {byte_range(trailer_start + 20, 4)} | `0x{trailer[3]:08x}` | Trailer CRC32C |
| {byte_range(trailer_start + 24, 8)} | `ACTAEND\\n` | Commit magic |

## Validation rules exercised

1. A `date32` value is a timezone-free calendar day count from `1970-01-01`;
   this fixture stores days {fields[4]:,}–{fields[5]:,}, spanning a weekend
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
uv run spec/v0.1/fixtures/date32/date32.py \\
  --output spec/v0.1/fixtures/date32/date32.acta \\
  --markdown-output spec/v0.1/fixtures/date32/date32.md
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
        output_name = args.output.name if args.output else "date32.acta"
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(render_markdown(fixture, output_name))
        print(f"wrote {args.markdown_output}")
    if not args.output and not args.markdown_output:
        print(
            f"Acta v0.1 date32 fixture passed: {len(fixture)} bytes, "
            f"sha256={digest}"
        )


if __name__ == "__main__":
    main()
