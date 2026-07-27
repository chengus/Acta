#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["google-crc32c>=1.6"]
# ///
"""Generate and validate a no-primary Acta v0.2 fixture (utf8 + int64)."""

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

FILE_ID = b"ACTA-NO-PRIMARY!"
SCHEMA_ID = 1
PRIMARY_TIMESTAMP_ID = 0
UTF8_TYPE_ID = 14
INT64_TYPE_ID = 5
TS_SORTED = 0x2
UINT64_MAX = (1 << 64) - 1

EVENTS = ("alpha", "beta", "gamma")
VALUES = (10, 20, 30)
ROW_COUNT = len(EVENTS)


def build_fixture(
    ts_min: int = 0,
    ts_max: int = 0,
    block_flags: int = 0,
    base_row_id: int = UINT64_MAX,
) -> bytes:
    # Build schema frame
    schema_payload = b""
    for column_id, type_id, name in (
        (1, UTF8_TYPE_ID, b"event"),
        (2, INT64_TYPE_ID, b"value"),
    ):
        unpadded = probe.COLUMN_SCHEMA.size + len(name)
        descriptor_length = unpadded + (-unpadded) % 8
        schema_payload += probe.pad8(
            probe.COLUMN_SCHEMA.pack(
                descriptor_length, column_id, type_id, 0, len(name), 0, 0
            )
            + name
        )
    schema_header = probe.SCHEMA_HEADER.pack(
        SCHEMA_ID, 2, PRIMARY_TIMESTAMP_ID, 0, 0
    )
    schema_frame = probe.make_frame(1, 0, schema_header, schema_payload)

    # Plain variable-width data has separate values and uint32 lengths streams.
    string_bytes = b"".join(e.encode("utf-8") for e in EVENTS)
    lengths_bytes = struct.pack(
        f"<{ROW_COUNT}I", *(len(event.encode("utf-8")) for event in EVENTS)
    )
    int64_payload = struct.pack(f"<{ROW_COUNT}q", *VALUES)
    values_offset = 0
    lengths_offset = len(probe.pad8(string_bytes))
    int64_offset = lengths_offset + len(probe.pad8(lengths_bytes))
    payload = (
        probe.pad8(string_bytes)
        + probe.pad8(lengths_bytes)
        + int64_payload
    )

    column_table_offset = probe.BLOCK_HEADER.size
    stream_table_offset = column_table_offset + 2 * probe.COLUMN_DESCRIPTOR.size
    statistics_offset = stream_table_offset + 3 * probe.STREAM_DESCRIPTOR.size
    block_header = probe.BLOCK_HEADER.pack(
        SCHEMA_ID,
        base_row_id,
        ROW_COUNT,
        2,
        ts_min,
        ts_max,
        column_table_offset,
        stream_table_offset,
        statistics_offset,
        0,
        block_flags,
        0,
    )
    event_descriptor = probe.COLUMN_DESCRIPTOR.pack(
        1, 0, 0, 0, ROW_COUNT, 0, 2, 0, 0, 0
    )
    value_descriptor = probe.COLUMN_DESCRIPTOR.pack(
        2, 0, 0, 0, ROW_COUNT, 2, 1, 0, 0, 0
    )
    event_values_stream = probe.STREAM_DESCRIPTOR.pack(
        2,
        0,
        0,
        0,
        values_offset,
        len(string_bytes),
        len(string_bytes),
        ROW_COUNT,
        probe.crc32c(string_bytes),
        0,
    )
    event_lengths_stream = probe.STREAM_DESCRIPTOR.pack(
        3,
        0,
        0,
        0,
        lengths_offset,
        len(lengths_bytes),
        len(lengths_bytes),
        ROW_COUNT,
        probe.crc32c(lengths_bytes),
        0,
    )
    value_stream = probe.STREAM_DESCRIPTOR.pack(
        2,
        0,
        0,
        0,
        int64_offset,
        len(int64_payload),
        len(int64_payload),
        ROW_COUNT,
        probe.crc32c(int64_payload),
        0,
    )
    data_header = (
        block_header
        + event_descriptor
        + value_descriptor
        + event_values_stream
        + event_lengths_stream
        + value_stream
    )
    data_frame = probe.make_frame(2, 1, data_header, payload)
    return probe.make_prologue(FILE_ID) + schema_frame + data_frame


def decode_data_block(
    data: bytes,
) -> tuple[tuple[object, ...], tuple[str, ...], tuple[int, ...]]:
    """Return block-header fields, decoded events, and values."""
    result = probe.scan_frames(data)
    if result.incomplete_tail or len(result.frames) != 2:
        raise AssertionError("fixture does not contain two complete frames")
    schema, block = result.frames
    if (schema.frame_type, block.frame_type) != (1, 2):
        raise AssertionError("fixture frame types are wrong")
    schema_fields = probe.SCHEMA_HEADER.unpack_from(schema.header)
    if schema_fields != (SCHEMA_ID, 2, PRIMARY_TIMESTAMP_ID, 0, 0):
        raise AssertionError("fixture schema header is wrong")
    first_schema = probe.COLUMN_SCHEMA.unpack_from(schema.payload)
    second_offset = first_schema[0]
    second_schema = probe.COLUMN_SCHEMA.unpack_from(schema.payload, second_offset)
    if (
        first_schema[1:4] != (1, UTF8_TYPE_ID, 0)
        or second_schema[1:4] != (2, INT64_TYPE_ID, 0)
    ):
        raise AssertionError("fixture column schema is wrong")

    fields = probe.BLOCK_HEADER.unpack_from(block.header)
    row_count, column_table_offset, stream_table_offset = (
        fields[2],
        fields[6],
        fields[7],
    )
    event_column = probe.COLUMN_DESCRIPTOR.unpack_from(
        block.header, column_table_offset
    )
    value_column = probe.COLUMN_DESCRIPTOR.unpack_from(
        block.header, column_table_offset + probe.COLUMN_DESCRIPTOR.size
    )
    if event_column[:8] != (1, 0, 0, 0, ROW_COUNT, 0, 2, 0):
        raise AssertionError("event column descriptor is wrong")
    if value_column[:8] != (2, 0, 0, 0, ROW_COUNT, 2, 1, 0):
        raise AssertionError("value column descriptor is wrong")

    event_values_fields = probe.STREAM_DESCRIPTOR.unpack_from(
        block.header, stream_table_offset
    )
    event_lengths_fields = probe.STREAM_DESCRIPTOR.unpack_from(
        block.header, stream_table_offset + probe.STREAM_DESCRIPTOR.size
    )
    stored_events = block.payload[
        event_values_fields[4] : event_values_fields[4] + event_values_fields[5]
    ]
    stored_lengths = block.payload[
        event_lengths_fields[4] : event_lengths_fields[4]
        + event_lengths_fields[5]
    ]
    if (
        event_values_fields[0] != 2
        or event_lengths_fields[0] != 3
        or probe.crc32c(stored_events) != event_values_fields[8]
        or probe.crc32c(stored_lengths) != event_lengths_fields[8]
    ):
        raise probe.Corruption("bad event stream metadata or CRC32C")
    lengths = struct.unpack(f"<{row_count}I", stored_lengths)
    cursor = 0
    decoded_events = []
    for length in lengths:
        decoded_events.append(stored_events[cursor : cursor + length].decode("utf-8"))
        cursor += length
    if cursor != len(stored_events):
        raise probe.Corruption("event lengths do not consume values stream")
    events = tuple(decoded_events)

    value_stream_fields = probe.STREAM_DESCRIPTOR.unpack_from(
        block.header, stream_table_offset + 2 * probe.STREAM_DESCRIPTOR.size
    )
    stored_value = block.payload[
        value_stream_fields[4] : value_stream_fields[4] + value_stream_fields[5]
    ]
    if probe.crc32c(stored_value) != value_stream_fields[8]:
        raise probe.Corruption("bad value stream CRC32C")
    if value_stream_fields[0] != 2:
        raise probe.Corruption("bad value stream kind")
    values = struct.unpack(f"<{value_stream_fields[7]}q", stored_value)

    return fields, events, values


def validate_no_primary_fixture(data: bytes) -> None:
    """Validate the fixture enforces no-primary (ID=0) rules."""
    fields, events, values = decode_data_block(data)
    base_row_id, ts_min, ts_max, flags = fields[1], fields[4], fields[5], fields[10]
    if base_row_id != UINT64_MAX:
        raise probe.Corruption("base row ID must be UINT64_MAX for no-primary")
    if ts_min != 0 or ts_max != 0:
        raise probe.Corruption("ts_min and ts_max must be zero when primary ID=0")
    if flags & TS_SORTED:
        raise probe.Corruption("TS_SORTED must be clear when primary ID=0")
    # Check events and values
    if events != EVENTS:
        raise probe.Corruption("decoded events do not match expected")
    if values != VALUES:
        raise probe.Corruption("decoded values do not match expected")


def self_test() -> bytes:
    # Build and validate the base fixture
    fixture = build_fixture()
    validate_no_primary_fixture(fixture)

    # Three invalid semantic variants that must be rejected
    # 1. Non-zero ts_min
    try:
        bad = build_fixture(ts_min=1)
        validate_no_primary_fixture(bad)
    except probe.Corruption:
        pass
    else:
        raise AssertionError("non-zero ts_min not rejected")

    # 2. Non-zero ts_max
    try:
        bad = build_fixture(ts_max=1)
        validate_no_primary_fixture(bad)
    except probe.Corruption:
        pass
    else:
        raise AssertionError("non-zero ts_max not rejected")

    # 3. TS_SORTED set
    try:
        bad = build_fixture(block_flags=TS_SORTED)
        validate_no_primary_fixture(bad)
    except probe.Corruption:
        pass
    else:
        raise AssertionError("TS_SORTED flag not rejected")

    # Truncation test: each cut inside data frame must leave one good frame
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

    # Corruption: flip a byte in schema type ID, block header flags, or data payload
    schema_frame, block_frame = result.frames
    # Offset of type ID for column 1 in schema payload: after prefix + schema_header + 8 byte descriptor header
    type_id_offset = 64 + probe.PREFIX.size + probe.SCHEMA_HEADER.size + 8
    flag_offset = data_frame_start + probe.PREFIX.size + 56
    # Payload offset: first byte of event data
    payload_offset = data_frame_start + probe.PREFIX.size + len(block_frame.header)
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
    event_values_stream = probe.STREAM_DESCRIPTOR.unpack_from(
        block.header, stream_table
    )
    event_lengths_stream = probe.STREAM_DESCRIPTOR.unpack_from(
        block.header, stream_table + probe.STREAM_DESCRIPTOR.size
    )
    value_stream = probe.STREAM_DESCRIPTOR.unpack_from(
        block.header, stream_table + 2 * probe.STREAM_DESCRIPTOR.size
    )
    trailer = probe.TRAILER.unpack_from(fixture, trailer_start)
    digest = hashlib.sha256(fixture).hexdigest()

    row_lines = [
        f"| {i} | `{EVENTS[i]}` | `{VALUES[i]}` |"
        for i in range(ROW_COUNT)
    ]

    event_lines = []
    event_cursor = 0
    for i in range(ROW_COUNT):
        encoded = EVENTS[i].encode()
        event_lines.append(
            f"| {byte_range(payload_start + event_values_stream[4] + event_cursor, len(encoded))} "
            f"| `{encoded.hex()}` | `{EVENTS[i]}` |"
        )
        event_cursor += len(encoded)
    length_lines = [
        f"| {byte_range(payload_start + event_lengths_stream[4] + 4 * i, 4)} "
        f"| `{struct.pack('<I', len(EVENTS[i].encode())).hex()}` "
        f"| `{len(EVENTS[i].encode())}` |"
        for i in range(ROW_COUNT)
    ]
    value_lines = [
        f"| {byte_range(payload_start + value_stream[4] + 8*i, 8)} "
        f"| `{struct.pack('<q', VALUES[i]).hex()}` | `{VALUES[i]}` |"
        for i in range(ROW_COUNT)
    ]

    return f"""# No-primary Acta v0.2 fixture (utf8 + int64)

This document annotates [`{output_name}`]({output_name}), a deterministic
compatibility fixture for a schema without a primary timestamp column,
defined in [`format_v0.2.md`](../../format_v0.2.md) sections 3, 7, and 8.
The file has a `utf8` column named `event` and an `int64` column named
`value`, with one three-row data block.  The primary timestamp column ID is
`{PRIMARY_TIMESTAMP_ID}`, both timestamp bounds are zero, and `TS_SORTED` is
clear. Row IDs are independently disabled for this fixture.

Logical contents:

| Row | `event` (utf8) | `value` (int64) |
| ---: | ---: | ---: |
{chr(10).join(row_lines)}

The block header declares row count {fields[2]}, base row ID `UINT64_MAX`,
`ts_min=0`, `ts_max=0`, and block flags `0x{fields[10]:x}` (clear).

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
`{FILE_ID.decode()}`.  Layout conventions follow the [minimal fixture](../minimal/minimal.md).

## Schema frame: 0x040–0x{data_start - 1:03x}

The schema header declares schema ID {SCHEMA_ID}, two columns, and primary
timestamp column ID {PRIMARY_TIMESTAMP_ID}.  Both column schema descriptors
have zero type-parameter length.

| Payload range | ID | Name | Type ID / logical type | Nullable |
| --- | ---: | --- | --- | --- |
| {byte_range(64 + probe.PREFIX.size + probe.SCHEMA_HEADER.size, 32)} | 1 | `event` | {UTF8_TYPE_ID} / `utf8` | no |
| {byte_range(64 + probe.PREFIX.size + probe.SCHEMA_HEADER.size + 32, 32)} | 2 | `value` | {INT64_TYPE_ID} / `int64` | no |

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
| {byte_range(header_start + 8, 8)} | `UINT64_MAX` | Base row ID (unavailable) |
| {byte_range(header_start + 16, 4)} | `{fields[2]}` | Row count |
| {byte_range(header_start + 20, 4)} | `{fields[3]}` | Column count |
| {byte_range(header_start + 24, 8)} | `{fields[4]}` | Primary minimum (`ts_min`, always 0) |
| {byte_range(header_start + 32, 8)} | `{fields[5]}` | Primary maximum (`ts_max`, always 0) |
| {byte_range(header_start + 40, 4)} | `{fields[6]}` | Column-table offset within this frame header |
| {byte_range(header_start + 44, 4)} | `{fields[7]}` | Stream-table offset within this frame header |
| {byte_range(header_start + 48, 4)} | `{fields[8]}` | Statistics-area offset (unused) |
| {byte_range(header_start + 52, 4)} | `{fields[9]}` | Statistics-area length (0) |
| {byte_range(header_start + 56, 4)} | `0x{fields[10]:x}` | Block flags (all clear) |
| {byte_range(header_start + 60, 4)} | `0` | Reserved |

**Column and stream descriptors: 0x{header_start + 64:03x}–0x{header_start + fields[8] - 1:03x}**

| Descriptor range | Column | Contents |
| --- | --- | --- |
| {byte_range(header_start + 64, 32)} | `event` | plain layout, 0 nulls, {ROW_COUNT} values, streams 0–1 |
| {byte_range(header_start + 96, 32)} | `value` | plain layout, 0 nulls, {ROW_COUNT} values, stream 2 |
| {byte_range(header_start + 128, 48)} | `event` values | raw, uncompressed, offset {event_values_stream[4]}, {event_values_stream[5]} stored bytes, CRC32C `0x{event_values_stream[8]:08x}` |
| {byte_range(header_start + 176, 48)} | `event` lengths | raw, uncompressed, offset {event_lengths_stream[4]}, {event_lengths_stream[5]} stored bytes, CRC32C `0x{event_lengths_stream[8]:08x}` |
| {byte_range(header_start + 224, 48)} | `value` data | raw, uncompressed, offset {value_stream[4]}, {value_stream[5]} stored bytes, CRC32C `0x{value_stream[8]:08x}` |

**Stream payload: 0x{payload_start:03x}–0x{trailer_start - 1:03x}**

The `event` column has a concatenated UTF-8 values stream and a separate
`uint32` lengths stream. Every physical stream begins at an eight-byte-aligned
payload offset.

#### Event values

| Offset | Stored bytes | Decoded `utf8` |
| --- | --- | --- |
{chr(10).join(event_lines)}

#### Event lengths

| Offset | Stored bytes | Decoded `uint32` |
| --- | --- | ---: |
{chr(10).join(length_lines)}

#### Value data (8‑byte aligned)

| Offset | Stored bytes | Decoded `int64` |
| --- | ---: | ---: |
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

1. When the primary timestamp column ID is `{PRIMARY_TIMESTAMP_ID}`, the
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
uv run spec/v0.2/fixtures/no_primary/no_primary.py \\
  --output spec/v0.2/fixtures/no_primary/no_primary.acta \\
  --markdown-output spec/v0.2/fixtures/no_primary/no_primary.md
```

Expected SHA‑256:

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
        output_name = args.output.name if args.output else "no_primary.acta"
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(render_markdown(fixture, output_name))
        print(f"wrote {args.markdown_output}")
    if not args.output and not args.markdown_output:
        print(
            f"Acta v0.2 no-primary fixture passed: {len(fixture)} bytes, "
            f"sha256={digest}"
        )


if __name__ == "__main__":
    main()
