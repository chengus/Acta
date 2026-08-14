#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["google-crc32c>=1.6"]
# ///
"""Generate and validate an IANA-timezone Acta v0.2 fixture.

The timezone name is the only variable-length type-parameter record in v0.2.
This fixture pins its encoding: the record is padded to the next eight-byte
boundary and the stored type-parameter length includes that padding.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import struct
import sys
from datetime import datetime, timezone as utc_timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROBE_PATH = SCRIPT_DIR.parent.parent / "format_probe.py"
PROBE_SPEC = importlib.util.spec_from_file_location("acta_format_probe", PROBE_PATH)
assert PROBE_SPEC and PROBE_SPEC.loader
probe = importlib.util.module_from_spec(PROBE_SPEC)
sys.modules[PROBE_SPEC.name] = probe
PROBE_SPEC.loader.exec_module(probe)

FILE_ID = b"ACTA-TIMEZONE!!!"
SCHEMA_ID = 1
PRIMARY_TIMESTAMP_ID = 1
TIMESTAMP64_TYPE_ID = 13
INT64_TYPE_ID = 5
TS_SORTED = 0x2
UINT64_MAX = (1 << 64) - 1

MILLISECOND_UNIT = 1
IANA_TIMEZONE_MODE = 2
# Thirteen bytes, so the parameter record needs three bytes of padding. A name
# whose length were already a multiple of eight would not exercise the rule.
TIMEZONE_NAME = b"Europe/Berlin"

INSTANTS = (
    datetime(2026, 1, 2, 8, 0, 0, tzinfo=utc_timezone.utc),
    datetime(2026, 1, 2, 8, 30, 0, tzinfo=utc_timezone.utc),
    datetime(2026, 1, 2, 9, 0, 0, tzinfo=utc_timezone.utc),
)
TIMESTAMPS = tuple(int(instant.timestamp()) * 1000 for instant in INSTANTS)
VALUES = (101, 102, 103)
ROW_COUNT = len(TIMESTAMPS)


def timestamp_parameters(
    unit: int = MILLISECOND_UNIT,
    timezone_mode: int = IANA_TIMEZONE_MODE,
    timezone_name: bytes = TIMEZONE_NAME,
    pad: bool = True,
) -> bytes:
    """Section 7: fixed prefix, timezone name, then padding to eight bytes."""
    record = struct.pack("<BBHI", unit, timezone_mode, 0, len(timezone_name))
    record += timezone_name
    return probe.pad8(record) if pad else record


def schema_descriptor(column_id: int, type_id: int, name: bytes, parameters: bytes) -> bytes:
    unpadded = probe.COLUMN_SCHEMA.size + len(name) + len(parameters)
    descriptor_length = unpadded + (-unpadded) % 8
    return probe.pad8(
        probe.COLUMN_SCHEMA.pack(
            descriptor_length, column_id, type_id, 0, len(name), len(parameters), 0
        )
        + name
        + parameters
    )


def build_fixture(
    parameters: bytes | None = None,
    block_flags: int = TS_SORTED,
    ts_min: int | None = None,
    ts_max: int | None = None,
) -> bytes:
    parameters = timestamp_parameters() if parameters is None else parameters
    ts_min = TIMESTAMPS[0] if ts_min is None else ts_min
    ts_max = TIMESTAMPS[-1] if ts_max is None else ts_max

    schema_payload = schema_descriptor(
        1, TIMESTAMP64_TYPE_ID, b"time", parameters
    ) + schema_descriptor(2, INT64_TYPE_ID, b"value", b"")
    schema_header = probe.SCHEMA_HEADER.pack(SCHEMA_ID, 2, PRIMARY_TIMESTAMP_ID, 0, 0)
    schema_frame = probe.make_frame(1, 0, schema_header, schema_payload)

    time_bytes = struct.pack(f"<{ROW_COUNT}q", *TIMESTAMPS)
    value_bytes = struct.pack(f"<{ROW_COUNT}q", *VALUES)
    value_offset = len(probe.pad8(time_bytes))
    payload = probe.pad8(time_bytes) + value_bytes

    column_table_offset = probe.BLOCK_HEADER.size
    stream_table_offset = column_table_offset + 2 * probe.COLUMN_DESCRIPTOR.size
    statistics_offset = stream_table_offset + 2 * probe.STREAM_DESCRIPTOR.size
    data_header = (
        probe.BLOCK_HEADER.pack(
            SCHEMA_ID,
            UINT64_MAX,
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
        + probe.COLUMN_DESCRIPTOR.pack(1, 0, 0, 0, ROW_COUNT, 0, 1, 0, 0, 0)
        + probe.COLUMN_DESCRIPTOR.pack(2, 0, 0, 0, ROW_COUNT, 1, 1, 0, 0, 0)
        + probe.STREAM_DESCRIPTOR.pack(
            2, 0, 0, 0, 0, len(time_bytes), len(time_bytes), ROW_COUNT,
            probe.crc32c(time_bytes), 0,
        )
        + probe.STREAM_DESCRIPTOR.pack(
            2, 0, 0, 0, value_offset, len(value_bytes), len(value_bytes), ROW_COUNT,
            probe.crc32c(value_bytes), 0,
        )
    )
    data_frame = probe.make_frame(2, 1, data_header, payload)
    return probe.make_prologue(FILE_ID) + schema_frame + data_frame


def decode(data: bytes) -> tuple[bytes, tuple[int, ...], tuple[int, ...], tuple[object, ...]]:
    """Return the timezone name, timestamps, values, and block-header fields."""
    result = probe.scan_frames(data)
    if result.incomplete_tail or len(result.frames) != 2:
        raise AssertionError("fixture does not contain two complete frames")
    schema, block = result.frames

    descriptor = probe.COLUMN_SCHEMA.unpack_from(schema.payload)
    descriptor_length, column_id, type_id = descriptor[0], descriptor[1], descriptor[2]
    name_length, parameters_length = descriptor[4], descriptor[5]
    if (column_id, type_id) != (PRIMARY_TIMESTAMP_ID, TIMESTAMP64_TYPE_ID):
        raise probe.Corruption("primary column is not a timestamp64 column")

    start = probe.COLUMN_SCHEMA.size + name_length
    parameters = schema.payload[start : start + parameters_length]
    if start + parameters_length > descriptor_length:
        raise probe.Corruption("type parameters exceed their descriptor")
    if parameters_length % 8 != 0:
        raise probe.Corruption("type-parameter length is not padded to eight bytes")

    unit, timezone_mode, _reserved, timezone_name_length = struct.unpack_from(
        "<BBHI", parameters
    )
    if unit != MILLISECOND_UNIT:
        raise probe.Corruption("unexpected timestamp unit")
    if timezone_mode != IANA_TIMEZONE_MODE or timezone_name_length == 0:
        raise probe.Corruption("IANA mode requires a nonempty timezone name")
    expected_length = 8 + timezone_name_length + (-(8 + timezone_name_length)) % 8
    if expected_length != parameters_length:
        raise probe.Corruption("type-parameter length does not include its padding")
    timezone_name = parameters[8 : 8 + timezone_name_length]
    if parameters[8 + timezone_name_length :] != bytes(
        parameters_length - 8 - timezone_name_length
    ):
        raise probe.Corruption("type-parameter padding is not zero")

    fields = probe.BLOCK_HEADER.unpack_from(block.header)
    stream_table = fields[7]
    streams = [
        probe.STREAM_DESCRIPTOR.unpack_from(
            block.header, stream_table + index * probe.STREAM_DESCRIPTOR.size
        )
        for index in range(2)
    ]
    decoded = []
    for stream in streams:
        stored = block.payload[stream[4] : stream[4] + stream[5]]
        if probe.crc32c(stored) != stream[8]:
            raise probe.Corruption("bad stream CRC32C")
        decoded.append(struct.unpack(f"<{stream[7]}q", stored))

    return timezone_name, decoded[0], decoded[1], fields


def validate_fixture(data: bytes) -> None:
    timezone_name, timestamps, values, fields = decode(data)
    if timezone_name != TIMEZONE_NAME:
        raise probe.Corruption("decoded timezone name does not match expected")
    if timestamps != TIMESTAMPS or values != VALUES:
        raise probe.Corruption("decoded values do not match expected")

    ts_min, ts_max, flags = fields[4], fields[5], fields[10]
    if flags & TS_SORTED:
        if list(timestamps) != sorted(timestamps):
            raise probe.Corruption("TS_SORTED declared over unsorted timestamps")
        if ts_min != timestamps[0] or ts_max != timestamps[-1]:
            raise probe.Corruption("TS_SORTED bounds are not the first and last values")


def self_test() -> bytes:
    fixture = build_fixture()
    validate_fixture(fixture)

    # An unpadded parameter record is not a v0.2 record.
    unpadded = build_fixture(parameters=timestamp_parameters(pad=False))
    try:
        validate_fixture(unpadded)
    except probe.Corruption:
        pass
    else:
        raise AssertionError("an unpadded type-parameter record was not rejected")

    # IANA mode requires a name; an empty one must not be accepted.
    empty_name = build_fixture(parameters=timestamp_parameters(timezone_name=b""))
    try:
        validate_fixture(empty_name)
    except probe.Corruption:
        pass
    else:
        raise AssertionError("IANA mode with no timezone name was not rejected")

    # Section 8 ties TS_SORTED to the first and last stored values.
    try:
        validate_fixture(build_fixture(ts_min=TIMESTAMPS[1]))
    except probe.Corruption:
        pass
    else:
        raise AssertionError("a wrong TS_SORTED minimum was not rejected")

    # Every truncation inside the data frame leaves the schema frame readable.
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

    # Corruption anywhere CRC-covered must be detected.
    timezone_offset = (
        64 + probe.PREFIX.size + probe.SCHEMA_HEADER.size + probe.COLUMN_SCHEMA.size + 4 + 8
    )
    payload_offset = data_frame_start + probe.PREFIX.size + len(result.frames[-1].header)
    for offset in (timezone_offset, payload_offset):
        damaged = bytearray(fixture)
        damaged[offset] ^= 0x02
        try:
            probe.scan_frames(bytes(damaged))
        except probe.Corruption:
            pass
        else:
            raise AssertionError(f"corruption at byte {offset} was not detected")

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


def render_markdown(fixture: bytes, output_name: str) -> str:
    result = probe.scan_frames(fixture)
    schema, block = result.frames
    descriptor = probe.COLUMN_SCHEMA.unpack_from(schema.payload)
    parameters_length = descriptor[5]
    descriptor_start = 64 + probe.PREFIX.size + probe.SCHEMA_HEADER.size
    parameters_start = descriptor_start + probe.COLUMN_SCHEMA.size + descriptor[4]
    fields = probe.BLOCK_HEADER.unpack_from(block.header)
    digest = hashlib.sha256(fixture).hexdigest()
    padding = parameters_length - 8 - len(TIMEZONE_NAME)

    rows = "\n".join(
        f"| {index} | `{TIMESTAMPS[index]}` | {INSTANTS[index].isoformat()} | `{VALUES[index]}` |"
        for index in range(ROW_COUNT)
    )

    return f"""# IANA timezone Acta v0.2 fixture

This document annotates [`{output_name}`]({output_name}), the compatibility
fixture for the only variable-length type-parameter record in v0.2: the
`timestamp64` timezone name defined in [`format_v0.2.md`](../../format_v0.2.md)
section 7.

The file has a non-nullable `timestamp64` primary column named `time` with
millisecond unit and IANA timezone `{TIMEZONE_NAME.decode()}`, an `int64` column
named `value`, and one three-row data block with `TS_SORTED` set. Row IDs are
disabled.

Logical contents:

| Row | `time` (`timestamp64[ms, {TIMEZONE_NAME.decode()}]`) | UTC instant | `value` (int64) |
| ---: | ---: | --- | ---: |
{rows}

Section 3.1 stores zoned values as UTC epoch counts, so the timezone name is
descriptive schema metadata and does not change the stored count.

## The type-parameter record

The record begins at `0x{parameters_start:03x}` and is `{parameters_length}`
bytes long, which is what the descriptor's type-parameter length field stores:

| Offset | Size | Value | Meaning |
| --- | ---: | --- | --- |
| `0x{parameters_start:03x}` | 1 | `{MILLISECOND_UNIT}` | Unit: millisecond |
| `0x{parameters_start + 1:03x}` | 1 | `{IANA_TIMEZONE_MODE}` | Timezone mode: IANA name |
| `0x{parameters_start + 2:03x}` | 2 | `0` | Reserved |
| `0x{parameters_start + 4:03x}` | 4 | `{len(TIMEZONE_NAME)}` | Timezone name length |
| `0x{parameters_start + 8:03x}` | {len(TIMEZONE_NAME)} | `{TIMEZONE_NAME.decode()}` | Timezone name |
| `0x{parameters_start + 8 + len(TIMEZONE_NAME):03x}` | {padding} | zero | Padding to eight bytes |

The name is {len(TIMEZONE_NAME)} bytes, so the record carries {padding} bytes of
padding and the stored type-parameter length is
`8 + {len(TIMEZONE_NAME)}` rounded up to `{parameters_length}`. A reader that
expected `{8 + len(TIMEZONE_NAME)}` here would reject this file, and a writer
that stored `{8 + len(TIMEZONE_NAME)}` would produce one this fixture rejects.
Descriptor padding is separate and follows the parameters.

## Hex dump

```text
{hexdump(fixture)}
```

Total: `0x{len(fixture):x} = {len(fixture)}` bytes, with file ID
`{FILE_ID.decode()}`. Layout conventions follow the
[minimal fixture](../minimal/minimal.md).

The block header declares row count {fields[2]}, base row ID `UINT64_MAX`,
`ts_min={fields[4]}`, `ts_max={fields[5]}`, and block flags
`0x{fields[10]:x}` (`TS_SORTED`).

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
uv run spec/v0.2/fixtures/timezone/timezone.py \\
  --output spec/v0.2/fixtures/timezone/timezone.acta \\
  --markdown-output spec/v0.2/fixtures/timezone/timezone.md
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
        output_name = args.output.name if args.output else "timezone.acta"
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(render_markdown(fixture, output_name))
        print(f"wrote {args.markdown_output}")
    if not args.output and not args.markdown_output:
        print(f"Acta v0.2 timezone fixture passed: {len(fixture)} bytes, sha256={digest}")


if __name__ == "__main__":
    main()
