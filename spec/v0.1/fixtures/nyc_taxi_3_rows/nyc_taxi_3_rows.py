#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["google-crc32c>=1.6"]
# ///
"""Generate and validate the three-row NYC taxi Acta v0.1 fixture."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import struct
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable


SCRIPT_DIR = Path(__file__).resolve().parent
PROBE_PATH = SCRIPT_DIR.parent / "format_probe.py"
PROBE_SPEC = importlib.util.spec_from_file_location("acta_format_probe", PROBE_PATH)
assert PROBE_SPEC and PROBE_SPEC.loader
probe = importlib.util.module_from_spec(PROBE_SPEC)
sys.modules[PROBE_SPEC.name] = probe
PROBE_SPEC.loader.exec_module(probe)

SOURCE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
)
SOURCE_SHA256 = "8b3933fe6f0d7b6d8826613c0dd724edc680ff7c49e2bd4c7635c05102728637"
BENCHMARK_RESULT = "benchmarks/0_taxi_data/results/nyc_taxi_65536.json"
FILE_ID = b"ACTA-TAXI-3ROWS!"
SCHEMA_ID = 1
ROW_COUNT = 3
PRIMARY_TIMESTAMP_ID = 7


@dataclass(frozen=True)
class ColumnSpec:
    column_id: int
    name: str
    type_id: int
    logical_type: str
    values: tuple[object, ...]
    nullable: bool = False
    validity: tuple[bool, ...] | None = None
    type_parameters: bytes = b""
    parameter_description: str = "none"


@dataclass(frozen=True)
class Stream:
    index: int
    column_id: int
    column_name: str
    kind: int
    kind_name: str
    payload_offset: int
    stored: bytes
    element_count: int


@dataclass(frozen=True)
class ColumnBlock:
    column: ColumnSpec
    flags: int
    null_count: int
    dense_count: int
    first_stream: int
    stream_count: int


@dataclass(frozen=True)
class SchemaEntry:
    column: ColumnSpec
    payload_offset: int
    descriptor: bytes


@dataclass(frozen=True)
class FixtureBuild:
    data: bytes
    schema_frame: bytes
    data_frame: bytes
    schema_entries: tuple[SchemaEntry, ...]
    column_blocks: tuple[ColumnBlock, ...]
    streams: tuple[Stream, ...]
    data_header_length: int
    data_payload_length: int


def decimal_parameters(precision: int, scale: int) -> bytes:
    return struct.pack("<HhI", precision, scale, 0)


def timestamp_parameters() -> bytes:
    # The benchmark converts Arrow's timezone-free timestamps to datetime64[us].
    return struct.pack("<BBHI", 2, 0, 0, 0)


def categorical_parameters() -> bytes:
    return struct.pack("<B7s", 0, bytes(7))


def fixed_binary_parameters(width: int) -> bytes:
    return struct.pack("<II", width, 0)


COLUMNS = (
    ColumnSpec(1, "stored_and_forwarded", 1, "bool", (False, False, False)),
    ColumnSpec(2, "zone_delta", 4, "int32", (-1, -1, 194)),
    ColumnSpec(3, "pickup_zone", 8, "uint32", (239, 163, 43)),
    ColumnSpec(
        4,
        "passenger_count",
        5,
        "int64",
        (1, 0, 0),
        nullable=True,
        validity=(True, True, True),
    ),
    ColumnSpec(5, "trip_distance", 11, "float64", (0.97, 0.9, 1.4)),
    ColumnSpec(
        6,
        "fare_cents",
        12,
        "decimal64(18,2)",
        (720, 790, 1070),
        type_parameters=decimal_parameters(18, 2),
        parameter_description="precision=18, scale=2",
    ),
    ColumnSpec(
        7,
        "pickup_time",
        13,
        "timestamp64[us, naive]",
        (1767228844000000, 1767227644000000, 1767229026000000),
        type_parameters=timestamp_parameters(),
        parameter_description="unit=microsecond, timezone=naive",
    ),
    ColumnSpec(
        8,
        "route",
        14,
        "utf8",
        (
            b"zone:239->zone:238",
            b"zone:163->zone:162",
            b"zone:43->zone:237",
        ),
    ),
    ColumnSpec(
        9,
        "payment",
        15,
        "categorical",
        (b"credit_card", b"cash", b"credit_card"),
        type_parameters=categorical_parameters(),
        parameter_description="ordered=false",
    ),
    ColumnSpec(
        10,
        "route_key",
        16,
        "binary",
        (
            bytes.fromhex("ef000000ee00000001"),
            bytes.fromhex("a3000000a200000002"),
            bytes.fromhex("2b000000ed00000001"),
        ),
    ),
    ColumnSpec(
        11,
        "trip_id",
        17,
        "fixed_binary[16]",
        (
            bytes.fromhex("4ff64384c5e171e154cd3f6a63d9bdd6"),
            bytes.fromhex("d8334a1b4071932f75b5bc0b2948113f"),
            bytes.fromhex("4d97ac7fc5767286b2afeb64d7397dd6"),
        ),
        type_parameters=fixed_binary_parameters(16),
        parameter_description="width=16",
    ),
    ColumnSpec(12, "passenger_validity", 1, "bool", (True, True, True)),
    ColumnSpec(
        13,
        "regular_sensor_time",
        13,
        "timestamp64[us, naive]",
        (1700000002000000, 1700000003000000, 1700000004000000),
        type_parameters=timestamp_parameters(),
        parameter_description="unit=microsecond, timezone=naive",
    ),
    ColumnSpec(14, "monotonic_counter", 9, "uint64", (2, 3, 4)),
    ColumnSpec(
        15,
        "smooth_sensor",
        11,
        "float64",
        (0.013989386228953918, 0.10332694225775996, 0.18895417315694527),
    ),
    ColumnSpec(16, "sparse_validity", 1, "bool", (False, True, True)),
)


def pack_bits(values: Iterable[bool]) -> bytes:
    result = 0
    count = 0
    output = bytearray()
    for count, value in enumerate(values, start=1):
        if value:
            result |= 1 << ((count - 1) % 8)
        if count % 8 == 0:
            output.append(result)
            result = 0
    if count and count % 8:
        output.append(result)
    return bytes(output)


def raw_value_streams(
    column: ColumnSpec, values: tuple[object, ...]
) -> list[tuple[int, str, bytes, int]]:
    count = len(values)
    if column.type_id == 1:
        return [(2, "values", pack_bits(bool(value) for value in values), count)]
    formats = {
        4: "i",
        5: "q",
        8: "I",
        9: "Q",
        11: "d",
        12: "q",
        13: "q",
    }
    if column.type_id in formats:
        stored = struct.pack(f"<{count}{formats[column.type_id]}", *values)
        return [(2, "values", stored, count)]
    if column.type_id in {14, 15, 16}:
        byte_values = tuple(bytes(value) for value in values)
        stored_values = b"".join(byte_values)
        lengths = struct.pack(f"<{count}I", *(len(value) for value in byte_values))
        return [
            (2, "values", stored_values, count),
            (3, "lengths", lengths, count),
        ]
    if column.type_id == 17:
        width = struct.unpack_from("<I", column.type_parameters)[0]
        byte_values = tuple(bytes(value) for value in values)
        if any(len(value) != width for value in byte_values):
            raise ValueError(f"{column.name} contains a value with the wrong width")
        return [(2, "values", b"".join(byte_values), count)]
    raise ValueError(f"unsupported fixture type for {column.name}")


def schema_descriptor(column: ColumnSpec) -> bytes:
    name = column.name.encode()
    flags = 1 if column.nullable else 0
    unpadded_length = probe.COLUMN_SCHEMA.size + len(name) + len(column.type_parameters)
    descriptor_length = unpadded_length + (-unpadded_length) % 8
    descriptor = probe.COLUMN_SCHEMA.pack(
        descriptor_length,
        column.column_id,
        column.type_id,
        flags,
        len(name),
        len(column.type_parameters),
        0,
    )
    return probe.pad8(descriptor + name + column.type_parameters)


def build_fixture() -> FixtureBuild:
    schema_payload = bytearray()
    schema_entries: list[SchemaEntry] = []
    for column in COLUMNS:
        descriptor = schema_descriptor(column)
        schema_entries.append(SchemaEntry(column, len(schema_payload), descriptor))
        schema_payload.extend(descriptor)
    schema_header = probe.SCHEMA_HEADER.pack(
        SCHEMA_ID, len(COLUMNS), PRIMARY_TIMESTAMP_ID, 0, 0
    )
    schema_frame = probe.make_frame(1, 0, schema_header, bytes(schema_payload))

    pending_streams: list[tuple[int, str, int, str, bytes, int]] = []
    column_blocks: list[ColumnBlock] = []
    for column in COLUMNS:
        validity = column.validity or (True,) * ROW_COUNT
        if len(column.values) != ROW_COUNT or len(validity) != ROW_COUNT:
            raise ValueError(f"{column.name} does not contain exactly three rows")
        null_count = sum(not valid for valid in validity)
        dense_values = tuple(
            value for value, valid in zip(column.values, validity) if valid
        )
        flags = 0
        first_stream = len(pending_streams)
        if column.nullable and null_count in {0, ROW_COUNT}:
            flags |= 1  # implicit all-valid or all-null validity
        elif column.nullable:
            pending_streams.append(
                (
                    column.column_id,
                    column.name,
                    1,
                    "validity",
                    pack_bits(validity),
                    ROW_COUNT,
                )
            )
        if dense_values:
            for kind, kind_name, stored, element_count in raw_value_streams(
                column, dense_values
            ):
                pending_streams.append(
                    (
                        column.column_id,
                        column.name,
                        kind,
                        kind_name,
                        stored,
                        element_count,
                    )
                )
        column_blocks.append(
            ColumnBlock(
                column,
                flags,
                null_count,
                len(dense_values),
                first_stream,
                len(pending_streams) - first_stream,
            )
        )

    payload = bytearray()
    streams: list[Stream] = []
    for index, (
        column_id,
        column_name,
        kind,
        kind_name,
        stored,
        element_count,
    ) in enumerate(pending_streams):
        if len(payload) % 8:
            raise AssertionError("stream payload lost eight-byte alignment")
        streams.append(
            Stream(
                index,
                column_id,
                column_name,
                kind,
                kind_name,
                len(payload),
                stored,
                element_count,
            )
        )
        payload.extend(stored)
        payload.extend(bytes((-len(payload)) % 8))

    column_table_offset = probe.BLOCK_HEADER.size
    stream_table_offset = column_table_offset + probe.COLUMN_DESCRIPTOR.size * len(
        COLUMNS
    )
    statistics_offset = stream_table_offset + probe.STREAM_DESCRIPTOR.size * len(
        streams
    )
    pickup_time = COLUMNS[PRIMARY_TIMESTAMP_ID - 1].values
    block_header = probe.BLOCK_HEADER.pack(
        SCHEMA_ID,
        0,
        ROW_COUNT,
        len(COLUMNS),
        min(pickup_time),
        max(pickup_time),
        column_table_offset,
        stream_table_offset,
        statistics_offset,
        0,
        1,
        0,
    )
    column_table = b"".join(
        probe.COLUMN_DESCRIPTOR.pack(
            block.column.column_id,
            0,
            block.flags,
            block.null_count,
            block.dense_count,
            block.first_stream,
            block.stream_count,
            0,
            0,
            0,
        )
        for block in column_blocks
    )
    stream_table = b"".join(
        probe.STREAM_DESCRIPTOR.pack(
            stream.kind,
            0,
            0,
            0,
            stream.payload_offset,
            len(stream.stored),
            len(stream.stored),
            stream.element_count,
            probe.crc32c(stream.stored),
            0,
        )
        for stream in streams
    )
    data_header = block_header + column_table + stream_table
    if len(data_header) != statistics_offset or len(data_header) % 8:
        raise AssertionError("invalid data-frame header layout")
    data_frame = probe.make_frame(2, 1, data_header, bytes(payload))
    data = probe.make_prologue(FILE_ID, feature_flags=1) + schema_frame + data_frame
    build = FixtureBuild(
        data,
        schema_frame,
        data_frame,
        tuple(schema_entries),
        tuple(column_blocks),
        tuple(streams),
        len(data_header),
        len(payload),
    )
    validate_fixture(build)
    return build


def unpack_bits(stored: bytes, count: int) -> tuple[bool, ...]:
    return tuple(
        bool(stored[index // 8] & (1 << (index % 8))) for index in range(count)
    )


def decode_dense(
    column: ColumnSpec, streams: dict[int, tuple[bytes, int]], count: int
) -> tuple[object, ...]:
    if count == 0:
        return ()
    values, value_count = streams[2]
    if value_count != count:
        raise AssertionError(f"bad value count for {column.name}")
    if column.type_id == 1:
        return unpack_bits(values, count)
    formats = {4: "i", 5: "q", 8: "I", 9: "Q", 11: "d", 12: "q", 13: "q"}
    if column.type_id in formats:
        return tuple(struct.unpack(f"<{count}{formats[column.type_id]}", values))
    if column.type_id in {14, 15, 16}:
        lengths, length_count = streams[3]
        if length_count != count:
            raise AssertionError(f"bad length count for {column.name}")
        decoded_lengths = struct.unpack(f"<{count}I", lengths)
        decoded: list[bytes] = []
        offset = 0
        for length in decoded_lengths:
            decoded.append(values[offset : offset + length])
            offset += length
        if offset != len(values):
            raise AssertionError(f"bad variable-width payload for {column.name}")
        return tuple(decoded)
    if column.type_id == 17:
        width = struct.unpack_from("<I", column.type_parameters)[0]
        return tuple(
            values[index : index + width] for index in range(0, len(values), width)
        )
    raise AssertionError(f"unsupported fixture type for {column.name}")


def values_equal(left: object, right: object) -> bool:
    if isinstance(left, float) and isinstance(right, float):
        return struct.pack("<d", left) == struct.pack("<d", right)
    return left == right


def validate_fixture(build: FixtureBuild) -> None:
    result = probe.scan_frames(build.data)
    if result.incomplete_tail or len(result.frames) != 2:
        raise AssertionError("fixture does not contain two complete frames")
    prologue = probe.PROLOGUE.unpack_from(build.data)
    if prologue[4] != 1 or prologue[5] != FILE_ID:
        raise AssertionError("fixture row-ID feature or file ID is wrong")
    schema, data = result.frames

    for cut in range(data.offset + 1, len(build.data)):
        truncated = probe.scan_frames(build.data[:cut])
        if (
            not truncated.incomplete_tail
            or len(truncated.frames) != 1
            or truncated.last_good_offset != data.offset
        ):
            raise AssertionError(f"unexpected truncation result at byte {cut}")
    if probe.SCHEMA_HEADER.unpack_from(schema.header)[:3] != (
        SCHEMA_ID,
        len(COLUMNS),
        PRIMARY_TIMESTAMP_ID,
    ):
        raise AssertionError("fixture schema header is wrong")

    schema_offset = 0
    for column in COLUMNS:
        fields = probe.COLUMN_SCHEMA.unpack_from(schema.payload, schema_offset)
        (
            descriptor_length,
            column_id,
            type_id,
            flags,
            name_length,
            parameter_length,
            reserved,
        ) = fields
        start = schema_offset + probe.COLUMN_SCHEMA.size
        name = schema.payload[start : start + name_length]
        parameters = schema.payload[
            start + name_length : start + name_length + parameter_length
        ]
        if (
            column_id != column.column_id
            or type_id != column.type_id
            or flags != (1 if column.nullable else 0)
            or reserved != 0
            or name != column.name.encode()
            or parameters != column.type_parameters
        ):
            raise AssertionError(f"bad schema descriptor for {column.name}")
        schema_offset += descriptor_length
    if schema_offset != len(schema.payload):
        raise AssertionError("schema descriptors do not consume the payload")

    block = probe.BLOCK_HEADER.unpack_from(data.header)
    if (
        block[0] != SCHEMA_ID
        or block[1] != 0
        or block[2] != ROW_COUNT
        or block[3] != len(COLUMNS)
        or block[10] != 1
    ):
        raise AssertionError("fixture block header or row-ID fields are wrong")
    if block[4:6] != (
        min(COLUMNS[PRIMARY_TIMESTAMP_ID - 1].values),
        max(COLUMNS[PRIMARY_TIMESTAMP_ID - 1].values),
    ):
        raise AssertionError("fixture primary timestamp bounds are wrong")

    column_table_offset, stream_table_offset = block[6:8]
    for expected in build.column_blocks:
        descriptor_offset = (
            column_table_offset
            + (expected.column.column_id - 1) * probe.COLUMN_DESCRIPTOR.size
        )
        fields = probe.COLUMN_DESCRIPTOR.unpack_from(data.header, descriptor_offset)
        if fields != (
            expected.column.column_id,
            0,
            expected.flags,
            expected.null_count,
            expected.dense_count,
            expected.first_stream,
            expected.stream_count,
            0,
            0,
            0,
        ):
            raise AssertionError(f"bad block descriptor for {expected.column.name}")

        stream_values: dict[int, tuple[bytes, int]] = {}
        for stream_index in range(
            expected.first_stream, expected.first_stream + expected.stream_count
        ):
            stream = build.streams[stream_index]
            stream_offset = (
                stream_table_offset + stream_index * probe.STREAM_DESCRIPTOR.size
            )
            stream_fields = probe.STREAM_DESCRIPTOR.unpack_from(
                data.header, stream_offset
            )
            stored = data.payload[
                stream_fields[4] : stream_fields[4] + stream_fields[5]
            ]
            if (
                stream_fields[0] != stream.kind
                or stream_fields[1:4] != (0, 0, 0)
                or stream_fields[5] != len(stream.stored)
                or stream_fields[6] != len(stream.stored)
                or stream_fields[7] != stream.element_count
                or stream_fields[8] != probe.crc32c(stored)
                or stored != stream.stored
            ):
                raise AssertionError(
                    f"bad stream {stream.index} for {stream.column_name}"
                )
            stream_values[stream.kind] = (stored, stream.element_count)

        if expected.column.nullable:
            if expected.flags & 1:
                validity = (expected.null_count == 0,) * ROW_COUNT
            else:
                validity_bytes, validity_count = stream_values[1]
                validity = unpack_bits(validity_bytes, validity_count)
        else:
            validity = (True,) * ROW_COUNT
        dense = decode_dense(expected.column, stream_values, expected.dense_count)
        dense_index = 0
        decoded: list[object | None] = []
        for valid in validity:
            if valid:
                decoded.append(dense[dense_index])
                dense_index += 1
            else:
                decoded.append(None)
        expected_values = [
            value if valid else None
            for value, valid in zip(expected.column.values, validity)
        ]
        if len(decoded) != len(expected_values) or any(
            not values_equal(left, right)
            for left, right in zip(decoded, expected_values)
        ):
            raise AssertionError(
                f"logical round-trip failed for {expected.column.name}"
            )


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


def timestamp_text(value: int) -> str:
    instant = datetime(1970, 1, 1) + timedelta(microseconds=value)
    return f"{value} (`{instant.isoformat(timespec='microseconds')}`)"


def logical_value(column: ColumnSpec, value: object, row: int) -> str:
    if column.validity is not None and not column.validity[row]:
        return "`NULL`"
    if column.type_id == 1:
        return "`true`" if value else "`false`"
    if column.type_id == 12:
        return f"{int(value)} (`{int(value) / 100:.2f}`)"
    if column.type_id == 13:
        return timestamp_text(int(value))
    if column.type_id in {14, 15}:
        return f"`{bytes(value).decode()}`"
    if column.type_id in {16, 17}:
        return f"`0x{bytes(value).hex()}`"
    if column.type_id == 11:
        return f"`{float(value)!r}`"
    return f"`{value}`"


def byte_range(start: int, length: int) -> str:
    return f"`0x{start:04x}–0x{start + length - 1:04x}`"


def prefix_fields(data: bytes, offset: int) -> tuple[object, ...]:
    return probe.PREFIX.unpack_from(data, offset)


def trailer_fields(data: bytes, offset: int) -> tuple[object, ...]:
    return probe.TRAILER.unpack_from(data, offset)


def render_markdown(build: FixtureBuild, output_name: str) -> str:
    schema_start = probe.PROLOGUE.size
    schema_header_start = schema_start + probe.PREFIX.size
    schema_payload_start = schema_header_start + probe.SCHEMA_HEADER.size
    schema_trailer_start = schema_start + len(build.schema_frame) - probe.TRAILER.size
    data_start = schema_start + len(build.schema_frame)
    data_header_start = data_start + probe.PREFIX.size
    data_payload_start = data_header_start + build.data_header_length
    data_trailer_start = data_start + len(build.data_frame) - probe.TRAILER.size
    schema_prefix = prefix_fields(build.data, schema_start)
    data_prefix = prefix_fields(build.data, data_start)
    schema_trailer = trailer_fields(build.data, schema_trailer_start)
    data_trailer = trailer_fields(build.data, data_trailer_start)
    prologue = probe.PROLOGUE.unpack_from(build.data)
    digest = hashlib.sha256(build.data).hexdigest()

    lines = [
        "# NYC taxi three-row Acta v0.1 fixture",
        "",
        f"This document annotates every region of [`{output_name}`]({output_name}).",
        "The fixture captures the first three rows of the first sample in",
        f"[`nyc_taxi_65536.json`](../../../{BENCHMARK_RESULT}). That benchmark",
        "uses sample offset 0 for its first 65,536-row block.",
        "",
        f"- Source: `{SOURCE_URL}`",
        f"- Source SHA-256: `{SOURCE_SHA256}`",
        "- Fixture rows: source rows 0, 1, and 2",
        "- Fixture row IDs: 0, 1, and 2",
        "- Columns: all 16 real, derived, validity, and controlled benchmark cases",
        "- Physical representation: plain layout, raw transforms, no compression",
        "",
        "Raw, uncompressed streams keep a three-row compatibility fixture auditable.",
        "They do not reproduce the encodings selected from the complete 65,536-row",
        "blocks, where transform and compression overheads have different trade-offs.",
        "",
        "## Logical rows by column",
        "",
        "Row IDs are implicit: `row_id = base_row_id + row_offset`, with base row ID 0.",
        "The timestamp schemas are timezone-naive because the benchmark converts the",
        "source and controlled values to timezone-free `datetime64[us]` arrays.",
        "",
        "| ID | Column | Logical type | Row ID 0 | Row ID 1 | Row ID 2 |",
        "| ---: | --- | --- | --- | --- | --- |",
    ]
    for column in COLUMNS:
        values = [
            logical_value(column, value, row) for row, value in enumerate(column.values)
        ]
        lines.append(
            f"| {column.column_id} | `{column.name}` | `{column.logical_type}` | "
            + " | ".join(values)
            + " |"
        )

    lines.extend(
        [
            "",
            "## File map",
            "",
            "| Absolute range | Length | Region |",
            "| --- | ---: | --- |",
            f"| {byte_range(0, probe.PROLOGUE.size)} | 64 | File prologue |",
            f"| {byte_range(schema_start, len(build.schema_frame))} | {len(build.schema_frame)} | Schema frame, sequence 0 |",
            f"| {byte_range(data_start, len(build.data_frame))} | {len(build.data_frame)} | Data frame, sequence 1 |",
            "",
            f"Total fixture length: `{len(build.data):,}` bytes (`0x{len(build.data):x}`).",
            "",
            "## Hex dump",
            "",
            "`xxd`-style two-byte groups are shown below. Multi-byte fields decode",
            "little-endian.",
            "",
            "```text",
            hexdump(build.data),
            "```",
            "",
            "## File prologue",
            "",
            "| Absolute range | Decoded value | Meaning |",
            "| --- | --- | --- |",
            f"| {byte_range(0, 8)} | `ACTA\\r\\n\\x1a\\n` | File magic |",
            f"| {byte_range(8, 2)} | {prologue[1]} | Format major |",
            f"| {byte_range(10, 2)} | {prologue[2]} | Format minor |",
            f"| {byte_range(12, 4)} | {prologue[3]} | Prologue size |",
            f"| {byte_range(16, 8)} | `0x{prologue[4]:x}` | Feature flags; bit 0 enables row IDs |",
            f"| {byte_range(24, 16)} | `{prologue[5].hex()}` / `{prologue[5].decode()}` | File ID |",
            f"| {byte_range(40, 8)} | {prologue[6]} | Schema-frame offset |",
            f"| {byte_range(48, 12)} | all zero | Reserved |",
            f"| {byte_range(60, 4)} | `0x{prologue[8]:08x}` | Prologue CRC32C |",
            "",
            "The row-ID feature is enabled. The data block therefore carries base row",
            "ID 0, and its three row offsets produce IDs 0, 1, and 2 without a stream.",
            "",
            "## Schema frame",
            "",
            "### Prefix and header",
            "",
            "| Absolute range | Decoded value | Meaning |",
            "| --- | --- | --- |",
            f"| {byte_range(schema_start, 8)} | `ACTAFRM\\n` | Frame magic |",
            f"| {byte_range(schema_start + 8, 2)} | {schema_prefix[1]} | Frame type: schema |",
            f"| {byte_range(schema_start + 16, 4)} | {schema_prefix[4]} | Header length |",
            f"| {byte_range(schema_start + 24, 8)} | {schema_prefix[6]} | Payload length |",
            f"| {byte_range(schema_start + 32, 8)} | {schema_prefix[7]} | Sequence number |",
            f"| {byte_range(schema_start + 40, 4)} | `0x{schema_prefix[8]:08x}` | Header CRC32C |",
            f"| {byte_range(schema_start + 44, 4)} | `0x{schema_prefix[9]:08x}` | Prefix CRC32C |",
            f"| {byte_range(schema_header_start, 8)} | {SCHEMA_ID} | Schema ID |",
            f"| {byte_range(schema_header_start + 8, 4)} | {len(COLUMNS)} | Column count |",
            f"| {byte_range(schema_header_start + 12, 4)} | {PRIMARY_TIMESTAMP_ID} | Primary timestamp column ID (`pickup_time`) |",
            f"| {byte_range(schema_header_start + 16, 8)} | all zero | Schema flags and reserved |",
            "",
            "### Column schema descriptors",
            "",
            "Each descriptor consists of its fixed 24-byte prefix, UTF-8 name, type",
            "parameters, and zero padding to eight-byte alignment.",
            "",
            "| Absolute range | ID | Name | Type ID / logical type | Schema flags | Parameters |",
            "| --- | ---: | --- | --- | ---: | --- |",
        ]
    )
    for entry in build.schema_entries:
        column = entry.column
        absolute = schema_payload_start + entry.payload_offset
        lines.append(
            f"| {byte_range(absolute, len(entry.descriptor))} | {column.column_id} | "
            f"`{column.name}` | {column.type_id} / `{column.logical_type}` | "
            f"{1 if column.nullable else 0} | {column.parameter_description} |"
        )

    lines.extend(
        [
            "",
            "### Commit trailer",
            "",
            "| Absolute range | Decoded value | Meaning |",
            "| --- | --- | --- |",
            f"| {byte_range(schema_trailer_start, 8)} | {schema_trailer[0]} | Total frame length |",
            f"| {byte_range(schema_trailer_start + 8, 8)} | {schema_trailer[1]} | Repeated sequence number |",
            f"| {byte_range(schema_trailer_start + 16, 4)} | `0x{schema_trailer[2]:08x}` | Body CRC32C |",
            f"| {byte_range(schema_trailer_start + 20, 4)} | `0x{schema_trailer[3]:08x}` | Trailer CRC32C |",
            f"| {byte_range(schema_trailer_start + 24, 8)} | `ACTAEND\\n` | Commit magic |",
            "",
            "## Data frame",
            "",
            "### Prefix and block header",
            "",
            "| Absolute range | Decoded value | Meaning |",
            "| --- | --- | --- |",
            f"| {byte_range(data_start, 8)} | `ACTAFRM\\n` | Frame magic |",
            f"| {byte_range(data_start + 8, 2)} | {data_prefix[1]} | Frame type: data block |",
            f"| {byte_range(data_start + 16, 4)} | {data_prefix[4]} | Header length |",
            f"| {byte_range(data_start + 24, 8)} | {data_prefix[6]} | Payload length |",
            f"| {byte_range(data_start + 32, 8)} | {data_prefix[7]} | Sequence number |",
            f"| {byte_range(data_start + 40, 4)} | `0x{data_prefix[8]:08x}` | Header CRC32C |",
            f"| {byte_range(data_start + 44, 4)} | `0x{data_prefix[9]:08x}` | Prefix CRC32C |",
            f"| {byte_range(data_header_start, 8)} | {SCHEMA_ID} | Schema ID |",
            f"| {byte_range(data_header_start + 8, 8)} | 0 | Base row ID |",
            f"| {byte_range(data_header_start + 16, 4)} | {ROW_COUNT} | Row count |",
            f"| {byte_range(data_header_start + 20, 4)} | {len(COLUMNS)} | Column count |",
            f"| {byte_range(data_header_start + 24, 8)} | {min(COLUMNS[6].values)} | Primary timestamp minimum |",
            f"| {byte_range(data_header_start + 32, 8)} | {max(COLUMNS[6].values)} | Primary timestamp maximum |",
            f"| {byte_range(data_header_start + 40, 4)} | 64 | Column-table offset |",
            f"| {byte_range(data_header_start + 44, 4)} | {64 + 32 * len(COLUMNS)} | Stream-table offset |",
            f"| {byte_range(data_header_start + 48, 4)} | {build.data_header_length} | Empty statistics-area offset |",
            f"| {byte_range(data_header_start + 52, 4)} | 0 | Statistics length |",
            f"| {byte_range(data_header_start + 56, 4)} | `0x1` | Block flags: row IDs enabled |",
            f"| {byte_range(data_header_start + 60, 4)} | 0 | Reserved |",
            "",
            "The pickup timestamps are not sorted by row. Block bounds are therefore",
            "the minimum and maximum rather than the first and last values.",
            "",
            "### Column descriptors",
            "",
            "All columns use plain layout and omit optional statistics. Data-column",
            "flag bit 0 is set only for the nullable `passenger_count`, whose three",
            "values are all valid and therefore use implicit all-valid validity.",
            "",
            "| Absolute range | ID / column | Flags | Null / dense | First stream / count |",
            "| --- | --- | ---: | --- | --- |",
        ]
    )
    column_table_start = data_header_start + probe.BLOCK_HEADER.size
    for index, block in enumerate(build.column_blocks):
        absolute = column_table_start + index * probe.COLUMN_DESCRIPTOR.size
        lines.append(
            f"| {byte_range(absolute, probe.COLUMN_DESCRIPTOR.size)} | "
            f"{block.column.column_id} / `{block.column.name}` | {block.flags} | "
            f"{block.null_count} / {block.dense_count} | "
            f"{block.first_stream} / {block.stream_count} |"
        )

    lines.extend(
        [
            "",
            "### Stream descriptors",
            "",
            "Every stream uses transform 0 (`raw`) and codec 0 (`none`). Stored length",
            "excludes alignment padding; payload offsets are relative to the start of",
            "the data-frame payload.",
            "",
            "| Descriptor range | Index | Column | Kind | Payload offset | Stored bytes | Elements | CRC32C |",
            "| --- | ---: | --- | --- | ---: | ---: | ---: | --- |",
        ]
    )
    stream_table_start = (
        column_table_start + len(COLUMNS) * probe.COLUMN_DESCRIPTOR.size
    )
    for stream in build.streams:
        descriptor_absolute = (
            stream_table_start + stream.index * probe.STREAM_DESCRIPTOR.size
        )
        lines.append(
            f"| {byte_range(descriptor_absolute, probe.STREAM_DESCRIPTOR.size)} | "
            f"{stream.index} | `{stream.column_name}` | {stream.kind}: {stream.kind_name} | "
            f"{stream.payload_offset} | {len(stream.stored)} | {stream.element_count} | "
            f"`0x{probe.crc32c(stream.stored):08x}` |"
        )

    lines.extend(
        [
            "",
            "### Stored stream payloads",
            "",
            "Each stream begins on an eight-byte boundary. Bytes between the stored",
            "range and the next stream are zero alignment padding and are covered by",
            "the frame body CRC, but not by the individual stream CRC.",
            "",
            "| Absolute stored range | Index / column / kind | Stored bytes (hex) |",
            "| --- | --- | --- |",
        ]
    )
    for stream in build.streams:
        absolute = data_payload_start + stream.payload_offset
        lines.append(
            f"| {byte_range(absolute, len(stream.stored))} | {stream.index} / "
            f"`{stream.column_name}` / {stream.kind_name} | `{stream.stored.hex()}` |"
        )

    lines.extend(
        [
            "",
            "Raw booleans are LSB-first bitmaps: `stored_and_forwarded` is `0x00`,",
            "`passenger_validity` is `0x07`, and `sparse_validity` is `0x06`.",
            "Variable-width columns have one concatenated values stream and one",
            "little-endian `uint32` lengths stream. Fixed-width numeric values are",
            "concatenated in canonical little-endian representation.",
            "",
            "### Commit trailer",
            "",
            "| Absolute range | Decoded value | Meaning |",
            "| --- | --- | --- |",
            f"| {byte_range(data_trailer_start, 8)} | {data_trailer[0]} | Total frame length |",
            f"| {byte_range(data_trailer_start + 8, 8)} | {data_trailer[1]} | Repeated sequence number |",
            f"| {byte_range(data_trailer_start + 16, 4)} | `0x{data_trailer[2]:08x}` | Body CRC32C |",
            f"| {byte_range(data_trailer_start + 20, 4)} | `0x{data_trailer[3]:08x}` | Trailer CRC32C |",
            f"| {byte_range(data_trailer_start + 24, 8)} | `ACTAEND\\n` | Commit magic |",
            "",
            "The three rows become visible only after this complete trailer validates.",
            "If the file ends anywhere inside this frame, recovery retains the schema",
            f"frame and reports `0x{data_start:x}` as `last_good_offset`.",
            "",
            "## Regenerating and validating",
            "",
            "From the repository root:",
            "",
            "```bash",
            "uv run spec/v0.1/fixtures/nyc_taxi_3_rows.py \\",
            "  --output spec/v0.1/fixtures/nyc_taxi_3_rows.acta \\",
            "  --markdown-output spec/v0.1/fixtures/nyc_taxi_3_rows.md",
            "```",
            "",
            "Expected SHA-256:",
            "",
            "```text",
            digest,
            "```",
            "",
            "The generator validates all frame CRCs, schema and block metadata, row-ID",
            "fields, stream offsets and CRCs, and a logical round trip of every column.",
            "It also checks every possible truncation point inside the data frame.",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build = build_fixture()
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(build.data)
        print(f"wrote {len(build.data):,} bytes to {args.output}")
    if args.markdown_output:
        output_name = args.output.name if args.output else "nyc_taxi_3_rows.acta"
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(render_markdown(build, output_name))
        print(f"wrote {args.markdown_output}")
    if not args.output and not args.markdown_output:
        digest = hashlib.sha256(build.data).hexdigest()
        print(
            f"NYC taxi fixture passed: {len(build.data):,} bytes, "
            f"{len(COLUMNS)} columns, {len(build.streams)} streams, "
            f"{len(build.data_frame) - 1:,} truncation points, sha256={digest}"
        )


if __name__ == "__main__":
    main()
