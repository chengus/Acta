#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["google-crc32c>=1.6"]
# ///
"""Executable framing probe and minimal fixture generator for Acta v0.1."""

from __future__ import annotations

import argparse
import struct
from dataclasses import dataclass
from pathlib import Path

import google_crc32c


FILE_MAGIC = b"ACTA\r\n\x1a\n"
FRAME_MAGIC = b"ACTAFRM\n"
COMMIT_MAGIC = b"ACTAEND\n"

PROLOGUE = struct.Struct("<8sHHIQ16sQ12sI")
PREFIX = struct.Struct("<8sHHIIIQQII")
TRAILER = struct.Struct("<QQII8s")
SCHEMA_HEADER = struct.Struct("<QIIII")
COLUMN_SCHEMA = struct.Struct("<IIHHIII")
BLOCK_HEADER = struct.Struct("<QQIIqqIIIIII")
COLUMN_DESCRIPTOR = struct.Struct("<IHHIIIHHII")
STREAM_DESCRIPTOR = struct.Struct("<HHHHQQQQII")


class Corruption(ValueError):
    pass


@dataclass(frozen=True)
class Frame:
    offset: int
    frame_type: int
    sequence: int
    header: bytes
    payload: bytes
    total_length: int


@dataclass(frozen=True)
class ScanResult:
    frames: tuple[Frame, ...]
    last_good_offset: int
    incomplete_tail: bool


def crc32c(data: bytes) -> int:
    return google_crc32c.value(data)


def pad8(data: bytes) -> bytes:
    return data + bytes((-len(data)) % 8)


def make_prologue(file_id: bytes = bytes(range(16))) -> bytes:
    if len(file_id) != 16:
        raise ValueError("file ID must contain exactly 16 bytes")
    without_crc = PROLOGUE.pack(
        FILE_MAGIC,
        0,
        1,
        PROLOGUE.size,
        0,
        file_id,
        PROLOGUE.size,
        bytes(12),
        0,
    )
    return without_crc[:-4] + struct.pack("<I", crc32c(without_crc[:-4]))


def make_frame(frame_type: int, sequence: int, header: bytes, payload: bytes) -> bytes:
    header = pad8(header)
    payload = pad8(payload)
    prefix = PREFIX.pack(
        FRAME_MAGIC,
        frame_type,
        0,
        0,
        len(header),
        0,
        len(payload),
        sequence,
        crc32c(header),
        0,
    )
    prefix = prefix[:-4] + struct.pack("<I", crc32c(prefix[:-4]))
    body = prefix + header + payload
    total_length = len(body) + TRAILER.size
    body_crc = crc32c(body)
    trailer_crc_input = (
        struct.pack("<QQI", total_length, sequence, body_crc) + COMMIT_MAGIC
    )
    trailer = TRAILER.pack(
        total_length,
        sequence,
        body_crc,
        crc32c(trailer_crc_input),
        COMMIT_MAGIC,
    )
    return body + trailer


def validate_prologue(data: bytes) -> None:
    if len(data) < PROLOGUE.size:
        raise Corruption("file is shorter than the 64-byte prologue")
    (
        magic,
        major,
        minor,
        size,
        flags,
        _file_id,
        schema_offset,
        reserved,
        stored_crc,
    ) = PROLOGUE.unpack_from(data)
    if magic != FILE_MAGIC:
        raise Corruption("bad file magic")
    if (major, minor, size, schema_offset) != (0, 1, 64, 64) or flags & ~1:
        raise Corruption("unsupported prologue fields")
    if reserved != bytes(12):
        raise Corruption("nonzero prologue reserved bytes")
    if stored_crc != crc32c(data[:60]):
        raise Corruption("bad prologue CRC32C")


def scan_frames(data: bytes, *, strict_body: bool = True) -> ScanResult:
    validate_prologue(data)
    frames: list[Frame] = []
    offset = PROLOGUE.size
    expected_sequence = 0
    while offset < len(data):
        remaining = len(data) - offset
        if remaining < PREFIX.size:
            return ScanResult(tuple(frames), offset, True)
        prefix_bytes = data[offset : offset + PREFIX.size]
        (
            magic,
            frame_type,
            frame_version,
            flags,
            header_length,
            reserved,
            payload_length,
            sequence,
            header_crc,
            prefix_crc,
        ) = PREFIX.unpack(prefix_bytes)
        if magic != FRAME_MAGIC:
            raise Corruption(f"bad frame magic at offset {offset}")
        if prefix_crc != crc32c(prefix_bytes[:44]):
            raise Corruption(f"bad prefix CRC32C at offset {offset}")
        if frame_version != 0 or flags != 0 or reserved != 0:
            raise Corruption(f"unsupported frame fields at offset {offset}")
        if header_length % 8 or payload_length % 8:
            raise Corruption(f"unaligned frame lengths at offset {offset}")
        if sequence != expected_sequence:
            raise Corruption(
                f"expected sequence {expected_sequence}, found {sequence} at offset {offset}"
            )
        total_length = PREFIX.size + header_length + payload_length + TRAILER.size
        if total_length < PREFIX.size + TRAILER.size:
            raise Corruption(f"frame length overflow at offset {offset}")
        if remaining < total_length:
            return ScanResult(tuple(frames), offset, True)

        header_start = offset + PREFIX.size
        payload_start = header_start + header_length
        trailer_start = payload_start + payload_length
        header = data[header_start:payload_start]
        payload = data[payload_start:trailer_start]
        trailer_bytes = data[trailer_start : trailer_start + TRAILER.size]
        (
            repeated_length,
            repeated_sequence,
            body_crc,
            trailer_crc,
            commit_magic,
        ) = TRAILER.unpack(trailer_bytes)
        if header_crc != crc32c(header):
            raise Corruption(f"bad header CRC32C at offset {offset}")
        if repeated_length != total_length or repeated_sequence != sequence:
            raise Corruption(f"mismatched trailer fields at offset {offset}")
        if commit_magic != COMMIT_MAGIC:
            raise Corruption(f"bad commit magic at offset {offset}")
        trailer_crc_input = trailer_bytes[:20] + trailer_bytes[24:]
        if trailer_crc != crc32c(trailer_crc_input):
            raise Corruption(f"bad trailer CRC32C at offset {offset}")
        if strict_body and body_crc != crc32c(data[offset:trailer_start]):
            raise Corruption(f"bad body CRC32C at offset {offset}")
        frames.append(
            Frame(
                offset,
                frame_type,
                sequence,
                header,
                payload,
                total_length,
            )
        )
        offset += total_length
        expected_sequence += 1
    return ScanResult(tuple(frames), offset, False)


def make_minimal_fixture() -> bytes:
    schema_id = 1
    timestamp_parameters = struct.pack("<BBHI", 2, 1, 0, 0)
    name = b"time"
    descriptor_length = 24 + len(name) + len(timestamp_parameters)
    descriptor_length += (-descriptor_length) % 8
    column = (
        COLUMN_SCHEMA.pack(
            descriptor_length,
            1,
            13,
            0,
            len(name),
            len(timestamp_parameters),
            0,
        )
        + name
        + timestamp_parameters
    )
    column = pad8(column)
    schema_header = SCHEMA_HEADER.pack(schema_id, 1, 1, 0, 0)
    schema_frame = make_frame(1, 0, schema_header, column)

    timestamps = (1_000_000, 2_000_000, 3_000_000)
    values = struct.pack("<qqq", *timestamps)
    statistics = struct.pack("<qq", min(timestamps), max(timestamps))
    column_table_offset = BLOCK_HEADER.size
    stream_table_offset = column_table_offset + COLUMN_DESCRIPTOR.size
    statistics_offset = stream_table_offset + STREAM_DESCRIPTOR.size
    block_header = BLOCK_HEADER.pack(
        schema_id,
        (1 << 64) - 1,
        len(timestamps),
        1,
        min(timestamps),
        max(timestamps),
        column_table_offset,
        stream_table_offset,
        statistics_offset,
        len(statistics),
        0,
        0,
    )
    column_descriptor = COLUMN_DESCRIPTOR.pack(
        1,
        0,
        3,
        0,
        len(timestamps),
        0,
        1,
        1,
        statistics_offset,
        len(statistics),
    )
    stream_descriptor = STREAM_DESCRIPTOR.pack(
        2,
        0,
        0,
        0,
        0,
        len(values),
        len(values),
        len(timestamps),
        crc32c(values),
        0,
    )
    data_header = block_header + column_descriptor + stream_descriptor + statistics
    data_frame = make_frame(2, 1, data_header, values)
    return make_prologue() + schema_frame + data_frame


def validate_minimal_fixture(data: bytes) -> None:
    result = scan_frames(data)
    if result.incomplete_tail or len(result.frames) != 2:
        raise AssertionError("minimal fixture did not contain two complete frames")
    schema, block = result.frames
    if (schema.frame_type, block.frame_type) != (1, 2):
        raise AssertionError("minimal fixture frame types are wrong")
    schema_id, column_count, time_column_id, schema_flags, schema_reserved = (
        SCHEMA_HEADER.unpack_from(schema.header)
    )
    if (schema_id, column_count, time_column_id, schema_flags, schema_reserved) != (
        1,
        1,
        1,
        0,
        0,
    ):
        raise AssertionError("minimal fixture schema header is wrong")
    (
        descriptor_length,
        column_id,
        logical_type,
        column_flags,
        name_length,
        parameter_length,
        column_reserved,
    ) = COLUMN_SCHEMA.unpack_from(schema.payload)
    if (
        descriptor_length != len(schema.payload)
        or column_id != time_column_id
        or logical_type != 13
        or column_flags != 0
        or column_reserved != 0
        or schema.payload[24 : 24 + name_length] != b"time"
        or parameter_length != 8
    ):
        raise AssertionError("minimal fixture timestamp schema is wrong")
    block_fields = BLOCK_HEADER.unpack_from(block.header)
    if (
        block_fields[0] != schema_id
        or block_fields[2] != 3
        or block_fields[3] != column_count
        or block_fields[4:6] != (1_000_000, 3_000_000)
    ):
        raise AssertionError("minimal fixture block metadata is wrong")
    column_offset, stream_offset, stats_offset, stats_length = block_fields[6:10]
    if (
        column_offset != BLOCK_HEADER.size
        or stream_offset != column_offset + COLUMN_DESCRIPTOR.size
        or stats_offset != stream_offset + STREAM_DESCRIPTOR.size
        or stats_offset + stats_length > len(block.header)
    ):
        raise AssertionError("minimal fixture header tables are out of bounds")
    column_fields = COLUMN_DESCRIPTOR.unpack_from(block.header, column_offset)
    if (
        column_fields[0] != column_id
        or column_fields[4] != block_fields[2]
        or column_fields[5] != 0
        or column_fields[6] != 1
        or column_fields[8:10] != (stats_offset, stats_length)
    ):
        raise AssertionError("minimal fixture column descriptor is wrong")
    stream_fields = STREAM_DESCRIPTOR.unpack_from(block.header, stream_offset)
    stored_offset, stored_length, stored_crc = (
        stream_fields[4],
        stream_fields[5],
        stream_fields[8],
    )
    if stored_offset + stored_length > len(block.payload):
        raise AssertionError("minimal fixture stream is out of payload bounds")
    stored = block.payload[stored_offset : stored_offset + stored_length]
    if crc32c(stored) != stored_crc:
        raise AssertionError("minimal fixture stream CRC32C is wrong")
    if struct.unpack("<qqq", stored) != (1_000_000, 2_000_000, 3_000_000):
        raise AssertionError("minimal fixture timestamp values are wrong")


def self_test() -> None:
    fixture = make_minimal_fixture()
    validate_minimal_fixture(fixture)
    result = scan_frames(fixture)
    last_frame_start = result.frames[-1].offset

    # Every possible interrupted write within the last frame preserves the
    # preceding schema frame and reports an incomplete tail.
    for cut in range(last_frame_start + 1, len(fixture)):
        truncated = scan_frames(fixture[:cut])
        if not truncated.incomplete_tail or len(truncated.frames) != 1:
            raise AssertionError(f"unexpected truncation result at byte {cut}")

    # Representative corruption in each protected region must be detected.
    corruption_offsets = [
        5,
        last_frame_start + 2,
        last_frame_start + PREFIX.size + 3,
        last_frame_start + PREFIX.size + len(result.frames[-1].header) + 3,
        len(fixture) - 2,
    ]
    for corruption_offset in corruption_offsets:
        damaged = bytearray(fixture)
        damaged[corruption_offset] ^= 0x01
        try:
            scan_frames(bytes(damaged))
        except Corruption:
            pass
        else:
            raise AssertionError(
                f"corruption at byte {corruption_offset} was not detected"
            )

    print(
        f"Acta v0.1 framing probe passed: {len(fixture)}-byte fixture, "
        f"{len(fixture) - last_frame_start - 1} truncation points, "
        f"{len(corruption_offsets)} corruption regions"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-fixture", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    self_test()
    if args.write_fixture:
        args.write_fixture.parent.mkdir(parents=True, exist_ok=True)
        fixture = make_minimal_fixture()
        args.write_fixture.write_bytes(fixture)
        print(f"wrote {len(fixture)} bytes to {args.write_fixture}")


if __name__ == "__main__":
    main()
