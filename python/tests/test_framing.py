"""Framing layer tests: struct sizes, round trips, truncation, corruption."""

from __future__ import annotations

import io
import struct

import pytest

from acta import constants, framing
from acta.errors import CorruptionError, UnsupportedFormatVersionError

from conftest import fixture_bytes


def test_struct_sizes_match_the_spec():
    assert constants.PROLOGUE.size == 64
    assert constants.PREFIX.size == 48
    assert constants.TRAILER.size == 32
    assert constants.SCHEMA_HEADER.size == 24
    assert constants.COLUMN_SCHEMA.size == 24
    assert constants.BLOCK_HEADER.size == 64
    assert constants.COLUMN_DESCRIPTOR.size == 32
    assert constants.STREAM_DESCRIPTOR.size == 48


def test_prologue_round_trip():
    file_id = bytes(range(16))
    data = framing.make_prologue(file_id, feature_flags=1)
    assert len(data) == 64
    prologue = framing.parse_prologue(data)
    assert prologue.format_version == (0, 1)
    assert prologue.file_id == file_id
    assert prologue.row_ids


def test_prologue_rejects_corruption():
    data = bytearray(framing.make_prologue(bytes(16)))
    data[5] ^= 1
    with pytest.raises(CorruptionError):
        framing.parse_prologue(bytes(data))


@pytest.mark.parametrize("version", [(0, 2), (1, 0)])
def test_prologue_preserves_version_and_scan_rejects_unsupported(version):
    data = bytearray(framing.make_prologue(bytes(16)))
    data[8:12] = struct.pack("<HH", *version)
    data[16:24] = struct.pack("<Q", 1 << 63)  # potentially assigned by that version
    data[60:64] = struct.pack("<I", constants.crc32c(bytes(data[:60])))

    prologue = framing.parse_prologue(bytes(data))
    assert prologue.format_version == version

    with pytest.raises(UnsupportedFormatVersionError) as caught:
        framing.scan_bytes(bytes(data))
    assert (caught.value.major, caught.value.minor) == version
    assert caught.value.supported == ((0, 1),)


def test_frame_round_trip():
    frame_bytes = framing.make_frame(2, 7, b"header!", b"payload")
    assert len(frame_bytes) % 8 == 0
    data = framing.make_prologue(bytes(16)) + framing.make_frame(1, 0, b"", b"")
    result = framing.scan_bytes(data + b"")
    assert not result.incomplete_tail
    assert len(result.frames) == 1


def test_scan_minimal_fixture():
    data = fixture_bytes("minimal")
    result = framing.scan_bytes(data)
    assert not result.incomplete_tail
    assert [frame.frame_type for frame in result.frames] == [1, 2]
    assert [frame.sequence for frame in result.frames] == [0, 1]
    assert result.last_good_offset == len(data)


def test_every_truncation_point_reports_an_incomplete_tail():
    data = fixture_bytes("minimal")
    result = framing.scan_bytes(data)
    last_frame = result.frames[-1]
    for cut in range(last_frame.offset + 1, len(data)):
        truncated = framing.scan_bytes(data[:cut])
        assert truncated.incomplete_tail
        assert len(truncated.frames) == 1
        assert truncated.last_good_offset == last_frame.offset


def test_corruption_in_each_protected_region_is_detected():
    data = fixture_bytes("minimal")
    result = framing.scan_bytes(data)
    last = result.frames[-1]
    offsets = [
        5,  # prologue
        last.offset + 2,  # frame prefix
        last.offset + constants.PREFIX.size + 3,  # frame header
        last.payload_offset + 3,  # payload (body CRC)
        len(data) - 2,  # trailer / commit magic
    ]
    for offset in offsets:
        damaged = bytearray(data)
        damaged[offset] ^= 0x01
        with pytest.raises(CorruptionError):
            framing.scan_bytes(bytes(damaged))


def test_lax_scan_skips_body_crc_but_not_structure():
    data = fixture_bytes("minimal")
    result = framing.scan_bytes(data)
    damaged = bytearray(data)
    damaged[result.frames[-1].payload_offset] ^= 0x01  # payload byte
    # Structure (prefix/header/trailer) still validates without the body CRC.
    lax = framing.scan_file(io.BytesIO(bytes(damaged)), len(damaged), strict_body=False)
    assert not lax.incomplete_tail
    with pytest.raises(CorruptionError):
        framing.scan_bytes(bytes(damaged))


def test_sequence_gap_is_corruption():
    data = framing.make_prologue(bytes(16)) + framing.make_frame(1, 0, b"", b"")
    data += framing.make_frame(2, 2, b"", b"")  # skips sequence 1
    with pytest.raises(CorruptionError):
        framing.scan_bytes(data)
