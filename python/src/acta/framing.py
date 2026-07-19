"""File prologue and self-delimiting frame layer (spec sections 5, 6, 13)."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import BinaryIO

from .constants import (
    COMMIT_MAGIC,
    FILE_MAGIC,
    FRAME_MAGIC,
    PREFIX,
    PROLOGUE,
    TRAILER,
    crc32c,
    pad8,
)
from .enums import FEATURE_ROW_IDS
from .errors import CorruptionError


@dataclass(frozen=True)
class Prologue:
    feature_flags: int
    file_id: bytes

    @property
    def row_ids(self) -> bool:
        return bool(self.feature_flags & FEATURE_ROW_IDS)


@dataclass(frozen=True)
class Frame:
    offset: int
    frame_type: int
    sequence: int
    header: bytes
    payload_offset: int
    payload_length: int
    total_length: int
    payload: bytes | None

    @property
    def end_offset(self) -> int:
        return self.offset + self.total_length


@dataclass(frozen=True)
class ScanResult:
    prologue: Prologue
    frames: tuple[Frame, ...]
    last_good_offset: int
    incomplete_tail: bool


def make_prologue(file_id: bytes, feature_flags: int = 0) -> bytes:
    if len(file_id) != 16:
        raise ValueError("file ID must contain exactly 16 bytes")
    if feature_flags & ~FEATURE_ROW_IDS:
        raise ValueError("unknown v0.1 feature flags")
    without_crc = PROLOGUE.pack(
        FILE_MAGIC,
        0,
        1,
        PROLOGUE.size,
        feature_flags,
        file_id,
        PROLOGUE.size,
        bytes(12),
        0,
    )
    return without_crc[:-4] + struct.pack("<I", crc32c(without_crc[:-4]))


def parse_prologue(data: bytes) -> Prologue:
    if len(data) < PROLOGUE.size:
        raise CorruptionError("file is shorter than the 64-byte prologue", offset=0)
    magic, major, minor, size, flags, file_id, schema_offset, reserved, stored_crc = (
        PROLOGUE.unpack_from(data)
    )
    if magic != FILE_MAGIC:
        raise CorruptionError("bad file magic", offset=0)
    if (major, minor, size, schema_offset) != (0, 1, 64, 64):
        raise CorruptionError("unsupported prologue fields", offset=0)
    if flags & ~FEATURE_ROW_IDS:
        raise CorruptionError("unknown feature flags", offset=0)
    if reserved != bytes(12):
        raise CorruptionError("nonzero prologue reserved bytes", offset=0)
    if stored_crc != crc32c(data[:60]):
        raise CorruptionError("bad prologue CRC32C", offset=0)
    return Prologue(feature_flags=flags, file_id=file_id)


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


def read_frame(
    file: BinaryIO,
    offset: int,
    expected_sequence: int,
    file_size: int,
    *,
    strict_body: bool = False,
    with_payload: bool = False,
) -> Frame | None:
    """Read and validate one frame starting at ``offset``.

    Returns ``None`` when the file ends inside the frame (an incomplete tail).
    Raises :class:`CorruptionError` for any other violation.
    """
    if file_size - offset < PREFIX.size:
        return None
    file.seek(offset)
    prefix_bytes = file.read(PREFIX.size)
    if len(prefix_bytes) < PREFIX.size:
        return None
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
        raise CorruptionError(f"bad frame magic at offset {offset}", offset=offset)
    if prefix_crc != crc32c(prefix_bytes[:44]):
        raise CorruptionError(f"bad prefix CRC32C at offset {offset}", offset=offset)
    if frame_version != 0 or flags != 0 or reserved != 0:
        raise CorruptionError(
            f"unsupported frame fields at offset {offset}", offset=offset
        )
    if header_length % 8 or payload_length % 8:
        raise CorruptionError(
            f"unaligned frame lengths at offset {offset}", offset=offset
        )
    if sequence != expected_sequence:
        raise CorruptionError(
            f"expected sequence {expected_sequence}, found {sequence} "
            f"at offset {offset}",
            offset=offset,
        )
    total_length = PREFIX.size + header_length + payload_length + TRAILER.size
    if file_size - offset < total_length:
        return None

    header = file.read(header_length)
    payload_offset = offset + PREFIX.size + header_length
    need_payload = with_payload or strict_body
    if need_payload:
        payload = file.read(payload_length)
    else:
        payload = None
        file.seek(payload_offset + payload_length)
    trailer_bytes = file.read(TRAILER.size)
    if len(header) < header_length or len(trailer_bytes) < TRAILER.size:
        return None
    if payload is not None and len(payload) < payload_length:
        return None
    repeated_length, repeated_sequence, body_crc, trailer_crc, commit_magic = (
        TRAILER.unpack(trailer_bytes)
    )
    if header_crc != crc32c(header):
        raise CorruptionError(f"bad header CRC32C at offset {offset}", offset=offset)
    if repeated_length != total_length or repeated_sequence != sequence:
        raise CorruptionError(
            f"mismatched trailer fields at offset {offset}", offset=offset
        )
    if commit_magic != COMMIT_MAGIC:
        raise CorruptionError(f"bad commit magic at offset {offset}", offset=offset)
    if trailer_crc != crc32c(trailer_bytes[:20] + trailer_bytes[24:]):
        raise CorruptionError(f"bad trailer CRC32C at offset {offset}", offset=offset)
    if strict_body:
        assert payload is not None
        if body_crc != crc32c(prefix_bytes + header + payload):
            raise CorruptionError(f"bad body CRC32C at offset {offset}", offset=offset)
    return Frame(
        offset=offset,
        frame_type=frame_type,
        sequence=sequence,
        header=header,
        payload_offset=payload_offset,
        payload_length=payload_length,
        total_length=total_length,
        payload=payload if with_payload else None,
    )


def scan_file(
    file: BinaryIO,
    file_size: int,
    *,
    strict_body: bool = False,
    with_payload: bool = False,
) -> ScanResult:
    """Validate the prologue and walk every complete frame (spec 13.1)."""
    file.seek(0)
    prologue = parse_prologue(file.read(PROLOGUE.size))
    frames: list[Frame] = []
    offset = PROLOGUE.size
    sequence = 0
    while offset < file_size:
        frame = read_frame(
            file,
            offset,
            sequence,
            file_size,
            strict_body=strict_body,
            with_payload=with_payload,
        )
        if frame is None:
            return ScanResult(prologue, tuple(frames), offset, True)
        frames.append(frame)
        offset = frame.end_offset
        sequence += 1
    return ScanResult(prologue, tuple(frames), offset, False)


def scan_bytes(
    data: bytes, *, strict_body: bool = True, with_payload: bool = True
) -> ScanResult:
    import io

    return scan_file(
        io.BytesIO(data),
        len(data),
        strict_body=strict_body,
        with_payload=with_payload,
    )
