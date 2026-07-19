"""Recovery and full-file verification (spec section 13.2)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .constants import PROLOGUE
from .decode import verify_block_bounds, verify_ts_sorted
from .enums import FrameType
from .errors import CorruptionError
from .framing import parse_prologue, read_frame
from .reader import Reader


@dataclass(frozen=True)
class RecoveryReport:
    path: Path
    file_size: int
    last_good_offset: int
    complete_frames: int
    complete_blocks: int
    incomplete_tail: bool
    tail_error: str | None
    truncated: bool


def recover(path: str | os.PathLike, *, truncate: bool = False) -> RecoveryReport:
    """Inspect a file after an interrupted append; optionally truncate it.

    Walks frames with full body-CRC validation. A truncated or corrupt final
    region is reported as the tail; ``truncate=True`` cuts the file back to
    the byte after the last complete frame, which is the one mutating repair
    this module performs. Corruption in the prologue or schema frame is not
    repairable here and raises instead.
    """
    path = Path(path)
    with open(path, "rb") as file:
        file_size = os.fstat(file.fileno()).st_size
        parse_prologue(file.read(PROLOGUE.size))
        offset = PROLOGUE.size
        sequence = 0
        frames = 0
        blocks = 0
        tail_error: str | None = None
        incomplete = False
        while offset < file_size:
            try:
                frame = read_frame(file, offset, sequence, file_size, strict_body=True)
            except CorruptionError as error:
                if sequence == 0:
                    raise
                tail_error = str(error)
                incomplete = True
                break
            if frame is None:
                incomplete = True
                break
            if sequence == 0 and frame.frame_type != FrameType.SCHEMA:
                raise CorruptionError("frame sequence 0 is not a schema frame")
            frames += 1
            if frame.frame_type == FrameType.DATA:
                blocks += 1
            offset = frame.end_offset
            sequence += 1
        if frames == 0:
            raise CorruptionError(
                "file has no complete schema frame; nothing to recover"
            )
        last_good_offset = offset

    truncated = False
    if truncate and last_good_offset < file_size:
        os.truncate(path, last_good_offset)
        truncated = True
    return RecoveryReport(
        path=path,
        file_size=file_size,
        last_good_offset=last_good_offset,
        complete_frames=frames,
        complete_blocks=blocks,
        incomplete_tail=incomplete,
        tail_error=tail_error,
        truncated=truncated,
    )


def verify(path: str | os.PathLike) -> int:
    """Full strict validation pass; returns the number of verified blocks.

    Validates every frame's structure and body CRC, every stream CRC, decodes
    every column, and checks primary-timestamp bounds and declared TS_SORTED
    ordering. Raises :class:`CorruptionError` on the first violation,
    including an incomplete tail.
    """
    with Reader(path, strict=True) as reader:
        file_size = os.fstat(reader._file.fileno()).st_size
        if reader._next_offset != file_size:
            raise CorruptionError(
                f"incomplete tail after offset {reader._next_offset}",
                offset=reader._next_offset,
            )
        for block in reader.blocks():
            data = block.read()  # decodes every column, verifying stream CRCs
            primary = data[reader.schema.primary.name].values
            verify_block_bounds(block._entry.meta, primary)
            verify_ts_sorted(block._entry.meta, primary)
        return reader.num_blocks
