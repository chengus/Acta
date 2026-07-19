"""Reader: block index, time-range pruning, projection, and tailing."""

from __future__ import annotations

import os
import time as _time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Mapping, Sequence

import numpy as np

from . import transforms
from .columns import FIXED_DTYPES, ColumnData, is_bytes_like, time_bound_to_int
from .constants import PROLOGUE, crc32c
from .decode import (
    BlockMeta,
    decode_column,
    parse_block_header,
    verify_block_bounds,
    verify_ts_sorted,
)
from .enums import Codec, FrameType
from .errors import ActaError, CorruptionError
from .framing import Frame, Prologue, parse_prologue, read_frame
from .schema import Schema


class ReadResult(Mapping[str, ColumnData]):
    """Column values for the rows selected by a read."""

    def __init__(
        self,
        columns: dict[str, ColumnData],
        row_count: int,
        row_ids: np.ndarray | None,
    ) -> None:
        self._columns = columns
        self.row_count = row_count
        self.row_ids = row_ids

    def __getitem__(self, name: str) -> ColumnData:
        return self._columns[name]

    def __iter__(self):
        return iter(self._columns)

    def __len__(self) -> int:
        return len(self._columns)

    def __repr__(self) -> str:
        names = ", ".join(self._columns)
        return f"ReadResult(rows={self.row_count}, columns=[{names}])"


@dataclass(frozen=True)
class _BlockEntry:
    frame: Frame
    meta: BlockMeta


class Block:
    """One committed data block; decoding happens on demand."""

    def __init__(self, reader: "Reader", entry: _BlockEntry) -> None:
        self._reader = reader
        self._entry = entry

    @property
    def sequence(self) -> int:
        return self._entry.frame.sequence

    @property
    def offset(self) -> int:
        return self._entry.frame.offset

    @property
    def row_count(self) -> int:
        return self._entry.meta.row_count

    @property
    def ts_min(self) -> int:
        return self._entry.meta.ts_min

    @property
    def ts_max(self) -> int:
        return self._entry.meta.ts_max

    @property
    def ts_sorted(self) -> bool:
        return self._entry.meta.ts_sorted

    @property
    def base_row_id(self) -> int | None:
        return self._entry.meta.base_row_id

    def read(self, columns: Sequence[str] | None = None) -> ReadResult:
        """Decode the requested columns for every row of this block."""
        return self._reader._read_block(self._entry, columns)

    def __repr__(self) -> str:
        return (
            f"Block(sequence={self.sequence}, rows={self.row_count}, "
            f"bounds=[{self.ts_min}, {self.ts_max}], sorted={self.ts_sorted})"
        )


class Reader:
    """Reads one Acta file; safe to use while another process appends."""

    def __init__(self, path: str | os.PathLike, *, strict: bool = False) -> None:
        self.path = Path(path)
        self.strict = strict
        self._file = open(self.path, "rb")
        self._closed = False
        try:
            self._file.seek(0)
            self.prologue: Prologue = parse_prologue(self._file.read(PROLOGUE.size))
            self._entries: list[_BlockEntry] = []
            self._next_offset = PROLOGUE.size
            self._next_sequence = 0
            self.schema: Schema | None = None
            self._catch_up()
            if self.schema is None:
                raise CorruptionError(
                    "file does not contain a complete schema frame yet"
                )
        except BaseException:
            self._file.close()
            raise

    # -- lifecycle ---------------------------------------------------------

    def __enter__(self) -> "Reader":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._file.close()

    @property
    def file_id(self) -> bytes:
        return self.prologue.file_id

    @property
    def row_ids(self) -> bool:
        return self.prologue.row_ids

    @property
    def num_blocks(self) -> int:
        return len(self._entries)

    @property
    def num_rows(self) -> int:
        return sum(entry.meta.row_count for entry in self._entries)

    # -- discovery ---------------------------------------------------------

    def _ingest(self, frame: Frame) -> _BlockEntry | None:
        if frame.sequence == 0:
            if frame.frame_type != FrameType.SCHEMA:
                raise CorruptionError("frame sequence 0 is not a schema frame")
            self._file.seek(frame.payload_offset)
            payload = self._file.read(frame.payload_length)
            self.schema = Schema.from_frame(frame.header, payload)
            return None
        if frame.frame_type != FrameType.DATA:
            raise CorruptionError(
                f"unsupported frame type {frame.frame_type} at offset {frame.offset}",
                offset=frame.offset,
            )
        meta = parse_block_header(
            frame.header,
            self.schema,
            payload_length=frame.payload_length,
            file_row_ids=self.prologue.row_ids,
            offset=frame.offset,
        )
        entry = _BlockEntry(frame, meta)
        self._entries.append(entry)
        return entry

    def _catch_up(self) -> list[_BlockEntry]:
        """Scan forward from the last complete frame (spec 13.1 step 7)."""
        file_size = os.fstat(self._file.fileno()).st_size
        new_entries: list[_BlockEntry] = []
        while self._next_offset < file_size:
            frame = read_frame(
                self._file,
                self._next_offset,
                self._next_sequence,
                file_size,
                strict_body=self.strict,
            )
            if frame is None:
                break
            entry = self._ingest(frame)
            if entry is not None:
                new_entries.append(entry)
            self._next_offset = frame.end_offset
            self._next_sequence += 1
        return new_entries

    def refresh(self) -> int:
        """Pick up frames appended since the file was opened."""
        return len(self._catch_up())

    # -- reading -----------------------------------------------------------

    def _bounds(self, start, end) -> tuple[int | None, int | None]:
        primary = self.schema.primary
        low = None if start is None else time_bound_to_int(primary, start)
        high = None if end is None else time_bound_to_int(primary, end)
        return low, high

    def _pruned(self, low: int | None, high: int | None) -> Iterator[_BlockEntry]:
        for entry in self._entries:
            if high is not None and entry.meta.ts_min >= high:
                continue
            if low is not None and entry.meta.ts_max < low:
                continue
            yield entry

    def blocks(self, *, start=None, end=None) -> Iterator[Block]:
        """Iterate blocks whose header bounds intersect ``[start, end)``."""
        low, high = self._bounds(start, end)
        for entry in self._pruned(low, high):
            yield Block(self, entry)

    def _stream_reader(self, entry: _BlockEntry):
        """Projection reads: fetch and CRC-check exactly one stream's bytes."""

        def read(stream) -> bytes:
            self._file.seek(entry.frame.payload_offset + stream.payload_offset)
            stored = self._file.read(stream.stored_length)
            if len(stored) != stream.stored_length:
                raise CorruptionError(
                    f"stream {stream.index}: stored bytes are truncated",
                    offset=entry.frame.offset,
                )
            if crc32c(stored) != stream.crc:
                raise CorruptionError(
                    f"stream {stream.index}: bad stream CRC32C",
                    offset=entry.frame.offset,
                )
            if stream.codec == Codec.ZSTD:
                return transforms.decompress(stored, stream.transformed_length)
            return stored

        return read

    def _decode_columns(
        self, entry: _BlockEntry, names: Sequence[str]
    ) -> dict[str, ColumnData]:
        read = self._stream_reader(entry)
        decoded: dict[str, ColumnData] = {}
        for name in names:
            self.schema.column(name)  # raises SchemaError for unknown names
            decoded[name] = decode_column(
                entry.meta, entry.meta.column_meta(name), read
            )
        primary_name = self.schema.primary.name
        if self.strict and primary_name in decoded:
            primary_values = decoded[primary_name].values
            verify_block_bounds(entry.meta, primary_values)
            verify_ts_sorted(entry.meta, primary_values)
        return decoded

    def _read_block(
        self, entry: _BlockEntry, columns: Sequence[str] | None
    ) -> ReadResult:
        names = (
            [column.name for column in self.schema]
            if columns is None
            else list(columns)
        )
        decoded = self._decode_columns(entry, names)
        row_ids = None
        if entry.meta.base_row_id is not None:
            row_ids = entry.meta.base_row_id + np.arange(
                entry.meta.row_count, dtype=np.uint64
            )
        return ReadResult(decoded, entry.meta.row_count, row_ids)

    def read(
        self,
        columns: Sequence[str] | None = None,
        *,
        start=None,
        end=None,
        order: str = "file",
    ) -> ReadResult:
        """Read rows in ``[start, end)`` with optional column projection.

        Rows come back in physical order (block sequence, then row order
        within each block); Acta imposes no time ordering across blocks.
        ``order="time"`` stably sorts the result by the primary column.
        """
        if order not in ("file", "time"):
            raise ValueError('order must be "file" or "time"')
        if self.schema is None:
            raise ActaError("reader is not initialized")
        names = (
            [column.name for column in self.schema]
            if columns is None
            else list(columns)
        )
        for name in names:
            self.schema.column(name)
        low, high = self._bounds(start, end)
        primary_name = self.schema.primary.name
        need_primary = low is not None or high is not None or order == "time"
        decode_names = list(names)
        if need_primary and primary_name not in decode_names:
            decode_names.append(primary_name)

        parts: list[dict[str, ColumnData]] = []
        primary_parts: list[np.ndarray] = []
        row_id_parts: list[np.ndarray] = []
        for entry in self._pruned(low, high):
            decoded = self._decode_columns(entry, decode_names)
            selection: slice | np.ndarray = slice(None)
            if need_primary:
                primary_values = decoded[primary_name].values.astype(np.int64)
                if low is not None or high is not None:
                    if entry.meta.ts_sorted:
                        first = (
                            0
                            if low is None
                            else int(np.searchsorted(primary_values, low, side="left"))
                        )
                        last = (
                            len(primary_values)
                            if high is None
                            else int(np.searchsorted(primary_values, high, side="left"))
                        )
                        selection = slice(first, last)
                    else:
                        mask = np.ones(len(primary_values), dtype=np.bool_)
                        if low is not None:
                            mask &= primary_values >= low
                        if high is not None:
                            mask &= primary_values < high
                        selection = np.flatnonzero(mask)
                selected_primary = primary_values[selection]
                if len(selected_primary) == 0:
                    continue
                primary_parts.append(selected_primary)
            part: dict[str, ColumnData] = {}
            selected_rows = None
            for name in names:
                data = decoded[name]
                values = data.values[selection]
                validity = None if data.validity is None else data.validity[selection]
                part[name] = ColumnData(data.column, values, validity)
                selected_rows = len(values)
            if selected_rows is None:  # no projected columns; count via primary
                selected_rows = (
                    len(primary_parts[-1]) if need_primary else (entry.meta.row_count)
                )
            if entry.meta.base_row_id is not None:
                block_row_ids = entry.meta.base_row_id + np.arange(
                    entry.meta.row_count, dtype=np.uint64
                )
                row_id_parts.append(block_row_ids[selection])
            parts.append(part)

        return self._assemble(names, parts, primary_parts, row_id_parts, order)

    def _assemble(
        self,
        names: Sequence[str],
        parts: list[dict[str, ColumnData]],
        primary_parts: list[np.ndarray],
        row_id_parts: list[np.ndarray],
        order: str,
    ) -> ReadResult:
        columns: dict[str, ColumnData] = {}
        row_count = 0
        for name in names:
            column = self.schema.column(name)
            if parts:
                values = np.concatenate([part[name].values for part in parts])
                if any(part[name].validity is not None for part in parts):
                    validity = np.concatenate(
                        [
                            part[name].validity
                            if part[name].validity is not None
                            else np.ones(len(part[name].values), dtype=np.bool_)
                            for part in parts
                        ]
                    )
                else:
                    validity = None
            else:
                dtype = object if is_bytes_like(column) else FIXED_DTYPES[column.type]
                values = np.empty(0, dtype=dtype)
                validity = None
            columns[name] = ColumnData(column, values, validity)
            row_count = len(values)
        row_ids = np.concatenate(row_id_parts) if row_id_parts else None
        if not names and primary_parts:
            row_count = int(sum(len(part) for part in primary_parts))

        if order == "time" and row_count:
            primary = np.concatenate(primary_parts)
            permutation = np.argsort(primary, kind="stable")
            columns = {
                name: ColumnData(
                    data.column,
                    data.values[permutation],
                    None if data.validity is None else data.validity[permutation],
                )
                for name, data in columns.items()
            }
            if row_ids is not None:
                row_ids = row_ids[permutation]
        return ReadResult(columns, row_count, row_ids)

    # -- tailing -----------------------------------------------------------

    def follow(
        self,
        *,
        poll_interval: float = 0.25,
        timeout: float | None = None,
        start=None,
        end=None,
    ) -> Iterator[Block]:
        """Yield existing then newly appended blocks as the file grows.

        Discovery continues from the byte after the last complete frame; an
        incomplete tail is simply retried on the next poll (spec 13.1). The
        generator returns when ``timeout`` seconds elapse with no new frame.
        """
        low, high = self._bounds(start, end)
        served = 0
        deadline = None if timeout is None else _time.monotonic() + timeout
        while True:
            while served < len(self._entries):
                entry = self._entries[served]
                served += 1
                if high is not None and entry.meta.ts_min >= high:
                    continue
                if low is not None and entry.meta.ts_max < low:
                    continue
                yield Block(self, entry)
            if self._catch_up():
                deadline = None if timeout is None else _time.monotonic() + timeout
                continue
            if deadline is not None and _time.monotonic() >= deadline:
                return
            _time.sleep(poll_interval)


def open_reader(path: str | os.PathLike, *, strict: bool = False) -> Reader:
    """Open an Acta file for reading. ``strict`` also validates body CRCs."""
    return Reader(path, strict=strict)
