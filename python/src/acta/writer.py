"""Sequential block writer: create, append, flush, and append-to-existing."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

import numpy as np

from .columns import is_bytes_like, normalize
from .constants import BLOCK_HEADER
from .encode import BlockOptions, encode_block
from .enums import FEATURE_ROW_IDS, FrameType
from .errors import ActaError, CorruptionError, SchemaError
from .framing import make_frame, make_prologue, scan_file
from .schema import Schema

_SYNC_MODES = ("frame", "close", "none")


class Writer:
    """Appends immutable data blocks to one Acta file.

    Rows accumulate in an in-memory buffer; each :meth:`flush` (explicit, or
    automatic at ``block_rows``/``block_bytes``) encodes the buffer as one
    data frame and appends it with a single write. Previously committed
    frames are never touched.
    """

    def __init__(
        self,
        path: str | os.PathLike,
        schema: Schema,
        file,
        *,
        row_ids: bool,
        next_sequence: int,
        next_base_row_id: int,
        block_rows: int = 65_536,
        block_bytes: int = 64 << 20,
        sync: str = "close",
        options: BlockOptions | None = None,
    ) -> None:
        if sync not in _SYNC_MODES:
            raise ValueError(f"sync must be one of {_SYNC_MODES}")
        if block_rows < 1:
            raise ValueError("block_rows must be positive")
        self.path = Path(path)
        self.schema = schema
        self.options = options or BlockOptions()
        self.block_rows = block_rows
        self.block_bytes = block_bytes
        self.sync_mode = sync
        self.rows_written = 0
        self.blocks_written = 0
        self._file = file
        self._row_ids = row_ids
        self._next_sequence = next_sequence
        self._next_base_row_id = next_base_row_id
        self._chunks: dict[int, list] = {column.id: [] for column in schema}
        self._buffered_rows = 0
        self._buffered_bytes = 0
        self._closed = False

    # -- lifecycle ---------------------------------------------------------

    def __enter__(self) -> "Writer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.close()
        else:
            # Do not commit a partially appended buffer on error; frames
            # already written remain complete and valid.
            self.close(flush=False)

    @property
    def buffered_rows(self) -> int:
        return self._buffered_rows

    @property
    def row_ids(self) -> bool:
        return self._row_ids

    def close(self, *, flush: bool = True) -> None:
        if self._closed:
            return
        try:
            if flush:
                self.flush()
            if self.sync_mode in ("frame", "close"):
                self._file.flush()
                os.fsync(self._file.fileno())
        finally:
            self._closed = True
            self._file.close()

    # -- appending ---------------------------------------------------------

    def append(self, columns: Mapping[str, object]) -> None:
        """Buffer one batch of rows given as ``{column name: values}``."""
        if self._closed:
            raise ActaError("writer is closed")
        provided = set(columns)
        expected = {column.name for column in self.schema}
        if provided != expected:
            missing = sorted(expected - provided)
            unknown = sorted(provided - expected)
            detail = []
            if missing:
                detail.append(f"missing columns {missing}")
            if unknown:
                detail.append(f"unknown columns {unknown}")
            raise SchemaError("append batch: " + ", ".join(detail))

        batch: dict[int, tuple[object, np.ndarray | None]] = {}
        batch_rows: int | None = None
        batch_bytes = 0
        for column in self.schema:
            dense, validity, rows = normalize(column, columns[column.name])
            if batch_rows is None:
                batch_rows = rows
            elif rows != batch_rows:
                raise SchemaError(
                    f"append batch: column {column.name!r} has {rows} rows, "
                    f"expected {batch_rows}"
                )
            batch[column.id] = (dense, validity)
            if is_bytes_like(column):
                batch_bytes += sum(len(v) for v in dense) + 4 * rows
            else:
                batch_bytes += dense.nbytes
        if not batch_rows:
            return
        for column_id, chunk in batch.items():
            self._chunks[column_id].append((chunk[0], chunk[1], batch_rows))
        self._buffered_rows += batch_rows
        self._buffered_bytes += batch_bytes
        while self._buffered_rows >= self.block_rows or (
            self._buffered_rows and self._buffered_bytes >= self.block_bytes
        ):
            self._flush_rows(min(self._buffered_rows, self.block_rows))

    def flush(self) -> int | None:
        """Commit the buffered rows as one block; returns its file offset."""
        if self._closed:
            raise ActaError("writer is closed")
        if not self._buffered_rows:
            return None
        return self._flush_rows(self._buffered_rows)

    def sync(self) -> None:
        self._file.flush()
        os.fsync(self._file.fileno())

    # -- internals ---------------------------------------------------------

    def _take_rows(
        self, column_id: int, count: int
    ) -> tuple[object, np.ndarray | None]:
        """Remove the first ``count`` rows for one column from the buffer."""
        column = self.schema.column_by_id(column_id)
        dense_parts: list = []
        validity_parts: list[np.ndarray] = []
        any_null = False
        taken = 0
        chunks = self._chunks[column_id]
        while taken < count:
            dense, validity, rows = chunks[0]
            take = min(rows, count - taken)
            if take == rows:
                chunks.pop(0)
                part_dense, part_validity = dense, validity
            else:
                if validity is None:
                    keep_validity = None
                    part_validity = None
                    dense_take = take
                else:
                    part_validity = validity[:take]
                    keep_validity = validity[take:]
                    dense_take = int(part_validity.sum())
                part_dense = dense[:dense_take]
                chunks[0] = (dense[dense_take:], keep_validity, rows - take)
            dense_parts.append(part_dense)
            if part_validity is None:
                validity_parts.append(np.ones(take, dtype=np.bool_))
            else:
                validity_parts.append(np.asarray(part_validity, dtype=np.bool_))
                if not validity_parts[-1].all():
                    any_null = True
            taken += take
        if is_bytes_like(column):
            dense_all: object = [value for part in dense_parts for value in part]
        elif dense_parts:
            dense_all = np.concatenate(dense_parts)
        else:
            dense_all = np.empty(0, dtype=np.bool_)
        validity_all = np.concatenate(validity_parts) if any_null else None
        return dense_all, validity_all

    def _flush_rows(self, count: int) -> int:
        dense_by_id: dict[int, object] = {}
        validity_by_id: dict[int, np.ndarray | None] = {}
        estimated = 0
        for column in self.schema:
            dense, validity = self._take_rows(column.id, count)
            dense_by_id[column.id] = dense
            validity_by_id[column.id] = validity
            if is_bytes_like(column):
                estimated += sum(len(v) for v in dense) + 4 * count
            else:
                estimated += dense.nbytes
        base_row_id = self._next_base_row_id if self._row_ids else None
        header, payload = encode_block(
            self.schema,
            dense_by_id,
            validity_by_id,
            count,
            base_row_id=base_row_id,
            options=self.options,
        )
        frame = make_frame(FrameType.DATA, self._next_sequence, header, payload)
        self._file.seek(0, os.SEEK_END)
        offset = self._file.tell()
        self._file.write(frame)
        self._file.flush()
        if self.sync_mode == "frame":
            os.fsync(self._file.fileno())
        self._next_sequence += 1
        if self._row_ids:
            self._next_base_row_id += count
        self._buffered_rows -= count
        self._buffered_bytes = max(0, self._buffered_bytes - estimated)
        self.rows_written += count
        self.blocks_written += 1
        return offset


def create(
    path: str | os.PathLike,
    schema: Schema,
    *,
    row_ids: bool = False,
    block_rows: int = 65_536,
    block_bytes: int = 64 << 20,
    zstd_level: int = 3,
    sync: str = "close",
    file_id: bytes | None = None,
    options: BlockOptions | None = None,
) -> Writer:
    """Create a new Acta file and return a :class:`Writer` positioned on it.

    Refuses to overwrite an existing file. The prologue and schema frame are
    committed immediately; a file closed without any appended rows is valid
    and empty.
    """
    if file_id is None:
        file_id = os.urandom(16)
    if options is None:
        options = BlockOptions(zstd_level=zstd_level)
    file = open(path, "x+b")
    try:
        file.write(make_prologue(file_id, FEATURE_ROW_IDS if row_ids else 0))
        file.write(
            make_frame(
                FrameType.SCHEMA, 0, schema.to_frame_header(), schema.to_frame_payload()
            )
        )
        file.flush()
        if sync == "frame":
            os.fsync(file.fileno())
    except BaseException:
        file.close()
        raise
    return Writer(
        path,
        schema,
        file,
        row_ids=row_ids,
        next_sequence=1,
        next_base_row_id=0,
        block_rows=block_rows,
        block_bytes=block_bytes,
        sync=sync,
        options=options,
    )


def open_append(
    path: str | os.PathLike,
    *,
    expected_schema: Schema | None = None,
    block_rows: int = 65_536,
    block_bytes: int = 64 << 20,
    zstd_level: int = 3,
    sync: str = "close",
    options: BlockOptions | None = None,
) -> Writer:
    """Resume appending to an existing, healthy Acta file.

    A file with an interrupted tail is refused; run
    ``acta.recover(path, truncate=True)`` first. The writer never truncates
    on its own.
    """
    file = open(path, "r+b")
    try:
        file_size = os.fstat(file.fileno()).st_size
        result = scan_file(file, file_size, strict_body=False, with_payload=False)
        if result.incomplete_tail:
            raise CorruptionError(
                f"{path} has an incomplete tail at offset "
                f"{result.last_good_offset}; run acta.recover(path, "
                "truncate=True) before appending",
                offset=result.last_good_offset,
            )
        if not result.frames or result.frames[0].frame_type != FrameType.SCHEMA:
            raise CorruptionError("file does not begin with a schema frame")
        schema_frame = result.frames[0]
        file.seek(schema_frame.payload_offset)
        schema_payload = file.read(schema_frame.payload_length)
        schema = Schema.from_frame(schema_frame.header, schema_payload)
        if expected_schema is not None and schema != expected_schema:
            raise SchemaError("existing file schema does not match expected_schema")
        rows = 0
        for frame in result.frames[1:]:
            if frame.frame_type != FrameType.DATA:
                raise CorruptionError(
                    f"unsupported frame type {frame.frame_type} at offset "
                    f"{frame.offset}",
                    offset=frame.offset,
                )
            rows += BLOCK_HEADER.unpack_from(frame.header)[2]
        file.seek(result.last_good_offset)
    except BaseException:
        file.close()
        raise
    if options is None:
        options = BlockOptions(zstd_level=zstd_level)
    return Writer(
        path,
        schema,
        file,
        row_ids=result.prologue.row_ids,
        next_sequence=result.frames[-1].sequence + 1,
        next_base_row_id=rows,
        block_rows=block_rows,
        block_bytes=block_bytes,
        sync=sync,
        options=options,
    )
