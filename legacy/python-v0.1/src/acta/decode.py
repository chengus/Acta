"""Data-frame header parsing and block decoding (spec section 8)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np

from .columns import (
    FIXED_DTYPES,
    ColumnData,
    canonical_width,
    is_bytes_like,
    scatter,
    split_varwidth,
)
from .constants import (
    BLOCK_HEADER,
    COLUMN_DESCRIPTOR,
    STREAM_DESCRIPTOR,
    UINT64_MAX,
    crc32c,
)
from .enums import (
    BLOCK_ROW_IDS,
    BLOCK_TS_SORTED,
    COLDESC_HAS_STATS,
    COLDESC_IMPLICIT_VALIDITY,
    Codec,
    ColumnLayout,
    LogicalType,
    StatsKind,
    StreamKind,
    Transform,
)
from .errors import CorruptionError
from .schema import Column, Schema
from . import transforms

StreamReader = Callable[["StreamMeta"], bytes]


@dataclass(frozen=True)
class StreamMeta:
    index: int
    kind: StreamKind
    transform: Transform
    codec: Codec
    payload_offset: int
    stored_length: int
    transformed_length: int
    element_count: int
    crc: int


@dataclass(frozen=True)
class ColumnBlockMeta:
    column: Column
    layout: ColumnLayout
    flags: int
    null_count: int
    dense_count: int
    first_stream: int
    stream_count: int
    stats_kind: int
    stats_offset: int
    stats_length: int


@dataclass(frozen=True)
class BlockMeta:
    schema_id: int
    base_row_id: int | None
    row_count: int
    ts_min: int
    ts_max: int
    flags: int
    columns: tuple[ColumnBlockMeta, ...]
    streams: tuple[StreamMeta, ...]
    header: bytes

    @property
    def ts_sorted(self) -> bool:
        return bool(self.flags & BLOCK_TS_SORTED)

    def column_meta(self, name: str) -> ColumnBlockMeta:
        for meta in self.columns:
            if meta.column.name == name:
                return meta
        raise KeyError(name)


def parse_block_header(
    header: bytes,
    schema: Schema,
    *,
    payload_length: int,
    file_row_ids: bool,
    offset: int | None = None,
) -> BlockMeta:
    def bad(message: str) -> CorruptionError:
        return CorruptionError(f"data block: {message}", offset=offset)

    if len(header) < BLOCK_HEADER.size:
        raise bad("header is shorter than 64 bytes")
    (
        schema_id,
        base_row_id,
        row_count,
        column_count,
        ts_min,
        ts_max,
        column_table_offset,
        stream_table_offset,
        stats_offset,
        stats_length,
        flags,
        reserved,
    ) = BLOCK_HEADER.unpack_from(header)
    if reserved:
        raise bad("nonzero reserved header field")
    if schema_id != schema.schema_id:
        raise bad(f"schema ID {schema_id} does not match the schema frame")
    if column_count != len(schema):
        raise bad("column count does not match the schema frame")
    if row_count == 0:
        raise bad("empty data blocks are invalid")
    if flags & ~(BLOCK_ROW_IDS | BLOCK_TS_SORTED):
        raise bad(f"unknown block flags 0x{flags:x}")
    if bool(flags & BLOCK_ROW_IDS) != file_row_ids:
        raise bad("ROW_IDS block flag does not match the file feature flag")
    if file_row_ids:
        if base_row_id == UINT64_MAX:
            raise bad("ROW_IDS block is missing its base row ID")
    elif base_row_id != UINT64_MAX:
        raise bad("base row ID must be UINT64_MAX when row IDs are disabled")
    if column_table_offset != BLOCK_HEADER.size:
        raise bad("column table offset must be 64")
    expected_stream_table = BLOCK_HEADER.size + COLUMN_DESCRIPTOR.size * column_count
    if stream_table_offset != expected_stream_table:
        raise bad("stream table does not immediately follow the column table")
    if stats_offset < stream_table_offset:
        raise bad("statistics area precedes the stream table")
    if (stats_offset - stream_table_offset) % STREAM_DESCRIPTOR.size:
        raise bad("stream table length is not a multiple of 48")
    stream_count = (stats_offset - stream_table_offset) // STREAM_DESCRIPTOR.size
    if stats_offset + stats_length > len(header):
        raise bad("statistics area overruns the frame header")

    streams: list[StreamMeta] = []
    for index in range(stream_count):
        (
            kind,
            transform,
            codec,
            stream_flags,
            payload_offset,
            stored_length,
            transformed_length,
            element_count,
            crc,
            stream_reserved,
        ) = STREAM_DESCRIPTOR.unpack_from(
            header, stream_table_offset + index * STREAM_DESCRIPTOR.size
        )
        if stream_flags or stream_reserved:
            raise bad(f"stream {index}: nonzero flags or reserved field")
        try:
            kind = StreamKind(kind)
            transform = Transform(transform)
            codec = Codec(codec)
        except ValueError as error:
            raise bad(f"stream {index}: {error}") from None
        if payload_offset % 8:
            raise bad(f"stream {index}: payload offset is not eight-byte aligned")
        if payload_offset + stored_length > payload_length:
            raise bad(f"stream {index}: stored range overruns the frame payload")
        if codec == Codec.NONE and stored_length != transformed_length:
            raise bad(f"stream {index}: uncompressed lengths disagree")
        streams.append(
            StreamMeta(
                index,
                kind,
                transform,
                codec,
                payload_offset,
                stored_length,
                transformed_length,
                element_count,
                crc,
            )
        )
    for earlier, later in zip(
        sorted(streams, key=lambda s: s.payload_offset),
        sorted(streams, key=lambda s: s.payload_offset)[1:],
    ):
        if earlier.payload_offset + earlier.stored_length > later.payload_offset:
            raise bad("stream ranges overlap")

    columns: list[ColumnBlockMeta] = []
    schema_columns = sorted(schema.columns, key=lambda c: c.id)
    for position, column in enumerate(schema_columns):
        (
            column_id,
            layout,
            column_flags,
            null_count,
            dense_count,
            first_stream,
            column_stream_count,
            stats_kind,
            column_stats_offset,
            column_stats_length,
        ) = COLUMN_DESCRIPTOR.unpack_from(
            header, column_table_offset + position * COLUMN_DESCRIPTOR.size
        )
        name = column.name
        if column_id != column.id:
            raise bad(f"column {name!r}: descriptor order does not match the schema")
        try:
            layout = ColumnLayout(layout)
        except ValueError:
            raise bad(f"column {name!r}: unknown layout {layout}") from None
        if column_flags & ~(COLDESC_IMPLICIT_VALIDITY | COLDESC_HAS_STATS):
            raise bad(f"column {name!r}: unknown descriptor flags 0x{column_flags:x}")
        if dense_count != row_count - null_count:
            raise bad(f"column {name!r}: dense count does not equal rows minus nulls")
        if not column.nullable and null_count:
            raise bad(f"column {name!r}: non-nullable column has nulls")
        if first_stream + column_stream_count > stream_count:
            raise bad(f"column {name!r}: stream range overruns the stream table")
        if stats_kind not in (StatsKind.NONE, StatsKind.MIN_MAX):
            raise bad(f"column {name!r}: unknown statistics kind {stats_kind}")
        has_stats = bool(column_flags & COLDESC_HAS_STATS)
        if has_stats != (stats_kind != StatsKind.NONE):
            raise bad(f"column {name!r}: statistics flag and kind disagree")
        if has_stats and not (
            stats_offset
            <= column_stats_offset
            <= column_stats_offset + column_stats_length
            <= stats_offset + stats_length
        ):
            raise bad(f"column {name!r}: statistics lie outside the statistics area")
        columns.append(
            ColumnBlockMeta(
                column,
                layout,
                column_flags,
                null_count,
                dense_count,
                first_stream,
                column_stream_count,
                stats_kind,
                column_stats_offset,
                column_stats_length,
            )
        )

    primary_meta = next(c for c in columns if c.column.id == schema.primary_id)
    if primary_meta.null_count:
        raise bad("primary timestamp column must be non-nullable")
    return BlockMeta(
        schema_id=schema_id,
        base_row_id=base_row_id if file_row_ids else None,
        row_count=row_count,
        ts_min=ts_min,
        ts_max=ts_max,
        flags=flags,
        columns=tuple(columns),
        streams=tuple(streams),
        header=header,
    )


def read_stream_from_payload(payload: bytes, stream: StreamMeta) -> bytes:
    """Extract, CRC-check, and decompress one stream from a full payload."""
    stored = payload[
        stream.payload_offset : stream.payload_offset + stream.stored_length
    ]
    if len(stored) != stream.stored_length:
        raise CorruptionError(f"stream {stream.index}: stored bytes are truncated")
    if crc32c(stored) != stream.crc:
        raise CorruptionError(f"stream {stream.index}: bad stream CRC32C")
    if stream.codec == Codec.ZSTD:
        return transforms.decompress(stored, stream.transformed_length)
    return stored


def _streams_by_kind(
    block: BlockMeta, meta: ColumnBlockMeta
) -> dict[StreamKind, StreamMeta]:
    result: dict[StreamKind, StreamMeta] = {}
    for stream in block.streams[
        meta.first_stream : meta.first_stream + meta.stream_count
    ]:
        if stream.kind in result:
            raise CorruptionError(
                f"column {meta.column.name!r}: duplicate {stream.kind.name} stream"
            )
        result[stream.kind] = stream
    return result


def _decode_bool_stream(
    payload: bytes, count: int, transform: Transform, what: str
) -> np.ndarray:
    if transform == Transform.RAW:
        return transforms.decode_bool_raw(payload, count)
    if transform == Transform.BOOLEAN_RLE:
        return transforms.decode_boolean_rle(payload, count)
    if transform == Transform.BIT_PACKED:
        return transforms.bitunpack(payload, count) != 0
    raise CorruptionError(f"{what}: transform {transform.name} is not boolean")


def _decode_numeric_stream(
    payload: bytes, count: int, transform: Transform, dtype: np.dtype, what: str
) -> np.ndarray:
    if transform == Transform.RAW:
        if len(payload) < count * dtype.itemsize:
            raise CorruptionError(f"{what}: raw stream is shorter than its elements")
        return np.frombuffer(payload, dtype=dtype, count=count).copy()
    if transform == Transform.BIT_PACKED:
        return transforms.decode_bit_packed(payload, count, dtype)
    if transform == Transform.FRAME_OF_REFERENCE:
        return transforms.decode_frame_of_reference(payload, count, dtype)
    if transform == Transform.DELTA:
        return transforms.decode_delta(payload, count, dtype)
    if transform == Transform.DELTA_OF_DELTA:
        return transforms.decode_delta_of_delta(payload, count, dtype)
    if transform == Transform.BYTE_STREAM_SPLIT:
        raw = transforms.decode_byte_stream_split(payload, count, dtype.itemsize)
        return np.frombuffer(raw, dtype=dtype, count=count).copy()
    raise CorruptionError(f"{what}: transform {transform.name} does not apply")


def _decode_lengths(
    payload: bytes, count: int, transform: Transform, what: str
) -> np.ndarray:
    return _decode_numeric_stream(
        payload, count, transform, np.dtype(np.uint32), what
    ).astype(np.int64)


def _require(
    streams: dict[StreamKind, StreamMeta], kind: StreamKind, name: str
) -> StreamMeta:
    try:
        return streams[kind]
    except KeyError:
        raise CorruptionError(
            f"column {name!r}: missing required {kind.name} stream"
        ) from None


def _expect_elements(stream: StreamMeta, expected: int, name: str) -> None:
    if stream.element_count != expected:
        raise CorruptionError(
            f"column {name!r}: {stream.kind.name} stream declares "
            f"{stream.element_count} elements, expected {expected}"
        )


def _decode_bytes_values(
    column: Column,
    streams: dict[StreamKind, StreamMeta],
    read: StreamReader,
    values_kind: StreamKind,
    lengths_kind: StreamKind,
    count: int,
) -> list[bytes]:
    name = column.name
    values_stream = _require(streams, values_kind, name)
    _expect_elements(values_stream, count, name)
    payload = read(values_stream)
    if column.type == LogicalType.FIXED_BINARY:
        width = column.width
        if values_stream.transform == Transform.BYTE_STREAM_SPLIT:
            payload = transforms.decode_byte_stream_split(payload, count, width)
        elif values_stream.transform != Transform.RAW:
            raise CorruptionError(
                f"column {name!r}: transform {values_stream.transform.name} "
                "does not apply to fixed binary"
            )
        if len(payload) != count * width:
            raise CorruptionError(f"column {name!r}: fixed-binary length mismatch")
        return [payload[i * width : (i + 1) * width] for i in range(count)]
    if values_stream.transform != Transform.RAW:
        raise CorruptionError(
            f"column {name!r}: variable-width values must use the raw transform"
        )
    lengths_stream = _require(streams, lengths_kind, name)
    _expect_elements(lengths_stream, count, name)
    lengths = _decode_lengths(
        read(lengths_stream),
        count,
        lengths_stream.transform,
        f"column {name!r} lengths",
    )
    if int(lengths.sum()) != len(payload):
        raise CorruptionError(
            f"column {name!r}: value lengths do not consume the values stream"
        )
    return split_varwidth(payload, lengths)


def decode_column(
    block: BlockMeta,
    meta: ColumnBlockMeta,
    read: StreamReader,
) -> ColumnData:
    """Decode one column of a block into full-row-length values."""
    column = meta.column
    name = column.name
    row_count = block.row_count
    streams = _streams_by_kind(block, meta)

    # Resolve validity.
    validity: np.ndarray | None = None
    if column.nullable:
        if meta.flags & COLDESC_IMPLICIT_VALIDITY:
            if meta.null_count == row_count:
                validity = np.zeros(row_count, dtype=np.bool_)
            elif meta.null_count != 0:
                raise CorruptionError(
                    f"column {name!r}: implicit validity with partial nulls"
                )
        elif meta.null_count:
            stream = _require(streams, StreamKind.VALIDITY, name)
            _expect_elements(stream, row_count, name)
            validity = _decode_bool_stream(
                read(stream), row_count, stream.transform, f"column {name!r} validity"
            )
            if int((~validity).sum()) != meta.null_count:
                raise CorruptionError(
                    f"column {name!r}: validity stream does not match the null count"
                )
        elif StreamKind.VALIDITY in streams:
            stream = streams[StreamKind.VALIDITY]
            _expect_elements(stream, row_count, name)
            validity = _decode_bool_stream(
                read(stream), row_count, stream.transform, f"column {name!r} validity"
            )
            if not validity.all():
                raise CorruptionError(
                    f"column {name!r}: validity stream does not match the null count"
                )

    dense_count = meta.dense_count
    if dense_count == 0:
        dense: np.ndarray | list[bytes] = (
            []
            if is_bytes_like(column)
            else np.empty(0, dtype=FIXED_DTYPES[column.type])
        )
        return ColumnData(column, scatter(column, dense, validity, row_count), validity)

    dtype = None if is_bytes_like(column) else FIXED_DTYPES[column.type]

    if meta.layout == ColumnLayout.PLAIN:
        if column.type == LogicalType.BOOL:
            stream = _require(streams, StreamKind.VALUES, name)
            _expect_elements(stream, dense_count, name)
            dense = _decode_bool_stream(
                read(stream), dense_count, stream.transform, f"column {name!r}"
            )
        elif is_bytes_like(column):
            dense = _decode_bytes_values(
                column,
                streams,
                read,
                StreamKind.VALUES,
                StreamKind.LENGTHS,
                dense_count,
            )
        else:
            stream = _require(streams, StreamKind.VALUES, name)
            _expect_elements(stream, dense_count, name)
            dense = _decode_numeric_stream(
                read(stream),
                dense_count,
                stream.transform,
                dtype,
                f"column {name!r}",
            )
    elif meta.layout == ColumnLayout.CONSTANT:
        if column.type == LogicalType.BOOL:
            stream = _require(streams, StreamKind.VALUES, name)
            _expect_elements(stream, 1, name)
            value = _decode_bool_stream(
                read(stream), 1, stream.transform, f"column {name!r}"
            )[0]
            dense = np.full(dense_count, value, dtype=np.bool_)
        elif is_bytes_like(column):
            single = _decode_bytes_values(
                column, streams, read, StreamKind.VALUES, StreamKind.LENGTHS, 1
            )[0]
            dense = [single] * dense_count
        else:
            stream = _require(streams, StreamKind.VALUES, name)
            _expect_elements(stream, 1, name)
            value = _decode_numeric_stream(
                read(stream), 1, stream.transform, dtype, f"column {name!r}"
            )[0]
            dense = np.full(dense_count, value, dtype=dtype)
    elif meta.layout == ColumnLayout.DICTIONARY:
        indices_stream = _require(streams, StreamKind.INDICES, name)
        _expect_elements(indices_stream, dense_count, name)
        if indices_stream.transform != Transform.BIT_PACKED:
            raise CorruptionError(
                f"column {name!r}: dictionary indices must be bit packed"
            )
        indices = transforms.bitunpack(read(indices_stream), dense_count).astype(
            np.int64
        )
        dict_stream = _require(streams, StreamKind.DICTIONARY_VALUES, name)
        dictionary_size = dict_stream.element_count
        if len(indices) and int(indices.max()) >= dictionary_size:
            raise CorruptionError(f"column {name!r}: dictionary index out of range")
        if is_bytes_like(column):
            dictionary = _decode_bytes_values(
                column,
                streams,
                read,
                StreamKind.DICTIONARY_VALUES,
                StreamKind.DICTIONARY_LENGTHS,
                dictionary_size,
            )
            dense = [dictionary[index] for index in indices.tolist()]
        else:
            dictionary = _decode_numeric_stream(
                read(dict_stream),
                dictionary_size,
                dict_stream.transform,
                dtype,
                f"column {name!r} dictionary",
            )
            dense = dictionary[indices]
    elif meta.layout == ColumnLayout.RUN_LENGTH:
        lengths_stream = _require(streams, StreamKind.RUN_LENGTHS, name)
        run_count = lengths_stream.element_count
        if lengths_stream.transform != Transform.BIT_PACKED:
            raise CorruptionError(f"column {name!r}: run lengths must be bit packed")
        run_lengths = transforms.bitunpack(read(lengths_stream), run_count).astype(
            np.int64
        )
        if np.any(run_lengths <= 0) or int(run_lengths.sum()) != dense_count:
            raise CorruptionError(
                f"column {name!r}: run lengths do not sum to the dense count"
            )
        if is_bytes_like(column):
            run_values = _decode_bytes_values(
                column,
                streams,
                read,
                StreamKind.RUN_VALUES,
                StreamKind.LENGTHS,
                run_count,
            )
            dense = [
                value
                for value, length in zip(run_values, run_lengths.tolist())
                for _ in range(length)
            ]
        elif column.type == LogicalType.BOOL:
            stream = _require(streams, StreamKind.RUN_VALUES, name)
            _expect_elements(stream, run_count, name)
            run_values = _decode_bool_stream(
                read(stream), run_count, stream.transform, f"column {name!r}"
            )
            dense = np.repeat(run_values, run_lengths)
        else:
            stream = _require(streams, StreamKind.RUN_VALUES, name)
            _expect_elements(stream, run_count, name)
            run_values = _decode_numeric_stream(
                read(stream), run_count, stream.transform, dtype, f"column {name!r}"
            )
            dense = np.repeat(run_values, run_lengths)
    else:  # pragma: no cover - ColumnLayout already validated
        raise CorruptionError(f"column {name!r}: unsupported layout")

    return ColumnData(column, scatter(column, dense, validity, row_count), validity)


def parse_min_max_stats(column: Column, data: bytes) -> tuple[object, object]:
    """Parse a kind-1 statistics record: canonical min then max."""
    width = canonical_width(column)
    if len(data) != 2 * width:
        raise CorruptionError(
            f"column {column.name!r}: statistics length does not match kind 1"
        )
    if column.type == LogicalType.BOOL:
        return bool(data[0]), bool(data[1])
    if column.type == LogicalType.FIXED_BINARY:
        return data[:width], data[width:]
    dtype = FIXED_DTYPES[column.type]
    values = np.frombuffer(data, dtype=dtype, count=2)
    return values[0], values[1]


def verify_ts_sorted(block: BlockMeta, primary_values: np.ndarray) -> None:
    """Strict-reader TS_SORTED semantic verification (spec section 8)."""
    values = primary_values.astype(np.int64)
    if not block.ts_sorted:
        return
    if len(values) > 1 and np.any(np.diff(values) < 0):
        raise CorruptionError(
            "TS_SORTED is set but primary timestamps decrease within the block"
        )
    if int(values[0]) != block.ts_min or int(values[-1]) != block.ts_max:
        raise CorruptionError(
            "TS_SORTED requires header bounds equal to the first and last values"
        )


def verify_block_bounds(block: BlockMeta, primary_values: np.ndarray) -> None:
    values = primary_values.astype(np.int64)
    if int(values.min()) != block.ts_min or int(values.max()) != block.ts_max:
        raise CorruptionError(
            "block header bounds do not match the stored primary timestamps"
        )
