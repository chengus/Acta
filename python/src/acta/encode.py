"""Block encoding: candidate generation, selection (spec 12), and assembly."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

import numpy as np

from .columns import encode_varwidth, is_bytes_like
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
from .schema import Column, Schema
from . import transforms

_INT_LIKE = {
    LogicalType.INT8,
    LogicalType.INT16,
    LogicalType.INT32,
    LogicalType.INT64,
    LogicalType.UINT8,
    LogicalType.UINT16,
    LogicalType.UINT32,
    LogicalType.UINT64,
    LogicalType.DECIMAL64,
    LogicalType.TIMESTAMP64,
    LogicalType.DATE32,
}


@dataclass(frozen=True)
class BlockOptions:
    """Writer knobs threaded down to block encoding.

    ``force_plain_raw`` disables encoding selection and compression so output
    matches the auditable plain/raw representation used by the spec fixtures.
    ``write_stats`` selects which fixed-width columns carry kind-1 min/max
    statistics: ``"non_primary"`` (default), ``"all"``, or ``"none"``.
    ``implicit_flag_non_nullable`` additionally sets column-descriptor flag
    bit 0 on non-nullable columns, matching the minimal/ts_sorted fixtures.
    ``set_ts_sorted`` controls whether the writer declares the TS_SORTED
    block flag when the primary values are nondecreasing (a spec SHOULD).
    """

    force_plain_raw: bool = False
    compress: bool = True
    zstd_level: int = 3
    write_stats: str = "non_primary"
    implicit_flag_non_nullable: bool = False
    set_ts_sorted: bool = True


@dataclass(frozen=True)
class StreamSpec:
    kind: StreamKind
    transform: Transform
    element_count: int
    data: bytes  # transformed, uncompressed


@dataclass(frozen=True)
class Candidate:
    layout: ColumnLayout
    streams: tuple[StreamSpec, ...]


@dataclass
class _StoredStream:
    spec: StreamSpec
    codec: Codec
    stored: bytes


@dataclass
class _EncodedColumn:
    column: Column
    layout: ColumnLayout
    flags: int
    null_count: int
    dense_count: int
    streams: list[_StoredStream] = field(default_factory=list)
    stats: bytes = b""


def _run_boundaries(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    boundaries = np.flatnonzero(values[1:] != values[:-1]) + 1
    starts = np.concatenate((np.array([0], dtype=np.int64), boundaries))
    ends = np.concatenate((boundaries, np.array([len(values)], dtype=np.int64)))
    return starts, (ends - starts).astype(np.uint64)


def _plain_raw_candidate(column: Column, dense) -> Candidate:
    if column.type == LogicalType.BOOL:
        return Candidate(
            ColumnLayout.PLAIN,
            (
                StreamSpec(
                    StreamKind.VALUES,
                    Transform.RAW,
                    len(dense),
                    transforms.encode_bool_raw(dense),
                ),
            ),
        )
    if column.type == LogicalType.FIXED_BINARY:
        return Candidate(
            ColumnLayout.PLAIN,
            (
                StreamSpec(
                    StreamKind.VALUES, Transform.RAW, len(dense), b"".join(dense)
                ),
            ),
        )
    if is_bytes_like(column):
        values, lengths = encode_varwidth(dense)
        return Candidate(
            ColumnLayout.PLAIN,
            (
                StreamSpec(StreamKind.VALUES, Transform.RAW, len(dense), values),
                StreamSpec(StreamKind.LENGTHS, Transform.RAW, len(dense), lengths),
            ),
        )
    return Candidate(
        ColumnLayout.PLAIN,
        (StreamSpec(StreamKind.VALUES, Transform.RAW, len(dense), dense.tobytes()),),
    )


def _constant_candidate(column: Column, dense) -> Candidate | None:
    if column.type == LogicalType.BOOL:
        if not (dense.all() or not dense.any()):
            return None
        return Candidate(
            ColumnLayout.CONSTANT,
            (
                StreamSpec(
                    StreamKind.VALUES,
                    Transform.RAW,
                    1,
                    transforms.encode_bool_raw(dense[:1]),
                ),
            ),
        )
    if is_bytes_like(column):
        if any(value != dense[0] for value in dense[1:]):
            return None
        if column.type == LogicalType.FIXED_BINARY:
            return Candidate(
                ColumnLayout.CONSTANT,
                (StreamSpec(StreamKind.VALUES, Transform.RAW, 1, dense[0]),),
            )
        values, lengths = encode_varwidth(dense[:1])
        return Candidate(
            ColumnLayout.CONSTANT,
            (
                StreamSpec(StreamKind.VALUES, Transform.RAW, 1, values),
                StreamSpec(StreamKind.LENGTHS, Transform.RAW, 1, lengths),
            ),
        )
    # Compare bit patterns so NaN counts as a repeatable constant.
    bits = dense.view(np.uint8).reshape(len(dense), dense.dtype.itemsize)
    if np.any(bits != bits[0]):
        return None
    return Candidate(
        ColumnLayout.CONSTANT,
        (StreamSpec(StreamKind.VALUES, Transform.RAW, 1, dense[:1].tobytes()),),
    )


def _numeric_candidates(column: Column, dense: np.ndarray) -> list[Candidate]:
    candidates: list[Candidate] = []
    count = len(dense)

    def plain(transform: Transform, payload: bytes | None) -> None:
        if payload is not None:
            candidates.append(
                Candidate(
                    ColumnLayout.PLAIN,
                    (StreamSpec(StreamKind.VALUES, transform, count, payload),),
                )
            )

    plain(Transform.FRAME_OF_REFERENCE, transforms.encode_frame_of_reference(dense))
    plain(Transform.DELTA, transforms.encode_delta(dense))
    plain(Transform.DELTA_OF_DELTA, transforms.encode_delta_of_delta(dense))

    starts, run_lengths = _run_boundaries(dense)
    if len(starts) <= max(2, count // 2):
        candidates.append(
            Candidate(
                ColumnLayout.RUN_LENGTH,
                (
                    StreamSpec(
                        StreamKind.RUN_VALUES,
                        Transform.RAW,
                        len(starts),
                        dense[starts].tobytes(),
                    ),
                    StreamSpec(
                        StreamKind.RUN_LENGTHS,
                        Transform.BIT_PACKED,
                        len(starts),
                        transforms.bitpack(run_lengths),
                    ),
                ),
            )
        )
    return candidates


def _float_candidates(column: Column, dense: np.ndarray) -> list[Candidate]:
    raw = dense.tobytes()
    return [
        Candidate(
            ColumnLayout.PLAIN,
            (
                StreamSpec(
                    StreamKind.VALUES,
                    Transform.BYTE_STREAM_SPLIT,
                    len(dense),
                    transforms.encode_byte_stream_split(raw, dense.dtype.itemsize),
                ),
            ),
        )
    ]


def _bool_candidates(dense: np.ndarray) -> list[Candidate]:
    return [
        Candidate(
            ColumnLayout.PLAIN,
            (
                StreamSpec(
                    StreamKind.VALUES,
                    Transform.BOOLEAN_RLE,
                    len(dense),
                    transforms.encode_boolean_rle(dense),
                ),
            ),
        )
    ]


def _dictionary_candidate(column: Column, dense: Sequence[bytes]) -> Candidate | None:
    dictionary: list[bytes] = []
    codes: dict[bytes, int] = {}
    indices = np.empty(len(dense), dtype=np.uint64)
    for position, value in enumerate(dense):
        code = codes.get(value)
        if code is None:
            code = len(dictionary)
            codes[value] = code
            dictionary.append(value)
        indices[position] = code
    limit = min(65_536, max(1, len(dense) // 2))
    if column.type == LogicalType.CATEGORICAL:
        limit = min(65_536, max(1, len(dense)))
    if len(dictionary) > limit:
        return None
    streams = []
    if column.type == LogicalType.FIXED_BINARY:
        streams.append(
            StreamSpec(
                StreamKind.DICTIONARY_VALUES,
                Transform.RAW,
                len(dictionary),
                b"".join(dictionary),
            )
        )
    else:
        values, lengths = encode_varwidth(dictionary)
        streams.append(
            StreamSpec(
                StreamKind.DICTIONARY_VALUES, Transform.RAW, len(dictionary), values
            )
        )
        streams.append(
            StreamSpec(
                StreamKind.DICTIONARY_LENGTHS,
                Transform.RAW,
                len(dictionary),
                lengths,
            )
        )
    streams.append(
        StreamSpec(
            StreamKind.INDICES,
            Transform.BIT_PACKED,
            len(dense),
            transforms.bitpack(indices),
        )
    )
    return Candidate(ColumnLayout.DICTIONARY, tuple(streams))


def _fixed_binary_candidates(column: Column, dense: list[bytes]) -> list[Candidate]:
    candidates = []
    if column.width > 1:
        candidates.append(
            Candidate(
                ColumnLayout.PLAIN,
                (
                    StreamSpec(
                        StreamKind.VALUES,
                        Transform.BYTE_STREAM_SPLIT,
                        len(dense),
                        transforms.encode_byte_stream_split(
                            b"".join(dense), column.width
                        ),
                    ),
                ),
            )
        )
    dictionary = _dictionary_candidate(column, dense)
    if dictionary is not None:
        candidates.append(dictionary)
    return candidates


def _value_candidates(column: Column, dense) -> list[Candidate]:
    """All non-baseline candidates for one column's dense values."""
    candidates: list[Candidate] = []
    constant = _constant_candidate(column, dense)
    if constant is not None:
        candidates.append(constant)
    if column.type == LogicalType.BOOL:
        candidates.extend(_bool_candidates(dense))
    elif column.type in _INT_LIKE:
        candidates.extend(_numeric_candidates(column, dense))
    elif column.type in (LogicalType.FLOAT32, LogicalType.FLOAT64):
        candidates.extend(_float_candidates(column, dense))
    elif column.type == LogicalType.FIXED_BINARY:
        candidates.extend(_fixed_binary_candidates(column, dense))
    else:  # utf8, categorical, binary
        dictionary = _dictionary_candidate(column, dense)
        if dictionary is not None:
            candidates.append(dictionary)
    return candidates


def _store(spec: StreamSpec, options: BlockOptions) -> _StoredStream:
    """Apply the per-stream codec decision (spec 10: save at least 8 bytes)."""
    if options.compress and len(spec.data) > 8:
        compressed = transforms.compress(spec.data, options.zstd_level)
        if len(compressed) + 8 < len(spec.data):
            return _StoredStream(spec, Codec.ZSTD, compressed)
    return _StoredStream(spec, Codec.NONE, spec.data)


def _stored_cost(streams: list[_StoredStream]) -> int:
    aligned = sum(len(s.stored) + (-len(s.stored)) % 8 for s in streams)
    return aligned + STREAM_DESCRIPTOR.size * len(streams)


def _select(
    column: Column, dense, options: BlockOptions
) -> tuple[ColumnLayout, list[_StoredStream]]:
    baseline_candidate = _plain_raw_candidate(column, dense)
    baseline = [_store(spec, options) for spec in baseline_candidate.streams]
    if options.force_plain_raw:
        return baseline_candidate.layout, baseline
    baseline_cost = _stored_cost(baseline)
    best: tuple[int, Candidate, list[_StoredStream]] | None = None
    for candidate in _value_candidates(column, dense):
        stored = [_store(spec, options) for spec in candidate.streams]
        cost = _stored_cost(stored)
        if best is None or cost < best[0]:
            best = (cost, candidate, stored)
    # A specialized encoding must beat raw plus its codec by the larger of
    # 64 bytes or 1% (spec 12); otherwise raw keeps decode complexity flat.
    threshold = max(64, -(-baseline_cost // 100))
    if best is not None and baseline_cost - best[0] >= threshold:
        return best[1].layout, best[2]
    return baseline_candidate.layout, baseline


def _column_stats(column: Column, dense, primary: bool, options: BlockOptions) -> bytes:
    if options.write_stats == "none" or len(dense) == 0:
        return b""
    if options.write_stats == "non_primary" and primary:
        return b""
    if is_bytes_like(column):
        if column.type != LogicalType.FIXED_BINARY:
            return b""  # variable-width statistics are not written in v0.1
        return min(dense) + max(dense)
    if column.type == LogicalType.BOOL:
        return bytes([int(dense.min()), int(dense.max())])
    if column.type in (LogicalType.FLOAT32, LogicalType.FLOAT64):
        finite = dense[~np.isnan(dense)]
        if len(finite) == 0:
            return b""  # an all-NaN column has no min/max statistic
        return finite.min().tobytes() + finite.max().tobytes()
    return dense.min().tobytes() + dense.max().tobytes()


def encode_block(
    schema: Schema,
    dense_by_id: dict[int, object],
    validity_by_id: dict[int, np.ndarray | None],
    row_count: int,
    *,
    base_row_id: int | None,
    options: BlockOptions,
) -> tuple[bytes, bytes]:
    """Encode one block; returns the (header, payload) frame content."""
    if row_count == 0:
        raise ValueError("empty data blocks are invalid")

    encoded: list[_EncodedColumn] = []
    for column in sorted(schema.columns, key=lambda c: c.id):
        dense = dense_by_id[column.id]
        validity = validity_by_id.get(column.id)
        null_count = 0 if validity is None else int((~validity).sum())
        dense_count = row_count - null_count
        flags = 0
        streams: list[_StoredStream] = []
        if column.nullable:
            if null_count in (0, row_count):
                flags |= COLDESC_IMPLICIT_VALIDITY
            else:
                raw_bits = StreamSpec(
                    StreamKind.VALIDITY,
                    Transform.RAW,
                    row_count,
                    transforms.encode_bool_raw(validity),
                )
                rle = StreamSpec(
                    StreamKind.VALIDITY,
                    Transform.BOOLEAN_RLE,
                    row_count,
                    transforms.encode_boolean_rle(validity),
                )
                if options.force_plain_raw or len(rle.data) >= len(raw_bits.data):
                    streams.append(_store(raw_bits, options))
                else:
                    streams.append(_store(rle, options))
        elif options.implicit_flag_non_nullable:
            flags |= COLDESC_IMPLICIT_VALIDITY

        if dense_count:
            layout, value_streams = _select(column, dense, options)
            streams.extend(value_streams)
        else:
            layout = ColumnLayout.PLAIN

        stats = _column_stats(column, dense, column.id == schema.primary_id, options)
        if stats:
            flags |= COLDESC_HAS_STATS
        encoded.append(
            _EncodedColumn(
                column, layout, flags, null_count, dense_count, streams, stats
            )
        )

    # Block flags and primary bounds.
    primary_dense = np.asarray(dense_by_id[schema.primary_id]).astype(np.int64)
    flags = 0
    if base_row_id is not None:
        flags |= BLOCK_ROW_IDS
    ts_sorted = options.set_ts_sorted and (
        len(primary_dense) < 2 or bool(np.all(np.diff(primary_dense) >= 0))
    )
    if ts_sorted:
        flags |= BLOCK_TS_SORTED
        ts_min, ts_max = int(primary_dense[0]), int(primary_dense[-1])
    else:
        ts_min, ts_max = int(primary_dense.min()), int(primary_dense.max())

    # Lay out the payload and the header tables.
    payload = bytearray()
    stream_table = bytearray()
    column_table = bytearray()
    stats_area = bytearray()
    total_streams = sum(len(c.streams) for c in encoded)
    stream_table_offset = BLOCK_HEADER.size + COLUMN_DESCRIPTOR.size * len(encoded)
    stats_offset = stream_table_offset + STREAM_DESCRIPTOR.size * total_streams

    stream_index = 0
    for item in encoded:
        first_stream = stream_index
        for stored_stream in item.streams:
            spec = stored_stream.spec
            offset = len(payload)
            payload.extend(stored_stream.stored)
            payload.extend(bytes((-len(payload)) % 8))
            stream_table.extend(
                STREAM_DESCRIPTOR.pack(
                    spec.kind,
                    spec.transform,
                    stored_stream.codec,
                    0,
                    offset,
                    len(stored_stream.stored),
                    len(spec.data),
                    spec.element_count,
                    crc32c(stored_stream.stored),
                    0,
                )
            )
            stream_index += 1
        if item.stats:
            column_stats_offset = stats_offset + len(stats_area)
            stats_area.extend(item.stats)
        else:
            column_stats_offset = 0
        column_table.extend(
            COLUMN_DESCRIPTOR.pack(
                item.column.id,
                item.layout,
                item.flags,
                item.null_count,
                item.dense_count,
                first_stream,
                len(item.streams),
                StatsKind.MIN_MAX if item.stats else StatsKind.NONE,
                column_stats_offset,
                len(item.stats),
            )
        )

    block_header = BLOCK_HEADER.pack(
        schema.schema_id,
        base_row_id if base_row_id is not None else UINT64_MAX,
        row_count,
        len(encoded),
        ts_min,
        ts_max,
        BLOCK_HEADER.size,
        stream_table_offset,
        stats_offset,
        len(stats_area),
        flags,
        0,
    )
    header = bytes(block_header + column_table + stream_table + stats_area)
    return header, bytes(payload)


__all__ = ["BlockOptions", "encode_block"]
