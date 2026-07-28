"""Column value handling: dtype mapping, normalization, and result types.

Value streams are dense (spec section 3): they hold only non-null values in
row order. ``normalize`` turns user input into ``(dense, validity)`` and
``scatter`` reverses it after decoding.
"""

from __future__ import annotations

import datetime as _dt
import decimal as _decimal
from dataclasses import dataclass
from typing import Sequence

import numpy as np

from .enums import LogicalType, TimestampUnit
from .errors import SchemaError
from .schema import Column

FIXED_DTYPES: dict[LogicalType, np.dtype] = {
    LogicalType.BOOL: np.dtype(np.bool_),
    LogicalType.INT8: np.dtype(np.int8),
    LogicalType.INT16: np.dtype(np.int16),
    LogicalType.INT32: np.dtype(np.int32),
    LogicalType.INT64: np.dtype(np.int64),
    LogicalType.UINT8: np.dtype(np.uint8),
    LogicalType.UINT16: np.dtype(np.uint16),
    LogicalType.UINT32: np.dtype(np.uint32),
    LogicalType.UINT64: np.dtype(np.uint64),
    LogicalType.FLOAT32: np.dtype(np.float32),
    LogicalType.FLOAT64: np.dtype(np.float64),
    LogicalType.DECIMAL64: np.dtype(np.int64),
    LogicalType.TIMESTAMP64: np.dtype(np.int64),
    LogicalType.DATE32: np.dtype(np.int32),
}

_UNIT_SUFFIX = {
    TimestampUnit.SECOND: "s",
    TimestampUnit.MILLISECOND: "ms",
    TimestampUnit.MICROSECOND: "us",
    TimestampUnit.NANOSECOND: "ns",
}

_UNIT_PER_SECOND = {
    TimestampUnit.SECOND: 1,
    TimestampUnit.MILLISECOND: 1_000,
    TimestampUnit.MICROSECOND: 1_000_000,
    TimestampUnit.NANOSECOND: 1_000_000_000,
}

_EPOCH_DATE = _dt.date(1970, 1, 1)


def dtype_for(column: Column) -> np.dtype:
    return FIXED_DTYPES[column.type]


def canonical_width(column: Column) -> int:
    """Canonical value width in bytes, as used by statistics (spec 11)."""
    if column.type == LogicalType.BOOL:
        return 1
    if column.type == LogicalType.FIXED_BINARY:
        return column.width
    return FIXED_DTYPES[column.type].itemsize


def is_bytes_like(column: Column) -> bool:
    return column.type in (
        LogicalType.UTF8,
        LogicalType.CATEGORICAL,
        LogicalType.BINARY,
        LogicalType.FIXED_BINARY,
    )


@dataclass(frozen=True)
class ColumnData:
    """Decoded values for one column across the requested rows.

    ``values`` always has one entry per row. For bytes-like columns it is an
    object array (``str`` for utf8/categorical, ``bytes`` otherwise) with
    ``None`` at null positions; for fixed-width columns nulls hold the dtype's
    zero and ``validity`` distinguishes them. ``validity`` is ``None`` when
    every row is valid.
    """

    column: Column
    values: np.ndarray
    validity: np.ndarray | None

    def __len__(self) -> int:
        return len(self.values)

    def to_pylist(self) -> list[object]:
        items = self.values.tolist()
        if self.validity is None:
            return items
        return [item if valid else None for item, valid in zip(items, self.validity)]

    def to_datetime64(self) -> np.ndarray:
        if self.column.type == LogicalType.TIMESTAMP64:
            suffix = _UNIT_SUFFIX[self.column.unit]
            return self.values.astype(f"datetime64[{suffix}]")
        if self.column.type == LogicalType.DATE32:
            return self.values.astype("datetime64[D]")
        raise SchemaError(
            f"column {self.column.name!r} is not a timestamp64 or date32 column"
        )

    def to_decimal(self) -> list[_decimal.Decimal | None]:
        if self.column.type != LogicalType.DECIMAL64:
            raise SchemaError(f"column {self.column.name!r} is not decimal64")
        exponent = _decimal.Decimal(1).scaleb(-self.column.scale)
        return [
            None if value is None else _decimal.Decimal(int(value)) * exponent
            for value in self.to_pylist()
        ]


def _cast_checked(array: np.ndarray, dtype: np.dtype, name: str) -> np.ndarray:
    if array.dtype == dtype:
        return np.ascontiguousarray(array)
    try:
        converted = array.astype(dtype)
    except (OverflowError, ValueError, TypeError) as error:
        raise SchemaError(f"column {name!r}: values do not fit {dtype}") from error
    if dtype.kind in "iu":
        if not np.array_equal(converted.astype(object), array.astype(object)):
            raise SchemaError(f"column {name!r}: values do not fit {dtype}")
    return converted


def _timestamp_to_int(column: Column, value: object) -> int:
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, np.datetime64):
        suffix = _UNIT_SUFFIX[column.unit]
        return int(value.astype(f"datetime64[{suffix}]").astype(np.int64))
    if isinstance(value, _dt.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(_dt.timezone.utc).replace(tzinfo=None)
        delta = value - _dt.datetime(1970, 1, 1)
        microseconds = (
            delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
        )
        per_second = _UNIT_PER_SECOND[column.unit]
        if per_second >= 1_000_000:
            return microseconds * (per_second // 1_000_000)
        divisor = 1_000_000 // per_second
        if microseconds % divisor:
            raise SchemaError(
                f"column {column.name!r}: datetime is finer than the column unit"
            )
        return microseconds // divisor
    raise SchemaError(
        f"column {column.name!r}: cannot interpret {type(value).__name__} "
        "as a timestamp"
    )


def _date_to_int(column: Column, value: object) -> int:
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, np.datetime64):
        return int(value.astype("datetime64[D]").astype(np.int64))
    if isinstance(value, _dt.datetime):
        raise SchemaError(f"column {column.name!r}: expected a date, got a datetime")
    if isinstance(value, _dt.date):
        return (value - _EPOCH_DATE).days
    raise SchemaError(
        f"column {column.name!r}: cannot interpret {type(value).__name__} as a date"
    )


def time_bound_to_int(column: Column, value: object) -> int:
    """Convert a user-supplied time-range bound to primary-column units."""
    if column.type == LogicalType.DATE32:
        return _date_to_int(column, value)
    return _timestamp_to_int(column, value)


def _dense_fixed(column: Column, items: Sequence[object]) -> np.ndarray:
    dtype = FIXED_DTYPES[column.type]
    if column.type == LogicalType.TIMESTAMP64:
        return np.array(
            [_timestamp_to_int(column, item) for item in items], dtype=dtype
        )
    if column.type == LogicalType.DATE32:
        converted = [_date_to_int(column, item) for item in items]
        array = np.array(converted, dtype=np.int64)
        return _cast_checked(array, dtype, column.name)
    if column.type == LogicalType.DECIMAL64:
        unscaled = []
        for item in items:
            if isinstance(item, _decimal.Decimal):
                scaled = item.scaleb(column.scale)
                if scaled != scaled.to_integral_value():
                    raise SchemaError(
                        f"column {column.name!r}: {item} does not fit scale "
                        f"{column.scale}"
                    )
                unscaled.append(int(scaled))
            elif isinstance(item, (int, np.integer)):
                unscaled.append(int(item))
            else:
                raise SchemaError(
                    f"column {column.name!r}: decimal64 accepts int or Decimal"
                )
        return _cast_checked(np.array(unscaled, dtype=object), dtype, column.name)
    if column.type == LogicalType.BOOL:
        for item in items:
            if not isinstance(item, (bool, np.bool_)) and item not in (0, 1):
                raise SchemaError(f"column {column.name!r}: expected boolean values")
        return np.array([bool(item) for item in items], dtype=np.bool_)
    if dtype.kind in "iu":
        # Route integers through an object array so values beyond int64 (for
        # uint64 columns) are not silently coerced to float by numpy.
        return _cast_checked(np.array(items, dtype=object), dtype, column.name)
    return _cast_checked(np.asarray(items), dtype, column.name)


def _dense_bytes(column: Column, items: Sequence[object]) -> list[bytes]:
    result: list[bytes] = []
    for item in items:
        if isinstance(item, str):
            if column.type in (LogicalType.BINARY, LogicalType.FIXED_BINARY):
                raise SchemaError(f"column {column.name!r}: expected bytes, got str")
            encoded = item.encode()
        elif isinstance(item, (bytes, bytearray, np.bytes_)):
            encoded = bytes(item)
            if column.type in (LogicalType.UTF8, LogicalType.CATEGORICAL):
                try:
                    encoded.decode()
                except UnicodeDecodeError as error:
                    raise SchemaError(
                        f"column {column.name!r}: values must be well-formed UTF-8"
                    ) from error
        else:
            raise SchemaError(
                f"column {column.name!r}: cannot store {type(item).__name__}"
            )
        if column.type == LogicalType.FIXED_BINARY and len(encoded) != column.width:
            raise SchemaError(
                f"column {column.name!r}: value length {len(encoded)} does not "
                f"match width {column.width}"
            )
        result.append(encoded)
    return result


def normalize(
    column: Column, data: object
) -> tuple[np.ndarray | list[bytes], np.ndarray | None, int]:
    """Normalize user input into ``(dense_values, validity, row_count)``.

    ``validity`` is ``None`` for fully valid input. Nulls are expressed by
    ``None`` entries in a sequence (never inferred from NaN — NaN is a value).
    """
    if isinstance(data, np.ma.MaskedArray):
        validity = ~np.ma.getmaskarray(data)
        items = [
            item if valid else None for item, valid in zip(data.data.tolist(), validity)
        ]
        return _finish_normalize(column, items, len(items))
    if isinstance(data, np.ndarray):
        if data.ndim != 1:
            raise SchemaError(f"column {column.name!r}: expected one-dimensional data")
        if data.dtype.kind == "M":
            suffix = (
                "D"
                if column.type == LogicalType.DATE32
                else _UNIT_SUFFIX.get(column.unit, "us")
            )
            integers = data.astype(f"datetime64[{suffix}]").astype(np.int64)
            dense = _cast_checked(integers, FIXED_DTYPES[column.type], column.name)
            return dense, None, len(dense)
        if data.dtype.kind == "O":
            return _finish_normalize(column, data.tolist(), len(data))
        if is_bytes_like(column):
            return _finish_normalize(column, data.tolist(), len(data))
        dense = _cast_checked(data, FIXED_DTYPES[column.type], column.name)
        return dense, None, len(dense)
    items = list(data)
    return _finish_normalize(column, items, len(items))


def _finish_normalize(
    column: Column, items: list[object], row_count: int
) -> tuple[np.ndarray | list[bytes], np.ndarray | None, int]:
    has_null = any(item is None for item in items)
    if has_null and not column.nullable:
        raise SchemaError(f"column {column.name!r} is not nullable but got None")
    validity = None
    if has_null:
        validity = np.array([item is not None for item in items], dtype=np.bool_)
        items = [item for item in items if item is not None]
    if is_bytes_like(column):
        return _dense_bytes(column, items), validity, row_count
    return _dense_fixed(column, items), validity, row_count


def scatter(
    column: Column,
    dense: np.ndarray | list[bytes],
    validity: np.ndarray | None,
    row_count: int,
) -> np.ndarray:
    """Expand dense non-null values back to one entry per row."""
    decode_text = column.type in (LogicalType.UTF8, LogicalType.CATEGORICAL)
    if is_bytes_like(column):
        full = np.empty(row_count, dtype=object)
        positions = (
            np.flatnonzero(validity) if validity is not None else np.arange(row_count)
        )
        for position, value in zip(positions, dense):
            full[position] = value.decode() if decode_text else value
        return full
    dense_array = np.asarray(dense, dtype=FIXED_DTYPES[column.type])
    if validity is None:
        return dense_array
    full = np.zeros(row_count, dtype=dense_array.dtype)
    full[np.flatnonzero(validity)] = dense_array
    return full


def encode_varwidth(dense: list[bytes]) -> tuple[bytes, bytes]:
    """Raw variable-width representation: byte values plus uint32 lengths."""
    lengths = np.array([len(value) for value in dense], dtype=np.uint32)
    return b"".join(dense), lengths.tobytes()


def split_varwidth(values: bytes, lengths: np.ndarray) -> list[bytes]:
    result: list[bytes] = []
    cursor = 0
    for length in lengths.tolist():
        result.append(values[cursor : cursor + length])
        cursor += length
    return result
