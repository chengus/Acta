"""Schema model, ergonomic column factories, and schema-frame serialization."""

from __future__ import annotations

import struct
from dataclasses import dataclass, replace
from typing import Iterator, Sequence

from .constants import COLUMN_SCHEMA, SCHEMA_HEADER, pad8
from .enums import (
    COLUMN_NULLABLE,
    VARIABLE_WIDTH_TYPES,
    LogicalType,
    TimestampUnit,
    TimezoneMode,
)
from .errors import CorruptionError, SchemaError

_UNIT_NAMES = {
    "s": TimestampUnit.SECOND,
    "ms": TimestampUnit.MILLISECOND,
    "us": TimestampUnit.MICROSECOND,
    "ns": TimestampUnit.NANOSECOND,
}

_DECIMAL_PARAMS = struct.Struct("<HhI")
_TIMESTAMP_PARAMS = struct.Struct("<BBHI")
_CATEGORICAL_PARAMS = struct.Struct("<B7s")
_FIXED_BINARY_PARAMS = struct.Struct("<II")

PRIMARY_CAPABLE_TYPES = (LogicalType.TIMESTAMP64, LogicalType.DATE32)


@dataclass(frozen=True)
class Column:
    """One schema column. Use the module-level factories to construct these."""

    name: str
    type: LogicalType
    nullable: bool = False
    id: int | None = None
    # Type parameters; only the fields relevant to ``type`` are set.
    precision: int | None = None
    scale: int | None = None
    unit: TimestampUnit | None = None
    timezone_mode: TimezoneMode | None = None
    timezone_name: str | None = None
    ordered: bool | None = None
    width: int | None = None

    def validate(self) -> None:
        if not self.name:
            raise SchemaError("column name must not be empty")
        if self.type == LogicalType.DECIMAL64:
            if self.precision is None or not 1 <= self.precision <= 18:
                raise SchemaError(
                    f"column {self.name!r}: decimal64 precision must be in [1, 18]"
                )
            if self.scale is None or not -(1 << 15) <= self.scale < (1 << 15):
                raise SchemaError(
                    f"column {self.name!r}: decimal64 scale must fit in int16"
                )
        elif self.type == LogicalType.TIMESTAMP64:
            if self.unit is None or self.timezone_mode is None:
                raise SchemaError(
                    f"column {self.name!r}: timestamp64 requires unit and timezone"
                )
            if self.timezone_mode == TimezoneMode.IANA and not self.timezone_name:
                raise SchemaError(
                    f"column {self.name!r}: IANA timezone mode requires a name"
                )
        elif self.type == LogicalType.CATEGORICAL:
            if self.ordered is None:
                raise SchemaError(
                    f"column {self.name!r}: categorical requires the ordered flag"
                )
        elif self.type == LogicalType.FIXED_BINARY:
            if self.width is None or self.width <= 0:
                raise SchemaError(
                    f"column {self.name!r}: fixed_binary width must be positive"
                )

    def pack_parameters(self) -> bytes:
        if self.type == LogicalType.DECIMAL64:
            return _DECIMAL_PARAMS.pack(self.precision, self.scale, 0)
        if self.type == LogicalType.TIMESTAMP64:
            name = (
                self.timezone_name.encode()
                if self.timezone_mode == TimezoneMode.IANA and self.timezone_name
                else b""
            )
            return (
                _TIMESTAMP_PARAMS.pack(self.unit, self.timezone_mode, 0, len(name))
                + name
            )
        if self.type == LogicalType.CATEGORICAL:
            return _CATEGORICAL_PARAMS.pack(int(self.ordered), bytes(7))
        if self.type == LogicalType.FIXED_BINARY:
            return _FIXED_BINARY_PARAMS.pack(self.width, 0)
        return b""


def _parse_parameters(
    name: str, logical_type: LogicalType, params: bytes
) -> dict[str, object]:
    try:
        if logical_type == LogicalType.DECIMAL64:
            precision, scale, reserved = _DECIMAL_PARAMS.unpack(params)
            if reserved:
                raise CorruptionError(f"column {name!r}: nonzero decimal64 reserved")
            return {"precision": precision, "scale": scale}
        if logical_type == LogicalType.TIMESTAMP64:
            unit, mode, reserved, name_length = _TIMESTAMP_PARAMS.unpack_from(params)
            if reserved:
                raise CorruptionError(f"column {name!r}: nonzero timestamp64 reserved")
            tz_name = params[
                _TIMESTAMP_PARAMS.size : _TIMESTAMP_PARAMS.size + name_length
            ]
            if len(tz_name) != name_length:
                raise CorruptionError(f"column {name!r}: truncated timezone name")
            return {
                "unit": TimestampUnit(unit),
                "timezone_mode": TimezoneMode(mode),
                "timezone_name": tz_name.decode() if name_length else None,
            }
        if logical_type == LogicalType.CATEGORICAL:
            ordered, reserved = _CATEGORICAL_PARAMS.unpack(params)
            if reserved != bytes(7) or ordered not in (0, 1):
                raise CorruptionError(f"column {name!r}: bad categorical parameters")
            return {"ordered": bool(ordered)}
        if logical_type == LogicalType.FIXED_BINARY:
            width, reserved = _FIXED_BINARY_PARAMS.unpack(params)
            if reserved:
                raise CorruptionError(f"column {name!r}: nonzero fixed_binary reserved")
            return {"width": width}
    except struct.error as error:
        raise CorruptionError(f"column {name!r}: bad type parameters") from error
    if params:
        raise CorruptionError(f"column {name!r}: unexpected type parameters")
    return {}


class Schema:
    """An ordered set of columns with one primary timestamp column."""

    def __init__(
        self,
        columns: Sequence[Column],
        *,
        primary: str | None = None,
        schema_id: int = 1,
    ) -> None:
        if not columns:
            raise SchemaError("schema requires at least one column")
        if schema_id == 0:
            raise SchemaError("schema ID must be nonzero")
        resolved: list[Column] = []
        used_ids = {column.id for column in columns if column.id is not None}
        next_id = 1
        for column in columns:
            column.validate()
            if column.id is None:
                while next_id in used_ids:
                    next_id += 1
                column = replace(column, id=next_id)
                used_ids.add(next_id)
            if column.id <= 0:
                raise SchemaError(f"column {column.name!r}: IDs must be positive")
            resolved.append(column)
        names = [column.name for column in resolved]
        if len(set(names)) != len(names):
            raise SchemaError("column names must be unique")
        ids = [column.id for column in resolved]
        if len(set(ids)) != len(ids):
            raise SchemaError("column IDs must be unique")

        if primary is None:
            candidates = [c for c in resolved if c.type in PRIMARY_CAPABLE_TYPES]
            if not candidates:
                raise SchemaError(
                    "schema requires a timestamp64 or date32 primary column"
                )
            primary_column = candidates[0]
        else:
            matches = [c for c in resolved if c.name == primary]
            if not matches:
                raise SchemaError(f"primary column {primary!r} does not exist")
            primary_column = matches[0]
        if primary_column.type not in PRIMARY_CAPABLE_TYPES:
            raise SchemaError(
                f"primary column {primary_column.name!r} must be timestamp64 or date32"
            )
        if primary_column.nullable:
            raise SchemaError(
                f"primary column {primary_column.name!r} must be non-nullable"
            )

        self.schema_id = schema_id
        self.columns: tuple[Column, ...] = tuple(resolved)
        self.primary_id: int = primary_column.id
        self._by_name = {column.name: column for column in self.columns}
        self._by_id = {column.id: column for column in self.columns}

    @property
    def primary(self) -> Column:
        return self._by_id[self.primary_id]

    def column(self, name: str) -> Column:
        try:
            return self._by_name[name]
        except KeyError:
            raise SchemaError(f"no column named {name!r}") from None

    def column_by_id(self, column_id: int) -> Column:
        try:
            return self._by_id[column_id]
        except KeyError:
            raise SchemaError(f"no column with ID {column_id}") from None

    def __iter__(self) -> Iterator[Column]:
        return iter(self.columns)

    def __len__(self) -> int:
        return len(self.columns)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Schema):
            return NotImplemented
        return (
            self.schema_id == other.schema_id
            and self.primary_id == other.primary_id
            and self.columns == other.columns
        )

    def __repr__(self) -> str:
        names = ", ".join(
            f"{column.name}:{column.type.name.lower()}" for column in self.columns
        )
        return f"Schema(id={self.schema_id}, primary={self.primary.name!r}, [{names}])"

    def to_frame_header(self) -> bytes:
        return SCHEMA_HEADER.pack(
            self.schema_id, len(self.columns), self.primary_id, 0, 0
        )

    def to_frame_payload(self) -> bytes:
        payload = bytearray()
        for column in self.columns:
            name = column.name.encode()
            params = column.pack_parameters()
            unpadded = COLUMN_SCHEMA.size + len(name) + len(params)
            descriptor_length = unpadded + (-unpadded) % 8
            payload += pad8(
                COLUMN_SCHEMA.pack(
                    descriptor_length,
                    column.id,
                    column.type,
                    COLUMN_NULLABLE if column.nullable else 0,
                    len(name),
                    len(params),
                    0,
                )
                + name
                + params
            )
        return bytes(payload)

    @classmethod
    def from_frame(cls, header: bytes, payload: bytes) -> "Schema":
        try:
            schema_id, column_count, primary_id, flags, reserved = (
                SCHEMA_HEADER.unpack_from(header)
            )
        except struct.error as error:
            raise CorruptionError("schema frame header is too short") from error
        if flags or reserved:
            raise CorruptionError("nonzero schema flags or reserved fields")
        if schema_id == 0:
            raise CorruptionError("schema ID must be nonzero")
        columns: list[Column] = []
        offset = 0
        for _ in range(column_count):
            try:
                (
                    descriptor_length,
                    column_id,
                    type_id,
                    column_flags,
                    name_length,
                    parameter_length,
                    descriptor_reserved,
                ) = COLUMN_SCHEMA.unpack_from(payload, offset)
            except struct.error as error:
                raise CorruptionError("schema payload is too short") from error
            if descriptor_reserved:
                raise CorruptionError("nonzero column descriptor reserved field")
            if column_flags & ~COLUMN_NULLABLE:
                raise CorruptionError(f"unknown column flags 0x{column_flags:x}")
            if descriptor_length % 8:
                raise CorruptionError("unaligned schema descriptor length")
            content_end = COLUMN_SCHEMA.size + name_length + parameter_length
            if content_end > descriptor_length or offset + descriptor_length > len(
                payload
            ):
                raise CorruptionError("schema descriptor overruns its payload")
            name_start = offset + COLUMN_SCHEMA.size
            name = payload[name_start : name_start + name_length].decode()
            params = payload[
                name_start + name_length : name_start + name_length + parameter_length
            ]
            try:
                logical_type = LogicalType(type_id)
            except ValueError as error:
                raise CorruptionError(f"unknown logical type ID {type_id}") from error
            columns.append(
                Column(
                    name=name,
                    type=logical_type,
                    nullable=bool(column_flags & COLUMN_NULLABLE),
                    id=column_id,
                    **_parse_parameters(name, logical_type, params),
                )
            )
            offset += descriptor_length
        # The remaining bytes are the frame-level zero padding added by pad8.
        if any(payload[offset:]):
            raise CorruptionError("schema descriptors do not consume the payload")
        primary_names = [c.name for c in columns if c.id == primary_id]
        if not primary_names:
            raise CorruptionError("primary timestamp column ID does not exist")
        try:
            return cls(columns, primary=primary_names[0], schema_id=schema_id)
        except SchemaError as error:
            raise CorruptionError(f"invalid stored schema: {error}") from error


def _column(name: str, logical_type: LogicalType, nullable: bool, **params) -> Column:
    column = Column(name=name, type=logical_type, nullable=nullable, **params)
    column.validate()
    return column


def bool_(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.BOOL, nullable)


def int8(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.INT8, nullable)


def int16(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.INT16, nullable)


def int32(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.INT32, nullable)


def int64(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.INT64, nullable)


def uint8(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.UINT8, nullable)


def uint16(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.UINT16, nullable)


def uint32(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.UINT32, nullable)


def uint64(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.UINT64, nullable)


def float32(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.FLOAT32, nullable)


def float64(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.FLOAT64, nullable)


def decimal64(
    name: str, *, precision: int, scale: int, nullable: bool = False
) -> Column:
    return _column(
        name, LogicalType.DECIMAL64, nullable, precision=precision, scale=scale
    )


def timestamp64(
    name: str,
    *,
    unit: str | TimestampUnit = "us",
    tz: str | None = None,
    nullable: bool = False,
) -> Column:
    if isinstance(unit, str):
        try:
            unit = _UNIT_NAMES[unit]
        except KeyError:
            raise SchemaError(
                f"unknown timestamp unit {unit!r}; expected one of "
                f"{sorted(_UNIT_NAMES)}"
            ) from None
    if tz is None:
        mode, tz_name = TimezoneMode.NAIVE, None
    elif tz == "UTC":
        mode, tz_name = TimezoneMode.UTC, None
    else:
        mode, tz_name = TimezoneMode.IANA, tz
    return _column(
        name,
        LogicalType.TIMESTAMP64,
        nullable,
        unit=unit,
        timezone_mode=mode,
        timezone_name=tz_name,
    )


def utf8(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.UTF8, nullable)


def categorical(name: str, *, ordered: bool = False, nullable: bool = False) -> Column:
    return _column(name, LogicalType.CATEGORICAL, nullable, ordered=ordered)


def binary(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.BINARY, nullable)


def fixed_binary(name: str, *, width: int, nullable: bool = False) -> Column:
    return _column(name, LogicalType.FIXED_BINARY, nullable, width=width)


def date32(name: str, *, nullable: bool = False) -> Column:
    return _column(name, LogicalType.DATE32, nullable)


def is_variable_width(column: Column) -> bool:
    return column.type in VARIABLE_WIDTH_TYPES
