"""Enumerated identifiers from the Acta v0.1 specification."""

from __future__ import annotations

from enum import IntEnum


class LogicalType(IntEnum):
    BOOL = 1
    INT8 = 2
    INT16 = 3
    INT32 = 4
    INT64 = 5
    UINT8 = 6
    UINT16 = 7
    UINT32 = 8
    UINT64 = 9
    FLOAT32 = 10
    FLOAT64 = 11
    DECIMAL64 = 12
    TIMESTAMP64 = 13
    UTF8 = 14
    CATEGORICAL = 15
    BINARY = 16
    FIXED_BINARY = 17
    DATE32 = 18


class FrameType(IntEnum):
    SCHEMA = 1
    DATA = 2
    CHECKPOINT = 3


class ColumnLayout(IntEnum):
    PLAIN = 0
    CONSTANT = 1
    DICTIONARY = 2
    RUN_LENGTH = 3


class StreamKind(IntEnum):
    VALIDITY = 1
    VALUES = 2
    LENGTHS = 3
    DICTIONARY_VALUES = 4
    DICTIONARY_LENGTHS = 5
    INDICES = 6
    RUN_VALUES = 7
    RUN_LENGTHS = 8


class Transform(IntEnum):
    RAW = 0
    BIT_PACKED = 1
    FRAME_OF_REFERENCE = 2
    DELTA = 3
    DELTA_OF_DELTA = 4
    BYTE_STREAM_SPLIT = 5
    BOOLEAN_RLE = 6


class Codec(IntEnum):
    NONE = 0
    ZSTD = 1


class StatsKind(IntEnum):
    NONE = 0
    MIN_MAX = 1


class TimestampUnit(IntEnum):
    SECOND = 0
    MILLISECOND = 1
    MICROSECOND = 2
    NANOSECOND = 3


class TimezoneMode(IntEnum):
    NAIVE = 0
    UTC = 1
    IANA = 2


# File prologue feature flags.
FEATURE_ROW_IDS = 1 << 0

# Data-block flags.
BLOCK_ROW_IDS = 1 << 0
BLOCK_TS_SORTED = 1 << 1

# Schema column flags.
COLUMN_NULLABLE = 1 << 0

# Block column-descriptor flags.
COLDESC_IMPLICIT_VALIDITY = 1 << 0
COLDESC_HAS_STATS = 1 << 1

# Variable-width logical types use a lengths stream alongside the byte values.
VARIABLE_WIDTH_TYPES = frozenset(
    {LogicalType.UTF8, LogicalType.CATEGORICAL, LogicalType.BINARY}
)
