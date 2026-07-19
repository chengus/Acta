"""Acta v0.1 reference implementation.

Common usage::

    import acta

    schema = acta.Schema([
        acta.timestamp64("time", unit="us", tz="UTC"),
        acta.float64("close", nullable=True),
    ])
    with acta.create("ticks.acta", schema) as writer:
        writer.append({"time": [1, 2, 3], "close": [1.0, None, 3.0]})
    with acta.open("ticks.acta") as reader:
        result = reader.read(columns=["close"], start=1, end=3)
"""

from .columns import ColumnData
from .encode import BlockOptions
from .enums import LogicalType, TimestampUnit, TimezoneMode
from .errors import ActaError, CorruptionError, SchemaError
from .reader import Block, Reader, ReadResult, open_reader
from .recovery import RecoveryReport, recover, verify
from .writer import Writer, create, open_append
from .schema import (
    Column,
    Schema,
    binary,
    bool_,
    categorical,
    date32,
    decimal64,
    fixed_binary,
    float32,
    float64,
    int8,
    int16,
    int32,
    int64,
    timestamp64,
    uint8,
    uint16,
    uint32,
    uint64,
    utf8,
)

# ``acta.open`` intentionally shadows the builtin inside this namespace.
open = open_reader

__all__ = [
    "ActaError",
    "Block",
    "BlockOptions",
    "Column",
    "ColumnData",
    "CorruptionError",
    "LogicalType",
    "ReadResult",
    "Reader",
    "RecoveryReport",
    "Schema",
    "SchemaError",
    "TimestampUnit",
    "TimezoneMode",
    "Writer",
    "create",
    "open",
    "open_append",
    "open_reader",
    "recover",
    "verify",
    "binary",
    "bool_",
    "categorical",
    "date32",
    "decimal64",
    "fixed_binary",
    "float32",
    "float64",
    "int8",
    "int16",
    "int32",
    "int64",
    "timestamp64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "utf8",
]
