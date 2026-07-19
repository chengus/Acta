"""Exception types raised by the Acta reference implementation."""

from __future__ import annotations


class ActaError(Exception):
    """Base class for all Acta errors."""


class SchemaError(ActaError):
    """A schema or user-supplied value violates the format's rules."""


class CorruptionError(ActaError, ValueError):
    """Stored bytes violate the format specification.

    ``offset`` is the file offset of the frame or region where the violation
    was detected, when known.
    """

    def __init__(self, message: str, *, offset: int | None = None) -> None:
        super().__init__(message)
        self.offset = offset
