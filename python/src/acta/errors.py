"""Exception types raised by the Acta reference implementation."""

from __future__ import annotations


class ActaError(Exception):
    """Base class for all Acta errors."""


class SchemaError(ActaError):
    """A schema or user-supplied value violates the format's rules."""


class UnsupportedFormatVersionError(ActaError):
    """The file is well identified as Acta but uses an unsupported version."""

    def __init__(
        self,
        major: int,
        minor: int,
        *,
        supported: tuple[tuple[int, int], ...],
    ) -> None:
        requested = f"{major}.{minor}"
        versions = ", ".join(
            f"{item_major}.{item_minor}" for item_major, item_minor in supported
        )
        super().__init__(
            f"unsupported Acta format version {requested}; supported: {versions}"
        )
        self.major = major
        self.minor = minor
        self.supported = supported


class CorruptionError(ActaError, ValueError):
    """Stored bytes violate the format specification.

    ``offset`` is the file offset of the frame or region where the violation
    was detected, when known.
    """

    def __init__(self, message: str, *, offset: int | None = None) -> None:
        super().__init__(message)
        self.offset = offset
