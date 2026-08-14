"""Stream transforms (spec section 9) and Zstandard compression (section 10).

Encoders return ``None`` when a transform's preconditions do not hold for the
given values, so callers can withdraw the candidate rather than wrap.
All functions operate on one physical stream's dense elements.
"""

from __future__ import annotations

import struct

import numpy as np
import zstandard

from .errors import CorruptionError


def bitpack(values: np.ndarray) -> bytes:
    """Spec 9.2: one uint8 bit width, then values concatenated LSB-first."""
    values = np.asarray(values, dtype=np.uint64)
    width = int(values.max()).bit_length() if len(values) else 0
    if width == 0:
        return b"\x00"
    shifts = np.arange(width, dtype=np.uint64)
    bits = ((values[:, None] >> shifts) & 1).astype(np.uint8, copy=False)
    return bytes((width,)) + np.packbits(bits, bitorder="little").tobytes()


def bitunpack(payload: bytes, count: int) -> np.ndarray:
    if not payload:
        raise CorruptionError("bit-packed stream is missing its width byte")
    width = payload[0]
    if width > 64:
        raise CorruptionError(f"bit width {width} exceeds 64")
    if width == 0:
        if len(payload) != 1:
            raise CorruptionError("width-zero bit-packed stream has data bytes")
        return np.zeros(count, dtype=np.uint64)
    if len(payload) - 1 < (count * width + 7) // 8:
        raise CorruptionError("bit-packed stream is shorter than its elements")
    bits = np.unpackbits(
        np.frombuffer(payload, dtype=np.uint8, offset=1), bitorder="little"
    )[: count * width]
    matrix = bits.reshape(count, width).astype(np.uint64, copy=False)
    return np.bitwise_or.reduce(matrix << np.arange(width, dtype=np.uint64), axis=1)


def zigzag(values: np.ndarray) -> np.ndarray:
    signed = np.asarray(values, dtype=np.int64)
    return (signed.astype(np.uint64) << np.uint64(1)) ^ (signed >> 63).astype(np.uint64)


def unzigzag(values: np.ndarray) -> np.ndarray:
    unsigned = np.asarray(values, dtype=np.uint64)
    return (unsigned >> np.uint64(1)).astype(np.int64) ^ -(
        unsigned & np.uint64(1)
    ).astype(np.int64)


def _as_uint64_bits(values: np.ndarray) -> np.ndarray:
    """Reinterpret integer values as uint64 preserving two's complement bits."""
    if values.dtype.kind == "u":
        return values.astype(np.uint64)
    return values.astype(np.int64).view(np.uint64)


def _checked_diffs(values: np.ndarray) -> np.ndarray | None:
    """Adjacent differences as int64, or None when one does not fit int64."""
    bits = _as_uint64_bits(values)
    with np.errstate(over="ignore"):
        diffs = (bits[1:] - bits[:-1]).view(np.int64)
    # The wrapped difference equals the true difference exactly when its sign
    # matches the comparison of the original values.
    if not np.array_equal(values[1:] >= values[:-1], diffs >= 0):
        return None
    return diffs


def encode_bit_packed(values: np.ndarray) -> bytes | None:
    """Transform 1 for nonnegative integer value streams."""
    if len(values) and values.dtype.kind == "i" and int(values.min()) < 0:
        return None
    return bitpack(values.astype(np.uint64))


def decode_bit_packed(payload: bytes, count: int, dtype: np.dtype) -> np.ndarray:
    return bitunpack(payload, count).astype(dtype)


def encode_frame_of_reference(values: np.ndarray) -> bytes | None:
    """Transform 2: canonical-width base (the minimum) plus packed offsets."""
    if len(values) == 0:
        return None
    minimum_index = int(np.argmin(values))
    base = values[minimum_index : minimum_index + 1]
    with np.errstate(over="ignore"):
        offsets = _as_uint64_bits(values) - _as_uint64_bits(base)[0]
    return base.tobytes() + bitpack(offsets)


def decode_frame_of_reference(
    payload: bytes, count: int, dtype: np.dtype
) -> np.ndarray:
    width = dtype.itemsize
    base = np.frombuffer(payload, dtype=dtype, count=1)
    offsets = bitunpack(payload[width:], count)
    with np.errstate(over="ignore"):
        restored = offsets + _as_uint64_bits(base)[0]
    if dtype.kind == "u":
        return restored.astype(dtype)
    return restored.view(np.int64).astype(dtype)


def encode_delta(values: np.ndarray) -> bytes | None:
    """Transform 3: first canonical-width value plus ZigZag deltas."""
    if len(values) < 2:
        return None
    diffs = _checked_diffs(values)
    if diffs is None:
        return None
    return values[:1].tobytes() + bitpack(zigzag(diffs))


def decode_delta(payload: bytes, count: int, dtype: np.dtype) -> np.ndarray:
    width = dtype.itemsize
    first = np.frombuffer(payload, dtype=dtype, count=1)
    if count <= 1:
        return first[:count].copy()
    deltas = unzigzag(bitunpack(payload[width:], count - 1))
    restored = np.empty(count, dtype=np.uint64)
    restored[0] = _as_uint64_bits(first)[0]
    with np.errstate(over="ignore"):
        restored[1:] = restored[0] + np.cumsum(deltas.view(np.uint64))
    if dtype.kind == "u":
        return restored.astype(dtype)
    return restored.view(np.int64).astype(dtype)


def encode_delta_of_delta(values: np.ndarray) -> bytes | None:
    """Transform 4: first value, first delta as int64, ZigZag second diffs."""
    if len(values) < 2:
        return None
    deltas = _checked_diffs(values)
    if deltas is None:
        return None
    second = _checked_diffs(deltas)
    if second is None:
        return None
    return (
        values[:1].tobytes()
        + struct.pack("<q", int(deltas[0]))
        + bitpack(zigzag(second))
    )


def decode_delta_of_delta(payload: bytes, count: int, dtype: np.dtype) -> np.ndarray:
    width = dtype.itemsize
    first = np.frombuffer(payload, dtype=dtype, count=1)
    if count <= 1:
        return first[:count].copy()
    first_delta = struct.unpack_from("<q", payload, width)[0]
    second = unzigzag(bitunpack(payload[width + 8 :], count - 2))
    deltas = np.empty(count - 1, dtype=np.int64)
    deltas[0] = first_delta
    first_delta_bits = np.array([first_delta], dtype=np.int64).view(np.uint64)[0]
    with np.errstate(over="ignore"):
        deltas.view(np.uint64)[1:] = first_delta_bits + np.cumsum(
            second.view(np.uint64)
        )
        restored = np.empty(count, dtype=np.uint64)
        restored[0] = _as_uint64_bits(first)[0]
        restored[1:] = restored[0] + np.cumsum(deltas.view(np.uint64))
    if dtype.kind == "u":
        return restored.astype(dtype)
    return restored.view(np.int64).astype(dtype)


def encode_byte_stream_split(data: bytes, width: int) -> bytes:
    """Transform 5 over the raw fixed-width byte representation."""
    count = len(data) // width
    matrix = np.frombuffer(data, dtype=np.uint8).reshape(count, width)
    return matrix.T.tobytes()


def decode_byte_stream_split(payload: bytes, count: int, width: int) -> bytes:
    if len(payload) != count * width:
        raise CorruptionError("byte-stream-split payload has the wrong length")
    matrix = np.frombuffer(payload, dtype=np.uint8).reshape(width, count)
    return np.ascontiguousarray(matrix.T).tobytes()


def encode_bool_raw(values: np.ndarray) -> bytes:
    """Spec 9.1: raw booleans are LSB-first bit packed without a width byte."""
    return np.packbits(np.asarray(values, dtype=np.bool_), bitorder="little").tobytes()


def decode_bool_raw(payload: bytes, count: int) -> np.ndarray:
    if len(payload) < (count + 7) // 8:
        raise CorruptionError("raw boolean stream is shorter than its elements")
    return np.unpackbits(np.frombuffer(payload, dtype=np.uint8), bitorder="little")[
        :count
    ].astype(np.bool_)


def encode_boolean_rle(values: np.ndarray) -> bytes:
    """Transform 6: run count, LSB-first run-value bits, packed run lengths."""
    values = np.asarray(values, dtype=np.bool_)
    if len(values) == 0:
        return struct.pack("<I", 0)
    boundaries = np.flatnonzero(values[1:] != values[:-1]) + 1
    starts = np.concatenate((np.array([0], dtype=np.int64), boundaries))
    ends = np.concatenate((boundaries, np.array([len(values)], dtype=np.int64)))
    run_values = np.packbits(values[starts], bitorder="little").tobytes()
    run_lengths = (ends - starts).astype(np.uint64)
    return struct.pack("<I", len(starts)) + run_values + bitpack(run_lengths)


def decode_boolean_rle(payload: bytes, count: int) -> np.ndarray:
    if len(payload) < 4:
        raise CorruptionError("boolean RLE stream is missing its run count")
    run_count = struct.unpack_from("<I", payload)[0]
    if run_count == 0:
        if count:
            raise CorruptionError("boolean RLE run lengths do not sum to the count")
        return np.zeros(0, dtype=np.bool_)
    values_size = (run_count + 7) // 8
    run_values = np.unpackbits(
        np.frombuffer(payload, dtype=np.uint8, count=values_size, offset=4),
        bitorder="little",
    )[:run_count].astype(np.bool_)
    run_lengths = bitunpack(payload[4 + values_size :], run_count)
    if np.any(run_lengths == 0):
        raise CorruptionError("boolean RLE contains a zero-length run")
    if int(run_lengths.sum()) != count:
        raise CorruptionError("boolean RLE run lengths do not sum to the count")
    return np.repeat(run_values, run_lengths.astype(np.int64))


def compress(payload: bytes, level: int = 3) -> bytes:
    return zstandard.ZstdCompressor(level=level).compress(payload)


def decompress(payload: bytes, transformed_length: int) -> bytes:
    result = zstandard.ZstdDecompressor().decompress(
        payload, max_output_size=max(transformed_length, 1)
    )
    if len(result) != transformed_length:
        raise CorruptionError(
            "decompressed stream length does not match its descriptor"
        )
    return result
