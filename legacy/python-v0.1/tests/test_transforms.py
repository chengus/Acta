"""Transform round-trip tests including bit-packing and overflow edges."""

from __future__ import annotations

import numpy as np
import pytest

from acta import transforms
from acta.errors import CorruptionError

RNG = np.random.default_rng(20260719)

INT_DTYPES = [
    np.int8,
    np.int16,
    np.int32,
    np.int64,
    np.uint8,
    np.uint16,
    np.uint32,
    np.uint64,
]


def random_ints(dtype, count):
    info = np.iinfo(dtype)
    return RNG.integers(info.min, info.max, size=count, endpoint=True, dtype=dtype)


@pytest.mark.parametrize("width", [0, 1, 7, 8, 33, 63, 64])
def test_bitpack_round_trip_at_every_width(width):
    if width == 0:
        values = np.zeros(17, dtype=np.uint64)
    elif width == 64:
        values = RNG.integers(
            0, np.iinfo(np.uint64).max, 17, endpoint=True, dtype=np.uint64
        )
        values[0] = np.uint64(1) << np.uint64(63)  # force the full width
    else:
        values = RNG.integers(0, 1 << width, 17, dtype=np.uint64)
        values[0] = (1 << width) - 1
    packed = transforms.bitpack(values)
    assert np.array_equal(transforms.bitunpack(packed, len(values)), values)


def test_bitpack_width_zero_is_the_single_width_byte():
    assert transforms.bitpack(np.zeros(100, dtype=np.uint64)) == b"\x00"
    assert transforms.bitpack(np.zeros(0, dtype=np.uint64)) == b"\x00"


def test_bitunpack_rejects_trailing_data_after_width_zero():
    with pytest.raises(CorruptionError):
        transforms.bitunpack(b"\x00\x00", 8)


def test_zigzag_extremes():
    values = np.array(
        [0, -1, 1, np.iinfo(np.int64).min, np.iinfo(np.int64).max], dtype=np.int64
    )
    assert np.array_equal(transforms.unzigzag(transforms.zigzag(values)), values)


@pytest.mark.parametrize("dtype", INT_DTYPES)
def test_frame_of_reference_round_trip(dtype):
    values = random_ints(dtype, 999)
    payload = transforms.encode_frame_of_reference(values)
    decoded = transforms.decode_frame_of_reference(
        payload, len(values), np.dtype(dtype)
    )
    assert np.array_equal(decoded, values)


def test_frame_of_reference_full_uint64_range():
    values = np.array([0, np.iinfo(np.uint64).max, 5], dtype=np.uint64)
    payload = transforms.encode_frame_of_reference(values)
    assert np.array_equal(
        transforms.decode_frame_of_reference(payload, 3, np.dtype(np.uint64)), values
    )


@pytest.mark.parametrize("dtype", INT_DTYPES)
def test_delta_round_trip(dtype):
    # Clustered values keep adjacent differences within int64 for all dtypes.
    base = random_ints(dtype, 1)[0]
    steps = RNG.integers(-100, 100, size=499)
    with np.errstate(over="ignore"):
        values = (base + np.cumsum(steps).astype(dtype)).astype(dtype)
    payload = transforms.encode_delta(values)
    assert payload is not None
    assert np.array_equal(
        transforms.decode_delta(payload, len(values), np.dtype(dtype)), values
    )


def test_delta_withdraws_when_a_difference_overflows_int64():
    values = np.array([-2, np.iinfo(np.int64).max], dtype=np.int64)
    assert transforms.encode_delta(values) is None
    values = np.array([0, (1 << 63) + 5], dtype=np.uint64)
    assert transforms.encode_delta(values) is None


def test_delta_of_delta_round_trip_and_minimum_elements():
    values = np.array([1_000_000, 2_000_000, 3_000_000, 3_999_000], dtype=np.int64)
    payload = transforms.encode_delta_of_delta(values)
    assert payload is not None
    assert np.array_equal(
        transforms.decode_delta_of_delta(payload, 4, np.dtype(np.int64)), values
    )
    two = values[:2]
    payload = transforms.encode_delta_of_delta(two)
    assert payload is not None
    assert np.array_equal(
        transforms.decode_delta_of_delta(payload, 2, np.dtype(np.int64)), two
    )
    assert transforms.encode_delta_of_delta(values[:1]) is None


def test_delta_of_delta_withdraws_on_second_difference_overflow():
    values = np.array(
        [0, np.iinfo(np.int64).min, np.iinfo(np.int64).max - 10], dtype=np.int64
    )
    assert transforms.encode_delta_of_delta(values) is None


def test_bit_packed_values_reject_negatives():
    assert transforms.encode_bit_packed(np.array([-1, 2], dtype=np.int64)) is None
    values = np.array([0, 5, 200], dtype=np.int64)
    payload = transforms.encode_bit_packed(values)
    assert np.array_equal(
        transforms.decode_bit_packed(payload, 3, np.dtype(np.int64)), values
    )


@pytest.mark.parametrize("dtype", [np.float32, np.float64])
def test_byte_stream_split_preserves_float_bits(dtype):
    values = RNG.standard_normal(257).astype(dtype)
    values[0] = np.nan
    values[1] = -0.0
    values[2] = np.inf
    raw = values.tobytes()
    width = np.dtype(dtype).itemsize
    encoded = transforms.encode_byte_stream_split(raw, width)
    decoded = transforms.decode_byte_stream_split(encoded, len(values), width)
    assert decoded == raw


def test_bool_raw_round_trip():
    values = RNG.integers(0, 2, 41).astype(np.bool_)
    payload = transforms.encode_bool_raw(values)
    assert np.array_equal(transforms.decode_bool_raw(payload, 41), values)


def test_boolean_rle_round_trip_and_sum_invariant():
    values = np.repeat(
        np.array([True, False, True, True, False], dtype=np.bool_), [7, 1, 900, 2, 13]
    )
    payload = transforms.encode_boolean_rle(values)
    assert np.array_equal(transforms.decode_boolean_rle(payload, len(values)), values)
    with pytest.raises(CorruptionError):
        transforms.decode_boolean_rle(payload, len(values) + 1)


def test_boolean_rle_empty():
    payload = transforms.encode_boolean_rle(np.zeros(0, dtype=np.bool_))
    assert np.array_equal(
        transforms.decode_boolean_rle(payload, 0), np.zeros(0, dtype=np.bool_)
    )


def test_single_element_streams():
    one = np.array([42], dtype=np.int32)
    for encode, decode in [
        (transforms.encode_frame_of_reference, transforms.decode_frame_of_reference),
    ]:
        payload = encode(one)
        assert np.array_equal(decode(payload, 1, np.dtype(np.int32)), one)
    assert transforms.encode_delta(one) is None


def test_zstd_round_trip():
    payload = b"acta" * 1000
    compressed = transforms.compress(payload)
    assert len(compressed) < len(payload)
    assert transforms.decompress(compressed, len(payload)) == payload
    with pytest.raises(CorruptionError):
        transforms.decompress(compressed, len(payload) - 1)
