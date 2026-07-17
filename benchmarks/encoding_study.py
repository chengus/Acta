#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "numpy>=2.0",
#   "pyarrow>=18.0",
#   "zstandard>=0.23",
# ]
# ///
"""Evaluate candidate Acta v0 column encodings on NYC TLC trip data.

This is a format-design probe, not a production throughput benchmark.  The
implementations favor clarity and round-trip verification; a C++ implementation
will use SIMD-aware bit packing and CRC primitives.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import struct
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd


Decoder = Callable[[bytes, int], object]


@dataclass(frozen=True)
class Candidate:
    name: str
    payload: bytes
    decode: Decoder


@dataclass(frozen=True)
class ColumnCase:
    name: str
    logical_type: str
    values: object
    nullable: bool = False
    validity: np.ndarray | None = None
    note: str = ""


def _bitpack(values: np.ndarray) -> bytes:
    values = np.asarray(values, dtype=np.uint64)
    width = int(values.max()).bit_length() if len(values) else 0
    if width == 0:
        return b"\x00"
    shifts = np.arange(width, dtype=np.uint64)
    bits = ((values[:, None] >> shifts) & 1).astype(np.uint8, copy=False)
    return bytes((width,)) + np.packbits(bits, bitorder="little").tobytes()


def _bitunpack(payload: bytes, count: int) -> np.ndarray:
    width = payload[0]
    if width == 0:
        return np.zeros(count, dtype=np.uint64)
    bits = np.unpackbits(
        np.frombuffer(payload, dtype=np.uint8, offset=1), bitorder="little"
    )[: count * width]
    matrix = bits.reshape(count, width).astype(np.uint64, copy=False)
    return np.sum(matrix << np.arange(width, dtype=np.uint64), axis=1)


def _zigzag(values: np.ndarray) -> np.ndarray:
    signed = np.asarray(values, dtype=np.int64)
    return (signed.astype(np.uint64) << 1) ^ (signed >> 63).astype(np.uint64)


def _unzigzag(values: np.ndarray) -> np.ndarray:
    unsigned = np.asarray(values, dtype=np.uint64)
    return (unsigned >> 1).astype(np.int64) ^ -(unsigned & 1).astype(np.int64)


def _run_boundaries(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if len(values) == 0:
        return np.array([], dtype=np.int64), np.array([], dtype=np.uint64)
    starts = np.concatenate(
        (np.array([0], dtype=np.int64), np.flatnonzero(values[1:] != values[:-1]) + 1)
    )
    ends = np.concatenate((starts[1:], np.array([len(values)], dtype=np.int64)))
    return starts, (ends - starts).astype(np.uint64)


def _numeric_candidates(values: np.ndarray) -> list[Candidate]:
    values = np.ascontiguousarray(values)
    dtype = values.dtype
    width = dtype.itemsize
    candidates: list[Candidate] = []

    def raw_decode(payload: bytes, count: int) -> np.ndarray:
        return np.frombuffer(payload, dtype=dtype, count=count).copy()

    candidates.append(Candidate("raw", values.tobytes(), raw_decode))
    if len(values) == 0:
        return candidates

    if np.all(values == values[0]):
        constant = values[:1].tobytes()

        def constant_decode(payload: bytes, count: int) -> np.ndarray:
            value = np.frombuffer(payload, dtype=dtype, count=1)[0]
            return np.full(count, value, dtype=dtype)

        candidates.append(Candidate("constant", constant, constant_decode))

    starts, run_lengths = _run_boundaries(values)
    if len(starts) <= max(2, len(values) // 2):
        run_values = values[starts]
        packed_lengths = _bitpack(run_lengths)
        payload = struct.pack("<I", len(starts)) + run_values.tobytes() + packed_lengths

        def rle_decode(payload: bytes, count: int) -> np.ndarray:
            run_count = struct.unpack_from("<I", payload)[0]
            value_start = 4
            value_end = value_start + run_count * width
            decoded_values = np.frombuffer(
                payload, dtype=dtype, count=run_count, offset=value_start
            )
            decoded_lengths = _bitunpack(payload[value_end:], run_count)
            result = np.repeat(decoded_values, decoded_lengths.astype(np.int64))
            return result.astype(dtype, copy=False)[:count]

        candidates.append(Candidate("rle", payload, rle_decode))

    bits = values.view(f"u{width}") if dtype.kind == "f" else None
    if dtype.kind in "iu":
        minimum = int(values.min())
        if dtype.kind == "u":
            base_bits = np.uint64(minimum)
            offsets = values.astype(np.uint64) - base_bits
        else:
            signed64 = values.astype(np.int64)
            base_bits = np.array([minimum], dtype=np.int64).view(np.uint64)[0]
            offsets = signed64.view(np.uint64) - base_bits
        packed = values[np.argmin(values) : np.argmin(values) + 1].tobytes() + _bitpack(
            offsets
        )

        def for_decode(payload: bytes, count: int) -> np.ndarray:
            base = np.frombuffer(payload, dtype=dtype, count=1)[0]
            decoded_offsets = _bitunpack(payload[width:], count)
            if dtype.kind == "u":
                return (decoded_offsets + np.uint64(base)).astype(dtype)
            decoded_base_bits = np.array([int(base)], dtype=np.int64).view(np.uint64)[0]
            return (decoded_offsets + decoded_base_bits).view(np.int64).astype(dtype)

        candidates.append(Candidate("frame_of_reference", packed, for_decode))

        integer_range = int(values.max()) - minimum
        signed_delta_safe = integer_range <= np.iinfo(np.int64).max and (
            dtype.kind == "i" or int(values.max()) <= np.iinfo(np.int64).max
        )
        if len(values) > 1 and signed_delta_safe:
            signed_values = values.astype(np.int64)
            deltas = np.diff(signed_values)
            delta_payload = values[:1].tobytes() + _bitpack(_zigzag(deltas))

            def delta_decode(payload: bytes, count: int) -> np.ndarray:
                first = np.frombuffer(payload, dtype=dtype, count=1)[0]
                if count == 1:
                    return np.array([first], dtype=dtype)
                deltas_decoded = _unzigzag(_bitunpack(payload[width:], count - 1))
                result = np.empty(count, dtype=np.int64)
                result[0] = int(first)
                result[1:] = int(first) + np.cumsum(deltas_decoded, dtype=np.int64)
                return result.astype(dtype)

            candidates.append(Candidate("delta", delta_payload, delta_decode))

            delta_span = int(deltas.max()) - int(deltas.min())
            if len(values) > 2 and delta_span <= np.iinfo(np.int64).max:
                second = np.diff(deltas)
                dod_payload = (
                    values[:1].tobytes()
                    + struct.pack("<q", int(deltas[0]))
                    + _bitpack(_zigzag(second))
                )

                def dod_decode(payload: bytes, count: int) -> np.ndarray:
                    first = int(np.frombuffer(payload, dtype=dtype, count=1)[0])
                    if count == 1:
                        return np.array([first], dtype=dtype)
                    first_delta = struct.unpack_from("<q", payload, width)[0]
                    if count == 2:
                        return np.array([first, first + first_delta], dtype=dtype)
                    second_decoded = _unzigzag(
                        _bitunpack(payload[width + 8 :], count - 2)
                    )
                    decoded_deltas = np.empty(count - 1, dtype=np.int64)
                    decoded_deltas[0] = first_delta
                    decoded_deltas[1:] = first_delta + np.cumsum(
                        second_decoded, dtype=np.int64
                    )
                    result = np.empty(count, dtype=np.int64)
                    result[0] = first
                    result[1:] = first + np.cumsum(decoded_deltas, dtype=np.int64)
                    return result.astype(dtype)

                candidates.append(Candidate("delta_of_delta", dod_payload, dod_decode))

    if dtype.kind == "f":
        byte_split = values.view(np.uint8).reshape(len(values), width).T.tobytes()

        def byte_split_decode(payload: bytes, count: int) -> np.ndarray:
            matrix = np.frombuffer(payload, dtype=np.uint8).reshape(width, count).T
            return np.ascontiguousarray(matrix).view(dtype).reshape(count)

        candidates.append(Candidate("byte_stream_split", byte_split, byte_split_decode))

        assert bits is not None
        if len(bits) > 1:
            xor_values = np.bitwise_xor(bits[1:], bits[:-1]).astype(np.uint64)
            xor_payload = values[:1].tobytes() + _bitpack(xor_values)

            def xor_decode(payload: bytes, count: int) -> np.ndarray:
                first_bits = np.frombuffer(payload, dtype=dtype, count=1).view(
                    f"u{width}"
                )[0]
                if count == 1:
                    return np.array([first_bits], dtype=f"u{width}").view(dtype)
                encoded = _bitunpack(payload[width:], count - 1).astype(f"u{width}")
                restored = np.empty(count, dtype=f"u{width}")
                restored[0] = first_bits
                restored[1:] = np.bitwise_xor.accumulate(
                    np.concatenate((np.array([first_bits], dtype=f"u{width}"), encoded))
                )[1:]
                return restored.view(dtype)

            candidates.append(Candidate("xor", xor_payload, xor_decode))

    # Dictionary coding is useful for numeric codes and repeated exact floats.
    key_values = bits if bits is not None else values
    unique, inverse = np.unique(key_values, return_inverse=True)
    if len(unique) <= min(65536, max(1, len(values) // 2)):
        dictionary = unique.tobytes()
        payload = struct.pack("<I", len(unique)) + dictionary + _bitpack(inverse)

        def dictionary_decode(payload: bytes, count: int) -> np.ndarray:
            dictionary_count = struct.unpack_from("<I", payload)[0]
            dictionary_end = 4 + dictionary_count * width
            decoded_dictionary = np.frombuffer(
                payload, dtype=key_values.dtype, count=dictionary_count, offset=4
            )
            indices = _bitunpack(payload[dictionary_end:], count).astype(np.int64)
            restored = decoded_dictionary[indices]
            return restored.view(dtype) if dtype.kind == "f" else restored.astype(dtype)

        candidates.append(Candidate("dictionary", payload, dictionary_decode))

    return candidates


def _bool_candidates(values: np.ndarray) -> list[Candidate]:
    values = np.asarray(values, dtype=np.bool_)
    packed = np.packbits(values, bitorder="little").tobytes()

    def bitpack_decode(payload: bytes, count: int) -> np.ndarray:
        return np.unpackbits(np.frombuffer(payload, dtype=np.uint8), bitorder="little")[
            :count
        ].astype(np.bool_)

    candidates = [Candidate("bitpack", packed, bitpack_decode)]
    if len(values) and np.all(values == values[0]):
        candidates.append(
            Candidate(
                "constant",
                bytes((int(values[0]),)),
                lambda payload, count: np.full(count, bool(payload[0]), dtype=np.bool_),
            )
        )
    starts, lengths = _run_boundaries(values)
    if len(starts) <= max(2, len(values) // 8):
        run_values = np.packbits(values[starts], bitorder="little").tobytes()
        payload = struct.pack("<I", len(starts)) + run_values + _bitpack(lengths)

        def rle_decode(payload: bytes, count: int) -> np.ndarray:
            run_count = struct.unpack_from("<I", payload)[0]
            values_size = math.ceil(run_count / 8)
            decoded_values = np.unpackbits(
                np.frombuffer(payload, dtype=np.uint8, count=values_size, offset=4),
                bitorder="little",
            )[:run_count]
            decoded_lengths = _bitunpack(payload[4 + values_size :], run_count)
            return np.repeat(decoded_values, decoded_lengths.astype(np.int64))[
                :count
            ].astype(np.bool_)

        candidates.append(Candidate("rle", payload, rle_decode))
    return candidates


def _encode_byte_values(values: Sequence[bytes]) -> bytes:
    lengths = np.fromiter((len(value) for value in values), dtype=np.uint32)
    return lengths.tobytes() + b"".join(values)


def _decode_byte_values(payload: bytes, count: int) -> list[bytes]:
    lengths = np.frombuffer(payload, dtype="<u4", count=count)
    cursor = count * 4
    result: list[bytes] = []
    for length in lengths:
        end = cursor + int(length)
        result.append(payload[cursor:end])
        cursor = end
    return result


def _bytes_candidates(
    values: Sequence[bytes], fixed_width: int | None
) -> list[Candidate]:
    values = list(values)
    if fixed_width is None:
        raw = _encode_byte_values(values)
        raw_decode: Decoder = _decode_byte_values
    else:
        raw = b"".join(values)

        def raw_decode(payload: bytes, count: int) -> list[bytes]:
            return [
                payload[index * fixed_width : (index + 1) * fixed_width]
                for index in range(count)
            ]

    candidates = [Candidate("raw", raw, raw_decode)]
    if not values:
        return candidates
    if all(value == values[0] for value in values):
        constant = values[0]
        candidates.append(
            Candidate("constant", constant, lambda payload, count: [payload] * count)
        )

    dictionary: list[bytes] = []
    dictionary_index: dict[bytes, int] = {}
    indices = np.empty(len(values), dtype=np.uint64)
    for index, value in enumerate(values):
        code = dictionary_index.get(value)
        if code is None:
            code = len(dictionary)
            dictionary_index[value] = code
            dictionary.append(value)
        indices[index] = code
    if len(dictionary) <= min(65536, max(1, len(values) // 2)):
        encoded_dictionary = _encode_byte_values(dictionary)
        payload = (
            struct.pack("<II", len(dictionary), len(encoded_dictionary))
            + encoded_dictionary
            + _bitpack(indices)
        )

        def dictionary_decode(payload: bytes, count: int) -> list[bytes]:
            dictionary_count, dictionary_size = struct.unpack_from("<II", payload)
            dictionary_start = 8
            dictionary_end = dictionary_start + dictionary_size
            decoded_dictionary = _decode_byte_values(
                payload[dictionary_start:dictionary_end], dictionary_count
            )
            decoded_indices = _bitunpack(payload[dictionary_end:], count)
            return [decoded_dictionary[int(index)] for index in decoded_indices]

        candidates.append(Candidate("dictionary", payload, dictionary_decode))

    object_values = np.asarray(values, dtype=object)
    starts, lengths = _run_boundaries(object_values)
    if len(starts) <= max(2, len(values) // 2):
        run_values = [values[int(index)] for index in starts]
        encoded_values = _encode_byte_values(run_values)
        payload = (
            struct.pack("<II", len(starts), len(encoded_values))
            + encoded_values
            + _bitpack(lengths)
        )

        def rle_decode(payload: bytes, count: int) -> list[bytes]:
            run_count, encoded_size = struct.unpack_from("<II", payload)
            values_end = 8 + encoded_size
            decoded_values = _decode_byte_values(payload[8:values_end], run_count)
            decoded_lengths = _bitunpack(payload[values_end:], run_count)
            return [
                value
                for value, length in zip(decoded_values, decoded_lengths)
                for _ in range(int(length))
            ][:count]

        candidates.append(Candidate("rle", payload, rle_decode))

    if fixed_width and fixed_width > 1:
        matrix = np.frombuffer(raw, dtype=np.uint8).reshape(len(values), fixed_width)
        transposed = matrix.T.tobytes()

        def byte_split_decode(payload: bytes, count: int) -> list[bytes]:
            restored = (
                np.frombuffer(payload, dtype=np.uint8).reshape(fixed_width, count).T
            )
            joined = np.ascontiguousarray(restored).tobytes()
            return [
                joined[index * fixed_width : (index + 1) * fixed_width]
                for index in range(count)
            ]

        candidates.append(Candidate("byte_stream_split", transposed, byte_split_decode))

    return candidates


def _equal(left: object, right: object) -> bool:
    if isinstance(left, np.ndarray) and isinstance(right, np.ndarray):
        if left.dtype.kind == "f":
            return np.array_equal(
                left.view(f"u{left.dtype.itemsize}"),
                right.view(f"u{right.dtype.itemsize}"),
            )
        return np.array_equal(left, right)
    return left == right


def _compress(payload: bytes, compressor: zstd.ZstdCompressor) -> tuple[str, bytes]:
    compressed = compressor.compress(payload)
    # Eight bytes accounts for codec and stored-size metadata in the stream table.
    if len(compressed) + 8 < len(payload):
        return "zstd", compressed
    return "none", payload


def _validity_candidates(validity: np.ndarray) -> list[Candidate]:
    if np.all(validity):
        return [
            Candidate(
                "all_valid", b"", lambda payload, count: np.ones(count, dtype=np.bool_)
            )
        ]
    if not np.any(validity):
        return [
            Candidate(
                "all_null", b"", lambda payload, count: np.zeros(count, dtype=np.bool_)
            )
        ]
    return _bool_candidates(validity)


def _slice_values(values: object, start: int, end: int) -> object:
    return values[start:end]  # type: ignore[index]


def _non_null(values: object, validity: np.ndarray | None) -> object:
    if validity is None:
        return values
    if isinstance(values, np.ndarray):
        return values[validity]
    return [value for value, valid in zip(values, validity) if valid]


def _candidates(case: ColumnCase, values: object) -> list[Candidate]:
    if case.logical_type == "bool":
        return _bool_candidates(np.asarray(values, dtype=np.bool_))
    if case.logical_type.startswith(("int", "uint", "float", "decimal", "timestamp")):
        return _numeric_candidates(np.asarray(values))
    if case.logical_type in {"utf8", "categorical", "binary"}:
        return _bytes_candidates(values, None)  # type: ignore[arg-type]
    if case.logical_type.startswith("fixed_binary"):
        width = int(case.logical_type.split("[")[1].rstrip("]"))
        return _bytes_candidates(values, width)  # type: ignore[arg-type]
    raise ValueError(f"unsupported benchmark type: {case.logical_type}")


def _raw_size(case: ColumnCase, values: object) -> int:
    if isinstance(values, np.ndarray):
        if values.dtype == np.bool_:
            return math.ceil(len(values) / 8)
        return values.nbytes
    if case.logical_type.startswith("fixed_binary"):
        return sum(len(value) for value in values)  # type: ignore[arg-type]
    return 4 * len(values) + sum(len(value) for value in values)  # type: ignore[arg-type]


def load_cases(
    dataset: Path, rows: int, block_rows: int, block_count: int, sampling: str
) -> tuple[list[ColumnCase], list[int], int]:
    source_columns = [
        "tpep_pickup_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "store_and_fwd_flag",
    ]
    source = pq.read_table(dataset, columns=source_columns)
    source_rows = source.num_rows
    if rows > source_rows:
        raise ValueError(f"requested {rows:,} rows from a {source_rows:,}-row dataset")
    if sampling == "head":
        sample_offsets = [index * block_rows for index in range(block_count)]
    else:
        sample_offsets = [
            int(offset)
            for offset in np.linspace(0, source_rows - block_rows, block_count)
        ]
    table = pa.concat_tables(
        [source.slice(offset, block_rows) for offset in sample_offsets]
    ).combine_chunks()

    pickup = (
        table["tpep_pickup_datetime"]
        .to_numpy(zero_copy_only=False)
        .astype("datetime64[us]")
        .astype(np.int64)
    )
    passenger_arrow = table["passenger_count"]
    passenger_validity = np.asarray(
        passenger_arrow.is_valid().to_numpy(), dtype=np.bool_
    )
    passenger = (
        passenger_arrow.fill_null(0).to_numpy(zero_copy_only=False).astype(np.int64)
    )
    distance = table["trip_distance"].to_numpy(zero_copy_only=False).astype(np.float64)
    pickup_zone = table["PULocationID"].to_numpy(zero_copy_only=False).astype(np.uint32)
    dropoff_zone = (
        table["DOLocationID"].to_numpy(zero_copy_only=False).astype(np.uint32)
    )
    signed_zone_delta = dropoff_zone.astype(np.int32) - pickup_zone.astype(np.int32)
    payment = table["payment_type"].to_numpy(zero_copy_only=False).astype(np.int64)
    fare = table["fare_amount"].to_numpy(zero_copy_only=False).astype(np.float64)
    cents = np.rint(fare * 100).astype(np.int64)
    store_arrow = table["store_and_fwd_flag"].fill_null("N")
    store = store_arrow.to_pylist()
    stored_bool = np.fromiter((value == "Y" for value in store), dtype=np.bool_)

    payment_names = {
        0: b"flex_fare",
        1: b"credit_card",
        2: b"cash",
        3: b"no_charge",
        4: b"dispute",
        5: b"unknown",
        6: b"voided",
    }
    categories = [payment_names.get(int(value), b"other") for value in payment]
    routes = [
        f"zone:{int(pickup_id)}->zone:{int(dropoff_id)}".encode()
        for pickup_id, dropoff_id in zip(pickup_zone, dropoff_zone)
    ]
    binary_routes = [
        struct.pack("<II", int(pickup_id), int(dropoff_id)) + bytes((int(pay) & 0xFF,))
        for pickup_id, dropoff_id, pay in zip(pickup_zone, dropoff_zone, payment)
    ]
    fixed_ids = [
        hashlib.blake2b(
            struct.pack("<qII", int(timestamp), int(pickup_id), int(dropoff_id)),
            digest_size=16,
        ).digest()
        for timestamp, pickup_id, dropoff_id in zip(pickup, pickup_zone, dropoff_zone)
    ]

    # Controlled probes complement the real event stream. They prevent encoding
    # decisions from overfitting the TLC file's deliberately messy timestamp order.
    sample_count = len(pickup)
    increments = np.full(sample_count, 1_000_000, dtype=np.int64)
    increments[::1009] = 2_000_000
    regular_time = 1_700_000_000_000_000 + np.cumsum(increments, dtype=np.int64)
    counter_increments = np.ones(sample_count, dtype=np.int64)
    counter_increments[::257] = 2
    monotonic_counter = np.cumsum(counter_increments, dtype=np.int64)
    random = np.random.default_rng(20260717)
    phase = np.arange(sample_count, dtype=np.float64) / 200.0
    smooth_sensor = np.sin(phase) * 20.0 + random.normal(0.0, 0.01, sample_count)
    sparse_validity = np.ones(sample_count, dtype=np.bool_)
    sparse_validity[::101] = False
    for start in range(2048, sample_count, 16384):
        sparse_validity[start : start + 128] = False

    cases = [
        ColumnCase(
            "stored_and_forwarded", "bool", stored_bool, note="derived from Y/N flag"
        ),
        ColumnCase("zone_delta", "int32", signed_zone_delta),
        ColumnCase("pickup_zone", "uint32", pickup_zone),
        ColumnCase(
            "passenger_count",
            "int64",
            passenger,
            nullable=True,
            validity=passenger_validity,
            note="dense non-null values; validity is reported separately",
        ),
        ColumnCase("trip_distance", "float64", distance),
        ColumnCase("fare_cents", "decimal64[scale=2]", cents),
        ColumnCase("pickup_time", "timestamp64[us]", pickup),
        ColumnCase(
            "route", "utf8", routes, note="derived from real pickup/dropoff zones"
        ),
        ColumnCase("payment", "categorical", categories),
        ColumnCase("route_key", "binary", binary_routes),
        ColumnCase("trip_id", "fixed_binary[16]", fixed_ids),
        ColumnCase(
            "passenger_validity",
            "bool",
            passenger_validity,
            note="NULL validity bitmap",
        ),
        ColumnCase(
            "regular_sensor_time",
            "timestamp64[us]",
            regular_time,
            note="controlled mostly-regular cadence",
        ),
        ColumnCase(
            "monotonic_counter",
            "uint64",
            monotonic_counter.astype(np.uint64),
            note="controlled increments of one or two",
        ),
        ColumnCase(
            "smooth_sensor",
            "float64",
            smooth_sensor,
            note="controlled smooth signal with noise",
        ),
        ColumnCase(
            "sparse_validity",
            "bool",
            sparse_validity,
            note="controlled isolated and burst NULL pattern",
        ),
    ]
    return cases, sample_offsets, source_rows


def benchmark_case(
    case: ColumnCase,
    block_rows: int,
    block_count: int,
    compressor: zstd.ZstdCompressor,
    decompressor: zstd.ZstdDecompressor,
) -> dict[str, object]:
    candidate_totals: dict[str, dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    candidate_codecs: dict[str, Counter[str]] = defaultdict(Counter)
    selected = Counter()
    raw_total = 0
    selected_total = 0
    selected_encode_seconds = 0.0
    selected_decode_seconds = 0.0
    search_seconds = 0.0
    rows_total = 0

    all_values = case.values
    validity_all = case.validity

    for block_index in range(block_count):
        start = block_index * block_rows
        end = min(start + block_rows, len(all_values))  # type: ignore[arg-type]
        if start >= end:
            break
        block_values = _slice_values(all_values, start, end)
        block_validity = validity_all[start:end] if validity_all is not None else None
        dense_values = _non_null(block_values, block_validity)
        count = len(dense_values)  # type: ignore[arg-type]
        rows_total += count
        raw_size = _raw_size(case, dense_values)
        raw_total += raw_size

        search_started = time.perf_counter()
        measured: list[tuple[Candidate, str, bytes, float, float]] = []
        for _ in range(1):
            encode_started = time.perf_counter()
            candidates = _candidates(case, dense_values)
            candidate_generation = time.perf_counter() - encode_started
        per_candidate_generation = candidate_generation / max(1, len(candidates))
        for candidate in candidates:
            compress_started = time.perf_counter()
            codec, stored = _compress(candidate.payload, compressor)
            compress_seconds = time.perf_counter() - compress_started
            measured.append(
                (candidate, codec, stored, per_candidate_generation, compress_seconds)
            )
            restored_payload = (
                decompressor.decompress(stored) if codec == "zstd" else stored
            )
            if not _equal(candidate.decode(restored_payload, count), dense_values):
                raise AssertionError(
                    f"round-trip failed for {case.name}/{candidate.name} block {block_index}"
                )
            candidate_totals[candidate.name]["encoded_bytes"] += len(candidate.payload)
            candidate_totals[candidate.name]["stored_bytes"] += len(stored)
            candidate_totals[candidate.name]["encode_seconds"] += (
                per_candidate_generation + compress_seconds
            )
            candidate_codecs[candidate.name][codec] += 1
        search_seconds += time.perf_counter() - search_started

        preference = {
            "raw": 0,
            "bitpack": 0,
            "constant": 1,
            "frame_of_reference": 2,
            "delta": 3,
            "dictionary": 4,
            "rle": 5,
            "byte_stream_split": 6,
            "xor": 7,
            "delta_of_delta": 8,
        }
        candidate, codec, stored, generation_seconds, compress_seconds = min(
            measured,
            key=lambda item: (
                len(item[2]),
                preference.get(item[0].name, 100),
                item[0].name,
            ),
        )
        selected[f"{candidate.name}+{codec}"] += 1
        selected_total += len(stored)
        selected_encode_seconds += generation_seconds + compress_seconds

        decode_started = time.perf_counter()
        encoded = decompressor.decompress(stored) if codec == "zstd" else stored
        decoded = candidate.decode(encoded, count)
        selected_decode_seconds += time.perf_counter() - decode_started
        if not _equal(decoded, dense_values):
            raise AssertionError(
                f"round-trip failed for {case.name}/{candidate.name} block {block_index}"
            )

    input_mib = raw_total / (1024 * 1024)
    details = []
    for name, totals in candidate_totals.items():
        details.append(
            {
                "encoding": name,
                "encoded_bytes": int(totals["encoded_bytes"]),
                "stored_bytes": int(totals["stored_bytes"]),
                "ratio_to_raw": totals["stored_bytes"] / raw_total
                if raw_total
                else 0.0,
                "codecs": dict(candidate_codecs[name]),
            }
        )
    details.sort(key=lambda row: (row["stored_bytes"], row["encoding"]))
    return {
        "name": case.name,
        "logical_type": case.logical_type,
        "rows": rows_total,
        "blocks": sum(selected.values()),
        "raw_bytes": raw_total,
        "selected_bytes": selected_total,
        "ratio_to_raw": selected_total / raw_total if raw_total else 0.0,
        "selected": dict(selected),
        "selected_encode_mib_s": input_mib / selected_encode_seconds
        if selected_encode_seconds
        else None,
        "selected_decode_mib_s": input_mib / selected_decode_seconds
        if selected_decode_seconds
        else None,
        "exhaustive_search_mib_s": input_mib / search_seconds
        if search_seconds
        else None,
        "candidates": details,
        "note": case.note,
    }


def to_markdown(report: dict[str, object]) -> str:
    configuration = report["configuration"]
    assert isinstance(configuration, dict)
    lines = [
        "# Encoding study results",
        "",
        "> Generated by `benchmarks/encoding_study.py`. Python throughput is a",
        "> relative design signal, not a projection of the planned C++ implementation.",
        "",
        f"- Dataset: `{configuration['dataset']}`",
        f"- Rows loaded: {configuration['rows_loaded']:,}",
        f"- Sampling: {configuration['sampling']} at offsets {configuration['sample_offsets']}",
        f"- Block rows: {configuration['block_rows']:,}",
        f"- Blocks per type: {configuration['blocks']}",
        f"- Zstandard level: {configuration['zstd_level']}",
        "",
        "| Column | Logical type | Best stored/raw | Selected encoding(s) | Estimated selected encode MiB/s | Decode MiB/s | Exhaustive search MiB/s |",
        "| --- | --- | ---: | --- | ---: | ---: | ---: |",
    ]
    results = report["results"]
    assert isinstance(results, list)
    for result in results:
        assert isinstance(result, dict)
        selected = ", ".join(
            f"{name} ×{count}" for name, count in result["selected"].items()
        )
        lines.append(
            "| {name} | `{logical_type}` | {ratio:.3f} | {selected} | {enc:.1f} | {dec:.1f} | {search:.1f} |".format(
                name=result["name"],
                logical_type=result["logical_type"],
                ratio=result["ratio_to_raw"],
                selected=selected,
                enc=result["selected_encode_mib_s"] or 0.0,
                dec=result["selected_decode_mib_s"] or 0.0,
                search=result["exhaustive_search_mib_s"] or 0.0,
            )
        )
    lines.extend(
        [
            "",
            "## Candidate sizes",
            "",
            "Ratios include the candidate transform followed by Zstandard when it saved",
            "at least eight bytes. They exclude the common frame and stream-directory",
            "metadata, which does not affect the candidate ranking.",
            "Nullable dense values and their validity stream are reported as separate",
            "cases so each representation remains visible.",
            "",
        ]
    )
    for result in results:
        assert isinstance(result, dict)
        lines.extend(
            [
                f"### {result['name']} (`{result['logical_type']}`)",
                "",
                "| Candidate | Stored bytes | Stored/raw |",
                "| --- | ---: | ---: |",
            ]
        )
        for candidate in result["candidates"]:
            codecs = ", ".join(
                f"{codec}×{count}" for codec, count in candidate["codecs"].items()
            )
            lines.append(
                f"| {candidate['encoding']} ({codecs}) | {candidate['stored_bytes']:,} | {candidate['ratio_to_raw']:.3f} |"
            )
        lines.append("")
    return "\n".join(lines)


def self_test_candidate_roundtrips() -> None:
    edge_cases = [
        ColumnCase("bool_edges", "bool", np.array([False, True, True, False])),
        ColumnCase(
            "int64_edges",
            "int64",
            np.array([np.iinfo(np.int64).min, 0, np.iinfo(np.int64).max]),
        ),
        ColumnCase(
            "uint64_edges",
            "uint64",
            np.array([0, 1, np.iinfo(np.uint64).max], dtype=np.uint64),
        ),
        ColumnCase(
            "float_bits",
            "float64",
            np.array([0.0, -0.0, math.nan, math.inf, -math.inf], dtype=np.float64),
        ),
        ColumnCase("utf8_edges", "utf8", [b"", b"a", "λ".encode(), b"a"]),
        ColumnCase(
            "fixed_edges",
            "fixed_binary[2]",
            [b"\x00\x01", b"\xff\x00", b"\x00\x01"],
        ),
    ]
    for case in edge_cases:
        for candidate in _candidates(case, case.values):
            decoded = candidate.decode(candidate.payload, len(case.values))  # type: ignore[arg-type]
            if not _equal(decoded, case.values):
                raise AssertionError(
                    f"edge-case round-trip failed for {case.name}/{candidate.name}"
                )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--block-rows", type=int, default=65_536)
    parser.add_argument("--blocks", type=int, default=4)
    parser.add_argument("--zstd-level", type=int, default=1)
    parser.add_argument("--sampling", choices=("evenly", "head"), default="evenly")
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    self_test_candidate_roundtrips()
    rows = args.block_rows * args.blocks
    cases, sample_offsets, source_rows = load_cases(
        args.dataset, rows, args.block_rows, args.blocks, args.sampling
    )
    compressor = zstd.ZstdCompressor(level=args.zstd_level)
    decompressor = zstd.ZstdDecompressor()
    results = [
        benchmark_case(case, args.block_rows, args.blocks, compressor, decompressor)
        for case in cases
    ]
    report = {
        "configuration": {
            "dataset": str(args.dataset),
            "rows_loaded": rows,
            "source_rows": source_rows,
            "block_rows": args.block_rows,
            "blocks": args.blocks,
            "sampling": args.sampling,
            "sample_offsets": sample_offsets,
            "zstd_level": args.zstd_level,
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "numpy": np.__version__,
            "pyarrow": pa.__version__,
            "zstandard": zstd.__version__,
        },
        "results": results,
    }
    rendered_json = json.dumps(report, indent=2) + "\n"
    rendered_markdown = to_markdown(report) + "\n"
    if args.json_output:
        args.json_output.write_text(rendered_json)
    if args.markdown_output:
        args.markdown_output.write_text(rendered_markdown)
    if not args.json_output and not args.markdown_output:
        print(rendered_markdown, end="")


if __name__ == "__main__":
    main()
