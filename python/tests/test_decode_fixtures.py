"""Logical decoding of every checked-in spec fixture (the decode oracle)."""

from __future__ import annotations

import numpy as np
import pytest

from acta import decode, framing
from acta.enums import BLOCK_TS_SORTED
from acta.schema import Schema

from conftest import fixture_bytes


def load_blocks(data: bytes):
    result = framing.scan_bytes(data)
    assert not result.incomplete_tail
    schema_frame = result.frames[0]
    schema = Schema.from_frame(schema_frame.header, schema_frame.payload)
    blocks = []
    for frame in result.frames[1:]:
        meta = decode.parse_block_header(
            frame.header,
            schema,
            payload_length=frame.payload_length,
            file_row_ids=result.prologue.row_ids,
        )
        blocks.append((meta, frame.payload))
    return schema, blocks


def decode_all(meta, payload):
    def read(stream):
        return decode.read_stream_from_payload(payload, stream)

    return {
        column_meta.column.name: decode.decode_column(meta, column_meta, read)
        for column_meta in meta.columns
    }


def test_minimal_fixture_decodes():
    schema, blocks = load_blocks(fixture_bytes("minimal"))
    assert len(blocks) == 1
    meta, payload = blocks[0]
    assert meta.base_row_id is None
    assert (meta.ts_min, meta.ts_max) == (1_000_000, 3_000_000)
    columns = decode_all(meta, payload)
    time = columns["time"]
    assert time.validity is None
    assert np.array_equal(time.values, [1_000_000, 2_000_000, 3_000_000])
    stats = decode.parse_min_max_stats(
        schema.column("time"),
        meta.header[meta.columns[0].stats_offset :][: meta.columns[0].stats_length],
    )
    assert (int(stats[0]), int(stats[1])) == (1_000_000, 3_000_000)


def test_nyc_taxi_fixture_decodes_every_column():
    schema, blocks = load_blocks(fixture_bytes("nyc_taxi_3_rows"))
    meta, payload = blocks[0]
    assert meta.base_row_id == 0
    assert meta.row_count == 3
    columns = decode_all(meta, payload)

    assert columns["stored_and_forwarded"].to_pylist() == [False, False, False]
    assert columns["zone_delta"].to_pylist() == [-1, -1, 194]
    assert columns["pickup_zone"].to_pylist() == [239, 163, 43]
    assert columns["passenger_count"].to_pylist() == [1, 0, 0]
    assert columns["trip_distance"].to_pylist() == [0.97, 0.9, 1.4]
    assert columns["fare_cents"].to_pylist() == [720, 790, 1070]
    assert [str(d) for d in columns["fare_cents"].to_decimal()] == [
        "7.20",
        "7.90",
        "10.70",
    ]
    assert columns["pickup_time"].to_pylist() == [
        1767228844000000,
        1767227644000000,
        1767229026000000,
    ]
    assert columns["route"].to_pylist() == [
        "zone:239->zone:238",
        "zone:163->zone:162",
        "zone:43->zone:237",
    ]
    assert columns["payment"].to_pylist() == ["credit_card", "cash", "credit_card"]
    assert columns["route_key"].to_pylist() == [
        bytes.fromhex("ef000000ee00000001"),
        bytes.fromhex("a3000000a200000002"),
        bytes.fromhex("2b000000ed00000001"),
    ]
    assert columns["trip_id"].to_pylist() == [
        bytes.fromhex("4ff64384c5e171e154cd3f6a63d9bdd6"),
        bytes.fromhex("d8334a1b4071932f75b5bc0b2948113f"),
        bytes.fromhex("4d97ac7fc5767286b2afeb64d7397dd6"),
    ]
    assert columns["passenger_validity"].to_pylist() == [True, True, True]
    assert columns["regular_sensor_time"].to_pylist() == [
        1700000002000000,
        1700000003000000,
        1700000004000000,
    ]
    assert columns["monotonic_counter"].to_pylist() == [2, 3, 4]
    assert columns["smooth_sensor"].to_pylist() == [
        0.013989386228953918,
        0.10332694225775996,
        0.18895417315694527,
    ]
    assert columns["sparse_validity"].to_pylist() == [False, True, True]

    primary = columns["pickup_time"].values
    decode.verify_block_bounds(meta, primary)
    assert not meta.ts_sorted


def test_ts_sorted_fixture_flag_and_verification():
    schema, blocks = load_blocks(fixture_bytes("ts_sorted"))
    meta, payload = blocks[0]
    assert meta.flags & BLOCK_TS_SORTED
    columns = decode_all(meta, payload)
    time = columns["time"].values
    assert time.tolist() == [1_000_000, 2_000_000, 2_000_000, 3_000_000]
    decode.verify_ts_sorted(meta, time)
    decode.verify_block_bounds(meta, time)


def test_date32_fixture_values_and_close_stats():
    schema, blocks = load_blocks(fixture_bytes("date32"))
    meta, payload = blocks[0]
    assert meta.ts_sorted
    assert (meta.ts_min, meta.ts_max) == (20455, 20459)
    columns = decode_all(meta, payload)
    dates = columns["date"]
    assert dates.values.tolist() == [20455, 20458, 20459]
    assert [str(d) for d in dates.to_datetime64()] == [
        "2026-01-02",
        "2026-01-05",
        "2026-01-06",
    ]
    assert columns["close"].to_pylist() == [101.25, 102.5, 101.75]
    decode.verify_ts_sorted(meta, dates.values)
    close_meta = meta.column_meta("close")
    stats = decode.parse_min_max_stats(
        schema.column("close"),
        meta.header[close_meta.stats_offset :][: close_meta.stats_length],
    )
    assert (float(stats[0]), float(stats[1])) == (101.25, 102.5)


def test_flipped_ts_sorted_bit_is_caught_by_the_header_crc():
    data = bytearray(fixture_bytes("ts_sorted"))
    result = framing.scan_bytes(bytes(data))
    block_frame = result.frames[1]
    flags_offset = block_frame.offset + 48 + 56  # prefix + block-flag field
    data[flags_offset] ^= BLOCK_TS_SORTED
    with pytest.raises(Exception):
        framing.scan_bytes(bytes(data))
