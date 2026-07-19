"""Byte-exact regeneration of every spec fixture through the public Writer."""

from __future__ import annotations

import acta

from conftest import fixture_bytes


def regenerate(tmp_path, name, schema, batch, *, file_id, row_ids, options):
    path = tmp_path / f"{name}.acta"
    with acta.create(
        path, schema, row_ids=row_ids, file_id=file_id, options=options
    ) as writer:
        writer.append(batch)
    return path.read_bytes()


def test_minimal_fixture_regenerates_byte_exactly(tmp_path):
    schema = acta.Schema([acta.timestamp64("time", unit="us", tz="UTC")])
    data = regenerate(
        tmp_path,
        "minimal",
        schema,
        {"time": [1_000_000, 2_000_000, 3_000_000]},
        file_id=bytes(range(16)),
        row_ids=False,
        options=acta.BlockOptions(
            force_plain_raw=True,
            compress=False,
            write_stats="all",
            implicit_flag_non_nullable=True,
            set_ts_sorted=False,
        ),
    )
    assert data == fixture_bytes("minimal")


def test_ts_sorted_fixture_regenerates_byte_exactly(tmp_path):
    schema = acta.Schema([acta.timestamp64("time", unit="us", tz="UTC")])
    data = regenerate(
        tmp_path,
        "ts_sorted",
        schema,
        {"time": [1_000_000, 2_000_000, 2_000_000, 3_000_000]},
        file_id=b"ACTA-TS-SORTED!!",
        row_ids=False,
        options=acta.BlockOptions(
            force_plain_raw=True,
            compress=False,
            write_stats="all",
            implicit_flag_non_nullable=True,
        ),
    )
    assert data == fixture_bytes("ts_sorted")


def test_date32_fixture_regenerates_byte_exactly(tmp_path):
    schema = acta.Schema([acta.date32("date"), acta.float64("close")])
    data = regenerate(
        tmp_path,
        "date32",
        schema,
        {"date": [20455, 20458, 20459], "close": [101.25, 102.5, 101.75]},
        file_id=b"ACTA-DATE32-EOD!",
        row_ids=False,
        options=acta.BlockOptions(
            force_plain_raw=True, compress=False, write_stats="non_primary"
        ),
    )
    assert data == fixture_bytes("date32")


def test_nyc_taxi_fixture_regenerates_byte_exactly(tmp_path):
    schema = acta.Schema(
        [
            acta.bool_("stored_and_forwarded"),
            acta.int32("zone_delta"),
            acta.uint32("pickup_zone"),
            acta.int64("passenger_count", nullable=True),
            acta.float64("trip_distance"),
            acta.decimal64("fare_cents", precision=18, scale=2),
            acta.timestamp64("pickup_time", unit="us"),
            acta.utf8("route"),
            acta.categorical("payment"),
            acta.binary("route_key"),
            acta.fixed_binary("trip_id", width=16),
            acta.bool_("passenger_validity"),
            acta.timestamp64("regular_sensor_time", unit="us"),
            acta.uint64("monotonic_counter"),
            acta.float64("smooth_sensor"),
            acta.bool_("sparse_validity"),
        ],
        primary="pickup_time",
    )
    batch = {
        "stored_and_forwarded": [False, False, False],
        "zone_delta": [-1, -1, 194],
        "pickup_zone": [239, 163, 43],
        "passenger_count": [1, 0, 0],
        "trip_distance": [0.97, 0.9, 1.4],
        "fare_cents": [720, 790, 1070],
        "pickup_time": [1767228844000000, 1767227644000000, 1767229026000000],
        "route": ["zone:239->zone:238", "zone:163->zone:162", "zone:43->zone:237"],
        "payment": ["credit_card", "cash", "credit_card"],
        "route_key": [
            bytes.fromhex("ef000000ee00000001"),
            bytes.fromhex("a3000000a200000002"),
            bytes.fromhex("2b000000ed00000001"),
        ],
        "trip_id": [
            bytes.fromhex("4ff64384c5e171e154cd3f6a63d9bdd6"),
            bytes.fromhex("d8334a1b4071932f75b5bc0b2948113f"),
            bytes.fromhex("4d97ac7fc5767286b2afeb64d7397dd6"),
        ],
        "passenger_validity": [True, True, True],
        "regular_sensor_time": [1700000002000000, 1700000003000000, 1700000004000000],
        "monotonic_counter": [2, 3, 4],
        "smooth_sensor": [
            0.013989386228953918,
            0.10332694225775996,
            0.18895417315694527,
        ],
        "sparse_validity": [False, True, True],
    }
    data = regenerate(
        tmp_path,
        "nyc_taxi_3_rows",
        schema,
        batch,
        file_id=b"ACTA-TAXI-3ROWS!",
        row_ids=True,
        options=acta.BlockOptions(
            force_plain_raw=True, compress=False, write_stats="none"
        ),
    )
    assert data == fixture_bytes("nyc_taxi_3_rows")
