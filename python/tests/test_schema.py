"""Schema model and schema-frame (de)serialization tests."""

from __future__ import annotations

import pytest

import acta
from acta import framing
from acta.enums import LogicalType, TimestampUnit, TimezoneMode
from acta.errors import SchemaError
from acta.schema import Schema


def make_schema() -> Schema:
    return Schema(
        [
            acta.timestamp64("time", unit="us", tz="UTC"),
            acta.float64("close", nullable=True),
            acta.decimal64("price", precision=18, scale=4),
            acta.categorical("route", ordered=True),
            acta.fixed_binary("digest", width=16),
            acta.date32("session"),
            acta.utf8("note", nullable=True),
        ]
    )


def test_ids_auto_assign_in_declaration_order():
    schema = make_schema()
    assert [column.id for column in schema] == [1, 2, 3, 4, 5, 6, 7]
    assert schema.primary.name == "time"


def test_primary_defaults_to_first_time_column():
    schema = Schema([acta.float64("x"), acta.date32("d"), acta.timestamp64("t")])
    assert schema.primary.name == "d"


def test_serialization_round_trip():
    schema = make_schema()
    restored = Schema.from_frame(schema.to_frame_header(), schema.to_frame_payload())
    assert restored == schema
    assert restored.to_frame_payload() == schema.to_frame_payload()


def test_iana_timezone_round_trip():
    schema = Schema([acta.timestamp64("time", unit="ns", tz="America/New_York")])
    column = schema.column("time")
    assert column.timezone_mode == TimezoneMode.IANA
    assert column.timezone_name == "America/New_York"
    restored = Schema.from_frame(schema.to_frame_header(), schema.to_frame_payload())
    assert restored == schema


@pytest.mark.parametrize(
    "build",
    [
        lambda: Schema([acta.float64("x")]),  # no time column
        lambda: Schema([acta.timestamp64("t", nullable=True)]),  # nullable primary
        lambda: Schema([acta.timestamp64("t")], primary="missing"),
        lambda: Schema([acta.timestamp64("t"), acta.float64("t")]),  # duplicate name
        lambda: Schema([acta.timestamp64("t")], schema_id=0),
        lambda: Schema([acta.timestamp64("t"), acta.float64("x")], primary="x"),
        lambda: acta.decimal64("d", precision=19, scale=0),
        lambda: acta.fixed_binary("f", width=0),
    ],
)
def test_invalid_schemas_are_rejected(build):
    with pytest.raises(SchemaError):
        build()


def test_fixture_schema_frames_reserialize_byte_identically(any_fixture):
    name, data = any_fixture
    result = framing.scan_bytes(data)
    schema_frame = result.frames[0]
    schema = Schema.from_frame(schema_frame.header, schema_frame.payload)
    assert framing.pad8(schema.to_frame_header()) == schema_frame.header
    assert framing.pad8(schema.to_frame_payload()) == schema_frame.payload
    if name == "nyc_taxi_3_rows":
        assert len(schema) == 16
        assert schema.primary.name == "pickup_time"
        assert schema.column("payment").type == LogicalType.CATEGORICAL
        assert schema.column("trip_id").width == 16
        assert schema.column("fare_cents").precision == 18
        assert schema.column("fare_cents").scale == 2
        assert schema.column("pickup_time").unit == TimestampUnit.MICROSECOND
        assert schema.column("passenger_count").nullable
    if name == "date32":
        assert schema.primary.type == LogicalType.DATE32
