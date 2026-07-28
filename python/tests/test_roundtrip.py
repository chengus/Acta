"""Write→read round trips through the default encoding selection."""

from __future__ import annotations

import datetime
from decimal import Decimal

import numpy as np
import pytest

import acta


def roundtrip(tmp_path, schema, batches, **create_kwargs):
    path = tmp_path / "roundtrip.acta"
    with acta.create(path, schema, **create_kwargs) as writer:
        for batch in batches:
            writer.append(batch)
    with acta.open(path, strict=True) as reader:
        return reader.read()


def all_types_schema() -> acta.Schema:
    return acta.Schema(
        [
            acta.timestamp64("time", unit="ns", tz="UTC"),
            acta.bool_("flag", nullable=True),
            acta.int8("i8", nullable=True),
            acta.int16("i16"),
            acta.int32("i32"),
            acta.int64("i64", nullable=True),
            acta.uint8("u8"),
            acta.uint16("u16"),
            acta.uint32("u32"),
            acta.uint64("u64"),
            acta.float32("f32"),
            acta.float64("f64", nullable=True),
            acta.decimal64("price", precision=18, scale=4, nullable=True),
            acta.utf8("text", nullable=True),
            acta.categorical("label"),
            acta.binary("blob", nullable=True),
            acta.fixed_binary("digest", width=8),
            acta.date32("session"),
        ]
    )


def test_every_logical_type_round_trips(tmp_path):
    rows = [
        {
            "time": 1_700_000_000_000_000_000,
            "flag": True,
            "i8": -128,
            "i16": -32768,
            "i32": 2147483647,
            "i64": None,
            "u8": 255,
            "u16": 65535,
            "u32": 4294967295,
            "u64": (1 << 64) - 1,
            "f32": 1.5,
            "f64": None,
            "price": Decimal("12.3456"),
            "text": "héllo",
            "label": "aa",
            "blob": b"\x00\x01",
            "digest": b"12345678",
            "session": datetime.date(2026, 7, 18),
        },
        {
            "time": 1_700_000_000_000_000_001,
            "flag": None,
            "i8": None,
            "i16": 7,
            "i32": -1,
            "i64": 42,
            "u8": 0,
            "u16": 1,
            "u32": 2,
            "u64": 3,
            "f32": -2.25,
            "f64": float("nan"),
            "price": None,
            "text": None,
            "label": "bb",
            "blob": None,
            "digest": b"abcdefgh",
            "session": datetime.date(2026, 7, 19),
        },
    ]
    batch = {name: [row[name] for row in rows] for name in rows[0]}
    result = roundtrip(tmp_path, all_types_schema(), [batch])
    assert result.row_count == 2
    for name in batch:
        expected = batch[name]
        actual = result[name].to_pylist()
        if name == "session":
            expected = [(d - datetime.date(1970, 1, 1)).days for d in expected]
        if name == "price":
            assert result[name].to_decimal() == [Decimal("12.3456"), None]
            continue
        if name == "f64":
            assert actual[0] is None
            assert np.isnan(actual[1])
            continue
        assert actual == expected, name


def test_nan_and_negative_zero_bits_survive(tmp_path):
    schema = acta.Schema([acta.timestamp64("t"), acta.float64("x")])
    payload_nan = np.frombuffer(b"\x01\x00\x00\x00\x00\x00\xf8\x7f", dtype=np.float64)[
        0
    ]
    values = np.array([payload_nan, -0.0, 0.0, np.inf], dtype=np.float64)
    result = roundtrip(tmp_path, schema, [{"t": [1, 2, 3, 4], "x": values}])
    stored = result["x"].values
    assert stored.tobytes() == values.tobytes()


@pytest.mark.parametrize("nullable_state", ["all_null", "all_valid", "mixed"])
def test_nullable_column_states(tmp_path, nullable_state):
    schema = acta.Schema([acta.timestamp64("t"), acta.int32("v", nullable=True)])
    values = {
        "all_null": [None] * 5,
        "all_valid": [1, 2, 3, 4, 5],
        "mixed": [1, None, 3, None, 5],
    }[nullable_state]
    result = roundtrip(tmp_path, schema, [{"t": list(range(5)), "v": values}])
    assert result["v"].to_pylist() == values


def test_multi_block_row_id_continuity_and_ordering(tmp_path):
    schema = acta.Schema([acta.timestamp64("t"), acta.int64("v")])
    path = tmp_path / "blocks.acta"
    with acta.create(path, schema, row_ids=True, block_rows=10) as writer:
        writer.append({"t": list(range(25)), "v": list(range(25))})
    with acta.open(path, strict=True) as reader:
        assert reader.num_blocks == 3
        assert [block.row_count for block in reader.blocks()] == [10, 10, 5]
        bases = [block.base_row_id for block in reader.blocks()]
        assert bases == [0, 10, 20]
        assert all(block.ts_sorted for block in reader.blocks())
        result = reader.read()
        assert result.row_ids.tolist() == list(range(25))
        assert result["v"].values.tolist() == list(range(25))


def test_unsorted_blocks_and_time_ordering(tmp_path):
    schema = acta.Schema([acta.timestamp64("t"), acta.utf8("v")])
    path = tmp_path / "unsorted.acta"
    with acta.create(path, schema) as writer:
        writer.append({"t": [30, 10, 20], "v": ["c", "a", "b"]})
        writer.flush()
        writer.append({"t": [5, 25], "v": ["z", "m"]})
    with acta.open(path, strict=True) as reader:
        blocks = list(reader.blocks())
        assert not blocks[0].ts_sorted
        assert blocks[1].ts_sorted
        in_file_order = reader.read()
        assert in_file_order["v"].to_pylist() == ["c", "a", "b", "z", "m"]
        in_time_order = reader.read(order="time")
        assert in_time_order["v"].to_pylist() == ["z", "a", "b", "m", "c"]
        assert in_time_order["t"].values.tolist() == [5, 10, 20, 25, 30]


def test_open_append_continues_sequences_and_row_ids(tmp_path):
    schema = acta.Schema([acta.timestamp64("t"), acta.int64("v")])
    path = tmp_path / "resume.acta"
    with acta.create(path, schema, row_ids=True) as writer:
        writer.append({"t": [1, 2], "v": [10, 20]})
    with acta.open_append(path) as writer:
        assert writer.schema == schema
        writer.append({"t": [3], "v": [30]})
    with acta.open(path, strict=True) as reader:
        result = reader.read()
        assert result["v"].values.tolist() == [10, 20, 30]
        assert result.row_ids.tolist() == [0, 1, 2]
        assert [block.sequence for block in reader.blocks()] == [1, 2]


def test_open_append_rejects_schema_mismatch(tmp_path):
    schema = acta.Schema([acta.timestamp64("t")])
    path = tmp_path / "mismatch.acta"
    with acta.create(path, schema) as writer:
        writer.append({"t": [1]})
    other = acta.Schema([acta.timestamp64("t"), acta.int64("v")])
    with pytest.raises(acta.SchemaError):
        acta.open_append(path, expected_schema=other)


def test_encoding_selection_still_round_trips_compressible_data(tmp_path):
    n = 4096
    schema = acta.Schema(
        [
            acta.timestamp64("t"),
            acta.int64("counter"),
            acta.int32("mostly_constant"),
            acta.float64("smooth"),
            acta.categorical("label"),
            acta.bool_("sparse_flag"),
        ]
    )
    rng = np.random.default_rng(7)
    batch = {
        "t": np.arange(n, dtype=np.int64) * 1_000_000,
        "counter": 10_000 + np.cumsum(rng.integers(0, 3, n)),
        "mostly_constant": np.full(n, 42, dtype=np.int32),
        "smooth": np.cumsum(rng.standard_normal(n)) / 1e3,
        "label": rng.choice(["aa", "bb", "cc"], n).tolist(),
        "sparse_flag": ([False] * (n - 1)) + [True],
    }
    path = tmp_path / "selection.acta"
    with acta.create(path, schema) as writer:
        writer.append(batch)
    with acta.open(path, strict=True) as reader:
        result = reader.read()
    assert result["counter"].values.tolist() == list(batch["counter"])
    assert result["mostly_constant"].values.tolist() == [42] * n
    assert result["smooth"].values.tobytes() == np.asarray(batch["smooth"]).tobytes()
    assert result["label"].to_pylist() == batch["label"]
    assert result["sparse_flag"].to_pylist() == batch["sparse_flag"]
    # The specialized encodings must actually pay for themselves.
    raw_path = tmp_path / "raw.acta"
    with acta.create(
        raw_path,
        schema,
        options=acta.BlockOptions(force_plain_raw=True, compress=False),
    ) as writer:
        writer.append(batch)
    assert path.stat().st_size < raw_path.stat().st_size


def test_empty_file_and_empty_append(tmp_path):
    schema = acta.Schema([acta.timestamp64("t")])
    path = tmp_path / "empty.acta"
    with acta.create(path, schema) as writer:
        writer.append({"t": []})
    with acta.open(path) as reader:
        assert reader.num_blocks == 0
        assert reader.read().row_count == 0


def test_exception_discards_uncommitted_buffer(tmp_path):
    schema = acta.Schema([acta.timestamp64("t")])
    path = tmp_path / "abort.acta"
    with pytest.raises(RuntimeError):
        with acta.create(path, schema) as writer:
            writer.append({"t": [1, 2, 3]})
            raise RuntimeError("boom")
    with acta.open(path) as reader:
        assert reader.num_blocks == 0


def test_writer_rejects_bad_batches(tmp_path):
    schema = acta.Schema([acta.timestamp64("t"), acta.int32("v")])
    path = tmp_path / "bad.acta"
    with acta.create(path, schema) as writer:
        with pytest.raises(acta.SchemaError):
            writer.append({"t": [1]})  # missing column
        with pytest.raises(acta.SchemaError):
            writer.append({"t": [1], "v": [1], "extra": [1]})
        with pytest.raises(acta.SchemaError):
            writer.append({"t": [1, 2], "v": [1]})  # ragged
        with pytest.raises(acta.SchemaError):
            writer.append({"t": [1], "v": [None]})  # null in non-nullable
        with pytest.raises(acta.SchemaError):
            writer.append({"t": [1], "v": [1 << 40]})  # out of range
