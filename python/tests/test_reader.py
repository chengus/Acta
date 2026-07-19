"""Reader behavior: pruning, projection, time bounds, and tailing."""

from __future__ import annotations

import datetime

import numpy as np
import pytest

import acta


@pytest.fixture()
def three_block_file(tmp_path):
    schema = acta.Schema(
        [acta.timestamp64("t", unit="us"), acta.float64("x"), acta.utf8("tag")]
    )
    path = tmp_path / "three.acta"
    with acta.create(path, schema, row_ids=True) as writer:
        writer.append({"t": [100, 200, 300], "x": [1.0, 2.0, 3.0], "tag": list("abc")})
        writer.flush()
        writer.append({"t": [400, 500], "x": [4.0, 5.0], "tag": list("de")})
        writer.flush()
        writer.append({"t": [900, 700, 800], "x": [9.0, 7.0, 8.0], "tag": list("igh")})
    return path


def test_block_pruning_uses_header_bounds(three_block_file):
    with acta.open(three_block_file) as reader:
        assert reader.num_blocks == 3
        assert len(list(reader.blocks(start=400, end=600))) == 1
        assert len(list(reader.blocks(end=100))) == 0  # half-open: t < 100
        assert len(list(reader.blocks(start=901))) == 0
        assert len(list(reader.blocks(start=300, end=401))) == 2


def test_read_time_range_within_sorted_and_unsorted_blocks(three_block_file):
    with acta.open(three_block_file) as reader:
        result = reader.read(columns=["t", "x"], start=200, end=800)
        assert result["t"].values.tolist() == [200, 300, 400, 500, 700]
        assert result["x"].values.tolist() == [2.0, 3.0, 4.0, 5.0, 7.0]
        # The third block is unsorted, so selection there used a row mask.
        assert result.row_ids.tolist() == [1, 2, 3, 4, 6]


def test_read_projection_without_primary(three_block_file):
    with acta.open(three_block_file) as reader:
        result = reader.read(columns=["tag"])
        assert result["tag"].to_pylist() == list("abcdeigh")
        with pytest.raises(acta.SchemaError):
            reader.read(columns=["missing"])


def test_datetime_bounds_convert_via_the_column_unit(tmp_path):
    schema = acta.Schema([acta.timestamp64("t", unit="ms", tz="UTC")])
    base = datetime.datetime(2026, 7, 19, tzinfo=datetime.timezone.utc)
    epoch_ms = int(base.timestamp() * 1000)
    path = tmp_path / "dt.acta"
    with acta.create(path, schema) as writer:
        writer.append({"t": [epoch_ms, epoch_ms + 1000, epoch_ms + 2000]})
    with acta.open(path) as reader:
        result = reader.read(start=base, end=base + datetime.timedelta(seconds=2))
        assert result.row_count == 2
        result = reader.read(start=np.datetime64(base.replace(tzinfo=None), "ms"))
        assert result.row_count == 3


def test_date_bounds_for_date32_primary(tmp_path):
    schema = acta.Schema([acta.date32("d"), acta.float64("x")])
    path = tmp_path / "dates.acta"
    days = [
        (datetime.date(2026, 1, day) - datetime.date(1970, 1, 1)).days
        for day in (2, 5, 6)
    ]
    with acta.create(path, schema) as writer:
        writer.append({"d": days, "x": [1.0, 2.0, 3.0]})
    with acta.open(path) as reader:
        result = reader.read(
            start=datetime.date(2026, 1, 3), end=datetime.date(2026, 1, 6)
        )
        assert result["d"].values.tolist() == [days[1]]


def test_refresh_sees_new_blocks(three_block_file):
    with acta.open(three_block_file) as reader:
        assert reader.num_blocks == 3
        with acta.open_append(three_block_file) as writer:
            writer.append({"t": [1000], "x": [10.0], "tag": ["z"]})
        assert reader.refresh() == 1
        assert reader.num_blocks == 4
        assert reader.read(columns=["tag"])["tag"].to_pylist()[-1] == "z"


def test_follow_yields_existing_then_new_blocks(three_block_file):
    with acta.open(three_block_file) as reader:
        seen = []
        follower = reader.follow(poll_interval=0.01, timeout=0.2)
        for block in follower:
            seen.append(block.sequence)
            if len(seen) == 3:
                break
        assert seen == [1, 2, 3]
        with acta.open_append(three_block_file) as writer:
            writer.append({"t": [1000], "x": [10.0], "tag": ["z"]})
        remaining = list(follower)
        assert [block.sequence for block in remaining] == [4]


def test_incomplete_tail_is_invisible_to_readers(three_block_file, tmp_path):
    data = three_block_file.read_bytes()
    with acta.open(three_block_file) as reader:
        last_offset = list(reader.blocks())[-1].offset
    torn = tmp_path / "torn.acta"
    torn.write_bytes(data[: last_offset + 20])  # mid-prefix tear
    with acta.open(torn) as reader:
        assert reader.num_blocks == 2
        assert reader.read(columns=["x"])["x"].values.tolist() == [
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
        ]


def test_strict_open_detects_payload_corruption(three_block_file, tmp_path):
    data = bytearray(three_block_file.read_bytes())
    with acta.open(three_block_file) as reader:
        block = list(reader.blocks())[0]
        payload_offset = block._entry.frame.payload_offset
    data[payload_offset] ^= 0x01
    damaged = tmp_path / "damaged.acta"
    damaged.write_bytes(bytes(data))
    with pytest.raises(acta.CorruptionError):
        acta.open(damaged, strict=True)
    # A lax open defers to per-stream CRCs, which fail on projection.
    with acta.open(damaged) as reader:
        with pytest.raises(acta.CorruptionError):
            reader.read(columns=["t"])
