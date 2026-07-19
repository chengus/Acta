"""Truncation, corruption, recovery, and verification tests."""

from __future__ import annotations

import pytest

import acta


@pytest.fixture()
def healthy(tmp_path):
    schema = acta.Schema([acta.timestamp64("t"), acta.int64("v", nullable=True)])
    path = tmp_path / "healthy.acta"
    with acta.create(path, schema, row_ids=True, sync="close") as writer:
        writer.append({"t": [1, 2, 3], "v": [10, None, 30]})
        writer.flush()
        writer.append({"t": [4, 5], "v": [40, 50]})
        writer.flush()
        writer.append({"t": [6, 7, 8], "v": [None, 70, 80]})
    return path


def block_offsets(path):
    with acta.open(path) as reader:
        return [block.offset for block in reader.blocks()]


def test_verify_passes_on_a_healthy_file(healthy):
    assert acta.verify(healthy) == 3


def test_recover_reports_a_clean_file(healthy):
    report = acta.recover(healthy)
    assert not report.incomplete_tail
    assert not report.truncated
    assert report.complete_frames == 4
    assert report.complete_blocks == 3
    assert report.last_good_offset == healthy.stat().st_size


def test_every_truncation_point_in_the_final_frame_recovers(healthy, tmp_path):
    data = healthy.read_bytes()
    last_offset = block_offsets(healthy)[-1]
    target = tmp_path / "torn.acta"
    for cut in range(last_offset + 1, len(data)):
        target.write_bytes(data[:cut])
        report = acta.recover(target)
        assert report.incomplete_tail
        assert report.last_good_offset == last_offset
        with acta.open(target) as reader:
            assert reader.num_blocks == 2


def test_recover_truncate_then_append_produces_a_valid_file(healthy):
    data = healthy.read_bytes()
    last_offset = block_offsets(healthy)[-1]
    healthy.write_bytes(data[: last_offset + 37])  # tear inside the final frame

    with pytest.raises(acta.CorruptionError):
        acta.open_append(healthy)

    report = acta.recover(healthy, truncate=True)
    assert report.truncated
    assert healthy.stat().st_size == last_offset

    with acta.open_append(healthy) as writer:
        assert writer._next_base_row_id == 5  # rows surviving in blocks 1-2
        writer.append({"t": [100], "v": [1]})
    assert acta.verify(healthy) == 3
    with acta.open(healthy) as reader:
        result = reader.read()
        assert result["t"].values.tolist() == [1, 2, 3, 4, 5, 100]
        assert result.row_ids.tolist() == [0, 1, 2, 3, 4, 5]


def test_corrupt_final_body_is_reported_as_a_tail_error(healthy):
    data = bytearray(healthy.read_bytes())
    last_offset = block_offsets(healthy)[-1]
    data[last_offset + 60] ^= 0x01  # inside the final frame's header/payload
    healthy.write_bytes(bytes(data))
    report = acta.recover(healthy)
    assert report.incomplete_tail
    assert report.tail_error is not None
    assert report.last_good_offset == last_offset


def test_mid_file_corruption_is_never_silently_truncated(healthy):
    data = bytearray(healthy.read_bytes())
    offsets = block_offsets(healthy)
    data[offsets[0] + 90] ^= 0x01  # first data frame body
    healthy.write_bytes(bytes(data))
    with pytest.raises(acta.CorruptionError):
        acta.verify(healthy)
    # recover() stops at the corrupt frame and reports; the file keeps its
    # later (unreachable) bytes unless the caller explicitly truncates.
    report = acta.recover(healthy)
    assert report.incomplete_tail
    assert report.last_good_offset == offsets[0]
    assert healthy.stat().st_size > offsets[0]


def test_prologue_corruption_is_not_recoverable(healthy):
    data = bytearray(healthy.read_bytes())
    data[3] ^= 0x01
    healthy.write_bytes(bytes(data))
    with pytest.raises(acta.CorruptionError):
        acta.recover(healthy)


def test_create_refuses_to_overwrite(healthy):
    schema = acta.Schema([acta.timestamp64("t")])
    with pytest.raises(FileExistsError):
        acta.create(healthy, schema)


def test_verify_catches_stream_level_corruption(healthy):
    data = bytearray(healthy.read_bytes())
    with acta.open(healthy) as reader:
        block = list(reader.blocks())[-1]
        payload_offset = block._entry.frame.payload_offset
    data[payload_offset + 1] ^= 0x01
    healthy.write_bytes(bytes(data))
    with pytest.raises(acta.CorruptionError):
        acta.verify(healthy)
