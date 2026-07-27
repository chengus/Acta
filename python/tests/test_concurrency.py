"""Multi-process concurrency: a tailing reader against a live writer."""

from __future__ import annotations

import multiprocessing
import time

import acta

BLOCKS = 20
ROWS_PER_BLOCK = 16


def _schema() -> acta.Schema:
    return acta.Schema([acta.timestamp64("t", unit="us"), acta.int64("v")])


def _writer_process(path: str) -> None:
    with acta.create(path, _schema(), row_ids=True, sync="frame") as writer:
        for block in range(BLOCKS):
            base = block * ROWS_PER_BLOCK
            writer.append(
                {
                    "t": [1_000_000 * (base + row) for row in range(ROWS_PER_BLOCK)],
                    "v": list(range(base, base + ROWS_PER_BLOCK)),
                }
            )
            writer.flush()
            time.sleep(0.01)


def test_tailing_reader_follows_a_live_writer(tmp_path):
    path = tmp_path / "live.acta"
    context = multiprocessing.get_context("spawn")
    process = context.Process(target=_writer_process, args=(str(path),))
    process.start()
    try:
        # The reader may start before the schema frame is durable.
        deadline = time.monotonic() + 30
        reader = None
        while reader is None:
            try:
                reader = acta.open(path)
            except (FileNotFoundError, acta.CorruptionError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.02)
        with reader:
            sequences = []
            values = []
            row_ids = []
            for block in reader.follow(poll_interval=0.02, timeout=30):
                sequences.append(block.sequence)
                data = block.read(columns=["v"])
                values.extend(data["v"].values.tolist())
                row_ids.extend(data.row_ids.tolist())
                if len(sequences) == BLOCKS:
                    break
        assert sequences == list(range(1, BLOCKS + 1))
        assert values == list(range(BLOCKS * ROWS_PER_BLOCK))
        assert row_ids == list(range(BLOCKS * ROWS_PER_BLOCK))
    finally:
        process.join(timeout=30)
        assert process.exitcode == 0


def test_interrupted_append_then_recover_then_tail(tmp_path):
    """Deterministic torn-write drill: recover, resume, and re-read."""
    path = tmp_path / "torn.acta"
    with acta.create(path, _schema(), sync="close") as writer:
        writer.append({"t": [1, 2], "v": [1, 2]})
        writer.flush()
        writer.append({"t": [3, 4], "v": [3, 4]})
    healthy = path.read_bytes()
    with acta.open(path) as reader:
        last_offset = list(reader.blocks())[-1].offset
    # Simulate the writer dying mid-append of the final frame.
    path.write_bytes(healthy[: last_offset + 51])

    report = acta.recover(path, truncate=True)
    assert report.incomplete_tail and report.truncated

    with acta.open_append(path) as writer:
        writer.append({"t": [3, 4], "v": [3, 4]})
    assert acta.verify(path) == 2
    with acta.open(path) as reader:
        assert reader.read()["v"].values.tolist() == [1, 2, 3, 4]
