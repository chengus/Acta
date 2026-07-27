# acta-format

Pure-Python reference implementation of the [Acta v0.1 file
format](../spec/v0.1/format_v0.1.md): an append-only, strongly typed,
block-columnar file for time-series data.

This package favors spec fidelity and readability over throughput. It is the
correctness oracle for the future C++ core and doubles as a pure-Python
fallback.

## Usage

```python
import acta

schema = acta.Schema(
    [
        acta.timestamp64("time", unit="us", tz="UTC"),
        acta.float64("close", nullable=True),
        acta.categorical("venue"),
    ]
)

# Write: appended rows become immutable compressed blocks.
with acta.create("ticks.acta", schema, row_ids=True) as writer:
    writer.append(
        {
            "time": [1_000_000, 2_000_000, 3_000_000],
            "close": [101.25, None, 101.75],
            "venue": ["NYSE", "NYSE", "ARCA"],
        }
    )

# Read: time-range pruning plus column projection.
with acta.open("ticks.acta") as reader:
    result = reader.read(columns=["time", "close"], start=1_000_000, end=3_000_000)
    print(result["close"].to_pylist())

# Tail a file that another process is still appending to.
with acta.open("ticks.acta") as reader:
    for block in reader.follow(poll_interval=0.25, timeout=5):
        print(block.sequence, block.row_count)

# Recover after an interrupted append.
report = acta.recover("ticks.acta")
if report.incomplete_tail:
    acta.recover("ticks.acta", truncate=True)
```

## Tests

```bash
uv run --directory python pytest
```

The suite round-trips every transform, decodes and byte-exactly regenerates
all four spec fixtures under `spec/v0.1/fixtures/`, exercises truncation and
corruption recovery, and drives a live writer/tailing-reader pair across
processes.
