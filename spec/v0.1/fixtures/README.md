# v0.1 fixtures

`minimal.acta` is a deterministic 472-byte file containing:

- the 64-byte v0.1 prologue;
- one schema frame with a non-nullable UTC microsecond timestamp column named
  `time`; and
- one uncompressed data frame containing timestamp values 1,000,000,
  2,000,000, and 3,000,000.

See [minimal.md](minimal.md) for a byte-by-byte annotation of the prologue,
schema frame, data frame, stored values, checksums, and recovery behavior.

SHA-256:

```text
1e40278e2f58a597faef56107bd6a31048cabf5b5173619f8112ad787c6f658a
```

Regenerate and validate it from the repository root:

```bash
uv run spec/v0/format_probe.py --write-fixture spec/v0/fixtures/minimal.acta
```

The generator tests every possible truncation point inside the final frame and
representative corruption in the prologue, frame prefix, header, payload, and
trailer before writing the fixture.
