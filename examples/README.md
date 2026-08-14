# Acta examples

These examples are small, runnable starting points for the crate's basic
operations:

```bash
cargo run --example writer
cargo run --example append
cargo run --example scan -- spec/v0.2/fixtures/nyc_taxi_3_rows/nyc_taxi_3_rows.acta
cargo run --example nullable
cargo run --example reader -- spec/v0.2/fixtures/minimal/minimal.acta
cargo run --example refresh_tail
cargo run --example live_append
cargo run --example validate_acta -- spec/v0.2/fixtures/minimal/minimal.acta
```

`writer` covers buffered ingestion and compression, `append` covers reopening
an existing file, `scan` covers projection and primary-range filtering,
`nullable` covers typed nullable columns, `reader` covers snapshot metadata and
block decoding, `refresh_tail` covers the refresh and tail APIs, `live_append`
shows a writer and tailing reader running concurrently on separate threads,
and `validate_acta` covers full-file validation.

The larger reproducible harnesses and real-data adapters live in
[`benchmarks/`](../benchmarks/). They remain explicit Cargo example targets, so
their commands use `cargo run --example ...` as documented by each benchmark.
