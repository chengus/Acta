# Stage 7b writer benchmark

[`stage7b_benchmark.rs`](stage7b_benchmark.rs) compares the four Stage 7b combinations on
one deterministic mixed dataset and identical batch/block geometry:

- raw;
- raw plus Zstandard;
- adaptive without Zstandard; and
- adaptive plus Zstandard.

The dataset is five columns chosen so that each combination has something to
find: a small-range `uint32`, a negative-offset `int64`, a regular millisecond
timestamp used as the primary column, a long-run boolean flag, and a
low-cardinality UTF-8 label.

`WriterEncoding::Fixed(WriterTransform::...)` separates the cost of a transform
from the cost of choosing it, but it is not a fifth case over this dataset: it
applies one transform to every column and never falls back, so it needs a
schema whose columns all offer that transform, and these five deliberately do
not share one. Under `WriterTransform::Delta`, for instance, the `uint32`, the
`int64`, and the timestamp qualify and the boolean and the label do not, so a
fixed run measures that three-column subset. `Writer::create` refuses a
transform a column does not offer, a block whose values cannot use it fails
when it is published, and validity streams stay raw.

## What every case checks

A benchmark that only counted rows could not tell a compact encoding from a
corrupt one, so each case is checked before its row is reported:

1. `acta::validate` accepts the complete file and reports no incomplete tail;
2. `Reader::open` and `Reader::scan` decode every block;
3. every decoded slot is compared with the value that was written, with
   floating-point values compared as bit patterns so a lost NaN payload or
   signed zero fails rather than matches.

The harness exits with status 2 if any check fails.

Cargo builds examples as test harnesses, so the `#[cfg(test)]` module at the
end of [`stage7b_benchmark.rs`](stage7b_benchmark.rs) runs under `cargo test --all-targets` in
both feature configurations. Those tests use a 256-row configuration and assert
on correctness, on the raw baseline transforming nothing, on adaptive output
being smaller than that baseline, on byte-for-byte reproducibility, and on the
report not depending on the append batch size. The Zstandard cases are absent
without the feature, so the no-default-features run simply covers fewer cases.
No test asserts an elapsed time.

## What it reports

Selected column layouts and stream transforms, complete output size, data-frame
size, ratio against the `raw-none` data-frame baseline, ingestion throughput,
the time `finish` spent publishing and synchronizing, and the fraction of
columns that fell back to plain layout with the raw transform.

Ingestion timing covers `append` calls, including automatic publication of
complete blocks. `finish` is timed separately rather than folded in or dropped,
because it publishes whatever is still buffered and that work is not free under
adaptive encoding. The `raw-none` and `raw-zstandard` rows report a 100%
fallback frequency by construction: the raw policy transforms nothing, so it is
the baseline the adaptive rows are measured against rather than a result.
Timing is host-dependent and observational.

## Commands

Run the smoke benchmark with:

```bash
cargo run --release --example stage7b_benchmark --features zstd -- \
  --rows 512 --batch-rows 64 --block-rows 128 \
  --output /tmp/acta-stage7b-smoke.md
```

The checked-in sample in [`results.md`](results.md) was generated with:

```bash
cargo run --release --example stage7b_benchmark --features zstd -- \
  --rows 4096 --batch-rows 512 --block-rows 1024 \
  --output benchmarks/stage7b/results.md
```

To run only the raw cases without the optional dependency:

```bash
cargo run --no-default-features --release --example stage7b_benchmark -- \
  --rows 512 --batch-rows 64 --block-rows 128
```

The full feature-matrix checks are:

```bash
cargo test --all-targets
cargo test --all-targets --no-default-features
```

## Reading the recorded sample

[`results.md`](results.md) used 4,096 rows, 512-row append batches, and
1,024-row blocks. It records data-frame ratios of 1.000 for raw, 5.679 for raw
plus Zstandard, 39.961 for adaptive, and 41.357 for adaptive plus Zstandard.
Adaptive selection emitted dictionary, run-length, and transform streams on the
no-codec run and retained a 40% plain/raw fallback frequency on the Zstandard
run, because Zstandard already compresses those raw streams below what a
transform would save.

Use a release build for comparisons, and run the same command on the same host
when comparing changes. The harness uses only fixed generated values and
downloads nothing. The report records selected IDs as compact sequences so that
a change to deterministic selection stays visible in review.
