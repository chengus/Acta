# Stage 7 writer benchmark

This benchmark measures the current Stage 7 writer only. It does not enable
writer transforms, adaptive encoding, statistics selection, or any Stage 7b
behavior. The harness is the dependency-free Rust target
[`stage7_benchmark.rs`](stage7_benchmark.rs).

## What it measures

Each run uses one deterministic generated dataset, one row block target, and
one codec. The same generated batches are reused for none and zstandard. The
harness currently covers:

- fixed-width signed, unsigned, and floating-point numeric values;
- nullable numeric values with a deterministic mixed validity pattern;
- repeated UTF-8 strings;
- high-cardinality UTF-8 strings containing non-ASCII UTF-8;
- repeated binary patterns;
- pseudorandom binary values;
- monotonic UTC microsecond timestamps plus a numeric value column.

The generator uses fixed per-dataset SplitMix64 seeds compiled into the
harness. No external data is downloaded. --rows, --batch-rows, and
--block-targets keep the data size and block geometry configurable.

Every output is checked before its row is reported:

1. acta::validate accepts the complete file;
2. Reader::open discovers the expected blocks;
3. Reader::scan decodes every block;
4. every decoded scalar is compared with the generated input;
5. buffered rows and published block rows stay within the configured row
   target.

The harness exits with status 2 if any check fails. These correctness checks
are not ordinary performance tests and do not assert on wall-clock values.

Cargo builds examples as test harnesses, so the `#[cfg(test)]` module at the
end of [`stage7_benchmark.rs`](stage7_benchmark.rs) runs under `cargo test --all-targets` in
both feature configurations. Those tests use a 256-row configuration, cover
every dataset, codec, and two block targets, and assert only on correctness,
determinism-independent sizes, and buffering bounds. The Zstandard test is
`#[cfg(feature = "zstd")]`, so the no-default-features run simply skips it.
No test asserts an elapsed time.

## Commands

A short smoke run, suitable for local or CI checks, is:

~~~bash
cargo run --release --example stage7_benchmark --features zstd -- \
  --rows 512 \
  --batch-rows 64 \
  --block-targets 32,128 \
  --output /tmp/acta-stage7-smoke.md
~~~

The checked-in baseline was generated with:

~~~bash
cargo run --release --example stage7_benchmark --features zstd -- \
  --rows 16384 \
  --batch-rows 1024 \
  --block-targets 512,2048 \
  --output benchmarks/stage7/results/baseline.md
~~~

To run only raw output without the optional dependency:

~~~bash
cargo run --no-default-features --release \
  --example stage7_benchmark -- \
  --rows 2048 \
  --batch-rows 256 \
  --block-targets 64,256 \
  --codecs none
~~~

Zstandard cases are feature-gated. Without the feature, selecting zstd fails
with an explanatory error; the harness itself still compiles and raw cases
remain available. The full feature-matrix checks are:

~~~bash
cargo test --all-targets
cargo test --all-targets --no-default-features
~~~

Use a release build for comparisons. The harness only knows its target
operating system and architecture, so the toolchain, CPU model, filesystem,
run date, and contention lines in a checked-in report are added by hand after
the run. Record the Rust version, target triple, operating system, CPU model,
filesystem, and whether the run was thermally or otherwise contended. The
baseline is an engineering reference, not a portable performance claim. Run
the same command on the same host when comparing changes.

The checked-in baseline was measured on macOS 15.6.1 (Darwin 24.6.0), Apple
M1 Pro, rustc 1.93.0, release profile, local APFS, no CPU pinning, and no
deliberate concurrent load. Debug builds are one to two orders of magnitude
slower and are not comparable.

## Reading the report

raw_input_bytes is the writer's exact estimated total size of the raw
serialized data frames. stored_data_frame_bytes is the sum of actual committed
data-frame lengths discovered by the reader. Therefore:

~~~text
compression_ratio = raw_input_bytes / stored_data_frame_bytes
~~~

This makes raw and Zstandard comparable without allowing the fixed prologue
and schema frame to dominate the ratio. output_bytes is the complete file size
and does include those fixed structures. A raw run should have a ratio of
1.000; compare the Zstandard row with the same dataset and block target to see
the storage change. Smaller row targets add more per-block metadata and
independent Zstandard frames, while larger targets generally amortize that
overhead.

Ingestion timing covers append calls, including automatic publication of
complete blocks. It excludes deterministic data generation, input-batch
cloning, validation, and decode verification. finish_sync_seconds is reported
separately and covers finish, including final publication, filesystem flush,
and synchronization. Rows/s and raw bytes/s are derived from ingestion time
only.

Three columns describe buffered memory, each shown as observed / configured
bound. Max buffered rows and max buffered bytes are the largest
WriteAccounting::buffered_rows and buffered_bytes observed between append
calls. The byte value is the writer's raw frame-size estimate, not process
RSS. A large input batch can cross a threshold and publish inside one append
call, so those sampled maxima can read far below the instantaneous internal
peak, and read 0 whenever every batch publishes as it arrives.

Peak block rows closes that gap. A published block contains exactly the rows
the buffer held at publication, so the largest committed block is the true
peak row occupancy even when no sample observed it. It normally equals the row
target once the dataset is larger than one block. There is no equivalent
peak-byte column for Zstandard runs, because the reader only reports stored
block lengths; in a raw run, the largest block length is that peak, and the
raw byte accounting is codec-independent, so the raw run's peak also describes
the Zstandard run of the same dataset and target.

Buffered memory is bounded rather than merely sampled: the writer admits rows
only while the buffer stays within the row target, so buffered_rows never
exceeds it. The byte target is bounded the same way, except for a single row
too large for an empty block, which is written as an unavoidably oversize
block instead of being split. The harness fails the run if buffered rows or
published block rows ever exceed the configured row target.

The current writer stores raw streams or independently compressed raw streams.
It does not transform values before compression. Consequently, these results
are a Stage 7 baseline and are not evidence for Stage 7b transform or
adaptive-encoding performance.
