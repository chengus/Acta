# Stage 7c statistics benchmark

[`stage7c_benchmark.rs`](stage7c_benchmark.rs) compares the three public writer statistics
policies on one deterministic fixed-width dataset:

- `WriterStatistics::None`;
- `WriterStatistics::MinMax`; and
- `WriterStatistics::Automatic`.

Each case uses the same append and block geometry. The harness validates the
complete file at both the structural and full levels, opens it through the
reader, and executes a primary timestamp range scan. It reports the number and
exact encoded bytes of optional statistics, ingestion throughput, finish time,
and the Stage 5 block-pruning counters.

Each case is written several times and the best timings are reported.
A single measurement at these sizes is dominated by allocator warm-up and page
faults rather than by the work being compared, which was enough noise to rank
the policies backwards in earlier samples. Every repetition writes identical
bytes, and the harness fails if two repetitions of one policy disagree on
output size or statistics count, so the counted columns are exact while the
timings are best-of-N.

## Reading the report

The size and count columns are exact. The timing columns support one claim:
writing statistics costs ingestion throughput. They do not support a comparison
between `MinMax` and `Automatic`, which trade places across runs, and they are
not thresholds — regenerate them on the host in question.

The range scan is intentionally identical across policies. The primary
block-header bounds are the mandatory pruning metadata and nothing reads
optional column statistics, so `pruned` must not change. This benchmark
measures cost with pruning held fixed; it does not measure a pruning benefit,
because no reader consumes optional statistics until Stage 9. The `Automatic`
thresholds are correspondingly a conservative size floor rather than a measured
trade, and are stated exactly on `WriterStatistics::Automatic`:

- skip the primary timestamp or date column, whose block header already carries
  the complete pruning bounds;
- require at least 64 non-null, non-NaN values;
- require the raw dense value bytes to be at least eight times the pair, which
  binds only on `bool`, where it raises the floor to 121 values.

## Running it

Run a short smoke benchmark with:

```bash
cargo run --release --example stage7c_benchmark --features zstd -- \
  --rows 512 --batch-rows 64 --block-rows 128
```

`--repeats N` sets the repetition count and `--output PATH` writes the report to
a file. The sample report is in [`results.md`](results.md). The harness uses no
external data. Its test module runs under `cargo test --all-targets` and checks
that all policies produce readable files; it does not assert on wall-clock
timings. The feature-free matrix is also supported:

```bash
cargo test --all-targets --no-default-features
```
