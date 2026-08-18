# ClickBench hits Parquet → Acta, Parquet, and CSV benchmark

This benchmark uses the official ClickHouse ClickBench `hits`-compatible
Parquet artifact: 99,997,497 rows and 105 columns. It compares Acta v0.2,
Parquet, and CSV on the same complete source dataset.

The source file is large: the recorded input is about 14.8 GiB and a plain CSV
is estimated at about 75 GiB. CSV write/read throughput is not measured because
the complete plain CSV does not fit in available local space. Its total size
is extrapolated from a 1,000,000-row `pyarrow.csv` sample.

## Reproduce

Download the canonical source from the Acta benchmark dataset:

```bash
hf download lu-chengass/acta-data \
  --repo-type dataset \
  --revision main \
  --include 'clickbench_hits/v0.2/hits.parquet' \
  --local-dir /tmp/acta-hf
```

Or use an existing local copy:

```bash
INPUT=/Users/lucentlu/Downloads/hits.parquet
```

Build the benchmark binary:

```bash
cargo build --release \
  --features parquet-example \
  --example clickbench_hits_benchmark
```

Run the complete benchmark with a Python environment containing `pyarrow` and
`psutil`:

```bash
python3 benchmarks/v0.2/3_clickbench_hits/run_benchmark.py \
  "$INPUT" \
  /tmp/acta-benchmark/clickbench_hits \
  --result benchmarks/v0.2/3_clickbench_hits/results/clickbench_hits.json
```

The script creates and reads back the Acta target, uses the original Parquet
input for the Parquet size baseline, and estimates CSV size from a sample. It
refuses to overwrite an existing Acta target.

## Timing scope and metrics

For each source Parquet batch, source decoding completes before the target
timer starts. The write result therefore measures target serialization, not
Parquet decoding. Acta timing includes Arrow-to-Acta typed conversion,
`Writer::append`, and Zstandard compression; `finish`/filesystem sync is
reported separately. The result also records source decode time, output bytes,
bytes per row, logical MiB/s, rows/s, peak RSS, full-scan time, read rows/s,
blocks or row groups, and Acta scan counters.

CSV throughput and read timing are intentionally blank because the full file
does not fit locally. Parquet throughput is also blank: the uploaded original
Parquet is used only as the size baseline.

## Recorded artifacts

The benchmark result files are checked into this directory after a run:

```text
results/clickbench_hits.json
results/clickbench_hits.md
```

The large Acta target is uploaded under `clickbench_hits/v0.2/` before local
cleanup. The source Parquet and result manifest are published alongside it in
the Acta Hugging Face dataset.

The retained 1M-row CSV sample is generated at:

```text
/tmp/acta-benchmark/clickbench_hits/hits_1m.csv
```

It is 801,653,457 bytes with SHA-256
`8355799f72ed09d03af458e4456ad8e2057533c809ceebfbb91f316b2d27bea5`.

## Upload command

After the Acta file and sample exist, upload the reproducibility artifacts with:

```bash
hf upload lu-chengass/acta-data \
  /tmp/acta-benchmark/clickbench_hits/targets/clickbench_hits.acta \
  clickbench_hits/v0.2/hits.acta \
  --repo-type dataset \
  --commit-message 'Add ClickBench Acta target and benchmark results'

hf upload lu-chengass/acta-data \
  /tmp/acta-benchmark/clickbench_hits/hits_1m.csv \
  clickbench_hits/v0.2/hits_1m.csv \
  --repo-type dataset \
  --commit-message 'Add 1M-row ClickBench CSV sample'

hf upload lu-chengass/acta-data \
  benchmarks/v0.2/3_clickbench_hits/results/clickbench_hits.json \
  clickbench_hits/v0.2/benchmark.json \
  --repo-type dataset \
  --commit-message 'Add ClickBench benchmark JSON result'

hf upload lu-chengass/acta-data \
  benchmarks/v0.2/3_clickbench_hits/results/clickbench_hits.md \
  clickbench_hits/v0.2/benchmark.md \
  --repo-type dataset \
  --commit-message 'Add ClickBench benchmark summary'

hf upload lu-chengass/acta-data \
  benchmarks/v0.2/HF_DATASET_CARD.md \
  README.md \
  --repo-type dataset \
  --commit-message 'Document ClickBench hits benchmark artifacts'
```

Only delete the local Acta and CSV sample after the corresponding uploads
complete successfully.
