# Stage 5 scan benchmark

[`stage5_benchmark.rs`](stage5_benchmark.rs) reports the reader's full scan, sparse
projection, empty projection, and selective range paths, over both a sorted and
an unsorted primary column. It adds no writer or wire-format behavior; it uses
the existing raw writer so that what it measures is the scan.

## What it measures

Every case reads the same generated rows. The sorted and unsorted files hold
the same multiset of rows in different orders, and the cases meant to be
compared use the same projection and the same range, so a difference between
two rows of the report comes from the scan rather than from the data:

- `full_scan` and `sparse_projection` differ only in the projection;
- `range_sorted` and `range_unsorted` differ only in the order on disk;
- `range_primary_projected` differs from `range_sorted` only in whether the
  primary column reaches the output.

Each row reports blocks considered, blocks pruned, streams decoded, stream
bytes decoded, bytes read, rows returned, and wall-clock time.

## What the byte counters mean

Both come from `ScanMetrics`, which the reader increments from the lengths it
actually reads. Neither is inferred from a file size.

- `stream bytes decoded` is the stored size of the streams the scan decoded.
  This is what projection and pruning reduce.
- `bytes read` is every byte the scan read from the file, and it is larger.
  Reading a block streams its whole frame body to check it against the commit
  trailer before any stream is decoded, and the block header is read again to
  locate the streams. Projection narrows what a scan decodes, not what it
  reads.

A pruned block contributes nothing to either counter, which is the one case
where projection and pruning do reduce physical reads.

## What every case checks

A benchmark that only counted rows could not tell a fast scan from a wrong one,
so each case is checked before its row is reported:

1. the projection is the requested columns, in the requested order;
2. every returned slot is compared with the value the generator wrote for that
   row, so a filter that returned the wrong rows fails rather than looking
   fast;
3. the row count agrees with `ScanMetrics::rows_returned`.

The harness exits with status 2 if any check fails.

Cargo builds examples as test harnesses, so the `#[cfg(test)]` module at the
end of the file runs under `cargo test --all-targets`. It asserts on counters
and on decoded values only; no test here asserts on a duration.

## Running it

```bash
cargo run --release --example stage5_benchmark --features zstd -- \
  --rows 4096 --block-rows 256
```

Write the report to a file with `--output PATH`. Without `zstd` the harness
behaves identically; the raw writer it uses does not compress.

```bash
cargo run --no-default-features --release --example stage5_benchmark -- \
  --rows 512 --block-rows 64
```

The Stage 7 and Stage 7b benchmark harnesses remain separate and should be run
as their own compatibility smoke checks.
