//! Deterministic Stage 5 scan benchmark smoke harness.
//!
//! Every case reads the same generated rows. The sorted and unsorted files
//! hold the same multiset of rows in different orders, and the cases that are
//! meant to be compared use the same projection and the same range, so a
//! difference between two rows of the report is a difference in the scan and
//! not in the data.
//!
//! Timing is informational. The counters are not: they come from
//! `ScanMetrics`, which the reader increments from the lengths it actually
//! reads, so no number here is inferred from a file size.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use acta::{
    Array, Column, LogicalType, PrimaryRange, PrimitiveArray, Reader, RecordBatch, ScalarValue,
    ScanMetrics, Schema, TimeUnit, TimeZone, TimestampArray, Utf8Array, Writer, WriterOptions,
};

const DEFAULT_ROWS: usize = 4_096;
const DEFAULT_BLOCK_ROWS: u64 = 256;

/// Which of the two files a case reads. Both hold the same rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Order {
    Sorted,
    Unsorted,
}

#[derive(Clone, Copy, Debug)]
struct Case {
    name: &'static str,
    order: Order,
    projection: &'static [&'static str],
    ranged: bool,
}

/// The case list. `sparse_projection` differs from `full_scan` only in its
/// projection, and `range_unsorted` differs from `range_sorted` only in the
/// order of the rows on disk.
const CASES: [Case; 6] = [
    Case {
        name: "full_scan",
        order: Order::Sorted,
        projection: &["timestamp", "value", "label"],
        ranged: false,
    },
    Case {
        name: "sparse_projection",
        order: Order::Sorted,
        projection: &["value"],
        ranged: false,
    },
    Case {
        name: "empty_projection",
        order: Order::Sorted,
        projection: &[],
        ranged: false,
    },
    Case {
        name: "range_sorted",
        order: Order::Sorted,
        projection: &["value"],
        ranged: true,
    },
    Case {
        name: "range_unsorted",
        order: Order::Unsorted,
        projection: &["value"],
        ranged: true,
    },
    Case {
        name: "range_primary_projected",
        order: Order::Sorted,
        projection: &["timestamp", "value"],
        ranged: true,
    },
];

#[derive(Debug)]
struct Config {
    rows: usize,
    block_rows: u64,
    output: Option<PathBuf>,
}

#[derive(Debug)]
struct ResultRow {
    case: Case,
    metrics: ScanMetrics,
    rows: usize,
    elapsed_seconds: f64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("stage5 benchmark failed: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let config = parse_args()?;
    let files = Files::write(&config)?;
    let mut rows = Vec::new();
    for case in CASES {
        rows.push(run_case(&files, &config, case)?);
    }

    let report = render_report(&config, &rows);
    print!("{report}");
    if let Some(path) = &config.output {
        std::fs::write(path, &report).map_err(|error| format!("writing the report: {error}"))?;
    }
    Ok(())
}

// ------------------------------------------------------------------ the data

/// The primary values of the two files, in file order.
///
/// The unsorted file is a permutation of the sorted one, so the two contain
/// exactly the same rows and any difference the report shows between them
/// comes from the order alone.
fn sorted_timestamps(rows: usize) -> Vec<i64> {
    (0..rows as i64).collect()
}

fn unsorted_timestamps(rows: usize) -> Vec<i64> {
    // A stride coprime with a power-of-two row count visits every row once.
    let count = rows as i64;
    let stride = 37;
    (0..count).map(|row| (row * stride) % count).collect()
}

fn value_of(timestamp: i64) -> i64 {
    timestamp * 10
}

fn label_of(timestamp: i64) -> String {
    format!("v{}", timestamp % 32)
}

fn schema() -> Schema {
    Schema::new(
        60,
        vec![
            Column::new(
                1,
                "timestamp",
                LogicalType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: TimeZone::Utc,
                },
                false,
            ),
            Column::new(2, "value", LogicalType::Int64, false),
            Column::new(3, "label", LogicalType::Utf8, false),
        ],
        Some(1),
    )
}

fn batch(schema: &Schema, timestamps: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Timestamp(TimestampArray::new(
                timestamps.to_vec(),
                None,
                TimeUnit::Microsecond,
                TimeZone::Utc,
            )),
            Array::Int64(PrimitiveArray::new(
                timestamps.iter().copied().map(value_of).collect(),
                None,
            )),
            Array::Utf8(Utf8Array::new(
                timestamps.iter().copied().map(label_of).collect(),
                None,
            )),
        ],
        timestamps.len(),
    )
    .expect("the benchmark batch is well formed")
}

/// The half-open range every ranged case uses: a slice out of the middle of
/// the key space, wide enough to span several blocks and narrow enough that
/// most blocks can be pruned.
fn range_of(rows: usize) -> (i64, i64) {
    let start = (rows / 4) as i64;
    let width = (rows / 16).max(1) as i64;
    (start, start + width)
}

struct Files {
    sorted: TempPath,
    unsorted: TempPath,
    sorted_timestamps: Vec<i64>,
    unsorted_timestamps: Vec<i64>,
}

impl Files {
    fn write(config: &Config) -> Result<Self, String> {
        let sorted_timestamps = sorted_timestamps(config.rows);
        let unsorted_timestamps = unsorted_timestamps(config.rows);
        Ok(Self {
            sorted: write_file("sorted", &sorted_timestamps, config.block_rows)?,
            unsorted: write_file("unsorted", &unsorted_timestamps, config.block_rows)?,
            sorted_timestamps,
            unsorted_timestamps,
        })
    }

    fn path(&self, order: Order) -> &Path {
        match order {
            Order::Sorted => self.sorted.path(),
            Order::Unsorted => self.unsorted.path(),
        }
    }

    fn timestamps(&self, order: Order) -> &[i64] {
        match order {
            Order::Sorted => &self.sorted_timestamps,
            Order::Unsorted => &self.unsorted_timestamps,
        }
    }
}

fn write_file(label: &str, timestamps: &[i64], block_rows: u64) -> Result<TempPath, String> {
    let path = TempPath::new(label);
    let schema = schema();
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default().with_row_block_target(block_rows),
    )
    .map_err(|error| format!("creating the benchmark file: {error}"))?;
    writer
        .append(batch(&schema, timestamps))
        .map_err(|error| format!("appending the benchmark rows: {error}"))?;
    let _ = writer
        .finish()
        .map_err(|error| format!("finishing the benchmark file: {error}"))?;
    Ok(path)
}

// ------------------------------------------------------------- one benchmark

/// Run one case and check its output before it is allowed into the report.
///
/// A benchmark that only counted rows could not tell a fast scan from a wrong
/// one, so every returned slot is compared with the value the generator wrote
/// for that row.
fn run_case(files: &Files, config: &Config, case: Case) -> Result<ResultRow, String> {
    let reader = Reader::open(files.path(case.order))
        .map_err(|error| format!("{}: open: {error}", case.name))?;

    let started = Instant::now();
    let mut scan = reader
        .scan()
        .project(case.projection)
        .map_err(|error| format!("{}: projection: {error}", case.name))?;
    if case.ranged {
        let (start, end) = range_of(config.rows);
        scan = scan
            .primary_range(PrimaryRange::timestamp(start, end))
            .map_err(|error| format!("{}: range: {error}", case.name))?
            .file_order();
    }
    let mut batches = Vec::new();
    for batch in scan.by_ref() {
        batches.push(batch.map_err(|error| format!("{}: scan: {error}", case.name))?);
    }
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let metrics = scan.metrics();

    let expected = expected_rows(files.timestamps(case.order), config, case);
    verify(&batches, &expected, case)?;

    let rows: usize = batches.iter().map(RecordBatch::row_count).sum();
    if rows as u64 != metrics.rows_returned() {
        return Err(format!(
            "{}: the metrics report {} rows and the batches hold {rows}",
            case.name,
            metrics.rows_returned()
        ));
    }
    Ok(ResultRow {
        case,
        metrics,
        rows,
        elapsed_seconds,
    })
}

/// The primary values this case must return, in the order it must return them.
fn expected_rows(timestamps: &[i64], config: &Config, case: Case) -> Vec<i64> {
    if !case.ranged {
        return timestamps.to_vec();
    }
    let (start, end) = range_of(config.rows);
    timestamps
        .iter()
        .copied()
        .filter(|value| *value >= start && *value < end)
        .collect()
}

fn verify(batches: &[RecordBatch], expected: &[i64], case: Case) -> Result<(), String> {
    let rows: usize = batches.iter().map(RecordBatch::row_count).sum();
    if rows != expected.len() {
        return Err(format!(
            "{}: returned {rows} rows, expected {}",
            case.name,
            expected.len()
        ));
    }
    for batch in batches {
        if batch.schema().column_count() != case.projection.len() {
            return Err(format!(
                "{}: returned {} columns, expected {}",
                case.name,
                batch.schema().column_count(),
                case.projection.len()
            ));
        }
        for (position, name) in case.projection.iter().enumerate() {
            if batch.schema().columns()[position].name() != *name {
                return Err(format!(
                    "{}: column {position} is {}, expected {name}",
                    case.name,
                    batch.schema().columns()[position].name()
                ));
            }
        }
    }

    let mut row = 0;
    for batch in batches {
        for position in 0..batch.row_count() {
            let timestamp = expected[row];
            for (column, name) in case.projection.iter().enumerate() {
                let found = batch
                    .column(column)
                    .expect("a projected column")
                    .value_at(position);
                let matches = match (*name, found) {
                    ("timestamp", Some(ScalarValue::Timestamp { value, .. })) => value == timestamp,
                    ("value", Some(ScalarValue::Int64(value))) => value == value_of(timestamp),
                    ("label", Some(ScalarValue::Utf8(value))) => value == label_of(timestamp),
                    _ => false,
                };
                if !matches {
                    return Err(format!(
                        "{}: row {row} column {name} decoded as {found:?}, which is not the \
                         value written for timestamp {timestamp}",
                        case.name
                    ));
                }
            }
            row += 1;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- the report

fn render_report(config: &Config, rows: &[ResultRow]) -> String {
    let (start, end) = range_of(config.rows);
    let mut output = String::new();
    let _ = writeln!(output, "# Acta Stage 5 scan benchmark");
    let _ = writeln!(output, "rows: {}", config.rows);
    let _ = writeln!(output, "block_rows: {}", config.block_rows);
    let _ = writeln!(output, "primary_range: [{start}, {end})");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "| case | order | projection | blocks considered | blocks pruned | streams decoded | \
         stream bytes decoded | bytes read | rows returned | elapsed ms |"
    );
    let _ = writeln!(
        output,
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );
    for row in rows {
        let projection = if row.case.projection.is_empty() {
            "(none)".to_owned()
        } else {
            row.case.projection.join(",")
        };
        let _ = writeln!(
            output,
            "| {} | {:?} | {projection} | {} | {} | {} | {} | {} | {} | {:.3} |",
            row.case.name,
            row.case.order,
            row.metrics.blocks_considered(),
            row.metrics.blocks_pruned(),
            row.metrics.streams_decoded(),
            row.metrics.stream_bytes_decoded(),
            row.metrics.bytes_read(),
            row.rows,
            row.elapsed_seconds * 1_000.0,
        );
    }
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "`stream bytes decoded` is the stored size of the streams each scan decoded, which is \
         what projection and pruning reduce. `bytes read` is every byte the scan read from the \
         file, including the frame body each candidate block verifies before any stream is \
         decoded. Both come from the reader's own counters."
    );
    output
}

// ------------------------------------------------------------------ plumbing

fn parse_args() -> Result<Config, String> {
    let mut config = Config {
        rows: DEFAULT_ROWS,
        block_rows: DEFAULT_BLOCK_ROWS,
        output: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--rows" => config.rows = positive(&mut args, "--rows")?,
            "--block-rows" => config.block_rows = positive(&mut args, "--block-rows")? as u64,
            "--output" => config.output = Some(PathBuf::from(next(&mut args, "--output")?)),
            "--help" | "-h" => {
                println!("stage5_benchmark [--rows N] [--block-rows N] [--output PATH]");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    Ok(config)
}

fn next(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next().ok_or_else(|| format!("{flag} needs a value"))
}

fn positive(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<usize, String> {
    let value = next(args, flag)?
        .parse::<usize>()
        .map_err(|error| format!("{flag}: {error}"))?;
    if value == 0 {
        return Err(format!("{flag} must be greater than zero"));
    }
    Ok(value)
}

struct TempPath(PathBuf);

impl TempPath {
    fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(std::env::temp_dir().join(format!(
            "acta-stage5-benchmark-{label}-{}-{}.acta",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Cargo builds examples as test harnesses, so these run under
/// `cargo test --all-targets` and keep the harness honest without asserting on
/// anything as unstable as a duration.
#[cfg(test)]
mod tests {
    use super::*;

    fn config(rows: usize) -> Config {
        Config {
            rows,
            block_rows: 32,
            output: None,
        }
    }

    #[test]
    fn the_two_files_hold_the_same_rows_in_different_orders() {
        let rows = 256;
        let sorted = sorted_timestamps(rows);
        let mut permuted = unsorted_timestamps(rows);
        assert_ne!(permuted, sorted, "the unsorted file is not permuted");
        permuted.sort_unstable();
        assert_eq!(permuted, sorted, "the two files hold different rows");
    }

    #[test]
    fn every_case_verifies_and_matches_its_input() {
        let config = config(256);
        let files = Files::write(&config).expect("write the benchmark files");
        for case in CASES {
            run_case(&files, &config, case)
                .unwrap_or_else(|error| panic!("{}: {error}", case.name));
        }
    }

    #[test]
    fn verification_rejects_a_wrong_answer() {
        let config = config(256);
        let files = Files::write(&config).expect("write the benchmark files");
        let case = CASES[1];
        let reader = Reader::open(files.path(case.order)).expect("open");
        let batches: Vec<RecordBatch> = reader
            .scan()
            .project(case.projection)
            .expect("projection")
            .map(|batch| batch.expect("scan"))
            .collect();

        let mut wrong = expected_rows(files.timestamps(case.order), &config, case);
        wrong[0] += 1;
        verify(&batches, &wrong, case).expect_err("a changed value must fail verification");
        wrong.pop();
        verify(&batches, &wrong, case).expect_err("a missing row must fail verification");
    }

    #[test]
    fn projection_reduces_the_streams_and_the_bytes_it_decodes() {
        let config = config(256);
        let files = Files::write(&config).expect("write the benchmark files");
        let full = run_case(&files, &config, CASES[0]).expect("full scan");
        let sparse = run_case(&files, &config, CASES[1]).expect("sparse projection");
        let empty = run_case(&files, &config, CASES[2]).expect("empty projection");

        assert!(sparse.metrics.streams_decoded() < full.metrics.streams_decoded());
        assert!(sparse.metrics.stream_bytes_decoded() < full.metrics.stream_bytes_decoded());
        assert_eq!(empty.metrics.streams_decoded(), 0);
        assert_eq!(empty.metrics.stream_bytes_decoded(), 0);
        // Every case returns the same rows; only the columns differ.
        assert_eq!(sparse.rows, full.rows);
        assert_eq!(empty.rows, full.rows);
    }

    #[test]
    fn a_selective_range_prunes_blocks_and_returns_the_same_rows_in_either_order() {
        let config = config(256);
        let files = Files::write(&config).expect("write the benchmark files");
        let sorted = run_case(&files, &config, CASES[3]).expect("sorted range");
        let unsorted = run_case(&files, &config, CASES[4]).expect("unsorted range");

        assert!(sorted.metrics.blocks_pruned() > 0, "nothing was pruned");
        assert_eq!(sorted.rows, unsorted.rows, "the same rows are in range");
        assert!(
            sorted.metrics.stream_bytes_decoded() < unsorted.metrics.stream_bytes_decoded(),
            "an ordered file should let pruning do more work"
        );
        assert!(sorted.metrics.bytes_read() > 0);
    }

    #[test]
    fn the_report_renders_one_row_for_every_case() {
        let config = config(256);
        let files = Files::write(&config).expect("write the benchmark files");
        let rows: Vec<ResultRow> = CASES
            .iter()
            .map(|case| run_case(&files, &config, *case).expect("case"))
            .collect();
        let report = render_report(&config, &rows);
        for case in CASES {
            assert!(
                report.contains(case.name),
                "the report is missing {}",
                case.name
            );
        }
        assert_eq!(
            report.lines().filter(|line| line.starts_with("| ")).count(),
            CASES.len() + 2,
            "the table should hold a header, a rule, and one row per case"
        );
    }
}
