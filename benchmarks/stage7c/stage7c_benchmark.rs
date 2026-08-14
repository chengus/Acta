//! Reproducible Stage 7c statistics-policy benchmark smoke harness.

use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use acta::{
    Array, BooleanArray, Column, LogicalType, PrimitiveArray, Reader, RecordBatch, Schema,
    TimeUnit, TimeZone, TimestampArray, ValidationLevel, ValidationOptions, Writer, WriterOptions,
    WriterStatistics,
};

const DEFAULT_ROWS: usize = 8_192;
const DEFAULT_BATCH_ROWS: usize = 512;
const DEFAULT_BLOCK_ROWS: u64 = 1_024;

/// How many times each case is written before its timings are reported.
///
/// One measurement at these sizes is dominated by allocator warm-up and page
/// faults rather than by the work being compared, which is enough noise to
/// rank the policies backwards. Reporting the best of several runs measures
/// the same work with less of that, and the counted columns are unaffected
/// because every repetition writes identical bytes.
const DEFAULT_REPEATS: usize = 5;

#[derive(Clone, Copy, Debug)]
struct Case {
    name: &'static str,
    statistics: WriterStatistics,
}

#[derive(Debug)]
struct ResultRow {
    case: Case,
    rows: u64,
    output_bytes: u64,
    data_bytes: u64,
    statistics: usize,
    statistics_bytes: usize,
    rows_per_second: f64,
    finish_seconds: f64,
    blocks_considered: u64,
    blocks_pruned: u64,
}

#[derive(Debug)]
struct Config {
    rows: usize,
    batch_rows: usize,
    block_rows: u64,
    repeats: usize,
    output: Option<PathBuf>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("stage7c benchmark failed: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let config = parse_args()?;
    let schema = benchmark_schema();
    let batches = benchmark_batches(&schema, config.rows, config.batch_rows)?;
    let mut rows = Vec::new();
    for case in cases() {
        rows.push(run_case(
            &schema,
            &batches,
            config.block_rows,
            config.repeats,
            case,
        )?);
    }
    let report = render_report(&config, &rows);
    if let Some(path) = config.output {
        std::fs::write(&path, &report).map_err(|error| format!("{}: {error}", path.display()))?;
    }
    print!("{report}");
    Ok(())
}

fn cases() -> [Case; 3] {
    [
        Case {
            name: "none",
            statistics: WriterStatistics::None,
        },
        Case {
            name: "minmax",
            statistics: WriterStatistics::MinMax,
        },
        Case {
            name: "automatic",
            statistics: WriterStatistics::Automatic,
        },
    ]
}

fn benchmark_schema() -> Schema {
    Schema::new(
        704,
        vec![
            Column::new(
                1,
                "timestamp",
                LogicalType::Timestamp {
                    unit: TimeUnit::Millisecond,
                    timezone: TimeZone::Utc,
                },
                false,
            ),
            Column::new(2, "value", LogicalType::Int64, false),
            Column::new(3, "flag", LogicalType::Bool, false),
        ],
        Some(1),
    )
}

fn benchmark_batches(
    schema: &Schema,
    rows: usize,
    batch_rows: usize,
) -> Result<Vec<RecordBatch>, String> {
    let mut batches = Vec::new();
    let mut start = 0;
    while start < rows {
        let end = (start + batch_rows).min(rows);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Array::Timestamp(TimestampArray::new(
                    (start..end).map(|row| row as i64 * 1_000).collect(),
                    None,
                    TimeUnit::Millisecond,
                    TimeZone::Utc,
                )),
                Array::Int64(PrimitiveArray::new(
                    (start..end)
                        .map(|row| (row as i64 % 2_048) - 1_024)
                        .collect(),
                    None,
                )),
                Array::Bool(BooleanArray::new(
                    (start..end).map(|row| row / 128 % 2 == 0).collect(),
                    None,
                )),
            ],
            end - start,
        )
        .map_err(|error| error.to_string())?;
        batches.push(batch);
        start = end;
    }
    Ok(batches)
}

/// Measure one policy `repeats` times and report its best timings.
///
/// Every repetition writes the same file from the same rows, so the byte and
/// statistics counts are checked to be identical across them: a policy that
/// did not produce deterministic output would fail here rather than have its
/// sizes silently averaged.
fn run_case(
    schema: &Schema,
    batches: &[RecordBatch],
    block_rows: u64,
    repeats: usize,
    case: Case,
) -> Result<ResultRow, String> {
    let mut best: Option<ResultRow> = None;
    for _ in 0..repeats.max(1) {
        let run = run_case_once(schema, batches, block_rows, case)?;
        best = Some(match best {
            None => run,
            Some(previous) => {
                if previous.output_bytes != run.output_bytes
                    || previous.statistics != run.statistics
                    || previous.statistics_bytes != run.statistics_bytes
                {
                    return Err(format!("{}: repeated runs disagreed on output", case.name));
                }
                ResultRow {
                    rows_per_second: previous.rows_per_second.max(run.rows_per_second),
                    finish_seconds: previous.finish_seconds.min(run.finish_seconds),
                    ..previous
                }
            }
        });
    }
    best.ok_or_else(|| format!("{}: no run completed", case.name))
}

fn run_case_once(
    schema: &Schema,
    batches: &[RecordBatch],
    block_rows: u64,
    case: Case,
) -> Result<ResultRow, String> {
    let path = scratch_path(case.name);
    let mut writer = Writer::create(
        &path,
        schema.clone(),
        WriterOptions::default()
            .with_row_block_target(block_rows)
            .with_statistics(case.statistics),
    )
    .map_err(|error| error.to_string())?;
    let started = Instant::now();
    for batch in batches.iter().cloned() {
        writer.append(batch).map_err(|error| error.to_string())?;
    }
    let ingest_seconds = started.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
    let finishing = Instant::now();
    let summary = writer.finish().map_err(|error| error.to_string())?;
    let finish_seconds = finishing.elapsed().as_secs_f64();
    let bytes = std::fs::read(&path).map_err(|error| error.to_string())?;
    acta::validate_with_options(
        &path,
        ValidationOptions::default().with_level(ValidationLevel::Full),
    )
    .map_err(|error| format!("{}: {error}", case.name))?;

    let reader = Reader::open(&path).map_err(|error| error.to_string())?;
    let range_start = block_rows as i64 * 1_000;
    let range_end = range_start + block_rows as i64 * 1_000;
    let mut scan = reader
        .scan()
        .primary_range(acta::PrimaryRange::timestamp(range_start, range_end))
        .map_err(|error| error.to_string())?;
    for batch in &mut scan {
        batch.map_err(|error| error.to_string())?;
    }
    let metrics = scan.metrics();
    let (statistics, statistics_bytes) = inspect_statistics(&bytes);
    let data_bytes = inspect_data_bytes(&bytes);
    let output_bytes = bytes.len() as u64;
    let _ = std::fs::remove_file(path);
    Ok(ResultRow {
        case,
        rows: summary.rows_written(),
        output_bytes,
        data_bytes,
        statistics,
        statistics_bytes,
        rows_per_second: summary.rows_written() as f64 / ingest_seconds,
        finish_seconds,
        blocks_considered: metrics.blocks_considered(),
        blocks_pruned: metrics.blocks_pruned(),
    })
}

fn inspect_statistics(bytes: &[u8]) -> (usize, usize) {
    let mut offset = 64;
    let mut count = 0;
    let mut total = 0;
    while offset < bytes.len() {
        let header_length = u32_at(bytes, offset + 16) as usize;
        let payload_length = u64_at(bytes, offset + 24) as usize;
        if u16_at(bytes, offset + 8) == 2 {
            let header = offset + 48;
            let columns = u32_at(bytes, header + 20) as usize;
            for index in 0..columns {
                let descriptor = header + 64 + index * 32;
                if u16_at(bytes, descriptor + 22) == 1 {
                    count += 1;
                    total += u32_at(bytes, descriptor + 28) as usize;
                }
            }
        }
        offset += 48 + header_length + payload_length + 32;
    }
    (count, total)
}

fn inspect_data_bytes(bytes: &[u8]) -> u64 {
    let mut offset = 64;
    let mut total = 0;
    while offset < bytes.len() {
        let header_length = u32_at(bytes, offset + 16) as u64;
        let payload_length = u64_at(bytes, offset + 24);
        if u16_at(bytes, offset + 8) == 2 {
            total += 48 + header_length + payload_length + 32;
        }
        offset += (48 + header_length + payload_length + 32) as usize;
    }
    total
}

fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("u16"))
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("u32"))
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("u64"))
}

fn scratch_path(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "acta-stage7c-{label}-{}-{id}.acta",
        std::process::id()
    ))
}

fn render_report(config: &Config, rows: &[ResultRow]) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# Acta Stage 7c benchmark");
    let _ = writeln!(output, "rows: {}", config.rows);
    let _ = writeln!(output, "batch_rows: {}", config.batch_rows);
    let _ = writeln!(output, "block_rows: {}", config.block_rows);
    let _ = writeln!(output, "repeats: {}", config.repeats);
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "| policy | rows | output bytes | data bytes | statistics | statistic bytes | rows/s | finish s | blocks | pruned |"
    );
    let _ = writeln!(
        output,
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );
    for row in rows {
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} | {:.0} | {:.6} | {} | {} |",
            row.case.name,
            row.rows,
            row.output_bytes,
            row.data_bytes,
            row.statistics,
            row.statistics_bytes,
            row.rows_per_second,
            row.finish_seconds,
            row.blocks_considered,
            row.blocks_pruned,
        );
    }
    output
}

fn parse_args() -> Result<Config, String> {
    let mut config = Config {
        rows: DEFAULT_ROWS,
        batch_rows: DEFAULT_BATCH_ROWS,
        block_rows: DEFAULT_BLOCK_ROWS,
        repeats: DEFAULT_REPEATS,
        output: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--rows" => config.rows = positive(&mut args, "--rows")?,
            "--batch-rows" => config.batch_rows = positive(&mut args, "--batch-rows")?,
            "--block-rows" => config.block_rows = positive(&mut args, "--block-rows")? as u64,
            "--repeats" => config.repeats = positive(&mut args, "--repeats")?,
            "--output" => config.output = Some(PathBuf::from(next(&mut args, "--output")?)),
            "--help" | "-h" => {
                println!(
                    "stage7c_benchmark [--rows N] [--batch-rows N] [--block-rows N] \
[--repeats N] [--output PATH]"
                );
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
        return Err(format!("{flag} must be positive"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{Config, benchmark_batches, benchmark_schema, cases, render_report, run_case};

    #[test]
    fn every_policy_round_trips_and_reports_statistics() {
        let schema = benchmark_schema();
        let batches = benchmark_batches(&schema, 256, 32).expect("batches");
        for case in cases() {
            let result = run_case(&schema, &batches, 64, 2, case).expect(case.name);
            assert_eq!(result.rows, 256, "{}", case.name);
            assert!(result.data_bytes > 0, "{}", case.name);
            if case.name == "none" {
                assert_eq!(result.statistics, 0);
            } else {
                assert!(result.statistics > 0, "{}", case.name);
            }
        }
    }

    #[test]
    fn report_has_one_row_per_policy() {
        let schema = benchmark_schema();
        let batches = benchmark_batches(&schema, 64, 16).expect("batches");
        let rows = cases()
            .into_iter()
            .map(|case| run_case(&schema, &batches, 32, 1, case).expect(case.name))
            .collect::<Vec<_>>();
        let report = render_report(
            &Config {
                rows: 64,
                batch_rows: 16,
                block_rows: 32,
                repeats: 1,
                output: None,
            },
            &rows,
        );
        for name in ["none", "minmax", "automatic"] {
            assert!(report.contains(&format!("| {name} |")));
        }
    }
}
