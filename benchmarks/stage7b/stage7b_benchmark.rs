//! Deterministic Stage 7b policy/codec benchmark smoke harness.

use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use acta::{
    Array, Column, LogicalType, PrimitiveArray, Reader, RecordBatch, ScalarValue, Schema, TimeUnit,
    TimeZone, TimestampArray, Utf8Array, Writer, WriterCodec, WriterEncoding, WriterOptions,
};

const DEFAULT_ROWS: usize = 8_192;
const DEFAULT_BATCH_ROWS: usize = 512;
const DEFAULT_BLOCK_ROWS: u64 = 2_048;

#[derive(Clone, Copy, Debug)]
struct Case {
    name: &'static str,
    encoding: WriterEncoding,
    codec: WriterCodec,
}

#[derive(Debug)]
struct ResultRow {
    case: Case,
    rows: u64,
    output_bytes: u64,
    data_bytes: u64,
    ratio: f64,
    rows_per_second: f64,
    finish_seconds: f64,
    layouts: String,
    transforms: String,
    fallback_frequency: f64,
}

#[derive(Debug)]
struct Config {
    rows: usize,
    batch_rows: usize,
    block_rows: u64,
    output: Option<PathBuf>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("stage7b benchmark failed: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let config = parse_args()?;
    let schema = benchmark_schema();
    let batches = benchmark_batches(&schema, config.rows, config.batch_rows);
    let cases = cases();
    let mut rows = Vec::new();
    let mut raw_data_bytes = None;
    for case in cases {
        let result = run_case(&schema, &batches, config.block_rows, case)?;
        if case.encoding == WriterEncoding::Raw && case.codec == WriterCodec::None {
            raw_data_bytes = Some(result.data_bytes);
        }
        rows.push(result);
    }
    let baseline = raw_data_bytes.ok_or("the raw baseline did not run")?;
    for row in &mut rows {
        row.ratio = baseline as f64 / row.data_bytes as f64;
    }
    let report = render_report(&config, baseline, &rows);
    if let Some(path) = config.output {
        std::fs::write(&path, &report).map_err(|error| format!("{}: {error}", path.display()))?;
    }
    print!("{report}");
    Ok(())
}

fn cases() -> Vec<Case> {
    #[cfg_attr(not(feature = "zstd"), allow(unused_mut))]
    let mut cases = vec![
        Case {
            name: "raw-none",
            encoding: WriterEncoding::Raw,
            codec: WriterCodec::None,
        },
        Case {
            name: "adaptive-none",
            encoding: WriterEncoding::Adaptive,
            codec: WriterCodec::None,
        },
    ];
    #[cfg(feature = "zstd")]
    cases.extend([
        Case {
            name: "raw-zstandard",
            encoding: WriterEncoding::Raw,
            codec: WriterCodec::Zstandard,
        },
        Case {
            name: "adaptive-zstandard",
            encoding: WriterEncoding::Adaptive,
            codec: WriterCodec::Zstandard,
        },
    ]);
    cases
}

fn benchmark_schema() -> Schema {
    Schema::new(
        701,
        vec![
            Column::new(1, "packed", LogicalType::UInt32, false),
            Column::new(2, "offset", LogicalType::Int64, false),
            Column::new(
                3,
                "timestamp",
                LogicalType::Timestamp {
                    unit: TimeUnit::Millisecond,
                    timezone: TimeZone::Utc,
                },
                false,
            ),
            Column::new(4, "flag", LogicalType::Bool, false),
            Column::new(5, "label", LogicalType::Utf8, false),
        ],
        Some(3),
    )
}

fn benchmark_batches(schema: &Schema, rows: usize, batch_rows: usize) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    let mut start = 0;
    while start < rows {
        let end = (start + batch_rows).min(rows);
        let count = end - start;
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Array::UInt32(PrimitiveArray::new(
                    (start..end).map(|row| (row % 4096) as u32).collect(),
                    None,
                )),
                Array::Int64(PrimitiveArray::new(
                    (start..end)
                        .map(|row| -1_000_000 + (row % 1024) as i64)
                        .collect(),
                    None,
                )),
                Array::Timestamp(TimestampArray::new(
                    (start..end)
                        .map(|row| 1_700_000_000_000 + row as i64 * 1_000)
                        .collect(),
                    None,
                    TimeUnit::Millisecond,
                    TimeZone::Utc,
                )),
                Array::Bool(PrimitiveArray::new(
                    (start..end).map(|row| row / 128 % 2 == 0).collect(),
                    None,
                )),
                Array::Utf8(Utf8Array::new(
                    (start..end)
                        .map(|row| format!("label-{}", row / 128 % 32))
                        .collect(),
                    None,
                )),
            ],
            count,
        )
        .expect("benchmark batch is valid");
        batches.push(batch);
        start = end;
    }
    batches
}

fn run_case(
    schema: &Schema,
    batches: &[RecordBatch],
    block_rows: u64,
    case: Case,
) -> Result<ResultRow, String> {
    // `Writer::create` is exclusive, so each run needs a path of its own even
    // when several run at once inside one test binary.
    let file = scratch_path(case.name);
    let mut writer = Writer::create(
        &file,
        schema.clone(),
        WriterOptions::default()
            .with_row_block_target(block_rows)
            .with_encoding(case.encoding)
            .with_codec(case.codec),
    )
    .map_err(|error| error.to_string())?;
    let started = Instant::now();
    for batch in batches.iter().cloned() {
        writer.append(batch).map_err(|error| error.to_string())?;
    }
    let elapsed = started.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
    // `finish` publishes whatever is still buffered, so its encoding work is
    // reported beside ingestion rather than folded into it or dropped.
    let finishing = Instant::now();
    let summary = writer.finish().map_err(|error| error.to_string())?;
    let finish_seconds = finishing.elapsed().as_secs_f64();
    let bytes = std::fs::read(&file).map_err(|error| error.to_string())?;

    // A benchmark that only counted rows could not tell a compact encoding from
    // a corrupt one, so every case is validated and every value is compared.
    let report = acta::validate(&file).map_err(|error| format!("{}: {error}", case.name))?;
    if report.incomplete_tail() {
        return Err(format!(
            "{}: the file ends in an incomplete frame",
            case.name
        ));
    }
    let reader = Reader::open(&file).map_err(|error| error.to_string())?;
    let mut decoded = Vec::new();
    for block in reader.scan() {
        decoded.push(block.map_err(|error| error.to_string())?);
    }
    verify_values(case.name, schema, batches, &decoded)?;
    let expected_rows = batches
        .iter()
        .map(|batch| batch.row_count() as u64)
        .sum::<u64>();
    let metadata = inspect_data_frames(&bytes);
    std::fs::remove_file(file).map_err(|error| error.to_string())?;
    let fallback_frequency = if metadata.columns == 0 {
        0.0
    } else {
        metadata.fallbacks as f64 / metadata.columns as f64
    };
    Ok(ResultRow {
        case,
        rows: summary.rows_written(),
        output_bytes: bytes.len() as u64,
        data_bytes: metadata.data_bytes,
        ratio: 0.0,
        rows_per_second: expected_rows as f64 / elapsed,
        finish_seconds,
        layouts: metadata.layouts,
        transforms: metadata.transforms,
        fallback_frequency,
    })
}

/// Compare every decoded slot with the value that was written, bit-exactly.
///
/// Floating-point values are compared as bit patterns so that a transform which
/// loses a NaN payload or the sign of a zero is a failure rather than a match.
fn verify_values(
    case: &str,
    schema: &Schema,
    written: &[RecordBatch],
    decoded: &[RecordBatch],
) -> Result<(), String> {
    for column in 0..schema.column_count() {
        let expected = slots(written, column);
        let actual = slots(decoded, column);
        if expected.len() != actual.len() {
            return Err(format!(
                "{case}: column {} decoded {} rows, expected {}",
                schema.columns()[column].name(),
                actual.len(),
                expected.len()
            ));
        }
        for (row, (expected, actual)) in expected.iter().zip(&actual).enumerate() {
            if expected != actual {
                return Err(format!(
                    "{case}: column {} row {row} decoded {actual:?}, expected {expected:?}",
                    schema.columns()[column].name()
                ));
            }
        }
    }
    Ok(())
}

fn slots(batches: &[RecordBatch], column: usize) -> Vec<Option<String>> {
    let mut values = Vec::new();
    for batch in batches {
        let Some(array) = batch.column(column) else {
            continue;
        };
        for row in 0..array.len() {
            values.push(array.value_at(row).map(|value| match value {
                ScalarValue::Float32(value) => format!("f32:{:#x}", value.to_bits()),
                ScalarValue::Float64(value) => format!("f64:{:#x}", value.to_bits()),
                other => format!("{other:?}"),
            }));
        }
    }
    values
}

/// A path no other run in this process is using.
fn scratch_path(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("acta-{label}-{}-{id}.acta", std::process::id()))
}

#[derive(Debug)]
struct FrameSummary {
    data_bytes: u64,
    columns: usize,
    fallbacks: usize,
    layouts: String,
    transforms: String,
}

fn inspect_data_frames(bytes: &[u8]) -> FrameSummary {
    let schema = 64_usize;
    let schema_header =
        u32::from_le_bytes(bytes[schema + 16..schema + 20].try_into().unwrap()) as usize;
    let schema_payload =
        u64::from_le_bytes(bytes[schema + 24..schema + 32].try_into().unwrap()) as usize;
    let data = schema + 48 + schema_header + schema_payload + 32;
    let mut offset = data;
    let mut data_bytes = 0_u64;
    let mut layouts = Vec::new();
    let mut transforms = Vec::new();
    let mut columns = 0;
    let mut fallbacks = 0;
    while offset < bytes.len() {
        let header_length =
            u32::from_le_bytes(bytes[offset + 16..offset + 20].try_into().unwrap()) as usize;
        let payload_length =
            u64::from_le_bytes(bytes[offset + 24..offset + 32].try_into().unwrap()) as usize;
        let total = 48 + header_length + payload_length + 32;
        let header = offset + 48;
        let column_count =
            u32::from_le_bytes(bytes[header + 20..header + 24].try_into().unwrap()) as usize;
        let stream_table =
            u32::from_le_bytes(bytes[header + 44..header + 48].try_into().unwrap()) as usize;
        let statistics =
            u32::from_le_bytes(bytes[header + 48..header + 52].try_into().unwrap()) as usize;
        for index in 0..column_count {
            let descriptor = header + 64 + index * 32;
            let layout =
                u16::from_le_bytes(bytes[descriptor + 4..descriptor + 6].try_into().unwrap());
            layouts.push(layout);
            columns += 1;
            let first_stream =
                u32::from_le_bytes(bytes[descriptor + 16..descriptor + 20].try_into().unwrap())
                    as usize;
            let stream_count =
                u16::from_le_bytes(bytes[descriptor + 20..descriptor + 22].try_into().unwrap())
                    as usize;
            let mut all_raw = true;
            for stream in first_stream..first_stream + stream_count {
                let stream_offset = header + stream_table + stream * 48;
                let transform = u16::from_le_bytes(
                    bytes[stream_offset + 2..stream_offset + 4]
                        .try_into()
                        .unwrap(),
                );
                transforms.push(transform);
                all_raw &= transform == 0;
            }
            if layout == 0 && all_raw {
                fallbacks += 1;
            }
        }
        data_bytes += total as u64;
        offset += total;
        let _ = statistics;
    }
    FrameSummary {
        data_bytes,
        columns,
        fallbacks,
        layouts: join_u16(&layouts),
        transforms: join_u16(&transforms),
    }
}

fn join_u16(values: &[u16]) -> String {
    let mut output = String::new();
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        let _ = write!(output, "{value}");
    }
    output
}

fn render_report(config: &Config, baseline: u64, rows: &[ResultRow]) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# Acta Stage 7b benchmark");
    let _ = writeln!(output, "rows: {}", config.rows);
    let _ = writeln!(output, "batch_rows: {}", config.batch_rows);
    let _ = writeln!(output, "block_rows: {}", config.block_rows);
    let _ = writeln!(output, "raw_baseline_data_bytes: {baseline}");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "| case | rows | output bytes | data bytes | ratio | rows/s | finish s | layouts | transforms | fallback frequency |"
    );
    let _ = writeln!(
        output,
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: |"
    );
    for row in rows {
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} | {:.3} | {:.0} | {:.6} | `{}` | `{}` | {:.1}% |",
            row.case.name,
            row.rows,
            row.output_bytes,
            row.data_bytes,
            row.ratio,
            row.rows_per_second,
            row.finish_seconds,
            row.layouts,
            row.transforms,
            row.fallback_frequency * 100.0,
        );
    }
    output
}

fn parse_args() -> Result<Config, String> {
    let mut config = Config {
        rows: DEFAULT_ROWS,
        batch_rows: DEFAULT_BATCH_ROWS,
        block_rows: DEFAULT_BLOCK_ROWS,
        output: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--rows" => config.rows = positive(&mut args, "--rows")?,
            "--batch-rows" => config.batch_rows = positive(&mut args, "--batch-rows")?,
            "--block-rows" => config.block_rows = positive(&mut args, "--block-rows")? as u64,
            "--output" => config.output = Some(PathBuf::from(next(&mut args, "--output")?)),
            "--help" | "-h" => {
                println!(
                    "stage7b_benchmark [--rows N] [--batch-rows N] [--block-rows N] [--output PATH]"
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

/// Cargo builds examples as test harnesses, so these run under
/// `cargo test --all-targets` in both feature configurations and keep the
/// harness itself from rotting. They assert on correctness and on sizes that do
/// not depend on the host. No test asserts an elapsed time.
#[cfg(test)]
mod tests {
    use super::{
        Case, Config, benchmark_batches, benchmark_schema, cases, inspect_data_frames,
        render_report, run_case, verify_values,
    };
    use acta::{Reader, WriterCodec, WriterEncoding};

    const ROWS: usize = 256;
    const BATCH_ROWS: usize = 32;
    const BLOCK_ROWS: u64 = 64;

    fn small(case: Case) -> super::ResultRow {
        let schema = benchmark_schema();
        let batches = benchmark_batches(&schema, ROWS, BATCH_ROWS);
        run_case(&schema, &batches, BLOCK_ROWS, case)
            .unwrap_or_else(|error| panic!("{}: {error}", case.name))
    }

    /// `run_case` validates, decodes, and compares every value itself, so a
    /// case that returns at all has already proved the file is well formed.
    #[test]
    fn every_case_validates_decodes_and_matches_its_input() {
        for case in cases() {
            let result = small(case);
            assert_eq!(result.rows, ROWS as u64, "{}", case.name);
            assert!(result.data_bytes > 0, "{}", case.name);
            assert!(result.output_bytes >= result.data_bytes, "{}", case.name);
        }
    }

    /// The comparison the harness relies on must fail when values differ, or
    /// every case would pass regardless of what was decoded.
    #[test]
    fn value_verification_rejects_a_mismatch() {
        let schema = benchmark_schema();
        let written = benchmark_batches(&schema, ROWS, BATCH_ROWS);
        let shifted = benchmark_batches(&schema, ROWS, BATCH_ROWS)
            .into_iter()
            .skip(1)
            .collect::<Vec<_>>();

        assert!(verify_values("probe", &schema, &written, &written).is_ok());
        assert!(verify_values("probe", &schema, &written, &shifted).is_err());
    }

    #[test]
    fn adaptive_encoding_selects_something_other_than_plain_raw() {
        let adaptive = small(Case {
            name: "adaptive-none",
            encoding: WriterEncoding::Adaptive,
            codec: WriterCodec::None,
        });

        assert!(
            adaptive.fallback_frequency < 1.0,
            "adaptive selected plain/raw for every column: {}",
            adaptive.transforms
        );
    }

    /// The raw baseline is what the ratio column divides by, so it must be the
    /// policy that transforms nothing.
    #[test]
    fn the_raw_baseline_selects_plain_layout_and_the_raw_transform_everywhere() {
        let raw = small(Case {
            name: "raw-none",
            encoding: WriterEncoding::Raw,
            codec: WriterCodec::None,
        });

        assert!(raw.layouts.split(',').all(|layout| layout == "0"));
        assert!(raw.transforms.split(',').all(|transform| transform == "0"));
        assert_eq!(raw.fallback_frequency, 1.0);
    }

    #[test]
    fn adaptive_output_is_smaller_than_the_raw_baseline_for_this_dataset() {
        let raw = small(Case {
            name: "raw-none",
            encoding: WriterEncoding::Raw,
            codec: WriterCodec::None,
        });
        let adaptive = small(Case {
            name: "adaptive-none",
            encoding: WriterEncoding::Adaptive,
            codec: WriterCodec::None,
        });

        assert!(
            adaptive.data_bytes < raw.data_bytes,
            "adaptive {} is not smaller than raw {}",
            adaptive.data_bytes,
            raw.data_bytes
        );
    }

    /// Two runs of the same case must produce the same file, or a recorded
    /// report describes nothing repeatable.
    #[test]
    fn a_case_is_byte_for_byte_reproducible() {
        let schema = benchmark_schema();
        let batches = benchmark_batches(&schema, ROWS, BATCH_ROWS);
        let case = Case {
            name: "adaptive-none",
            encoding: WriterEncoding::Adaptive,
            codec: WriterCodec::None,
        };
        let first = run_case(&schema, &batches, BLOCK_ROWS, case).expect("first run");
        let second = run_case(&schema, &batches, BLOCK_ROWS, case).expect("second run");

        assert_eq!(first.output_bytes, second.output_bytes);
        assert_eq!(first.layouts, second.layouts);
        assert_eq!(first.transforms, second.transforms);
    }

    /// Splitting the same rows into different appends must not change what the
    /// benchmark reports, or its numbers describe the batching, not the policy.
    #[test]
    fn the_report_does_not_depend_on_the_append_batch_size() {
        let schema = benchmark_schema();
        let case = Case {
            name: "adaptive-none",
            encoding: WriterEncoding::Adaptive,
            codec: WriterCodec::None,
        };
        let whole = run_case(
            &schema,
            &benchmark_batches(&schema, ROWS, ROWS),
            BLOCK_ROWS,
            case,
        )
        .expect("one batch");
        let split = run_case(
            &schema,
            &benchmark_batches(&schema, ROWS, 7),
            BLOCK_ROWS,
            case,
        )
        .expect("many batches");

        assert_eq!(whole.output_bytes, split.output_bytes);
        assert_eq!(whole.layouts, split.layouts);
        assert_eq!(whole.transforms, split.transforms);
    }

    #[test]
    fn the_report_renders_one_row_for_every_case() {
        let config = Config {
            rows: ROWS,
            batch_rows: BATCH_ROWS,
            block_rows: BLOCK_ROWS,
            output: None,
        };
        let rows: Vec<super::ResultRow> = cases().into_iter().map(small).collect();

        let report = render_report(&config, rows[0].data_bytes, &rows);

        for case in cases() {
            assert!(report.contains(case.name), "missing {}", case.name);
        }
    }

    /// Without the optional dependency the Zstandard cases are simply absent,
    /// and the raw ones still run.
    #[test]
    fn the_case_list_follows_the_zstd_feature() {
        let names: Vec<&str> = cases().into_iter().map(|case| case.name).collect();

        assert!(names.contains(&"raw-none"));
        assert!(names.contains(&"adaptive-none"));
        assert_eq!(
            names.contains(&"adaptive-zstandard"),
            cfg!(feature = "zstd")
        );
    }

    /// The layout and transform columns are read straight out of the file, so
    /// they must describe the blocks the reader discovers rather than a
    /// walk that lost its place.
    #[test]
    fn the_reported_metadata_describes_the_file_that_was_written() {
        let schema = benchmark_schema();
        let batches = benchmark_batches(&schema, ROWS, BATCH_ROWS);
        let file = super::scratch_path("metadata");
        let mut writer = acta::Writer::create(
            &file,
            schema.clone(),
            acta::WriterOptions::default()
                .with_row_block_target(BLOCK_ROWS)
                .with_encoding(WriterEncoding::Adaptive),
        )
        .expect("create");
        for batch in batches {
            writer.append(batch).expect("append");
        }
        let summary = writer.finish().expect("finish");

        let bytes = std::fs::read(&file).expect("read");
        let metadata = inspect_data_frames(&bytes);
        let blocks = Reader::open(&file).expect("open").scan().count();
        let _ = std::fs::remove_file(&file);

        assert_eq!(blocks as u64, summary.blocks_written());
        // One layout per column per block, and the walk consumed every frame.
        assert_eq!(metadata.columns, blocks * schema.column_count());
        assert_eq!(
            metadata.layouts.split(',').count(),
            blocks * schema.column_count()
        );
        assert!(metadata.data_bytes > 0);
        assert!(metadata.data_bytes < summary.bytes_written());
    }
}
