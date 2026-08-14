//! Reproducible Stage 7 writer benchmark harness.
//!
//! This is deliberately a small, dependency-free harness rather than an
//! ordinary unit test. It generates the same inputs for every codec and row
//! block target, validates every output, and reports wall-clock measurements
//! without asserting on their values.

use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use acta::{
    Array, BinaryArray, Column, LogicalType, PrimitiveArray, Reader, RecordBatch, Schema, TimeUnit,
    TimeZone, TimestampArray, Utf8Array, Writer, WriterCodec, WriterOptions,
};

const DEFAULT_ROWS: usize = 16_384;
const DEFAULT_BATCH_ROWS: usize = 1_024;
const DEFAULT_BYTE_BLOCK_TARGET: u64 = 64 * 1024 * 1024;
const DEFAULT_BLOCK_TARGETS: &[u64] = &[256, 1_024, 4_096];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DatasetKind {
    FixedNumeric,
    NullableNumeric,
    RepeatedUtf8,
    HighCardinalityUtf8,
    BinaryPattern,
    BinaryRandom,
    Timestamps,
}

impl DatasetKind {
    const ALL: &[Self] = &[
        Self::FixedNumeric,
        Self::NullableNumeric,
        Self::RepeatedUtf8,
        Self::HighCardinalityUtf8,
        Self::BinaryPattern,
        Self::BinaryRandom,
        Self::Timestamps,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::FixedNumeric => "fixed_numeric_compressible",
            Self::NullableNumeric => "nullable_numeric",
            Self::RepeatedUtf8 => "repeated_utf8_compressible",
            Self::HighCardinalityUtf8 => "high_cardinality_utf8",
            Self::BinaryPattern => "binary_pattern_compressible",
            Self::BinaryRandom => "binary_random_incompressible",
            Self::Timestamps => "timestamps_monotonic",
        }
    }

    fn seed(self) -> u64 {
        match self {
            Self::FixedNumeric => 0x517f_1ed0_0000_0001,
            Self::NullableNumeric => 0x517f_1ed0_0000_0002,
            Self::RepeatedUtf8 => 0x517f_1ed0_0000_0003,
            Self::HighCardinalityUtf8 => 0x517f_1ed0_0000_0004,
            Self::BinaryPattern => 0x517f_1ed0_0000_0005,
            Self::BinaryRandom => 0x517f_1ed0_0000_0006,
            Self::Timestamps => 0x517f_1ed0_0000_0007,
        }
    }

    fn parse(name: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|kind| kind.name() == name)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CodecChoice {
    None,
    #[cfg_attr(not(feature = "zstd"), allow(dead_code))]
    Zstandard,
}

impl CodecChoice {
    fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Zstandard => "zstandard",
        }
    }

    fn writer_codec(self) -> WriterCodec {
        match self {
            Self::None => WriterCodec::None,
            Self::Zstandard => {
                #[cfg(feature = "zstd")]
                {
                    WriterCodec::Zstandard
                }
                #[cfg(not(feature = "zstd"))]
                {
                    unreachable!("Zstandard is rejected by argument parsing without zstd")
                }
            }
        }
    }
}

#[derive(Debug)]
struct Config {
    rows: usize,
    batch_rows: usize,
    block_targets: Vec<u64>,
    datasets: Vec<DatasetKind>,
    codecs: Vec<CodecChoice>,
    output: Option<PathBuf>,
    keep_files: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rows: DEFAULT_ROWS,
            batch_rows: DEFAULT_BATCH_ROWS,
            block_targets: DEFAULT_BLOCK_TARGETS.to_vec(),
            datasets: DatasetKind::ALL.to_vec(),
            codecs: default_codecs(),
            output: None,
            keep_files: false,
        }
    }
}

fn default_codecs() -> Vec<CodecChoice> {
    #[cfg(feature = "zstd")]
    {
        vec![CodecChoice::None, CodecChoice::Zstandard]
    }
    #[cfg(not(feature = "zstd"))]
    {
        vec![CodecChoice::None]
    }
}

#[derive(Debug)]
struct Dataset {
    kind: DatasetKind,
    schema: Schema,
    batches: Vec<RecordBatch>,
    rows: usize,
}

#[derive(Debug)]
struct BenchmarkResult {
    dataset: &'static str,
    codec: &'static str,
    rows: u64,
    raw_input_bytes: u64,
    block_target: u64,
    blocks: u64,
    output_bytes: u64,
    stored_data_frame_bytes: u64,
    compression_ratio: f64,
    ingest_seconds: f64,
    finish_sync_seconds: f64,
    rows_per_second: f64,
    raw_bytes_per_second: f64,
    max_buffered_rows: u64,
    max_buffered_bytes: u64,
    peak_block_rows: u64,
    bounded_row_target: u64,
    bounded_byte_target: u64,
}

/// Case identifiers only have to be unique within one process, so that
/// concurrently running cases cannot choose the same temporary path.
static NEXT_CASE_ID: AtomicU64 = AtomicU64::new(1);

fn next_case_id() -> u64 {
    NEXT_CASE_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone, Copy)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("stage7 benchmark failed: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let config = parse_args()?;
    let datasets = config
        .datasets
        .iter()
        .copied()
        .map(|kind| generate_dataset(kind, config.rows, config.batch_rows))
        .collect::<Result<Vec<_>, _>>()?;

    let mut results = Vec::new();
    for dataset in &datasets {
        for &block_target in &config.block_targets {
            for &codec in &config.codecs {
                results.push(run_case(dataset, codec, block_target, config.keep_files)?);
            }
        }
    }

    let report = render_report(&config, &results);
    if let Some(path) = &config.output {
        std::fs::write(path, &report)
            .map_err(|error| format!("could not write {}: {error}", path.display()))?;
    }
    print!("{report}");
    Ok(())
}

fn parse_args() -> Result<Config, String> {
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--rows" => config.rows = parse_positive(&mut args, "--rows")?,
            "--batch-rows" => config.batch_rows = parse_positive(&mut args, "--batch-rows")?,
            "--block-targets" => {
                config.block_targets = parse_list(&mut args, "--block-targets")?
                    .into_iter()
                    .map(|value| {
                        value
                            .parse::<u64>()
                            .map_err(|error| format!("invalid row block target {value:?}: {error}"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if config.block_targets.is_empty() || config.block_targets.contains(&0) {
                    return Err("--block-targets must contain positive integers".into());
                }
            }
            "--datasets" => {
                let names = parse_list(&mut args, "--datasets")?;
                config.datasets = names
                    .iter()
                    .map(|name| {
                        DatasetKind::parse(name).ok_or_else(|| {
                            format!(
                                "unknown dataset {name:?}; use --help to see the supported names"
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if config.datasets.is_empty() {
                    return Err("--datasets must contain at least one dataset".into());
                }
            }
            "--codecs" => {
                let names = parse_list(&mut args, "--codecs")?;
                config.codecs = names
                    .iter()
                    .map(|name| match name.as_str() {
                        "none" => Ok(CodecChoice::None),
                        "zstd" | "zstandard" => {
                            #[cfg(feature = "zstd")]
                            {
                                Ok(CodecChoice::Zstandard)
                            }
                            #[cfg(not(feature = "zstd"))]
                            {
                                Err(
                                    "Zstandard benchmarks require the crate's zstd feature; run with --features zstd"
                                        .into(),
                                )
                            }
                        }
                        _ => Err(format!(
                            "unknown codec {name:?}; supported values are none,zstd"
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if config.codecs.is_empty() {
                    return Err("--codecs must contain at least one codec".into());
                }
            }
            "--output" => {
                config.output = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| "--output needs a path".to_string())?,
                ));
            }
            "--keep-files" => config.keep_files = true,
            other => return Err(format!("unknown argument {other:?}; use --help")),
        }
    }
    if config.rows == 0 || config.batch_rows == 0 {
        return Err("--rows and --batch-rows must be positive".into());
    }
    Ok(config)
}

fn parse_positive(args: &mut impl Iterator<Item = String>, option: &str) -> Result<usize, String> {
    let value = args
        .next()
        .ok_or_else(|| format!("{option} needs a positive integer"))?;
    let value = value
        .parse::<usize>()
        .map_err(|error| format!("invalid value for {option}: {error}"))?;
    if value == 0 {
        return Err(format!("{option} needs a positive integer"));
    }
    Ok(value)
}

fn parse_list(
    args: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<Vec<String>, String> {
    let value = args
        .next()
        .ok_or_else(|| format!("{option} needs a comma-separated list"))?;
    Ok(value
        .split(',')
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect())
}

fn print_help() {
    println!(
        "Stage 7 writer benchmark\n\n\
         Usage: cargo run --release --example stage7_benchmark -- [options]\n\n\
         Options:\n\
           --rows N                 rows per dataset (default: {DEFAULT_ROWS})\n\
           --batch-rows N           input batch size (default: {DEFAULT_BATCH_ROWS})\n\
           --block-targets A,B      row targets (default: 256,1024,4096)\n\
           --datasets A,B           dataset names (default: all)\n\
           --codecs none,zstd       codecs; zstd needs --features zstd\n\
           --output PATH             also write the Markdown report to PATH\n\
           --keep-files              retain temporary Acta outputs\n\
         Dataset names:\n\
           fixed_numeric_compressible, nullable_numeric, repeated_utf8_compressible,\n\
           high_cardinality_utf8, binary_pattern_compressible,\n\
           binary_random_incompressible, timestamps_monotonic"
    );
}

fn generate_dataset(kind: DatasetKind, rows: usize, batch_rows: usize) -> Result<Dataset, String> {
    let schema = schema_for(kind);
    let mut generator = SplitMix64::new(kind.seed());
    let mut batches = Vec::new();
    let mut start = 0_usize;
    while start < rows {
        let count = (rows - start).min(batch_rows);
        batches.push(build_batch(
            kind,
            Arc::new(schema.clone()),
            start,
            count,
            &mut generator,
        )?);
        start += count;
    }
    Ok(Dataset {
        kind,
        schema,
        batches,
        rows,
    })
}

fn schema_for(kind: DatasetKind) -> Schema {
    match kind {
        DatasetKind::FixedNumeric => Schema::new(
            7001,
            vec![
                Column::new(1, "signed", LogicalType::Int64, false),
                Column::new(2, "unsigned", LogicalType::UInt32, false),
                Column::new(3, "floating", LogicalType::Float64, false),
            ],
            None,
        ),
        DatasetKind::NullableNumeric => Schema::new(
            7002,
            vec![
                Column::new(1, "signed", LogicalType::Int64, true),
                Column::new(2, "floating", LogicalType::Float64, true),
            ],
            None,
        ),
        DatasetKind::RepeatedUtf8 | DatasetKind::HighCardinalityUtf8 => Schema::new(
            if matches!(kind, DatasetKind::RepeatedUtf8) {
                7003
            } else {
                7004
            },
            vec![Column::new(1, "text", LogicalType::Utf8, false)],
            None,
        ),
        DatasetKind::BinaryPattern | DatasetKind::BinaryRandom => Schema::new(
            if matches!(kind, DatasetKind::BinaryPattern) {
                7005
            } else {
                7006
            },
            vec![Column::new(1, "payload", LogicalType::Binary, false)],
            None,
        ),
        DatasetKind::Timestamps => Schema::new(
            7007,
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
            ],
            Some(1),
        ),
    }
}

fn build_batch(
    kind: DatasetKind,
    schema: Arc<Schema>,
    start: usize,
    count: usize,
    generator: &mut SplitMix64,
) -> Result<RecordBatch, String> {
    let end = start + count;
    let indices = start..end;
    let batch = match kind {
        DatasetKind::FixedNumeric => {
            let mut signed = Vec::with_capacity(count);
            let mut unsigned = Vec::with_capacity(count);
            let mut floating = Vec::with_capacity(count);
            for index in indices {
                let index = i64::try_from(index).map_err(|_| "row index exceeds i64")?;
                signed.push(index.wrapping_mul(17).wrapping_sub(9));
                unsigned.push((index as u32).wrapping_mul(31).wrapping_add(7));
                floating.push((index.rem_euclid(4096) as f64) / 10.0);
            }
            vec![
                Array::Int64(PrimitiveArray::new(signed, None)),
                Array::UInt32(PrimitiveArray::new(unsigned, None)),
                Array::Float64(PrimitiveArray::new(floating, None)),
            ]
        }
        DatasetKind::NullableNumeric => {
            let mut signed = Vec::with_capacity(count);
            let mut signed_validity = Vec::with_capacity(count);
            let mut floating = Vec::with_capacity(count);
            let mut floating_validity = Vec::with_capacity(count);
            for index in indices {
                let valid = index % 11 != 0;
                let random = generator.next();
                signed.push(if valid { random as i64 } else { 0 });
                signed_validity.push(valid);
                floating.push(if valid {
                    (generator.next() % 1_000_000) as f64 / 100.0
                } else {
                    0.0
                });
                floating_validity.push(valid);
            }
            vec![
                Array::Int64(PrimitiveArray::new(signed, Some(signed_validity))),
                Array::Float64(PrimitiveArray::new(floating, Some(floating_validity))),
            ]
        }
        DatasetKind::RepeatedUtf8 => {
            let vocabulary = ["madrid", "paris", "東京", "device-é", "warehouse-01"];
            let values = indices
                .map(|index| vocabulary[(index.wrapping_mul(7) + 3) % vocabulary.len()].into())
                .collect();
            vec![Array::Utf8(Utf8Array::new(values, None))]
        }
        DatasetKind::HighCardinalityUtf8 => {
            let values = indices
                .map(|index| format!("event-{index:08}-{}-µ-東京", generator.next()))
                .collect();
            vec![Array::Utf8(Utf8Array::new(values, None))]
        }
        DatasetKind::BinaryPattern => {
            let values = indices
                .map(|index| {
                    let mut value = vec![0_u8; 64];
                    for (offset, byte) in value.iter_mut().enumerate() {
                        *byte = 0xa5 ^ ((index / 32 + offset) as u8 & 0x0f);
                    }
                    value
                })
                .collect();
            vec![Array::Binary(BinaryArray::new(values, None))]
        }
        DatasetKind::BinaryRandom => {
            let values = indices
                .map(|_| {
                    let mut value = vec![0_u8; 64];
                    for byte in &mut value {
                        *byte = generator.next() as u8;
                    }
                    value
                })
                .collect();
            vec![Array::Binary(BinaryArray::new(values, None))]
        }
        DatasetKind::Timestamps => {
            let mut timestamps = Vec::with_capacity(count);
            let mut values = Vec::with_capacity(count);
            for index in indices {
                let index = i64::try_from(index).map_err(|_| "row index exceeds i64")?;
                timestamps.push(1_700_000_000_000_000_i64 + index * 1_000 + index % 17);
                values.push(index.wrapping_mul(13).wrapping_add(5));
            }
            vec![
                Array::Timestamp(TimestampArray::new(
                    timestamps,
                    None,
                    TimeUnit::Microsecond,
                    TimeZone::Utc,
                )),
                Array::Int64(PrimitiveArray::new(values, None)),
            ]
        }
    };
    RecordBatch::try_new(schema, batch, count).map_err(|error| error.to_string())
}

fn run_case(
    dataset: &Dataset,
    codec: CodecChoice,
    block_target: u64,
    keep_files: bool,
) -> Result<BenchmarkResult, String> {
    let path = benchmark_path(dataset.kind, codec, block_target, next_case_id());
    let _ = std::fs::remove_file(&path);
    // Cloning the inputs before the timed region keeps the measurement on the
    // writer rather than on the harness's own batch construction.
    let inputs = dataset.batches.to_vec();
    let options = WriterOptions::default()
        .with_row_block_target(block_target)
        .with_byte_block_target(DEFAULT_BYTE_BLOCK_TARGET)
        .with_codec(codec.writer_codec());
    let mut writer = Writer::create(&path, dataset.schema.clone(), options)
        .map_err(|error| format!("{} / create: {error}", dataset.kind.name()))?;

    let mut max_buffered_rows = 0_u64;
    let mut max_buffered_bytes = 0_u64;
    let ingest_start = Instant::now();
    for batch in inputs {
        writer
            .append(batch)
            .map_err(|error| format!("{} / append: {error}", dataset.kind.name()))?;
        let accounting = writer.accounting();
        if accounting.buffered_rows() > block_target {
            return Err(format!(
                "{} / buffered {} rows above the {block_target} row target",
                dataset.kind.name(),
                accounting.buffered_rows()
            ));
        }
        max_buffered_rows = max_buffered_rows.max(accounting.buffered_rows());
        max_buffered_bytes = max_buffered_bytes.max(accounting.buffered_bytes());
    }
    let ingest_seconds = ingest_start.elapsed().as_secs_f64();

    let finish_start = Instant::now();
    let summary = writer
        .finish()
        .map_err(|error| format!("{} / finish: {error}", dataset.kind.name()))?;
    let finish_sync_seconds = finish_start.elapsed().as_secs_f64();
    let accounting = summary.accounting();
    if accounting.buffered_rows() != 0 || accounting.buffered_bytes() != 0 {
        return Err(format!(
            "{} / finish left buffered data: {} rows, {} bytes",
            dataset.kind.name(),
            accounting.buffered_rows(),
            accounting.buffered_bytes()
        ));
    }

    let validation = acta::validate(&path)
        .map_err(|error| format!("{} / structural validation: {error}", dataset.kind.name()))?;
    if validation.incomplete_tail() {
        return Err(format!(
            "{} / output has an incomplete tail",
            dataset.kind.name()
        ));
    }
    let reader = Reader::open(&path)
        .map_err(|error| format!("{} / reader open: {error}", dataset.kind.name()))?;
    check_decoded(&reader, dataset)?;
    let blocks = reader.file_metadata().block_count();
    let stored_data_frame_bytes = reader
        .blocks()
        .iter()
        .map(|block| block.total_length())
        .sum::<u64>();
    // A block holds exactly the rows the buffer held when it was published, so
    // the largest committed block is the writer's true peak buffer occupancy.
    let peak_block_rows = reader
        .blocks()
        .iter()
        .map(|block| block.row_count())
        .max()
        .unwrap_or(0);
    if peak_block_rows > block_target {
        return Err(format!(
            "{} / published a {peak_block_rows} row block above the {block_target} row target",
            dataset.kind.name()
        ));
    }
    let output_bytes = std::fs::metadata(&path)
        .map_err(|error| format!("{} / stat: {error}", dataset.kind.name()))?
        .len();
    if output_bytes != summary.bytes_written() || blocks != summary.blocks_written() {
        return Err(format!(
            "{} / writer summary disagrees with reader metadata",
            dataset.kind.name()
        ));
    }
    let raw_input_bytes = accounting.total_bytes();
    if raw_input_bytes == 0 || stored_data_frame_bytes == 0 {
        return Err(format!(
            "{} / benchmark input or output was empty",
            dataset.kind.name()
        ));
    }
    let compression_ratio = raw_input_bytes as f64 / stored_data_frame_bytes as f64;
    let rows = summary.rows_written();
    let rows_per_second = rows as f64 / ingest_seconds.max(f64::MIN_POSITIVE);
    let raw_bytes_per_second = raw_input_bytes as f64 / ingest_seconds.max(f64::MIN_POSITIVE);
    let result = BenchmarkResult {
        dataset: dataset.kind.name(),
        codec: codec.name(),
        rows,
        raw_input_bytes,
        block_target,
        blocks,
        output_bytes,
        stored_data_frame_bytes,
        compression_ratio,
        ingest_seconds,
        finish_sync_seconds,
        rows_per_second,
        raw_bytes_per_second,
        max_buffered_rows,
        max_buffered_bytes,
        peak_block_rows,
        bounded_row_target: block_target,
        bounded_byte_target: DEFAULT_BYTE_BLOCK_TARGET,
    };

    if !keep_files {
        let _ = std::fs::remove_file(&path);
    }
    Ok(result)
}

fn benchmark_path(
    dataset: DatasetKind,
    codec: CodecChoice,
    block_target: u64,
    case_id: u64,
) -> PathBuf {
    std::env::temp_dir().join(format!(
        "acta-stage7-{}-{}-{}-{}-{}.acta",
        std::process::id(),
        case_id,
        dataset.name(),
        codec.name(),
        block_target
    ))
}

fn check_decoded(reader: &Reader, dataset: &Dataset) -> Result<(), String> {
    if reader.total_rows() != dataset.rows as u64 {
        return Err(format!(
            "{} / reader found {} rows, expected {}",
            dataset.kind.name(),
            reader.total_rows(),
            dataset.rows
        ));
    }
    let mut expected_batch = 0_usize;
    let mut expected_row = 0_usize;
    let mut decoded_rows = 0_usize;
    for decoded in reader.scan() {
        let decoded =
            decoded.map_err(|error| format!("{} / decode: {error}", dataset.kind.name()))?;
        for row in 0..decoded.row_count() {
            while expected_batch < dataset.batches.len()
                && expected_row == dataset.batches[expected_batch].row_count()
            {
                expected_batch += 1;
                expected_row = 0;
            }
            let expected = dataset.batches.get(expected_batch).ok_or_else(|| {
                format!("{} / decoded more rows than generated", dataset.kind.name())
            })?;
            for column in 0..expected.schema().column_count() {
                let expected_value = expected
                    .column(column)
                    .expect("schema and batch columns match")
                    .value_at(expected_row);
                let actual_value = decoded
                    .column(column)
                    .expect("schema and decoded columns match")
                    .value_at(row);
                if actual_value != expected_value {
                    return Err(format!(
                        "{} / mismatch at row {}, column {}",
                        dataset.kind.name(),
                        decoded_rows,
                        column
                    ));
                }
            }
            expected_row += 1;
            decoded_rows += 1;
        }
    }
    if decoded_rows != dataset.rows {
        return Err(format!(
            "{} / decoded {} rows, expected {}",
            dataset.kind.name(),
            decoded_rows,
            dataset.rows
        ));
    }
    Ok(())
}

fn render_report(config: &Config, results: &[BenchmarkResult]) -> String {
    let mut report = String::new();
    writeln!(report, "# Acta Stage 7 writer benchmark").unwrap();
    writeln!(
        report,
        "\nThis report was generated by benchmarks/stage7/stage7_benchmark.rs. Every case passed structural validation and a full reader decode before it was reported. Timings are observations, not test thresholds."
    )
    .unwrap();
    writeln!(report, "\n## Configuration\n").unwrap();
    writeln!(report, "- Rows per dataset: {}", config.rows).unwrap();
    writeln!(report, "- Input batch rows: {}", config.batch_rows).unwrap();
    writeln!(
        report,
        "- Row block targets: {}",
        config
            .block_targets
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
    .unwrap();
    writeln!(
        report,
        "- Codecs: {}",
        config
            .codecs
            .iter()
            .map(|codec| codec.name())
            .collect::<Vec<_>>()
            .join(", ")
    )
    .unwrap();
    writeln!(report, "- Byte block target: {DEFAULT_BYTE_BLOCK_TARGET}").unwrap();
    writeln!(
        report,
        "- Generator seeds are fixed per dataset and compiled into the harness."
    )
    .unwrap();
    writeln!(
        report,
        "- Host: {}/{}",
        std::env::consts::OS,
        std::env::consts::ARCH
    )
    .unwrap();
    writeln!(
        report,
        "- raw_input_bytes is the writer's exact estimated raw data-frame total; stored_data_frame_bytes is the sum of the committed frame lengths."
    )
    .unwrap();
    writeln!(
        report,
        "- compression_ratio is raw data-frame bytes divided by stored data-frame bytes; output_bytes includes the prologue and schema frame."
    )
    .unwrap();
    writeln!(
        report,
        "- Ingestion timing covers append calls, including blocks automatically published during append. finish_sync_seconds covers the final finish call, including the final flush and filesystem synchronization."
    )
    .unwrap();
    writeln!(
        report,
        "- Buffered-memory columns are the maximum accounting values observed between append calls. The configured row and byte targets are the writer's bounds; a batch that crosses a target can publish inside one append call."
    )
    .unwrap();
    writeln!(
        report,
        "- Peak block rows is the largest committed block, which is exactly the rows the buffer held at publication, so it measures peak occupancy that sampling between appends can miss. Every case additionally checks that neither value exceeds the configured row target."
    )
    .unwrap();

    writeln!(report, "\n## Results\n").unwrap();
    writeln!(report, "| Dataset | Codec | Rows | Raw bytes | Block target | Blocks | Output bytes | Stored data-frame bytes | Ratio | Ingest s | Finish/sync s | Rows/s | Raw bytes/s | Peak block rows | Max buffered rows | Max buffered bytes |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|").unwrap();
    for result in results {
        writeln!(
            report,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {:.3} | {:.6} | {:.6} | {:.0} | {:.0} | {} / {} | {} / {} | {} / {} |",
            result.dataset,
            result.codec,
            result.rows,
            result.raw_input_bytes,
            result.block_target,
            result.blocks,
            result.output_bytes,
            result.stored_data_frame_bytes,
            result.compression_ratio,
            result.ingest_seconds,
            result.finish_sync_seconds,
            result.rows_per_second,
            result.raw_bytes_per_second,
            result.peak_block_rows,
            result.bounded_row_target,
            result.max_buffered_rows,
            result.bounded_row_target,
            result.max_buffered_bytes,
            result.bounded_byte_target,
        )
        .unwrap();
    }
    report
}

/// Correctness coverage for the harness itself.
///
/// `cargo test --all-targets` builds and runs examples as test harnesses, so
/// these run in both feature configurations. They use the smallest useful
/// configuration and deliberately assert nothing about elapsed time, because
/// wall-clock measurements are observations rather than thresholds.
#[cfg(test)]
mod tests {
    use super::*;

    const TEST_ROWS: usize = 256;
    const TEST_BATCH_ROWS: usize = 64;
    const TEST_BLOCK_TARGETS: &[u64] = &[32, 128];

    fn smoke_dataset(kind: DatasetKind) -> Dataset {
        generate_dataset(kind, TEST_ROWS, TEST_BATCH_ROWS).expect("the generator accepts its input")
    }

    fn smoke_case(kind: DatasetKind, codec: CodecChoice, block_target: u64) -> BenchmarkResult {
        run_case(&smoke_dataset(kind), codec, block_target, false)
            .expect("a benchmark case validates and decodes")
    }

    #[test]
    fn every_dataset_and_codec_validates_and_decodes() {
        for &kind in DatasetKind::ALL {
            let dataset = smoke_dataset(kind);
            for &block_target in TEST_BLOCK_TARGETS {
                for &codec in &default_codecs() {
                    let result = run_case(&dataset, codec, block_target, false)
                        .unwrap_or_else(|error| panic!("{}: {error}", kind.name()));
                    assert_eq!(result.rows, TEST_ROWS as u64, "{}", kind.name());
                    assert_eq!(
                        result.blocks,
                        TEST_ROWS as u64 / block_target,
                        "{} at target {block_target}",
                        kind.name()
                    );
                    assert!(result.raw_input_bytes > 0, "{}", kind.name());
                    assert!(result.stored_data_frame_bytes > 0, "{}", kind.name());
                    assert!(result.output_bytes > result.stored_data_frame_bytes);
                }
            }
        }
    }

    #[test]
    fn buffered_memory_stays_within_the_configured_targets() {
        for &kind in DatasetKind::ALL {
            for &block_target in TEST_BLOCK_TARGETS {
                let result = smoke_case(kind, CodecChoice::None, block_target);
                assert_eq!(result.bounded_row_target, block_target);
                assert_eq!(result.bounded_byte_target, DEFAULT_BYTE_BLOCK_TARGET);
                assert!(result.peak_block_rows <= block_target, "{}", kind.name());
                assert!(result.max_buffered_rows <= block_target, "{}", kind.name());
                assert!(
                    result.max_buffered_bytes <= DEFAULT_BYTE_BLOCK_TARGET,
                    "{}",
                    kind.name()
                );
            }
        }
        // Every target here divides the row count, so each block is published
        // exactly at the target rather than as a smaller final block.
        let result = smoke_case(DatasetKind::FixedNumeric, CodecChoice::None, 128);
        assert_eq!(result.peak_block_rows, 128);
    }

    #[test]
    fn raw_output_stores_exactly_the_estimated_raw_bytes() {
        for &kind in DatasetKind::ALL {
            let result = smoke_case(kind, CodecChoice::None, 128);
            assert_eq!(
                result.raw_input_bytes,
                result.stored_data_frame_bytes,
                "{}",
                kind.name()
            );
            assert_eq!(result.compression_ratio, 1.0, "{}", kind.name());
        }
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstandard_shrinks_compressible_data_and_keeps_incompressible_data_readable() {
        let compressible = smoke_case(DatasetKind::RepeatedUtf8, CodecChoice::Zstandard, 128);
        assert!(
            compressible.compression_ratio > 1.0,
            "repeated UTF-8 should compress, got {}",
            compressible.compression_ratio
        );
        assert!(compressible.stored_data_frame_bytes < compressible.raw_input_bytes);

        // The incompressible case only has to stay correct; its ratio is a
        // property of Zstandard, not of this writer.
        let incompressible = smoke_case(DatasetKind::BinaryRandom, CodecChoice::Zstandard, 128);
        assert_eq!(incompressible.rows, TEST_ROWS as u64);
    }

    #[test]
    fn the_report_renders_one_row_for_every_case() {
        let config = Config {
            rows: TEST_ROWS,
            batch_rows: TEST_BATCH_ROWS,
            block_targets: TEST_BLOCK_TARGETS.to_vec(),
            datasets: vec![DatasetKind::FixedNumeric],
            codecs: default_codecs(),
            output: None,
            keep_files: false,
        };
        let results: Vec<BenchmarkResult> = config
            .block_targets
            .iter()
            .flat_map(|&target| {
                config
                    .codecs
                    .iter()
                    .map(move |&codec| smoke_case(DatasetKind::FixedNumeric, codec, target))
            })
            .collect();
        let report = render_report(&config, &results);
        let rows = report
            .lines()
            .filter(|line| line.starts_with("| fixed_numeric_compressible |"))
            .count();
        assert_eq!(rows, results.len());
        assert!(report.contains("Peak block rows"));
    }

    #[test]
    fn dataset_names_round_trip_through_parsing() {
        for &kind in DatasetKind::ALL {
            assert_eq!(DatasetKind::parse(kind.name()), Some(kind));
        }
        assert_eq!(DatasetKind::parse("not_a_dataset"), None);
    }
}
