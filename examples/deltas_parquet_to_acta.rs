//! Convert `deltas.parquet` export into Acta v0.2.
//!
//! This is deliberately not a generic converter. The source schema, Acta
//! logical types, primary column, and writer transform are fixed up front.
//! Only Parquet metadata is read before conversion; no values are sampled or
//! profiled to choose types or encodings.

use std::error::Error as StdError;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use acta::{
    Array as ActaArray, Column, LogicalType, PrimitiveArray as ActaPrimitiveArray,
    RecordBatch as ActaRecordBatch, Schema, TimeUnit, TimeZone, TimestampArray, Utf8Array, Writer,
    WriterCodec, WriterEncoding, WriterOptions, WriterStatistics, WriterTransform,
};
use arrow_array::types::{Float64Type, TimestampMicrosecondType, UInt64Type};
use arrow_array::{
    Array as ArrowArray, PrimitiveArray as ArrowPrimitiveArray, RecordBatch as ArrowRecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Schema as ArrowSchema, TimeUnit as ArrowTimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const DEFAULT_BATCH_ROWS: usize = 262_144;
const ROW_BLOCK_TARGET: u64 = 262_144;
const BYTE_BLOCK_TARGET: u64 = 64 * 1024 * 1024;
const ZSTD_LEVEL: i32 = 1;
const PRIMARY_COLUMN_ID: u32 = 1;
const MIB: f64 = 1024.0 * 1024.0;

#[derive(Debug)]
struct Args {
    input: PathBuf,
    output: PathBuf,
    stats: PathBuf,
    batch_rows: usize,
}

struct Report {
    file: BufWriter<File>,
}

fn main() -> Result<(), Box<dyn StdError>> {
    let args = Args::parse(std::env::args().skip(1))?;
    refuse_existing_output(&args.output, "Acta output")?;
    refuse_existing_output(&args.stats, "statistics report")?;

    let input_bytes = fs::metadata(&args.input)?.len();
    let input_file = File::open(&args.input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(input_file)?;
    validate_source_schema(builder.schema().as_ref())?;

    let parquet_rows = u64::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| invalid("Parquet metadata contains a negative row count"))?;
    let parquet_row_groups = builder.metadata().num_row_groups();
    let parquet_columns = builder.schema().fields().len();
    let parquet_compressed_bytes =
        builder
            .metadata()
            .row_groups()
            .iter()
            .try_fold(0_u64, |total, group| {
                let bytes = u64::try_from(group.compressed_size())
                    .map_err(|_| invalid("Parquet metadata contains a negative compressed size"))?;
                total
                    .checked_add(bytes)
                    .ok_or_else(|| invalid("Parquet compressed byte count overflow"))
            })?;
    let parquet_uncompressed_bytes =
        builder
            .metadata()
            .row_groups()
            .iter()
            .try_fold(0_u64, |total, group| {
                let bytes = u64::try_from(group.total_byte_size()).map_err(|_| {
                    invalid("Parquet metadata contains a negative uncompressed size")
                })?;
                total
                    .checked_add(bytes)
                    .ok_or_else(|| invalid("Parquet uncompressed byte count overflow"))
            })?;

    let schema = Arc::new(acta_schema());
    let mut report = Report::create(&args.stats)?;
    report.section("BEGINNING STATS")?;
    report.metric("started_unix_seconds", unix_seconds()?)?;
    report.metric("input", args.input.display())?;
    report.metric("output", args.output.display())?;
    report.metric("stats_report", args.stats.display())?;
    report.metric("input_file_bytes", input_bytes)?;
    report.metric(
        "input_file_mib",
        format_args!("{:.3}", input_bytes as f64 / MIB),
    )?;
    report.metric("parquet_rows", parquet_rows)?;
    report.metric("parquet_row_groups", parquet_row_groups)?;
    report.metric("parquet_columns", parquet_columns)?;
    report.metric(
        "parquet_column_chunks_compressed_bytes",
        parquet_compressed_bytes,
    )?;
    report.metric(
        "parquet_column_chunks_uncompressed_bytes",
        parquet_uncompressed_bytes,
    )?;
    report.metric("reader_batch_rows", args.batch_rows)?;
    report.metric("acta_row_block_target", ROW_BLOCK_TARGET)?;
    report.metric("acta_byte_block_target", BYTE_BLOCK_TARGET)?;
    report.metric("acta_primary", "received_at (column 1)")?;
    report.metric("acta_encoding", "fixed raw (no adaptive profiling)")?;
    report.metric("acta_codec", "zstandard")?;
    report.metric("acta_zstd_level", ZSTD_LEVEL)?;
    report.metric("acta_optional_statistics", "none")?;
    report.line("")?;
    report.section("FIXED SCHEMA")?;
    for column in schema.columns() {
        report.line(format_args!(
            "{}\t{}\t{:?}\tnullable={}\tprimary={}",
            column.id(),
            column.name(),
            column.logical_type(),
            column.is_nullable(),
            column.id() == PRIMARY_COLUMN_ID
        ))?;
    }
    report.line("")?;
    report.flush()?;

    let started = Instant::now();
    let options = WriterOptions::default()
        .with_row_block_target(ROW_BLOCK_TARGET)
        .with_byte_block_target(BYTE_BLOCK_TARGET)
        .with_codec(WriterCodec::Zstandard)
        .with_zstd_level(ZSTD_LEVEL)
        .with_encoding(WriterEncoding::Fixed(WriterTransform::Raw))
        .with_statistics(WriterStatistics::None);
    let mut writer = Writer::create(&args.output, (*schema).clone(), options)?;
    let mut parquet_reader = builder.with_batch_size(args.batch_rows).build()?;
    let mut rows_read = 0_u64;
    let mut batches_read = 0_u64;
    let mut next_progress = parquet_rows / 20;
    if next_progress == 0 {
        next_progress = 1;
    }

    for batch in &mut parquet_reader {
        let batch = batch?;
        rows_read = rows_read
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| invalid("converted row count overflow"))?;
        writer.append(convert_batch(&batch, &schema)?)?;
        batches_read += 1;

        if rows_read >= next_progress && rows_read < parquet_rows {
            let elapsed = started.elapsed().as_secs_f64();
            eprintln!(
                "progress: {:>6.2}%  {:>12} rows  {:>12.0} rows/s  {:>8.1}s",
                rows_read as f64 * 100.0 / parquet_rows as f64,
                rows_read,
                rows_read as f64 / elapsed,
                elapsed,
            );
            next_progress = next_progress.saturating_add((parquet_rows / 20).max(1));
        }
    }

    if rows_read != parquet_rows {
        return Err(invalid(format!(
            "Parquet reader produced {rows_read} rows but metadata declares {parquet_rows}"
        )));
    }

    let summary = writer.finish()?;
    let elapsed = started.elapsed();
    let seconds = elapsed.as_secs_f64();
    let output_bytes = summary.bytes_written();
    let accounting = summary.accounting();
    let size_ratio = output_bytes as f64 / input_bytes as f64;

    report.line("")?;
    report.section("FINAL STATS")?;
    report.metric("finished_unix_seconds", unix_seconds()?)?;
    report.metric("rows_read", rows_read)?;
    report.metric("rows_written", summary.rows_written())?;
    report.metric("batches_read", batches_read)?;
    report.metric("acta_blocks_written", summary.blocks_written())?;
    report.metric(
        "acta_last_block_sequence",
        optional_u64(summary.last_sequence()),
    )?;
    report.metric("acta_accounted_raw_bytes", accounting.total_bytes())?;
    report.metric("input_file_bytes", input_bytes)?;
    report.metric("output_file_bytes", output_bytes)?;
    report.metric(
        "output_file_mib",
        format_args!("{:.3}", output_bytes as f64 / MIB),
    )?;
    report.metric(
        "acta_to_parquet_size_ratio",
        format_args!("{size_ratio:.6}"),
    )?;
    report.metric(
        "size_change_percent",
        format_args!("{:.3}", (size_ratio - 1.0) * 100.0),
    )?;
    report.metric("elapsed_seconds", format_args!("{seconds:.6}"))?;
    report.metric(
        "throughput_rows_per_second",
        format_args!("{:.0}", rows_read as f64 / seconds),
    )?;
    report.metric(
        "throughput_input_mib_per_second",
        format_args!("{:.3}", input_bytes as f64 / MIB / seconds),
    )?;
    report.metric(
        "throughput_output_mib_per_second",
        format_args!("{:.3}", output_bytes as f64 / MIB / seconds),
    )?;
    report.metric("result", "complete")?;
    report.flush()?;

    println!(
        "complete: {rows_read} rows, {} blocks, {:.3} GiB -> {:.3} GiB in {:.3}s ({:.0} rows/s)",
        summary.blocks_written(),
        input_bytes as f64 / MIB / 1024.0,
        output_bytes as f64 / MIB / 1024.0,
        seconds,
        rows_read as f64 / seconds,
    );
    println!("stats: {}", args.stats.display());
    Ok(())
}

impl Args {
    fn parse(mut arguments: impl Iterator<Item = String>) -> Result<Self, Box<dyn StdError>> {
        let mut positionals = Vec::new();
        let mut stats = None;
        let mut batch_rows = DEFAULT_BATCH_ROWS;

        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "-h" | "--help" => {
                    println!(
                        "Usage: cargo run --release --features parquet-example \\\n+                         --example deltas_parquet_to_acta -- \\\n+                         <input.parquet> <output.acta> [--stats <report.txt>] \\\n+                         [--batch-rows <rows>]"
                    );
                    std::process::exit(0);
                }
                "--stats" => {
                    let path = arguments
                        .next()
                        .ok_or_else(|| invalid("--stats needs a path"))?;
                    stats = Some(PathBuf::from(path));
                }
                "--batch-rows" => {
                    let value = arguments
                        .next()
                        .ok_or_else(|| invalid("--batch-rows needs a value"))?;
                    batch_rows = value
                        .parse()
                        .map_err(|_| invalid("--batch-rows must be a positive integer"))?;
                }
                value if value.starts_with('-') => {
                    return Err(invalid(format!("unknown option {value}")));
                }
                value => positionals.push(PathBuf::from(value)),
            }
        }

        if positionals.len() != 2 {
            return Err(invalid(
                "expected <input.parquet> and <output.acta>; use --help for usage",
            ));
        }
        if batch_rows == 0 {
            return Err(invalid("--batch-rows must be positive"));
        }
        let input = positionals.remove(0);
        let output = positionals.remove(0);
        let stats = stats.unwrap_or_else(|| stats_path(&output));
        Ok(Self {
            input,
            output,
            stats,
            batch_rows,
        })
    }
}

impl Report {
    fn create(path: &Path) -> Result<Self, Box<dyn StdError>> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(Self {
            file: BufWriter::new(file),
        })
    }

    fn section(&mut self, name: impl std::fmt::Display) -> Result<(), Box<dyn StdError>> {
        self.line(format_args!("[{name}]"))
    }

    fn metric(
        &mut self,
        name: &str,
        value: impl std::fmt::Display,
    ) -> Result<(), Box<dyn StdError>> {
        self.line(format_args!("{name}={value}"))
    }

    fn line(&mut self, value: impl std::fmt::Display) -> Result<(), Box<dyn StdError>> {
        writeln!(self.file, "{value}")?;
        println!("{value}");
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Box<dyn StdError>> {
        self.file.flush()?;
        Ok(())
    }
}

fn acta_schema() -> Schema {
    Schema::new(
        1,
        vec![
            Column::new(
                1,
                "received_at",
                LogicalType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: TimeZone::Utc,
                },
                false,
            ),
            Column::new(
                2,
                "source_timestamp",
                LogicalType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: TimeZone::Utc,
                },
                true,
            ),
            Column::new(
                3,
                "sink_handoff_at",
                LogicalType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: TimeZone::Utc,
                },
                true,
            ),
            Column::new(4, "sequence", LogicalType::UInt64, true),
            Column::new(
                5,
                "worker_id",
                LogicalType::Categorical { ordered: false },
                true,
            ),
            Column::new(
                6,
                "venue",
                LogicalType::Categorical { ordered: false },
                false,
            ),
            Column::new(7, "market_key", LogicalType::Utf8, false),
            Column::new(8, "instrument_key", LogicalType::Utf8, false),
            Column::new(
                9,
                "outcome_label",
                LogicalType::Categorical { ordered: false },
                false,
            ),
            Column::new(
                10,
                "book_side",
                LogicalType::Categorical { ordered: false },
                false,
            ),
            Column::new(11, "price", LogicalType::Float64, false),
            Column::new(12, "size", LogicalType::Float64, false),
            Column::new(
                13,
                "update_kind",
                LogicalType::Categorical { ordered: false },
                false,
            ),
        ],
        Some(PRIMARY_COLUMN_ID),
    )
}

fn validate_source_schema(schema: &ArrowSchema) -> Result<(), Box<dyn StdError>> {
    let timestamp = DataType::Timestamp(ArrowTimeUnit::Microsecond, Some("UTC".into()));
    let expected = [
        ("received_at", timestamp.clone(), false),
        ("source_timestamp", timestamp.clone(), true),
        ("sink_handoff_at", timestamp, true),
        ("sequence", DataType::UInt64, true),
        ("worker_id", DataType::Utf8, true),
        ("venue", DataType::Utf8, false),
        ("market_key", DataType::Utf8, false),
        ("instrument_key", DataType::Utf8, false),
        ("outcome_label", DataType::Utf8, false),
        ("book_side", DataType::Utf8, false),
        ("price", DataType::Float64, false),
        ("size", DataType::Float64, false),
        ("update_kind", DataType::Utf8, false),
    ];

    if schema.fields().len() != expected.len() {
        return Err(invalid(format!(
            "source has {} columns; the fixed deltas schema requires {}",
            schema.fields().len(),
            expected.len()
        )));
    }

    for (index, (field, (name, data_type, nullable))) in
        schema.fields().iter().zip(expected).enumerate()
    {
        if field.name() != name
            || field.data_type() != &data_type
            || field.is_nullable() != nullable
        {
            return Err(invalid(format!(
                "source column {} is {} {:?} nullable={}; expected {} {:?} nullable={}",
                index + 1,
                field.name(),
                field.data_type(),
                field.is_nullable(),
                name,
                data_type,
                nullable,
            )));
        }
    }
    Ok(())
}

fn convert_batch(
    batch: &ArrowRecordBatch,
    schema: &Arc<Schema>,
) -> Result<ActaRecordBatch, Box<dyn StdError>> {
    if batch.num_columns() != schema.column_count() {
        return Err(invalid(format!(
            "Parquet batch has {} columns; expected {}",
            batch.num_columns(),
            schema.column_count()
        )));
    }

    let columns = vec![
        timestamp_column(batch.column(0).as_ref(), "received_at")?,
        timestamp_column(batch.column(1).as_ref(), "source_timestamp")?,
        timestamp_column(batch.column(2).as_ref(), "sink_handoff_at")?,
        primitive_column::<UInt64Type, _>(batch.column(3).as_ref(), "sequence", ActaArray::UInt64)?,
        string_column(
            batch.column(4).as_ref(),
            "worker_id",
            ActaArray::Categorical,
        )?,
        string_column(batch.column(5).as_ref(), "venue", ActaArray::Categorical)?,
        string_column(batch.column(6).as_ref(), "market_key", ActaArray::Utf8)?,
        string_column(batch.column(7).as_ref(), "instrument_key", ActaArray::Utf8)?,
        string_column(
            batch.column(8).as_ref(),
            "outcome_label",
            ActaArray::Categorical,
        )?,
        string_column(
            batch.column(9).as_ref(),
            "book_side",
            ActaArray::Categorical,
        )?,
        primitive_column::<Float64Type, _>(batch.column(10).as_ref(), "price", ActaArray::Float64)?,
        primitive_column::<Float64Type, _>(batch.column(11).as_ref(), "size", ActaArray::Float64)?,
        string_column(
            batch.column(12).as_ref(),
            "update_kind",
            ActaArray::Categorical,
        )?,
    ];

    Ok(ActaRecordBatch::try_new(
        Arc::clone(schema),
        columns,
        batch.num_rows(),
    )?)
}

fn timestamp_column(array: &dyn ArrowArray, name: &str) -> Result<ActaArray, Box<dyn StdError>> {
    let typed = array
        .as_any()
        .downcast_ref::<ArrowPrimitiveArray<TimestampMicrosecondType>>()
        .ok_or_else(|| invalid(format!("column {name:?} is not timestamp[us, UTC]")))?;
    Ok(ActaArray::Timestamp(TimestampArray::new(
        (0..typed.len()).map(|index| typed.value(index)).collect(),
        validity(typed),
        TimeUnit::Microsecond,
        TimeZone::Utc,
    )))
}

fn primitive_column<T, F>(
    array: &dyn ArrowArray,
    name: &str,
    build: F,
) -> Result<ActaArray, Box<dyn StdError>>
where
    T: arrow_array::ArrowPrimitiveType,
    T::Native: Copy,
    F: FnOnce(ActaPrimitiveArray<T::Native>) -> ActaArray,
{
    let typed = array
        .as_any()
        .downcast_ref::<ArrowPrimitiveArray<T>>()
        .ok_or_else(|| invalid(format!("column {name:?} does not match its fixed type")))?;
    Ok(build(ActaPrimitiveArray::new(
        (0..typed.len()).map(|index| typed.value(index)).collect(),
        validity(typed),
    )))
}

fn string_column<F>(
    array: &dyn ArrowArray,
    name: &str,
    build: F,
) -> Result<ActaArray, Box<dyn StdError>>
where
    F: FnOnce(Utf8Array) -> ActaArray,
{
    let typed = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid(format!("column {name:?} is not UTF-8")))?;
    Ok(build(Utf8Array::new(
        (0..typed.len())
            .map(|index| typed.value(index).to_owned())
            .collect(),
        validity(typed),
    )))
}

fn validity(array: &dyn ArrowArray) -> Option<Vec<bool>> {
    (array.null_count() != 0).then(|| {
        (0..array.len())
            .map(|index| array.is_valid(index))
            .collect()
    })
}

fn stats_path(output: &Path) -> PathBuf {
    let mut path = output.as_os_str().to_owned();
    path.push(".stats.txt");
    PathBuf::from(path)
}

fn refuse_existing_output(path: &Path, description: &str) -> Result<(), Box<dyn StdError>> {
    if path.exists() {
        return Err(invalid(format!(
            "{description} already exists at {}; choose a new path",
            path.display()
        )));
    }
    Ok(())
}

fn unix_seconds() -> Result<u64, Box<dyn StdError>> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "none".to_owned(), |value| value.to_string())
}

fn invalid(message: impl Into<String>) -> Box<dyn StdError> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    ))
}
