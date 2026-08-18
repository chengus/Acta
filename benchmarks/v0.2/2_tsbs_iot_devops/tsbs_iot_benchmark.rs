//! Write and read benchmark for the normalized TSBS IoT workload.
//!
//! The input Parquet file is fully decoded before a write timer starts. This
//! keeps source decoding out of the target-format write measurement.

use std::error::Error as StdError;
use std::fs::{self, File};
use std::io::{BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use acta::{
    Array as ActaArray, Column, LogicalType, PrimitiveArray as ActaPrimitiveArray, Reader,
    RecordBatch as ActaRecordBatch, Schema, TimeUnit, TimeZone, TimestampArray,
    Utf8Array as ActaUtf8Array, Writer, WriterCodec, WriterEncoding, WriterOptions,
};
use arrow_array::types::{Float64Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{
    Array as ArrowArray, PrimitiveArray as ArrowPrimitiveArray, RecordBatch as ArrowRecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Schema as ArrowSchema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

const BATCH_ROWS: usize = 65_536;
const ROW_GROUP_ROWS: usize = 65_536;
const ROW_BLOCK_TARGET: u64 = 65_536;
const ZSTD_LEVEL: i32 = 1;
const EXPECTED_ROWS: u64 = 10_000_000;
const COLUMN_NAMES: [&str; 20] = [
    "timestamp",
    "name",
    "fleet",
    "driver",
    "model",
    "device_version",
    "load_capacity",
    "fuel_capacity",
    "nominal_fuel_consumption",
    "measurement",
    "latitude",
    "longitude",
    "elevation",
    "velocity",
    "heading",
    "grade",
    "fuel_consumption",
    "fuel_state",
    "current_load",
    "status",
];

#[derive(Debug, Clone, Copy)]
enum Format {
    Acta,
    Parquet,
    Csv,
}

impl Format {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "acta" => Ok(Self::Acta),
            "parquet" => Ok(Self::Parquet),
            "csv" => Ok(Self::Csv),
            _ => Err(format!(
                "unknown format {value:?}; expected acta, parquet, or csv"
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Acta => "acta",
            Self::Parquet => "parquet",
            Self::Csv => "csv",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Write,
    Read,
}

impl Mode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "write" => Ok(Self::Write),
            "read" => Ok(Self::Read),
            _ => Err(format!("unknown mode {value:?}; expected write or read")),
        }
    }
}

#[derive(Debug)]
struct Args {
    mode: Mode,
    format: Format,
    input: PathBuf,
    output: PathBuf,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("tsbs_iot_benchmark failed: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn StdError>> {
    let args = Args::parse(std::env::args().skip(1))?;
    let result = match args.mode {
        Mode::Write => write_target(&args.input, &args.output, args.format)?,
        Mode::Read => read_target(&args.output, args.format)?,
    };
    println!("{}", serde_json(&result));
    Ok(())
}

#[derive(Debug)]
struct Metrics {
    mode: &'static str,
    format: &'static str,
    rows: u64,
    elapsed_seconds: f64,
    finish_seconds: Option<f64>,
    input_file_bytes: Option<u64>,
    output_file_bytes: u64,
    arrow_buffer_bytes: Option<u64>,
    blocks_or_row_groups: Option<u64>,
    blocks_considered: Option<u64>,
    blocks_pruned: Option<u64>,
    streams_decoded: Option<u64>,
    stream_bytes_decoded: Option<u64>,
    bytes_read: Option<u64>,
}

fn write_target(input: &Path, output: &Path, format: Format) -> Result<Metrics, Box<dyn StdError>> {
    if output.exists() {
        return Err(format!("refusing to overwrite {}", output.display()).into());
    }
    let input_bytes = fs::metadata(input)?.len();
    let (schema, batches, arrow_buffer_bytes, rows) = load_batches(input)?;
    if rows != EXPECTED_ROWS {
        return Err(format!("input has {rows} rows, expected {EXPECTED_ROWS}").into());
    }
    let started = Instant::now();
    let finish_seconds;
    let blocks_or_row_groups;
    match format {
        Format::Acta => {
            let acta_schema = Arc::new(acta_schema());
            let options = WriterOptions::default()
                .with_row_block_target(ROW_BLOCK_TARGET)
                .with_codec(WriterCodec::Zstandard)
                .with_zstd_level(ZSTD_LEVEL)
                .with_encoding(WriterEncoding::Adaptive);
            let mut writer = Writer::create(output, (*acta_schema).clone(), options)?;
            for batch in &batches {
                writer.append(convert_batch(batch, &acta_schema)?)?;
            }
            let finish_started = Instant::now();
            let summary = writer.finish()?;
            finish_seconds = Some(finish_started.elapsed().as_secs_f64());
            blocks_or_row_groups = Some(summary.blocks_written());
        }
        Format::Parquet => {
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(ZSTD_LEVEL)?))
                .set_max_row_group_size(ROW_GROUP_ROWS)
                .set_write_batch_size(BATCH_ROWS)
                .build();
            let file = File::create(output)?;
            let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))?;
            for batch in &batches {
                writer.write(batch)?;
            }
            let finish_started = Instant::now();
            let metadata = writer.close()?;
            finish_seconds = Some(finish_started.elapsed().as_secs_f64());
            blocks_or_row_groups = Some(metadata.row_groups.len() as u64);
        }
        Format::Csv => {
            let file = File::create(output)?;
            let mut writer = BufWriter::new(file);
            write_csv_header(&mut writer)?;
            for batch in &batches {
                write_csv_batch(&mut writer, batch)?;
            }
            let finish_started = Instant::now();
            writer.flush()?;
            writer.get_ref().sync_all()?;
            finish_seconds = Some(finish_started.elapsed().as_secs_f64());
            blocks_or_row_groups = None;
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let output_bytes = fs::metadata(output)?.len();
    Ok(Metrics {
        mode: "write",
        format: format.name(),
        rows,
        elapsed_seconds: elapsed,
        finish_seconds,
        input_file_bytes: Some(input_bytes),
        output_file_bytes: output_bytes,
        arrow_buffer_bytes: Some(arrow_buffer_bytes),
        blocks_or_row_groups,
        blocks_considered: None,
        blocks_pruned: None,
        streams_decoded: None,
        stream_bytes_decoded: None,
        bytes_read: None,
    })
}

fn read_target(path: &Path, format: Format) -> Result<Metrics, Box<dyn StdError>> {
    let input_bytes = fs::metadata(path)?.len();
    let started = Instant::now();
    let (rows, blocks_or_row_groups, scan_metrics) = match format {
        Format::Acta => {
            let reader = Reader::open(path)?;
            let mut scan = reader.scan();
            let mut rows = 0_u64;
            for batch in scan.by_ref() {
                rows += batch?.row_count() as u64;
            }
            let metrics = scan.metrics();
            (
                rows,
                Some(reader.blocks().len() as u64),
                Some((
                    metrics.blocks_considered(),
                    metrics.blocks_pruned(),
                    metrics.streams_decoded(),
                    metrics.stream_bytes_decoded(),
                    metrics.bytes_read(),
                )),
            )
        }
        Format::Parquet => {
            let file = File::open(path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let row_groups = builder.metadata().num_row_groups() as u64;
            let mut reader = builder.with_batch_size(BATCH_ROWS).build()?;
            let mut rows = 0_u64;
            for batch in &mut reader {
                rows += batch?.num_rows() as u64;
            }
            (rows, Some(row_groups), None)
        }
        Format::Csv => {
            let file = File::open(path)?;
            let mut reader = std::io::BufReader::new(file);
            let mut line = String::new();
            let mut lines = 0_u64;
            while reader.read_line(&mut line)? != 0 {
                lines += 1;
                line.clear();
            }
            (lines.saturating_sub(1), None, None)
        }
    };
    if rows != EXPECTED_ROWS {
        return Err(format!("{format:?} read {rows} rows, expected {EXPECTED_ROWS}").into());
    }
    let elapsed = started.elapsed().as_secs_f64();
    let (blocks_considered, blocks_pruned, streams_decoded, stream_bytes_decoded, bytes_read) =
        scan_metrics.map_or((None, None, None, None, None), |metrics| {
            (
                Some(metrics.0),
                Some(metrics.1),
                Some(metrics.2),
                Some(metrics.3),
                Some(metrics.4),
            )
        });
    Ok(Metrics {
        mode: "read",
        format: format.name(),
        rows,
        elapsed_seconds: elapsed,
        finish_seconds: None,
        input_file_bytes: Some(input_bytes),
        output_file_bytes: input_bytes,
        arrow_buffer_bytes: None,
        blocks_or_row_groups,
        blocks_considered,
        blocks_pruned,
        streams_decoded,
        stream_bytes_decoded,
        bytes_read,
    })
}

fn load_batches(
    input: &Path,
) -> Result<(Arc<ArrowSchema>, Vec<ArrowRecordBatch>, u64, u64), Box<dyn StdError>> {
    let file = File::open(input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = Arc::clone(builder.schema());
    validate_arrow_schema(&schema)?;
    let mut reader = builder.with_batch_size(BATCH_ROWS).build()?;
    let mut batches = Vec::new();
    let mut rows = 0_u64;
    let mut arrow_buffer_bytes = 0_u64;
    for batch in &mut reader {
        let batch = batch?;
        rows += batch.num_rows() as u64;
        arrow_buffer_bytes += batch.get_array_memory_size() as u64;
        batches.push(batch);
    }
    Ok((schema, batches, arrow_buffer_bytes, rows))
}

fn validate_arrow_schema(schema: &ArrowSchema) -> Result<(), Box<dyn StdError>> {
    if schema.fields().len() != COLUMN_NAMES.len() {
        return Err(format!(
            "expected {} columns, got {}",
            COLUMN_NAMES.len(),
            schema.fields().len()
        )
        .into());
    }
    for (field, expected) in schema.fields().iter().zip(COLUMN_NAMES) {
        if field.name() != expected {
            return Err(format!("expected column {expected:?}, got {:?}", field.name()).into());
        }
    }
    if schema.field(0).data_type()
        != &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
    {
        return Err("timestamp must be timestamp[us, UTC]".into());
    }
    Ok(())
}

fn acta_schema() -> Schema {
    let mut columns = Vec::with_capacity(COLUMN_NAMES.len());
    for (index, name) in COLUMN_NAMES.iter().enumerate() {
        let logical_type = match *name {
            "timestamp" => LogicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: TimeZone::Utc,
            },
            "name" | "fleet" | "driver" | "model" | "device_version" | "measurement" => {
                LogicalType::Categorical { ordered: false }
            }
            "load_capacity"
            | "fuel_capacity"
            | "nominal_fuel_consumption"
            | "latitude"
            | "longitude"
            | "fuel_consumption"
            | "fuel_state"
            | "current_load" => LogicalType::Float64,
            _ => LogicalType::Int64,
        };
        let nullable = !matches!(*name, "timestamp" | "measurement");
        columns.push(Column::new(index as u32 + 1, *name, logical_type, nullable));
    }
    Schema::new(20260818, columns, Some(1))
}

fn convert_batch(
    batch: &ArrowRecordBatch,
    schema: &Arc<Schema>,
) -> Result<ActaRecordBatch, Box<dyn StdError>> {
    let columns = vec![
        timestamp_column(batch.column(0))?,
        string_column(batch.column(1), true)?,
        string_column(batch.column(2), true)?,
        string_column(batch.column(3), true)?,
        string_column(batch.column(4), true)?,
        string_column(batch.column(5), true)?,
        primitive_column::<Float64Type>(batch.column(6), ActaArray::Float64)?,
        primitive_column::<Float64Type>(batch.column(7), ActaArray::Float64)?,
        primitive_column::<Float64Type>(batch.column(8), ActaArray::Float64)?,
        string_column(batch.column(9), true)?,
        primitive_column::<Float64Type>(batch.column(10), ActaArray::Float64)?,
        primitive_column::<Float64Type>(batch.column(11), ActaArray::Float64)?,
        primitive_column::<Int64Type>(batch.column(12), ActaArray::Int64)?,
        primitive_column::<Int64Type>(batch.column(13), ActaArray::Int64)?,
        primitive_column::<Int64Type>(batch.column(14), ActaArray::Int64)?,
        primitive_column::<Int64Type>(batch.column(15), ActaArray::Int64)?,
        primitive_column::<Float64Type>(batch.column(16), ActaArray::Float64)?,
        primitive_column::<Float64Type>(batch.column(17), ActaArray::Float64)?,
        primitive_column::<Float64Type>(batch.column(18), ActaArray::Float64)?,
        primitive_column::<Int64Type>(batch.column(19), ActaArray::Int64)?,
    ];
    Ok(ActaRecordBatch::try_new(
        Arc::clone(schema),
        columns,
        batch.num_rows(),
    )?)
}

fn timestamp_column(array: &dyn ArrowArray) -> Result<ActaArray, Box<dyn StdError>> {
    let typed = array
        .as_any()
        .downcast_ref::<ArrowPrimitiveArray<TimestampMicrosecondType>>()
        .ok_or("timestamp column has the wrong Arrow type")?;
    Ok(ActaArray::Timestamp(TimestampArray::new(
        typed.values().to_vec(),
        validity(typed),
        TimeUnit::Microsecond,
        TimeZone::Utc,
    )))
}

fn primitive_column<T>(
    array: &dyn ArrowArray,
    build: impl FnOnce(ActaPrimitiveArray<T::Native>) -> ActaArray,
) -> Result<ActaArray, Box<dyn StdError>>
where
    T: arrow_array::ArrowPrimitiveType,
    T::Native: Clone,
{
    let typed = array
        .as_any()
        .downcast_ref::<ArrowPrimitiveArray<T>>()
        .ok_or("numeric column has the wrong Arrow type")?;
    Ok(build(ActaPrimitiveArray::new(
        typed.values().to_vec(),
        validity(typed),
    )))
}

fn string_column(
    array: &dyn ArrowArray,
    categorical: bool,
) -> Result<ActaArray, Box<dyn StdError>> {
    let typed = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("string column has the wrong Arrow type")?;
    let values = (0..typed.len())
        .map(|index| typed.value(index).to_owned())
        .collect();
    let values = ActaUtf8Array::new(values, validity(typed));
    Ok(if categorical {
        ActaArray::Categorical(values)
    } else {
        ActaArray::Utf8(values)
    })
}

fn validity(array: &dyn ArrowArray) -> Option<Vec<bool>> {
    (array.null_count() != 0).then(|| {
        (0..array.len())
            .map(|index| array.is_valid(index))
            .collect()
    })
}

fn write_csv_header(writer: &mut BufWriter<File>) -> Result<(), Box<dyn StdError>> {
    writeln!(writer, "{}", COLUMN_NAMES.join(","))?;
    Ok(())
}

fn write_csv_batch(
    writer: &mut BufWriter<File>,
    batch: &ArrowRecordBatch,
) -> Result<(), Box<dyn StdError>> {
    for row in 0..batch.num_rows() {
        for (index, array) in batch.columns().iter().enumerate() {
            if index != 0 {
                writer.write_all(b",")?;
            }
            write_csv_value(writer, array.as_ref(), row)?;
        }
        writer.write_all(b"\n")?;
    }
    Ok(())
}

fn write_csv_value(
    writer: &mut BufWriter<File>,
    array: &dyn ArrowArray,
    row: usize,
) -> Result<(), Box<dyn StdError>> {
    if array.is_null(row) {
        return Ok(());
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        let value = values.value(row);
        if value
            .bytes()
            .any(|byte| matches!(byte, b',' | b'"' | b'\n' | b'\r'))
        {
            write!(writer, "\"{}\"", value.replace('"', "\"\""))?;
        } else {
            writer.write_all(value.as_bytes())?;
        }
        return Ok(());
    }
    if let Some(values) = array
        .as_any()
        .downcast_ref::<ArrowPrimitiveArray<TimestampMicrosecondType>>()
    {
        write!(writer, "{}", values.value(row))?;
    } else if let Some(values) = array
        .as_any()
        .downcast_ref::<ArrowPrimitiveArray<Float64Type>>()
    {
        write!(writer, "{}", values.value(row))?;
    } else if let Some(values) = array
        .as_any()
        .downcast_ref::<ArrowPrimitiveArray<Int64Type>>()
    {
        write!(writer, "{}", values.value(row))?;
    } else {
        return Err(format!("unsupported CSV Arrow type {:?}", array.data_type()).into());
    }
    Ok(())
}

fn serde_json(metrics: &Metrics) -> String {
    fn optional(value: Option<u64>) -> String {
        value.map_or_else(|| "null".to_owned(), |value| value.to_string())
    }
    format!(
        "{{\"mode\":\"{}\",\"format\":\"{}\",\"rows\":{},\"elapsed_seconds\":{:.9},\"finish_seconds\":{},\"input_file_bytes\":{},\"output_file_bytes\":{},\"arrow_buffer_bytes\":{},\"blocks_or_row_groups\":{},\"blocks_considered\":{},\"blocks_pruned\":{},\"streams_decoded\":{},\"stream_bytes_decoded\":{},\"bytes_read\":{}}}",
        metrics.mode,
        metrics.format,
        metrics.rows,
        metrics.elapsed_seconds,
        metrics
            .finish_seconds
            .map_or_else(|| "null".to_owned(), |value| format!("{value:.9}")),
        optional(metrics.input_file_bytes),
        metrics.output_file_bytes,
        optional(metrics.arrow_buffer_bytes),
        optional(metrics.blocks_or_row_groups),
        optional(metrics.blocks_considered),
        optional(metrics.blocks_pruned),
        optional(metrics.streams_decoded),
        optional(metrics.stream_bytes_decoded),
        optional(metrics.bytes_read),
    )
}

impl Args {
    fn parse(mut arguments: impl Iterator<Item = String>) -> Result<Self, Box<dyn StdError>> {
        let mut mode = Mode::Write;
        let mut format = Format::Acta;
        let mut input = None;
        let mut output = None;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--mode" => mode = Mode::parse(&next(&mut arguments, "--mode")?)?,
                "--format" => format = Format::parse(&next(&mut arguments, "--format")?)?,
                "--input" => input = Some(PathBuf::from(next(&mut arguments, "--input")?)),
                "--output" => output = Some(PathBuf::from(next(&mut arguments, "--output")?)),
                "--help" | "-h" => {
                    println!(
                        "tsbs_iot_benchmark --mode write|read --format acta|parquet|csv --input PATH --output PATH"
                    );
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument {other:?}").into()),
            }
        }
        Ok(Self {
            mode,
            format,
            input: input.ok_or("--input is required")?,
            output: output.ok_or("--output is required")?,
        })
    }
}

fn next(
    arguments: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<String, Box<dyn StdError>> {
    arguments
        .next()
        .ok_or_else(|| format!("{flag} needs a value").into())
}
