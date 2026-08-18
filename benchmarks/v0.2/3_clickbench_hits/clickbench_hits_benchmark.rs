//! ClickBench hits target-format benchmark.
//!
//! The Parquet reader advances to the next fully decoded Arrow batch before
//! each target-write timer starts. Source decoding is reported separately and
//! is deliberately excluded from target write throughput.

use std::error::Error as StdError;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use acta::{
    Array as ActaArray, Column, LogicalType, PrimitiveArray as ActaPrimitiveArray, Reader,
    RecordBatch as ActaRecordBatch, Schema, Writer, WriterCodec, WriterEncoding, WriterOptions,
};
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
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
const CSV_CHUNK_ROWS: u64 = 1_000_000;

#[derive(Debug, Clone, Copy)]
enum Format {
    Acta,
    Parquet,
}

impl Format {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "acta" => Ok(Self::Acta),
            "parquet" => Ok(Self::Parquet),
            _ => Err(format!("expected acta or parquet, got {value:?}")),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Acta => "acta",
            Self::Parquet => "parquet",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Write,
    Read,
    CsvStream,
}

impl Mode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "write" => Ok(Self::Write),
            "read" => Ok(Self::Read),
            "csv-stream" => Ok(Self::CsvStream),
            _ => Err(format!(
                "expected write, read, or csv-stream, got {value:?}"
            )),
        }
    }
}

#[derive(Debug)]
struct Args {
    mode: Mode,
    format: Option<Format>,
    input: PathBuf,
    output: PathBuf,
}

#[derive(Debug)]
struct Metrics {
    mode: &'static str,
    format: &'static str,
    rows: u64,
    columns: u64,
    input_file_bytes: u64,
    output_file_bytes: u64,
    logical_bytes: u64,
    source_decode_seconds: f64,
    target_write_seconds: f64,
    finish_seconds: f64,
    read_seconds: f64,
    blocks_or_row_groups: Option<u64>,
    blocks_considered: Option<u64>,
    blocks_pruned: Option<u64>,
    streams_decoded: Option<u64>,
    stream_bytes_decoded: Option<u64>,
    bytes_read: Option<u64>,
    csv_chunks: Option<u64>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("clickbench_hits_benchmark failed: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn StdError>> {
    let args = Args::parse(std::env::args().skip(1))?;
    let metrics = match args.mode {
        Mode::Write => write_target(
            &args.input,
            &args.output,
            args.format.ok_or("--format is required for write")?,
        )?,
        Mode::Read => read_target(
            &args.output,
            args.format.ok_or("--format is required for read")?,
        )?,
        Mode::CsvStream => csv_stream(&args.input, &args.output)?,
    };
    println!("{}", metrics_json(&metrics));
    Ok(())
}

fn open_source(
    input: &Path,
) -> Result<
    (
        u64,
        Arc<ArrowSchema>,
        parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    ),
    Box<dyn StdError>,
> {
    let input_bytes = fs::metadata(input)?.len();
    let file = File::open(input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = Arc::clone(builder.schema());
    let reader = builder.with_batch_size(BATCH_ROWS).build()?;
    Ok((input_bytes, schema, reader))
}

fn write_target(input: &Path, output: &Path, format: Format) -> Result<Metrics, Box<dyn StdError>> {
    if output.exists() {
        return Err(format!("refusing to overwrite {}", output.display()).into());
    }
    let started_total = Instant::now();
    let (input_bytes, arrow_schema, mut reader) = open_source(input)?;
    let columns = arrow_schema.fields().len() as u64;
    let acta_schema = (matches!(format, Format::Acta)).then(|| acta_schema(&arrow_schema));
    let mut rows = 0_u64;
    let mut logical_bytes = 0_u64;
    let mut source_decode_seconds = 0.0;
    let mut target_write_seconds = 0.0;
    let blocks_or_row_groups;

    match format {
        Format::Acta => {
            let schema = Arc::new(
                acta_schema
                    .as_ref()
                    .ok_or("Acta schema was not built")?
                    .clone(),
            );
            let options = WriterOptions::default()
                .with_row_block_target(ROW_BLOCK_TARGET)
                .with_codec(WriterCodec::Zstandard)
                .with_zstd_level(ZSTD_LEVEL)
                .with_encoding(WriterEncoding::Adaptive);
            let mut writer = Writer::create(output, (*schema).clone(), options)?;
            loop {
                let decode_started = Instant::now();
                let Some(result) = reader.next() else { break };
                let batch = timed_source_batch(result, &mut source_decode_seconds, decode_started)?;
                rows += batch.num_rows() as u64;
                logical_bytes += batch.get_array_memory_size() as u64;
                let write_started = Instant::now();
                writer.append(convert_batch(&batch, &schema)?)?;
                target_write_seconds += write_started.elapsed().as_secs_f64();
            }
            let finish_started = Instant::now();
            let summary = writer.finish()?;
            let finish_seconds = finish_started.elapsed().as_secs_f64();
            blocks_or_row_groups = Some(summary.blocks_written());
            let output_bytes = fs::metadata(output)?.len();
            let metrics = Metrics {
                mode: "write",
                format: format.name(),
                rows,
                columns,
                input_file_bytes: input_bytes,
                output_file_bytes: output_bytes,
                logical_bytes,
                source_decode_seconds,
                target_write_seconds,
                finish_seconds,
                read_seconds: 0.0,
                blocks_or_row_groups,
                blocks_considered: None,
                blocks_pruned: None,
                streams_decoded: None,
                stream_bytes_decoded: None,
                bytes_read: None,
                csv_chunks: None,
            };
            println!(
                "write_total_seconds={:.6}",
                started_total.elapsed().as_secs_f64()
            );
            Ok(metrics)
        }
        Format::Parquet => {
            let properties = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(ZSTD_LEVEL)?))
                .set_max_row_group_size(ROW_GROUP_ROWS)
                .set_write_batch_size(BATCH_ROWS)
                .build();
            let file = File::create(output)?;
            let mut writer =
                ArrowWriter::try_new(file, Arc::clone(&arrow_schema), Some(properties))?;
            loop {
                let decode_started = Instant::now();
                let Some(result) = reader.next() else { break };
                let batch = timed_source_batch(result, &mut source_decode_seconds, decode_started)?;
                rows += batch.num_rows() as u64;
                logical_bytes += batch.get_array_memory_size() as u64;
                let write_started = Instant::now();
                writer.write(&batch)?;
                target_write_seconds += write_started.elapsed().as_secs_f64();
            }
            let finish_started = Instant::now();
            let metadata = writer.close()?;
            let finish_seconds = finish_started.elapsed().as_secs_f64();
            blocks_or_row_groups = Some(metadata.row_groups.len() as u64);
            let output_bytes = fs::metadata(output)?.len();
            let metrics = Metrics {
                mode: "write",
                format: format.name(),
                rows,
                columns,
                input_file_bytes: input_bytes,
                output_file_bytes: output_bytes,
                logical_bytes,
                source_decode_seconds,
                target_write_seconds,
                finish_seconds,
                read_seconds: 0.0,
                blocks_or_row_groups,
                blocks_considered: None,
                blocks_pruned: None,
                streams_decoded: None,
                stream_bytes_decoded: None,
                bytes_read: None,
                csv_chunks: None,
            };
            println!(
                "write_total_seconds={:.6}",
                started_total.elapsed().as_secs_f64()
            );
            Ok(metrics)
        }
    }
}

fn timed_source_batch(
    result: Result<ArrowRecordBatch, arrow_schema::ArrowError>,
    source_decode_seconds: &mut f64,
    decode_started: Instant,
) -> Result<ArrowRecordBatch, Box<dyn StdError>> {
    let batch = result?;
    *source_decode_seconds += decode_started.elapsed().as_secs_f64();
    Ok(batch)
}

fn read_target(path: &Path, format: Format) -> Result<Metrics, Box<dyn StdError>> {
    let input_bytes = fs::metadata(path)?.len();
    let started = Instant::now();
    let (rows, columns, blocks_or_row_groups, scan) = match format {
        Format::Acta => {
            let reader = Reader::open(path)?;
            let columns = reader.schema().column_count() as u64;
            let block_count = reader.blocks().len() as u64;
            let mut scan = reader.scan();
            let mut rows = 0_u64;
            for batch in scan.by_ref() {
                rows += batch?.row_count() as u64;
            }
            let metrics = scan.metrics();
            (
                rows,
                columns,
                Some(block_count),
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
            let columns = builder.schema().fields().len() as u64;
            let row_groups = builder.metadata().num_row_groups() as u64;
            let mut reader = builder.with_batch_size(BATCH_ROWS).build()?;
            let mut rows = 0_u64;
            for batch in &mut reader {
                rows += batch?.num_rows() as u64;
            }
            (rows, columns, Some(row_groups), None)
        }
    };
    let elapsed = started.elapsed().as_secs_f64();
    let (blocks_considered, blocks_pruned, streams_decoded, stream_bytes_decoded, bytes_read) =
        scan.map_or((None, None, None, None, None), |value| {
            (
                Some(value.0),
                Some(value.1),
                Some(value.2),
                Some(value.3),
                Some(value.4),
            )
        });
    Ok(Metrics {
        mode: "read",
        format: format.name(),
        rows,
        columns,
        input_file_bytes: input_bytes,
        output_file_bytes: input_bytes,
        logical_bytes: 0,
        source_decode_seconds: 0.0,
        target_write_seconds: 0.0,
        finish_seconds: 0.0,
        read_seconds: elapsed,
        blocks_or_row_groups,
        blocks_considered,
        blocks_pruned,
        streams_decoded,
        stream_bytes_decoded,
        bytes_read,
        csv_chunks: None,
    })
}

fn csv_stream(input: &Path, scratch: &Path) -> Result<Metrics, Box<dyn StdError>> {
    if scratch.exists() {
        return Err(format!("refusing to overwrite {}", scratch.display()).into());
    }
    let (input_bytes, schema, mut reader) = open_source(input)?;
    let columns = schema.fields().len() as u64;
    let mut rows = 0_u64;
    let mut logical_bytes = 0_u64;
    let mut source_decode_seconds = 0.0;
    let mut target_write_seconds = 0.0;
    let mut finish_seconds = 0.0;
    let mut read_seconds = 0.0;
    let mut output_bytes = 0_u64;
    let mut csv_chunks = 0_u64;
    let mut header_needed = true;
    let mut current_chunk_has_header = false;
    let mut chunk_rows = 0_u64;
    let mut csv_writer: Option<BufWriter<File>> = None;

    loop {
        let decode_started = Instant::now();
        let Some(result) = reader.next() else { break };
        let batch = timed_source_batch(result, &mut source_decode_seconds, decode_started)?;
        logical_bytes += batch.get_array_memory_size() as u64;
        let mut offset = 0_usize;
        while offset < batch.num_rows() {
            if csv_writer.is_none() {
                let file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(scratch)?;
                let mut writer = BufWriter::new(file);
                current_chunk_has_header = header_needed;
                if header_needed {
                    write_csv_header(&mut writer, &schema)?;
                    header_needed = false;
                }
                csv_writer = Some(writer);
            }
            let remaining = (CSV_CHUNK_ROWS - chunk_rows) as usize;
            let take = remaining.min(batch.num_rows() - offset);
            let write_started = Instant::now();
            write_csv_batch(
                csv_writer
                    .as_mut()
                    .ok_or("CSV writer was not initialized")?,
                &batch,
                offset,
                offset + take,
            )?;
            target_write_seconds += write_started.elapsed().as_secs_f64();
            rows += take as u64;
            chunk_rows += take as u64;
            offset += take;
            if chunk_rows == CSV_CHUNK_ROWS {
                let finish_started = Instant::now();
                let writer = csv_writer.take().ok_or("CSV writer disappeared")?;
                let file = writer.into_inner().map_err(|error| error.into_error())?;
                file.sync_all()?;
                finish_seconds += finish_started.elapsed().as_secs_f64();
                let bytes = fs::metadata(scratch)?.len();
                output_bytes += bytes;
                let read_started = Instant::now();
                let read_rows = read_csv_rows(scratch, current_chunk_has_header)?;
                read_seconds += read_started.elapsed().as_secs_f64();
                if read_rows != chunk_rows {
                    return Err(format!("CSV chunk row count mismatch: {read_rows}").into());
                }
                fs::remove_file(scratch)?;
                csv_chunks += 1;
                chunk_rows = 0;
            }
        }
    }
    if let Some(writer) = csv_writer.take() {
        let finish_started = Instant::now();
        let file = writer.into_inner().map_err(|error| error.into_error())?;
        file.sync_all()?;
        finish_seconds += finish_started.elapsed().as_secs_f64();
        let bytes = fs::metadata(scratch)?.len();
        output_bytes += bytes;
        let read_started = Instant::now();
        let read_rows = read_csv_rows(scratch, current_chunk_has_header)?;
        read_seconds += read_started.elapsed().as_secs_f64();
        if read_rows != chunk_rows {
            return Err(format!("CSV final chunk row count mismatch: {read_rows}").into());
        }
        fs::remove_file(scratch)?;
        csv_chunks += 1;
    }
    if rows == 0 {
        return Err("CSV stream produced no rows".into());
    }
    Ok(Metrics {
        mode: "write_read",
        format: "csv",
        rows,
        columns,
        input_file_bytes: input_bytes,
        output_file_bytes: output_bytes,
        logical_bytes,
        source_decode_seconds,
        target_write_seconds,
        finish_seconds,
        read_seconds,
        blocks_or_row_groups: None,
        blocks_considered: None,
        blocks_pruned: None,
        streams_decoded: None,
        stream_bytes_decoded: None,
        bytes_read: Some(output_bytes),
        csv_chunks: Some(csv_chunks),
    })
}

fn read_csv_rows(path: &Path, has_header: bool) -> Result<u64, Box<dyn StdError>> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut line = String::new();
    let mut lines = 0_u64;
    while reader.read_line(&mut line)? != 0 {
        lines += 1;
        line.clear();
    }
    Ok(lines.saturating_sub(u64::from(has_header)))
}

fn acta_schema(schema: &ArrowSchema) -> Schema {
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            Column::new(
                index as u32 + 1,
                field.name(),
                logical_type(field.data_type()),
                field.is_nullable(),
            )
        })
        .collect();
    Schema::new(20260818_03, columns, None)
}

fn logical_type(data_type: &DataType) -> LogicalType {
    match data_type {
        DataType::Int8 => LogicalType::Int8,
        DataType::Int16 => LogicalType::Int16,
        DataType::Int32 => LogicalType::Int32,
        DataType::Int64 => LogicalType::Int64,
        DataType::UInt8 => LogicalType::UInt8,
        DataType::UInt16 => LogicalType::UInt16,
        DataType::UInt32 => LogicalType::UInt32,
        DataType::UInt64 => LogicalType::UInt64,
        DataType::Float32 => LogicalType::Float32,
        DataType::Float64 => LogicalType::Float64,
        DataType::Utf8 => LogicalType::Utf8,
        other => panic!("unsupported ClickBench Arrow type {other:?}"),
    }
}

fn convert_batch(
    batch: &ArrowRecordBatch,
    schema: &Arc<Schema>,
) -> Result<ActaRecordBatch, Box<dyn StdError>> {
    let columns = batch
        .columns()
        .iter()
        .zip(schema.columns())
        .map(|(array, column)| convert_array(array.as_ref(), column.logical_type()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ActaRecordBatch::try_new(
        Arc::clone(schema),
        columns,
        batch.num_rows(),
    )?)
}

fn convert_array(
    array: &dyn ArrowArray,
    logical_type: &LogicalType,
) -> Result<ActaArray, Box<dyn StdError>> {
    Ok(match logical_type {
        LogicalType::Int8 => primitive_column::<Int8Type>(array, ActaArray::Int8)?,
        LogicalType::Int16 => primitive_column::<Int16Type>(array, ActaArray::Int16)?,
        LogicalType::Int32 => primitive_column::<Int32Type>(array, ActaArray::Int32)?,
        LogicalType::Int64 => primitive_column::<Int64Type>(array, ActaArray::Int64)?,
        LogicalType::UInt8 => primitive_column::<UInt8Type>(array, ActaArray::UInt8)?,
        LogicalType::UInt16 => primitive_column::<UInt16Type>(array, ActaArray::UInt16)?,
        LogicalType::UInt32 => primitive_column::<UInt32Type>(array, ActaArray::UInt32)?,
        LogicalType::UInt64 => primitive_column::<UInt64Type>(array, ActaArray::UInt64)?,
        LogicalType::Float32 => primitive_column::<Float32Type>(array, ActaArray::Float32)?,
        LogicalType::Float64 => primitive_column::<Float64Type>(array, ActaArray::Float64)?,
        LogicalType::Utf8 => string_column(array)?,
        other => return Err(format!("unsupported Acta target type {other:?}").into()),
    })
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
        .ok_or_else(|| {
            format!(
                "numeric column has the wrong Arrow type: {:?}",
                array.data_type()
            )
        })?;
    Ok(build(ActaPrimitiveArray::new(
        typed.values().to_vec(),
        validity(typed),
    )))
}

fn string_column(array: &dyn ArrowArray) -> Result<ActaArray, Box<dyn StdError>> {
    let typed = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            format!(
                "string column has the wrong Arrow type: {:?}",
                array.data_type()
            )
        })?;
    Ok(ActaArray::Utf8(acta::Utf8Array::new(
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

fn write_csv_header(
    writer: &mut BufWriter<File>,
    schema: &ArrowSchema,
) -> Result<(), Box<dyn StdError>> {
    for (index, field) in schema.fields().iter().enumerate() {
        if index != 0 {
            writer.write_all(b",")?;
        }
        write_csv_string(writer, field.name())?;
    }
    writer.write_all(b"\n")?;
    Ok(())
}

fn write_csv_batch(
    writer: &mut BufWriter<File>,
    batch: &ArrowRecordBatch,
    start: usize,
    end: usize,
) -> Result<(), Box<dyn StdError>> {
    for row in start..end {
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
        return write_csv_string(writer, values.value(row));
    }
    macro_rules! primitive {
        ($ty:ty) => {
            if let Some(values) = array.as_any().downcast_ref::<ArrowPrimitiveArray<$ty>>() {
                write!(writer, "{}", values.value(row))?;
                return Ok(());
            }
        };
    }
    primitive!(Int8Type);
    primitive!(Int16Type);
    primitive!(Int32Type);
    primitive!(Int64Type);
    primitive!(UInt8Type);
    primitive!(UInt16Type);
    primitive!(UInt32Type);
    primitive!(UInt64Type);
    primitive!(Float32Type);
    primitive!(Float64Type);
    Err(format!("unsupported CSV Arrow type {:?}", array.data_type()).into())
}

fn write_csv_string(writer: &mut BufWriter<File>, value: &str) -> Result<(), Box<dyn StdError>> {
    if value
        .bytes()
        .any(|byte| matches!(byte, b',' | b'"' | b'\n' | b'\r'))
    {
        write!(writer, "\"{}\"", value.replace('"', "\"\""))?;
    } else {
        writer.write_all(value.as_bytes())?;
    }
    Ok(())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

fn metrics_json(metrics: &Metrics) -> String {
    format!(
        "{{\"mode\":\"{}\",\"format\":\"{}\",\"rows\":{},\"columns\":{},\"input_file_bytes\":{},\"output_file_bytes\":{},\"logical_bytes\":{},\"source_decode_seconds\":{:.9},\"target_write_seconds\":{:.9},\"finish_seconds\":{:.9},\"read_seconds\":{:.9},\"blocks_or_row_groups\":{},\"blocks_considered\":{},\"blocks_pruned\":{},\"streams_decoded\":{},\"stream_bytes_decoded\":{},\"bytes_read\":{},\"csv_chunks\":{}}}",
        metrics.mode,
        metrics.format,
        metrics.rows,
        metrics.columns,
        metrics.input_file_bytes,
        metrics.output_file_bytes,
        metrics.logical_bytes,
        metrics.source_decode_seconds,
        metrics.target_write_seconds,
        metrics.finish_seconds,
        metrics.read_seconds,
        optional_u64(metrics.blocks_or_row_groups),
        optional_u64(metrics.blocks_considered),
        optional_u64(metrics.blocks_pruned),
        optional_u64(metrics.streams_decoded),
        optional_u64(metrics.stream_bytes_decoded),
        optional_u64(metrics.bytes_read),
        optional_u64(metrics.csv_chunks),
    )
}

impl Args {
    fn parse(mut arguments: impl Iterator<Item = String>) -> Result<Self, Box<dyn StdError>> {
        let mut mode = None;
        let mut format = None;
        let mut input = None;
        let mut output = None;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--mode" => mode = Some(Mode::parse(&next(&mut arguments, "--mode")?)?),
                "--format" => format = Some(Format::parse(&next(&mut arguments, "--format")?)?),
                "--input" => input = Some(PathBuf::from(next(&mut arguments, "--input")?)),
                "--output" => output = Some(PathBuf::from(next(&mut arguments, "--output")?)),
                "--help" | "-h" => {
                    println!(
                        "--mode write|read|csv-stream --format acta|parquet --input PATH --output PATH"
                    );
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument {other:?}").into()),
            }
        }
        Ok(Self {
            mode: mode.ok_or("--mode is required")?,
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
