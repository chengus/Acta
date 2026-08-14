//! Convert the curated BTS Parquet demo file into a typed Acta file.
//!
//! The schema mapping is intentionally explicit. This is a real-data adapter
//! and benchmark, not a generic Parquet-to-Acta type guesser. It reads Arrow
//! record batches from Parquet, converts one batch at a time into native Acta
//! arrays, and lets the Acta writer publish bounded blocks.

use std::error::Error as StdError;
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use acta::{
    Array as ActaArray, BooleanArray as ActaBooleanArray, Column, LogicalType,
    PrimitiveArray as ActaPrimitiveArray, RecordBatch as ActaRecordBatch, Schema,
    Utf8Array as ActaUtf8Array, Writer, WriterCodec, WriterEncoding, WriterOptions,
};
use arrow_array::types::{Date32Type, Float32Type, Int8Type, Int16Type, Int32Type};
use arrow_array::{
    Array as ArrowArray, BooleanArray, PrimitiveArray as ArrowPrimitiveArray,
    RecordBatch as ArrowRecordBatch, StringArray,
};
use arrow_schema::{DataType, Schema as ArrowSchema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const DEFAULT_BATCH_ROWS: usize = 65_536;
const PRIMARY_COLUMN: &str = "flight_date";

const CATEGORICAL_COLUMNS: &[&str] = &[
    "reporting_airline",
    "reporting_airline_iata_code",
    "origin",
    "origin_city_name",
    "origin_state",
    "destination",
    "destination_city_name",
    "destination_state",
    "departure_time_block",
    "arrival_time_block",
    "cancellation_code",
];

#[derive(Debug)]
struct Args {
    input: PathBuf,
    output: PathBuf,
    batch_rows: usize,
}

fn main() -> Result<(), Box<dyn StdError>> {
    let args = Args::parse(std::env::args().skip(1))?;
    if args.batch_rows == 0 {
        return Err(invalid("--batch-rows must be positive"));
    }

    let input_bytes = fs::metadata(&args.input)?.len();
    let input_file = File::open(&args.input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(input_file)?;
    let parquet_rows = u64::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| invalid("Parquet row count is negative"))?;
    let parquet_row_groups = builder.metadata().num_row_groups();
    let arrow_schema = Arc::clone(builder.schema());
    let acta_schema = Arc::new(build_acta_schema(&arrow_schema)?);

    println!(
        "Parquet schema mapping ({} columns):",
        acta_schema.column_count()
    );
    for column in acta_schema.columns() {
        println!(
            "  {:35} {:?}{}",
            column.name(),
            column.logical_type(),
            if acta_schema.primary_column_id() == Some(column.id()) {
                " [primary]"
            } else {
                ""
            }
        );
    }

    let started = Instant::now();
    let mut parquet_reader = builder.with_batch_size(args.batch_rows).build()?;
    let options = WriterOptions::default()
        .with_codec(WriterCodec::Zstandard)
        .with_zstd_level(6)
        .with_encoding(WriterEncoding::Adaptive)
        .with_row_block_target(DEFAULT_BATCH_ROWS as u64);
    let mut writer = Writer::create(&args.output, (*acta_schema).clone(), options)?;

    let mut rows_read = 0_u64;
    let mut batches_read = 0_u64;
    for batch in &mut parquet_reader {
        let batch = batch?;
        rows_read = rows_read
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| invalid("Parquet row count overflow"))?;
        writer.append(convert_batch(&batch, &acta_schema)?)?;
        batches_read += 1;
    }

    if rows_read != parquet_rows {
        return Err(invalid(format!(
            "Parquet reader produced {rows_read} rows, metadata declares {parquet_rows}"
        )));
    }

    let summary = writer.finish()?;
    let elapsed = started.elapsed();
    let output_bytes = summary.bytes_written();
    let input_mib = input_bytes as f64 / 1024.0 / 1024.0;
    let output_mib = output_bytes as f64 / 1024.0 / 1024.0;
    let seconds = elapsed.as_secs_f64();

    println!();
    println!("Conversion complete");
    println!("  input:        {}", args.input.display());
    println!("  output:       {}", args.output.display());
    println!("  rows:         {rows_read}");
    println!("  columns:      {}", acta_schema.column_count());
    println!("  row groups:   {parquet_row_groups}");
    println!("  batches:      {batches_read}");
    println!("  Acta blocks:  {}", summary.blocks_written());
    println!("  Parquet size: {input_bytes} bytes ({input_mib:.2} MiB)");
    println!("  Acta size:    {output_bytes} bytes ({output_mib:.2} MiB)");
    println!(
        "  size ratio:   {:.3}x Acta / Parquet",
        output_bytes as f64 / input_bytes as f64
    );
    println!("  elapsed:      {seconds:.3} s");
    println!("  throughput:   {:.2} MiB/s input", input_mib / seconds);
    println!("  throughput:   {:.0} rows/s", rows_read as f64 / seconds);
    println!("  encoding:     adaptive + zstd level 6");

    Ok(())
}

impl Args {
    fn parse(mut arguments: impl Iterator<Item = String>) -> Result<Self, Box<dyn StdError>> {
        let mut positionals = Vec::new();
        let mut batch_rows = DEFAULT_BATCH_ROWS;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "-h" | "--help" => {
                    println!(
                        "Usage: cargo run --release --example parquet_to_acta \\\n                         --features parquet-example -- <input.parquet> <output.acta> \\\n                         [--batch-rows <rows>]"
                    );
                    std::process::exit(0);
                }
                "--batch-rows" => {
                    let value = arguments
                        .next()
                        .ok_or_else(|| invalid("--batch-rows needs a value"))?;
                    batch_rows = value
                        .parse()
                        .map_err(|_| invalid("--batch-rows must be an integer"))?;
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
        Ok(Self {
            input: positionals.remove(0),
            output: positionals.remove(0),
            batch_rows,
        })
    }
}

fn build_acta_schema(arrow_schema: &ArrowSchema) -> Result<Schema, Box<dyn StdError>> {
    let mut columns = Vec::with_capacity(arrow_schema.fields().len());
    let mut primary_column_id = None;

    for (index, field) in arrow_schema.fields().iter().enumerate() {
        let name = field.name();
        let logical_type = logical_type_for(name)
            .ok_or_else(|| invalid(format!("unexpected Parquet column {name:?}")))?;
        let expected_arrow_type = arrow_type_for(&logical_type);
        if field.data_type() != &expected_arrow_type {
            return Err(invalid(format!(
                "column {name:?} has Parquet type {:?}, expected {:?} for Acta {:?}",
                field.data_type(),
                expected_arrow_type,
                logical_type
            )));
        }

        let id = u32::try_from(index + 1)
            .map_err(|_| invalid("Parquet schema has too many columns for Acta IDs"))?;
        let nullable = if name == PRIMARY_COLUMN {
            primary_column_id = Some(id);
            false
        } else {
            field.is_nullable()
        };
        columns.push(Column::new(id, name, logical_type, nullable));
    }

    if primary_column_id.is_none() {
        return Err(invalid(format!(
            "Parquet schema has no {PRIMARY_COLUMN:?} column"
        )));
    }
    Ok(Schema::new(1, columns, primary_column_id))
}

fn logical_type_for(name: &str) -> Option<LogicalType> {
    let logical_type = match name {
        "year" => LogicalType::Int16,
        "quarter"
        | "month"
        | "day_of_month"
        | "day_of_week"
        | "departure_delay_group"
        | "arrival_delay_group"
        | "distance_group" => LogicalType::Int8,
        "flight_date" => LogicalType::Date32,
        "reporting_airline_dot_id"
        | "flight_number"
        | "origin_airport_id"
        | "destination_airport_id" => LogicalType::Int32,
        "scheduled_departure_time"
        | "actual_departure_time"
        | "wheels_off_time"
        | "wheels_on_time"
        | "scheduled_arrival_time"
        | "actual_arrival_time" => LogicalType::Int16,
        "departure_delay"
        | "departure_delay_minutes"
        | "taxi_out_minutes"
        | "taxi_in_minutes"
        | "arrival_delay"
        | "arrival_delay_minutes"
        | "scheduled_elapsed_minutes"
        | "actual_elapsed_minutes"
        | "air_time_minutes"
        | "flights"
        | "distance_miles"
        | "carrier_delay_minutes"
        | "weather_delay_minutes"
        | "nas_delay_minutes"
        | "security_delay_minutes"
        | "late_aircraft_delay_minutes" => LogicalType::Float32,
        "departure_delayed_15_minutes"
        | "arrival_delayed_15_minutes"
        | "cancelled"
        | "diverted" => LogicalType::Bool,
        "reporting_airline"
        | "reporting_airline_iata_code"
        | "tail_number"
        | "origin"
        | "origin_city_name"
        | "origin_state"
        | "destination"
        | "destination_city_name"
        | "destination_state"
        | "departure_time_block"
        | "arrival_time_block"
        | "cancellation_code" => {
            if CATEGORICAL_COLUMNS.contains(&name) {
                LogicalType::Categorical { ordered: false }
            } else {
                LogicalType::Utf8
            }
        }
        _ => return None,
    };
    Some(logical_type)
}

fn arrow_type_for(logical_type: &LogicalType) -> DataType {
    match logical_type {
        LogicalType::Bool => DataType::Boolean,
        LogicalType::Int8 => DataType::Int8,
        LogicalType::Int16 => DataType::Int16,
        LogicalType::Int32 => DataType::Int32,
        LogicalType::Date32 => DataType::Date32,
        LogicalType::Float32 => DataType::Float32,
        LogicalType::Utf8 | LogicalType::Categorical { .. } => DataType::Utf8,
        other => panic!("the BTS adapter has no Arrow mapping for {other:?}"),
    }
}

fn convert_batch(
    batch: &ArrowRecordBatch,
    schema: &Arc<Schema>,
) -> Result<ActaRecordBatch, Box<dyn StdError>> {
    if batch.num_columns() != schema.column_count() {
        return Err(invalid(format!(
            "Parquet batch has {} columns, expected {}",
            batch.num_columns(),
            schema.column_count()
        )));
    }

    let mut columns = Vec::with_capacity(schema.column_count());
    for (column, array) in schema.columns().iter().zip(batch.columns()) {
        let converted = match column.logical_type() {
            LogicalType::Bool => {
                let (values, validity) = boolean_values(array.as_ref(), column.name())?;
                ActaArray::Bool(ActaBooleanArray::new(values, validity))
            }
            LogicalType::Int8 => {
                primitive_column::<Int8Type, _>(array.as_ref(), column.name(), ActaArray::Int8)?
            }
            LogicalType::Int16 => {
                primitive_column::<Int16Type, _>(array.as_ref(), column.name(), ActaArray::Int16)?
            }
            LogicalType::Int32 => {
                primitive_column::<Int32Type, _>(array.as_ref(), column.name(), ActaArray::Int32)?
            }
            LogicalType::Float32 => primitive_column::<Float32Type, _>(
                array.as_ref(),
                column.name(),
                ActaArray::Float32,
            )?,
            LogicalType::Date32 => {
                primitive_column::<Date32Type, _>(array.as_ref(), column.name(), ActaArray::Date32)?
            }
            LogicalType::Utf8 => string_column(array.as_ref(), column.name(), ActaArray::Utf8)?,
            LogicalType::Categorical { .. } => {
                string_column(array.as_ref(), column.name(), ActaArray::Categorical)?
            }
            other => {
                return Err(invalid(format!(
                    "unsupported Acta type in BTS converter: {other:?}"
                )));
            }
        };
        columns.push(converted);
    }

    Ok(ActaRecordBatch::try_new(
        Arc::clone(schema),
        columns,
        batch.num_rows(),
    )?)
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
        .ok_or_else(|| {
            invalid(format!(
                "column {name:?} did not contain its declared primitive type"
            ))
        })?;
    let values = (0..typed.len()).map(|index| typed.value(index)).collect();
    let validity = validity(typed);
    Ok(build(ActaPrimitiveArray::new(values, validity)))
}

type BooleanValues = (Vec<bool>, Option<Vec<bool>>);

fn boolean_values(array: &dyn ArrowArray, name: &str) -> Result<BooleanValues, Box<dyn StdError>> {
    let typed = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| invalid(format!("column {name:?} did not contain boolean values")))?;
    let values = (0..typed.len()).map(|index| typed.value(index)).collect();
    Ok((values, validity(typed)))
}

fn string_column<F>(
    array: &dyn ArrowArray,
    name: &str,
    build: F,
) -> Result<ActaArray, Box<dyn StdError>>
where
    F: FnOnce(ActaUtf8Array) -> ActaArray,
{
    let typed = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid(format!("column {name:?} did not contain UTF-8 values")))?;
    let values = (0..typed.len())
        .map(|index| typed.value(index).to_owned())
        .collect();
    Ok(build(ActaUtf8Array::new(values, validity(typed))))
}

fn validity(array: &dyn ArrowArray) -> Option<Vec<bool>> {
    (array.null_count() != 0).then(|| {
        (0..array.len())
            .map(|index| array.is_valid(index))
            .collect()
    })
}

fn invalid(message: impl Into<String>) -> Box<dyn StdError> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    ))
}
