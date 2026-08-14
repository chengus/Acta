//! Schema, option, batch, array, and scalar validation for the writer.

use std::collections::HashSet;

use crate::array::{Array, ScalarValue};
use crate::batch::RecordBatch;
use crate::error::Result;
use crate::limits::Limits;
use crate::schema::{Column, LogicalType, Schema};

use super::api::{WriterCodec, WriterOptions};
use super::buffer::BlockRows;
use super::framing::type_id_and_parameters;
use super::{internal, invalid_batch, invalid_option, invalid_schema, resource};

/// Check every buffered part of one column, and that the parts account for
/// exactly the rows the block claims.
pub(super) fn validate_column_parts(
    column: &Column,
    rows: &BlockRows<'_>,
    index: usize,
) -> Result<()> {
    let mut counted = 0_usize;
    for chunk in rows.chunks {
        let array = chunk
            .column(index)
            .ok_or_else(|| invalid_batch("the batch is missing a schema column"))?;
        validate_array(column, array, chunk.row_count())?;
        counted += chunk.row_count();
    }
    if counted != rows.row_count {
        return Err(internal(
            "the buffered parts do not add up to the block row count",
        ));
    }
    Ok(())
}

/// Check that an array is the shape and representation its column declares.
///
/// Value-level constraints belong with the values in [`encode_dense_values`],
/// which sees only the rows validity marks present.
fn validate_array(column: &Column, array: &Array, row_count: usize) -> Result<()> {
    let validity = match (column.logical_type(), array) {
        (LogicalType::Bool, Array::Bool(array)) => array.validity(),
        (LogicalType::Int8, Array::Int8(array)) => array.validity(),
        (LogicalType::Int16, Array::Int16(array)) => array.validity(),
        (LogicalType::Int32, Array::Int32(array)) => array.validity(),
        (LogicalType::Int64, Array::Int64(array)) => array.validity(),
        (LogicalType::UInt8, Array::UInt8(array)) => array.validity(),
        (LogicalType::UInt16, Array::UInt16(array)) => array.validity(),
        (LogicalType::UInt32, Array::UInt32(array)) => array.validity(),
        (LogicalType::UInt64, Array::UInt64(array)) => array.validity(),
        (LogicalType::Float32, Array::Float32(array)) => array.validity(),
        (LogicalType::Float64, Array::Float64(array)) => array.validity(),
        (LogicalType::Decimal { precision, scale }, Array::Decimal(array))
            if array.precision() == *precision && array.scale() == *scale =>
        {
            array.validity()
        }
        (LogicalType::Timestamp { unit, timezone }, Array::Timestamp(array))
            if array.unit() == *unit && array.timezone() == timezone =>
        {
            array.validity()
        }
        (LogicalType::Utf8, Array::Utf8(array)) => array.validity(),
        (LogicalType::Categorical { .. }, Array::Categorical(array)) => array.validity(),
        (LogicalType::Binary, Array::Binary(array)) => array.validity(),
        (LogicalType::FixedBinary { .. }, Array::FixedBinary(array)) => array.validity(),
        (LogicalType::Date32, Array::Date32(array)) => array.validity(),
        _ => {
            return Err(invalid_batch(format!(
                "column {} array does not match its declared logical type",
                column.name()
            )));
        }
    };
    if array.len() != row_count {
        return Err(invalid_batch(format!(
            "column {} has {} rows, expected {row_count}",
            column.name(),
            array.len()
        )));
    }
    if let Some(validity) = validity {
        if validity.len() != row_count {
            return Err(invalid_batch(format!(
                "column {} has {} validity bits, expected {row_count}",
                column.name(),
                validity.len()
            )));
        }
        if !column.is_nullable() {
            return Err(invalid_batch(format!(
                "non-nullable column {} has a validity bitmap",
                column.name()
            )));
        }
    }
    Ok(())
}
pub(super) fn validate_schema(schema: &Schema) -> Result<()> {
    if schema.schema_id() == 0 {
        return Err(invalid_schema("schema ID must be nonzero"));
    }
    if schema.columns().is_empty() {
        return Err(invalid_schema("schema must declare at least one column"));
    }
    if u64::try_from(schema.column_count()).unwrap_or(u64::MAX)
        > Limits::default().max_schema_columns()
    {
        return Err(resource("schema column count exceeds the default limit"));
    }
    let mut ids = HashSet::new();
    let mut names = HashSet::new();
    for column in schema.columns() {
        if column.id() == 0 {
            return Err(invalid_schema("schema column IDs must be nonzero"));
        }
        if !ids.insert(column.id()) {
            return Err(invalid_schema("schema column IDs must be unique"));
        }
        if !names.insert(column.name().to_owned()) {
            return Err(invalid_schema("schema column names must be unique"));
        }
        // Rejecting the type parameters now keeps a bad decimal precision,
        // fixed width, or timezone name out of a file already on disk.
        type_id_and_parameters(column.logical_type())?;
        if column.name().len() > u32::MAX as usize {
            return Err(invalid_schema("column name exceeds uint32::MAX"));
        }
        if u64::try_from(column.name().len()).unwrap_or(u64::MAX)
            > Limits::default().max_schema_field_length()
        {
            return Err(resource(
                "column name exceeds the default schema field limit",
            ));
        }
    }
    if let Some(primary_id) = schema.primary_column_id() {
        let column = schema
            .column_by_id(primary_id)
            .ok_or_else(|| invalid_schema("primary column ID is not declared"))?;
        if column.is_nullable()
            || !matches!(
                column.logical_type(),
                LogicalType::Timestamp { .. } | LogicalType::Date32
            )
        {
            return Err(invalid_schema(
                "primary column must be a non-nullable timestamp or date32 column",
            ));
        }
    }
    Ok(())
}

/// Refuse options that could not produce a readable file.
///
/// Both targets are bounded by what a reader accepts, so the assembler cannot
/// be asked to fill a block that could never be published.
pub(super) fn validate_options(options: WriterOptions) -> Result<()> {
    let limits = Limits::default();
    if options.row_block_target == 0 {
        return Err(invalid_option("row block target must be greater than zero"));
    }
    if options.row_block_target > u64::from(u32::MAX)
        || options.row_block_target > limits.max_rows_per_block()
    {
        return Err(resource("row block target exceeds the writer block limit"));
    }
    if options.byte_block_target == 0 {
        return Err(invalid_option(
            "byte block target must be greater than zero",
        ));
    }
    if options.byte_block_target > limits.max_frame_payload_length() {
        return Err(resource("byte block target exceeds the writer frame limit"));
    }
    if matches!(options.codec, WriterCodec::Zstandard) {
        if !cfg!(feature = "zstd") {
            return Err(invalid_option(
                "Zstandard writer output requires the zstd feature",
            ));
        }
        #[cfg(feature = "zstd")]
        {
            let supported = zstd::compression_level_range();
            if !supported.contains(&options.zstd_level) {
                return Err(invalid_option(format!(
                    "Zstandard compression level {} is outside the supported range {}..={}",
                    options.zstd_level,
                    supported.start(),
                    supported.end(),
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_batch_input(schema: &Schema, batch: &RecordBatch) -> Result<()> {
    for (index, column) in schema.columns().iter().enumerate() {
        let array = batch
            .column(index)
            .ok_or_else(|| invalid_batch("the batch is missing a schema column"))?;
        validate_array(column, array, batch.row_count())?;
        for row in 0..batch.row_count() {
            let Some(value) = array.value_at(row) else {
                continue;
            };
            validate_scalar(column, value)?;
        }
    }
    Ok(())
}

fn validate_scalar(column: &Column, value: ScalarValue<'_>) -> Result<()> {
    let valid = match (column.logical_type(), value) {
        (LogicalType::Bool, ScalarValue::Bool(_))
        | (LogicalType::Int8, ScalarValue::Int8(_))
        | (LogicalType::Int16, ScalarValue::Int16(_))
        | (LogicalType::Int32, ScalarValue::Int32(_))
        | (LogicalType::Int64, ScalarValue::Int64(_))
        | (LogicalType::UInt8, ScalarValue::UInt8(_))
        | (LogicalType::UInt16, ScalarValue::UInt16(_))
        | (LogicalType::UInt32, ScalarValue::UInt32(_))
        | (LogicalType::UInt64, ScalarValue::UInt64(_))
        | (LogicalType::Float32, ScalarValue::Float32(_))
        | (LogicalType::Float64, ScalarValue::Float64(_))
        | (LogicalType::Decimal { .. }, ScalarValue::Decimal { .. })
        | (LogicalType::Timestamp { .. }, ScalarValue::Timestamp { .. })
        | (LogicalType::Date32, ScalarValue::Date32(_)) => true,
        (LogicalType::Utf8, ScalarValue::Utf8(value))
        | (LogicalType::Categorical { .. }, ScalarValue::Categorical(value)) => {
            if value.len() > u32::MAX as usize {
                return Err(invalid_batch(format!(
                    "column {} value exceeds uint32::MAX bytes",
                    column.name()
                )));
            }
            true
        }
        (LogicalType::Binary, ScalarValue::Binary(value)) => {
            if value.len() > u32::MAX as usize {
                return Err(invalid_batch(format!(
                    "column {} value exceeds uint32::MAX bytes",
                    column.name()
                )));
            }
            true
        }
        (LogicalType::FixedBinary { byte_width }, ScalarValue::FixedBinary(value)) => {
            if value.len() != *byte_width as usize {
                return Err(invalid_batch(format!(
                    "column {} has a {}-byte value in a fixed_binary({byte_width}) column",
                    column.name(),
                    value.len()
                )));
            }
            true
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(invalid_batch(format!(
            "column {} contains a value with the wrong logical type",
            column.name()
        )))
    }
}
