//! Existing plain-layout value and validity stream encoding.

use crate::array::ScalarValue;
use crate::error::Result;
use crate::format::constants::{
    COLUMN_LAYOUT_PLAIN, STREAM_KIND_LENGTHS, STREAM_KIND_VALIDITY, STREAM_KIND_VALUES,
    TRANSFORM_RAW,
};
use crate::schema::{Column, LogicalType};

use super::super::api::WriterCodec;
use super::super::buffer::{BlockRows, is_variable_width};
use super::super::invalid_batch;
use super::candidates::{candidate_stream, pack_booleans};
use super::{SelectedColumn, materialize_selected};

pub(super) fn raw_column_candidate(
    column: &Column,
    rows: &BlockRows<'_>,
    index: usize,
    valid_bits: &[bool],
    codec: WriterCodec,
    zstd_level: i32,
) -> Result<SelectedColumn> {
    let implicit = valid_bits.iter().all(|valid| *valid) || valid_bits.iter().all(|valid| !*valid);
    let mut streams = Vec::new();
    if !implicit {
        streams.push(candidate_stream(
            STREAM_KIND_VALIDITY,
            TRANSFORM_RAW,
            valid_bits.len(),
            pack_booleans(valid_bits),
        )?);
    }
    if valid_bits.iter().any(|valid| *valid) {
        let (values, lengths) = encode_dense_values(column, rows, index, valid_bits)?;
        streams.push(candidate_stream(
            STREAM_KIND_VALUES,
            TRANSFORM_RAW,
            valid_bits.iter().filter(|valid| **valid).count(),
            values,
        )?);
        if let Some(lengths) = lengths {
            streams.push(candidate_stream(
                STREAM_KIND_LENGTHS,
                TRANSFORM_RAW,
                valid_bits.iter().filter(|valid| **valid).count(),
                lengths,
            )?);
        }
    }
    materialize_selected(COLUMN_LAYOUT_PLAIN, streams, codec, zstd_level)
}

pub(super) fn encode_dense_values(
    column: &Column,
    rows: &BlockRows<'_>,
    index: usize,
    valid_bits: &[bool],
) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
    let mut values = Vec::new();
    let mut bools = Vec::new();
    let mut variable_values = Vec::new();
    let mut lengths = Vec::new();
    let variable = is_variable_width(column.logical_type());

    for ((array, row), valid) in rows.column(index).zip(valid_bits) {
        if !*valid {
            continue;
        }
        let scalar = array.value_at(row).ok_or_else(|| {
            invalid_batch(format!(
                "column {} has a missing valid value",
                column.name()
            ))
        })?;
        match (column.logical_type(), scalar) {
            (LogicalType::Bool, ScalarValue::Bool(value)) => bools.push(value),
            (LogicalType::Int8, ScalarValue::Int8(value)) => values.push(value as u8),
            (LogicalType::Int16, ScalarValue::Int16(value)) => {
                values.extend_from_slice(&value.to_le_bytes())
            }
            (LogicalType::Int32, ScalarValue::Int32(value)) => {
                values.extend_from_slice(&value.to_le_bytes())
            }
            (LogicalType::Int64, ScalarValue::Int64(value)) => {
                values.extend_from_slice(&value.to_le_bytes())
            }
            (LogicalType::UInt8, ScalarValue::UInt8(value)) => values.push(value),
            (LogicalType::UInt16, ScalarValue::UInt16(value)) => {
                values.extend_from_slice(&value.to_le_bytes())
            }
            (LogicalType::UInt32, ScalarValue::UInt32(value)) => {
                values.extend_from_slice(&value.to_le_bytes())
            }
            (LogicalType::UInt64, ScalarValue::UInt64(value)) => {
                values.extend_from_slice(&value.to_le_bytes())
            }
            (LogicalType::Float32, ScalarValue::Float32(value)) => {
                values.extend_from_slice(&value.to_bits().to_le_bytes())
            }
            (LogicalType::Float64, ScalarValue::Float64(value)) => {
                values.extend_from_slice(&value.to_bits().to_le_bytes())
            }
            (LogicalType::Decimal { .. }, ScalarValue::Decimal { unscaled, .. }) => {
                values.extend_from_slice(&unscaled.to_le_bytes())
            }
            (LogicalType::Timestamp { .. }, ScalarValue::Timestamp { value, .. }) => {
                values.extend_from_slice(&value.to_le_bytes())
            }
            (LogicalType::Date32, ScalarValue::Date32(value)) => {
                values.extend_from_slice(&value.to_le_bytes())
            }
            // Only the rows validity marks present are stored, so the width is
            // checked here rather than over the slots behind null positions,
            // which a decoded array leaves empty.
            (LogicalType::FixedBinary { byte_width }, ScalarValue::FixedBinary(value)) => {
                if value.len() != *byte_width as usize {
                    return Err(invalid_batch(format!(
                        "column {} has a {}-byte value in a fixed_binary({byte_width}) column",
                        column.name(),
                        value.len()
                    )));
                }
                values.extend_from_slice(value)
            }
            (LogicalType::Utf8, ScalarValue::Utf8(value))
            | (LogicalType::Categorical { .. }, ScalarValue::Categorical(value)) => {
                let length = u32::try_from(value.len()).map_err(|_| {
                    invalid_batch(format!(
                        "column {} value exceeds uint32::MAX bytes",
                        column.name()
                    ))
                })?;
                lengths.extend_from_slice(&length.to_le_bytes());
                variable_values.extend_from_slice(value.as_bytes());
            }
            (LogicalType::Binary, ScalarValue::Binary(value)) => {
                let length = u32::try_from(value.len()).map_err(|_| {
                    invalid_batch(format!(
                        "column {} value exceeds uint32::MAX bytes",
                        column.name()
                    ))
                })?;
                lengths.extend_from_slice(&length.to_le_bytes());
                variable_values.extend_from_slice(value);
            }
            _ => {
                return Err(invalid_batch(format!(
                    "column {} contains a value with the wrong logical type",
                    column.name()
                )));
            }
        }
    }

    if variable {
        Ok((variable_values, Some(lengths)))
    } else if matches!(column.logical_type(), LogicalType::Bool) {
        Ok((pack_booleans(&bools), None))
    } else {
        Ok((values, None))
    }
}
