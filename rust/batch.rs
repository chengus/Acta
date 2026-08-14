//! Native logical rows grouped by one Acta data block.

use std::sync::Arc;

use crate::array::Array;
use crate::error::{Error, ErrorContext, Result};
use crate::schema::{Column, LogicalType, Schema};

/// A decoded block with one logical array per schema column.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordBatch {
    schema: Arc<Schema>,
    columns: Vec<Array>,
    row_count: usize,
}

impl RecordBatch {
    /// Build a batch from arrays that already have one position per row.
    ///
    /// The row count is given rather than taken from the first array, so a
    /// batch keeps its shape independently of how many columns it has, and
    /// every array is checked against it.
    pub fn try_new(schema: Arc<Schema>, columns: Vec<Array>, row_count: usize) -> Result<Self> {
        if columns.len() != schema.column_count() {
            return Err(invalid("the column count does not match the schema"));
        }
        for (column, array) in schema.columns().iter().zip(&columns) {
            if array.len() != row_count {
                return Err(invalid(format!(
                    "column {} has {} values, expected {row_count}",
                    column.name(),
                    array.len()
                )));
            }
            if !holds(column, array) {
                return Err(invalid(format!(
                    "column {} was decoded as {array:?}, which is not {:?}",
                    column.name(),
                    column.logical_type()
                )));
            }
            if let Some(validity) = validity(array) {
                if validity.len() != row_count {
                    return Err(invalid(format!(
                        "column {} has a validity bitmap with {} bits, expected {row_count}",
                        column.name(),
                        validity.len()
                    )));
                }
                if !column.is_nullable() {
                    return Err(invalid(format!(
                        "non-nullable column {} has a validity bitmap",
                        column.name()
                    )));
                }
            }
        }
        Ok(Self {
            schema,
            columns,
            row_count,
        })
    }

    /// The schema shared by this batch.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// The decoded arrays in schema order.
    pub fn columns(&self) -> &[Array] {
        &self.columns
    }

    /// The number of logical rows in the block.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Find a decoded array by its schema column name.
    pub fn column_by_name(&self, name: &str) -> Option<&Array> {
        self.schema
            .columns()
            .iter()
            .position(|column| column.name() == name)
            .and_then(|index| self.columns.get(index))
    }

    /// Get a decoded array by schema position.
    pub fn column(&self, index: usize) -> Option<&Array> {
        self.columns.get(index)
    }

    /// Copy a row range for the writer's bounded block assembler.
    ///
    /// The writer slices an appended batch only where it crosses a block
    /// boundary, so every row is copied at most once on its way into a block.
    pub(crate) fn slice(&self, start: usize, end: usize) -> Self {
        assert!(start <= end && end <= self.row_count);
        Self {
            schema: Arc::clone(&self.schema),
            columns: self
                .columns
                .iter()
                .map(|column| column.slice(start, end))
                .collect(),
            row_count: end - start,
        }
    }

    /// Select rows in file order for a scan filter. This remains private to
    /// the reader so callers do not receive a row-oriented mutation API.
    pub(crate) fn take(&self, indices: &[usize]) -> Self {
        assert!(indices.iter().all(|&index| index < self.row_count));
        Self {
            schema: Arc::clone(&self.schema),
            columns: self
                .columns
                .iter()
                .map(|column| column.take(indices))
                .collect(),
            row_count: indices.len(),
        }
    }
}

/// Whether an array is the representation its schema column calls for.
fn holds(column: &Column, array: &Array) -> bool {
    matches!(
        (column.logical_type(), array),
        (LogicalType::Bool, Array::Bool(_))
            | (LogicalType::Int8, Array::Int8(_))
            | (LogicalType::Int16, Array::Int16(_))
            | (LogicalType::Int32, Array::Int32(_))
            | (LogicalType::Int64, Array::Int64(_))
            | (LogicalType::UInt8, Array::UInt8(_))
            | (LogicalType::UInt16, Array::UInt16(_))
            | (LogicalType::UInt32, Array::UInt32(_))
            | (LogicalType::UInt64, Array::UInt64(_))
            | (LogicalType::Float32, Array::Float32(_))
            | (LogicalType::Float64, Array::Float64(_))
            | (LogicalType::Decimal { .. }, Array::Decimal(_))
            | (LogicalType::Timestamp { .. }, Array::Timestamp(_))
            | (LogicalType::Utf8, Array::Utf8(_))
            | (LogicalType::Categorical { .. }, Array::Categorical(_))
            | (LogicalType::Binary, Array::Binary(_))
            | (LogicalType::FixedBinary { .. }, Array::FixedBinary(_))
            | (LogicalType::Date32, Array::Date32(_))
    )
}

fn validity(array: &Array) -> Option<&[bool]> {
    match array {
        Array::Bool(array) => array.validity(),
        Array::Int8(array) => array.validity(),
        Array::Int16(array) => array.validity(),
        Array::Int32(array) => array.validity(),
        Array::Int64(array) => array.validity(),
        Array::UInt8(array) => array.validity(),
        Array::UInt16(array) => array.validity(),
        Array::UInt32(array) => array.validity(),
        Array::UInt64(array) => array.validity(),
        Array::Float32(array) => array.validity(),
        Array::Float64(array) => array.validity(),
        Array::Decimal(array) => array.validity(),
        Array::Timestamp(array) => array.validity(),
        Array::Utf8(array) | Array::Categorical(array) => array.validity(),
        Array::Binary(array) | Array::FixedBinary(array) => array.validity(),
        Array::Date32(array) => array.validity(),
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::invalid_argument(message).with_context(ErrorContext::Payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{PrimitiveArray, ScalarValue, Utf8Array};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(
            1,
            vec![
                Column::new(1, "value", LogicalType::Int64, true),
                Column::new(2, "label", LogicalType::Utf8, false),
            ],
            None,
        ))
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Array::Int64(PrimitiveArray::new(
                    vec![1, 2, 3, 4],
                    Some(vec![true, false, true, true]),
                )),
                Array::Utf8(Utf8Array::new(
                    vec!["a".into(), "b".into(), "c".into(), "d".into()],
                    None,
                )),
            ],
            4,
        )
        .expect("a well-formed batch")
    }

    #[test]
    fn a_slice_reports_the_rows_of_its_range() {
        assert_eq!(batch().slice(1, 3).row_count(), 2);
    }

    #[test]
    fn a_slice_keeps_every_column() {
        assert_eq!(batch().slice(1, 3).columns().len(), 2);
    }

    #[test]
    fn a_slice_keeps_its_rows_aligned_across_columns() {
        let sliced = batch().slice(1, 3);

        assert_eq!(
            (
                sliced.column(0).unwrap().value_at(1),
                sliced.column(1).unwrap().value_at(1)
            ),
            (Some(ScalarValue::Int64(3)), Some(ScalarValue::Utf8("c")))
        );
    }

    #[test]
    fn a_slice_keeps_the_nulls_of_its_range() {
        let sliced = batch().slice(1, 3);

        assert_eq!(sliced.column(0).unwrap().value_at(0), None);
    }

    #[test]
    fn a_slice_shares_the_schema_it_came_from() {
        assert_eq!(batch().slice(0, 1).schema(), schema().as_ref());
    }

    #[test]
    fn slicing_a_whole_batch_reproduces_it() {
        assert_eq!(batch().slice(0, 4), batch());
    }
}
