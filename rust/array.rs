//! Native logical arrays returned by the Acta reader.
//!
//! The wire format stores nullable values densely and keeps validity in a
//! separate stream. These arrays restore one logical position per row and
//! keep that physical detail private to the decoder.

use std::fmt;

use crate::schema::{TimeUnit, TimeZone};

/// A typed fixed-width array with optional row validity.
#[derive(Clone, PartialEq, Eq)]
pub struct PrimitiveArray<T> {
    values: Vec<T>,
    validity: Option<Vec<bool>>,
}

impl<T> PrimitiveArray<T> {
    /// Construct a primitive array with an optional row-validity bitmap.
    pub fn new(values: Vec<T>, validity: Option<Vec<bool>>) -> Self {
        Self { values, validity }
    }

    /// The logical row count.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether this array has no rows.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// The stored values, including the value slot at null positions.
    pub fn values(&self) -> &[T] {
        &self.values
    }

    /// The optional validity bitmap. A set bit means the value is present.
    pub fn validity(&self) -> Option<&[bool]> {
        self.validity.as_deref()
    }

    /// Whether the logical position at `index` is null.
    ///
    /// A position outside the array is not null, the same answer a
    /// non-nullable array gives for any index. Bound the index with
    /// [`len`](Self::len) to tell the two apart.
    pub fn is_null(&self, index: usize) -> bool {
        is_null_at(self.validity.as_deref(), index)
    }
}

/// The one place a validity bitmap is consulted, so every array answers an
/// out-of-range index the same way.
fn is_null_at(validity: Option<&[bool]>, index: usize) -> bool {
    validity.is_some_and(|validity| validity.get(index) == Some(&false))
}

impl<T: fmt::Debug> fmt::Debug for PrimitiveArray<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrimitiveArray")
            .field("values", &self.values)
            .field("validity", &self.validity)
            .finish()
    }
}

/// A nullable boolean array.
pub type BooleanArray = PrimitiveArray<bool>;

/// A UTF-8 array with one string slot per logical row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Utf8Array {
    values: Vec<String>,
    validity: Option<Vec<bool>>,
}

impl Utf8Array {
    /// Construct a UTF-8 array with an optional row-validity bitmap.
    pub fn new(values: Vec<String>, validity: Option<Vec<bool>>) -> Self {
        Self { values, validity }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[String] {
        &self.values
    }

    pub fn validity(&self) -> Option<&[bool]> {
        self.validity.as_deref()
    }

    /// Whether the logical position at `index` is null.
    pub fn is_null(&self, index: usize) -> bool {
        is_null_at(self.validity.as_deref(), index)
    }
}

/// A binary array. `FixedBinary` uses the same representation and carries its
/// width in the corresponding schema column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryArray {
    values: Vec<Vec<u8>>,
    validity: Option<Vec<bool>>,
}

impl BinaryArray {
    /// Construct a variable- or fixed-width binary array with an optional
    /// row-validity bitmap.
    pub fn new(values: Vec<Vec<u8>>, validity: Option<Vec<bool>>) -> Self {
        Self { values, validity }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[Vec<u8>] {
        &self.values
    }

    pub fn validity(&self) -> Option<&[bool]> {
        self.validity.as_deref()
    }

    /// Whether the logical position at `index` is null.
    pub fn is_null(&self, index: usize) -> bool {
        is_null_at(self.validity.as_deref(), index)
    }
}

/// A logical column array. Each variant has exactly one value position per
/// row; nullability is carried by the typed array's validity bitmap.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Array {
    Bool(BooleanArray),
    Int8(PrimitiveArray<i8>),
    Int16(PrimitiveArray<i16>),
    Int32(PrimitiveArray<i32>),
    Int64(PrimitiveArray<i64>),
    UInt8(PrimitiveArray<u8>),
    UInt16(PrimitiveArray<u16>),
    UInt32(PrimitiveArray<u32>),
    UInt64(PrimitiveArray<u64>),
    Float32(PrimitiveArray<f32>),
    Float64(PrimitiveArray<f64>),
    Decimal(DecimalArray),
    Timestamp(TimestampArray),
    Utf8(Utf8Array),
    Categorical(Utf8Array),
    Binary(BinaryArray),
    FixedBinary(BinaryArray),
    Date32(PrimitiveArray<i32>),
}

/// A decimal64 array whose values are signed unscaled integers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalArray {
    values: PrimitiveArray<i64>,
    precision: u16,
    scale: i16,
}

impl DecimalArray {
    /// Construct a decimal array of signed unscaled values.
    pub fn new(values: Vec<i64>, validity: Option<Vec<bool>>, precision: u16, scale: i16) -> Self {
        Self {
            values: PrimitiveArray::new(values, validity),
            precision,
            scale,
        }
    }

    pub fn values(&self) -> &[i64] {
        self.values.values()
    }

    pub fn validity(&self) -> Option<&[bool]> {
        self.values.validity()
    }

    pub fn precision(&self) -> u16 {
        self.precision
    }

    pub fn scale(&self) -> i16 {
        self.scale
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn is_null(&self, index: usize) -> bool {
        self.values.is_null(index)
    }
}

/// A timestamp64 array retaining its schema unit and timezone annotation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampArray {
    values: PrimitiveArray<i64>,
    unit: TimeUnit,
    timezone: TimeZone,
}

impl TimestampArray {
    /// Construct a timestamp array retaining its schema metadata.
    pub fn new(
        values: Vec<i64>,
        validity: Option<Vec<bool>>,
        unit: TimeUnit,
        timezone: TimeZone,
    ) -> Self {
        Self {
            values: PrimitiveArray::new(values, validity),
            unit,
            timezone,
        }
    }

    pub fn values(&self) -> &[i64] {
        self.values.values()
    }

    pub fn validity(&self) -> Option<&[bool]> {
        self.values.validity()
    }

    pub fn unit(&self) -> TimeUnit {
        self.unit
    }

    pub fn timezone(&self) -> &TimeZone {
        &self.timezone
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn is_null(&self, index: usize) -> bool {
        self.values.is_null(index)
    }
}

impl Array {
    /// The logical row count.
    pub fn len(&self) -> usize {
        match self {
            Self::Bool(array) => array.len(),
            Self::Int8(array) => array.len(),
            Self::Int16(array) => array.len(),
            Self::Int32(array) => array.len(),
            Self::Int64(array) => array.len(),
            Self::UInt8(array) => array.len(),
            Self::UInt16(array) => array.len(),
            Self::UInt32(array) => array.len(),
            Self::UInt64(array) => array.len(),
            Self::Float32(array) => array.len(),
            Self::Float64(array) => array.len(),
            Self::Decimal(array) => array.len(),
            Self::Timestamp(array) => array.len(),
            Self::Utf8(array) | Self::Categorical(array) => array.len(),
            Self::Binary(array) | Self::FixedBinary(array) => array.len(),
            Self::Date32(array) => array.len(),
        }
    }

    /// Whether this array has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the logical position at `index` is null. A position outside the
    /// array is not null; bound the index with [`len`](Self::len).
    pub fn is_null(&self, index: usize) -> bool {
        match self {
            Self::Bool(array) => array.is_null(index),
            Self::Int8(array) => array.is_null(index),
            Self::Int16(array) => array.is_null(index),
            Self::Int32(array) => array.is_null(index),
            Self::Int64(array) => array.is_null(index),
            Self::UInt8(array) => array.is_null(index),
            Self::UInt16(array) => array.is_null(index),
            Self::UInt32(array) => array.is_null(index),
            Self::UInt64(array) => array.is_null(index),
            Self::Float32(array) => array.is_null(index),
            Self::Float64(array) => array.is_null(index),
            Self::Decimal(array) => array.is_null(index),
            Self::Timestamp(array) => array.is_null(index),
            Self::Utf8(array) | Self::Categorical(array) => array.is_null(index),
            Self::Binary(array) | Self::FixedBinary(array) => array.is_null(index),
            Self::Date32(array) => array.is_null(index),
        }
    }

    /// Borrow the logical value at a row, or `None` for a null position.
    pub fn value_at(&self, index: usize) -> Option<ScalarValue<'_>> {
        if index >= self.len() {
            return None;
        }
        if self.is_null(index) {
            return None;
        }
        Some(match self {
            Self::Bool(array) => ScalarValue::Bool(array.values()[index]),
            Self::Int8(array) => ScalarValue::Int8(array.values()[index]),
            Self::Int16(array) => ScalarValue::Int16(array.values()[index]),
            Self::Int32(array) => ScalarValue::Int32(array.values()[index]),
            Self::Int64(array) => ScalarValue::Int64(array.values()[index]),
            Self::UInt8(array) => ScalarValue::UInt8(array.values()[index]),
            Self::UInt16(array) => ScalarValue::UInt16(array.values()[index]),
            Self::UInt32(array) => ScalarValue::UInt32(array.values()[index]),
            Self::UInt64(array) => ScalarValue::UInt64(array.values()[index]),
            Self::Float32(array) => ScalarValue::Float32(array.values()[index]),
            Self::Float64(array) => ScalarValue::Float64(array.values()[index]),
            Self::Decimal(array) => ScalarValue::Decimal {
                unscaled: array.values()[index],
                precision: array.precision(),
                scale: array.scale(),
            },
            Self::Timestamp(array) => ScalarValue::Timestamp {
                value: array.values()[index],
                unit: array.unit(),
                timezone: array.timezone(),
            },
            Self::Utf8(array) => ScalarValue::Utf8(&array.values()[index]),
            Self::Categorical(array) => ScalarValue::Categorical(&array.values()[index]),
            Self::Binary(array) => ScalarValue::Binary(&array.values()[index]),
            Self::FixedBinary(array) => ScalarValue::FixedBinary(&array.values()[index]),
            Self::Date32(array) => ScalarValue::Date32(array.values()[index]),
        })
    }

    /// Copy a row range while preserving the array's logical type and
    /// nullable representation. This is crate-private because it is used to
    /// split an ingestion batch at writer block boundaries.
    pub(crate) fn slice(&self, start: usize, end: usize) -> Self {
        assert!(start <= end && end <= self.len());
        match self {
            Self::Bool(array) => Self::Bool(PrimitiveArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::Int8(array) => Self::Int8(PrimitiveArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::Int16(array) => Self::Int16(PrimitiveArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::Int32(array) => Self::Int32(PrimitiveArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::Int64(array) => Self::Int64(PrimitiveArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::UInt8(array) => Self::UInt8(PrimitiveArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::UInt16(array) => Self::UInt16(PrimitiveArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::UInt32(array) => Self::UInt32(PrimitiveArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::UInt64(array) => Self::UInt64(PrimitiveArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::Float32(array) => Self::Float32(PrimitiveArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::Float64(array) => Self::Float64(PrimitiveArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::Decimal(array) => Self::Decimal(DecimalArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
                array.precision(),
                array.scale(),
            )),
            Self::Timestamp(array) => Self::Timestamp(TimestampArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
                array.unit(),
                array.timezone().clone(),
            )),
            Self::Utf8(array) => Self::Utf8(Utf8Array::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::Categorical(array) => Self::Categorical(Utf8Array::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::Binary(array) => Self::Binary(BinaryArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::FixedBinary(array) => Self::FixedBinary(BinaryArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
            Self::Date32(array) => Self::Date32(PrimitiveArray::new(
                array.values()[start..end].to_vec(),
                slice_validity(array.validity(), start, end),
            )),
        }
    }

    /// Select logical rows in the supplied order. This is intentionally
    /// crate-private: scans need a bounded native row-filtering primitive, but
    /// the public API remains block- and batch-oriented.
    pub(crate) fn take(&self, indices: &[usize]) -> Self {
        assert!(indices.iter().all(|&index| index < self.len()));
        match self {
            Self::Bool(array) => Self::Bool(PrimitiveArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::Int8(array) => Self::Int8(PrimitiveArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::Int16(array) => Self::Int16(PrimitiveArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::Int32(array) => Self::Int32(PrimitiveArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::Int64(array) => Self::Int64(PrimitiveArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::UInt8(array) => Self::UInt8(PrimitiveArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::UInt16(array) => Self::UInt16(PrimitiveArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::UInt32(array) => Self::UInt32(PrimitiveArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::UInt64(array) => Self::UInt64(PrimitiveArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::Float32(array) => Self::Float32(PrimitiveArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::Float64(array) => Self::Float64(PrimitiveArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::Decimal(array) => Self::Decimal(DecimalArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
                array.precision(),
                array.scale(),
            )),
            Self::Timestamp(array) => Self::Timestamp(TimestampArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
                array.unit(),
                array.timezone().clone(),
            )),
            Self::Utf8(array) => Self::Utf8(Utf8Array::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::Categorical(array) => Self::Categorical(Utf8Array::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::Binary(array) => Self::Binary(BinaryArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::FixedBinary(array) => Self::FixedBinary(BinaryArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
            Self::Date32(array) => Self::Date32(PrimitiveArray::new(
                take_values(array.values(), indices),
                take_validity(array.validity(), indices),
            )),
        }
    }
}

fn slice_validity(validity: Option<&[bool]>, start: usize, end: usize) -> Option<Vec<bool>> {
    validity.map(|validity| validity[start..end].to_vec())
}

fn take_values<T: Clone>(values: &[T], indices: &[usize]) -> Vec<T> {
    indices.iter().map(|&index| values[index].clone()).collect()
}

fn take_validity(validity: Option<&[bool]>, indices: &[usize]) -> Option<Vec<bool>> {
    validity.map(|validity| indices.iter().map(|&index| validity[index]).collect())
}

/// A borrowed logical scalar used by display and small native clients.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum ScalarValue<'a> {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Decimal {
        unscaled: i64,
        precision: u16,
        scale: i16,
    },
    Timestamp {
        value: i64,
        unit: TimeUnit,
        timezone: &'a TimeZone,
    },
    Utf8(&'a str),
    Categorical(&'a str),
    Binary(&'a [u8]),
    FixedBinary(&'a [u8]),
    Date32(i32),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mixed_int32() -> Array {
        Array::Int32(PrimitiveArray::new(
            vec![10, 20, 30, 40],
            Some(vec![true, false, true, false]),
        ))
    }

    #[test]
    fn a_slice_keeps_the_values_of_its_row_range() {
        let sliced = mixed_int32().slice(1, 3);

        assert_eq!(sliced.value_at(1), Some(ScalarValue::Int32(30)));
    }

    #[test]
    fn a_slice_keeps_the_nulls_of_its_row_range() {
        let sliced = mixed_int32().slice(1, 3);

        assert_eq!((sliced.is_null(0), sliced.is_null(1)), (true, false));
    }

    #[test]
    fn a_slice_of_a_bitmap_free_array_stays_bitmap_free() {
        let array = Array::Int64(PrimitiveArray::new(vec![1, 2, 3], None));

        let Array::Int64(sliced) = array.slice(0, 2) else {
            panic!("slicing preserves the logical type");
        };
        assert_eq!(sliced.validity(), None);
    }

    #[test]
    fn an_empty_slice_has_no_rows() {
        assert_eq!(mixed_int32().slice(2, 2).len(), 0);
    }

    #[test]
    fn a_variable_width_slice_keeps_its_own_values() {
        let array = Array::Utf8(Utf8Array::new(
            vec!["a".into(), "bb".into(), "ccc".into()],
            None,
        ));

        assert_eq!(array.slice(1, 3).value_at(0), Some(ScalarValue::Utf8("bb")));
    }

    #[test]
    fn a_decimal_slice_keeps_its_precision_and_scale() {
        let array = Array::Decimal(DecimalArray::new(vec![1, 2, 3], None, 10, 2));

        assert_eq!(
            array.slice(0, 1).value_at(0),
            Some(ScalarValue::Decimal {
                unscaled: 1,
                precision: 10,
                scale: 2
            })
        );
    }

    #[test]
    fn a_timestamp_slice_keeps_its_unit_and_timezone() {
        let array = Array::Timestamp(TimestampArray::new(
            vec![5, 6],
            None,
            TimeUnit::Millisecond,
            TimeZone::Utc,
        ));

        assert_eq!(
            array.slice(1, 2).value_at(0),
            Some(ScalarValue::Timestamp {
                value: 6,
                unit: TimeUnit::Millisecond,
                timezone: &TimeZone::Utc
            })
        );
    }

    #[test]
    fn every_logical_type_survives_a_slice() {
        let arrays = [
            Array::Bool(BooleanArray::new(vec![true, false], None)),
            Array::Int8(PrimitiveArray::new(vec![1, 2], None)),
            Array::Int16(PrimitiveArray::new(vec![1, 2], None)),
            Array::Int32(PrimitiveArray::new(vec![1, 2], None)),
            Array::Int64(PrimitiveArray::new(vec![1, 2], None)),
            Array::UInt8(PrimitiveArray::new(vec![1, 2], None)),
            Array::UInt16(PrimitiveArray::new(vec![1, 2], None)),
            Array::UInt32(PrimitiveArray::new(vec![1, 2], None)),
            Array::UInt64(PrimitiveArray::new(vec![1, 2], None)),
            Array::Float32(PrimitiveArray::new(vec![1.0, 2.0], None)),
            Array::Float64(PrimitiveArray::new(vec![1.0, 2.0], None)),
            Array::Decimal(DecimalArray::new(vec![1, 2], None, 5, 1)),
            Array::Timestamp(TimestampArray::new(
                vec![1, 2],
                None,
                TimeUnit::Second,
                TimeZone::Naive,
            )),
            Array::Utf8(Utf8Array::new(vec!["a".into(), "b".into()], None)),
            Array::Categorical(Utf8Array::new(vec!["a".into(), "b".into()], None)),
            Array::Binary(BinaryArray::new(vec![vec![1], vec![2]], None)),
            Array::FixedBinary(BinaryArray::new(vec![vec![1], vec![2]], None)),
            Array::Date32(PrimitiveArray::new(vec![1, 2], None)),
        ];

        for array in arrays {
            let sliced = array.slice(1, 2);
            assert_eq!(sliced.len(), 1, "{array:?} lost its row");
            assert_eq!(sliced.value_at(0), array.value_at(1), "{array:?}");
        }
    }
}
