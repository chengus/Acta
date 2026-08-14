//! Dense wire values and the logical arrays built from them.
//!
//! The wire format stores only the non-null values of a column, in row order.
//! This module holds that dense form while a layout is expanded, then scatters
//! it into one logical position per row. The dense form is already the
//! column's own value type: transforms narrow their values as they decode, so
//! nothing here converts a representation, and a non-nullable column is handed
//! through to its array rather than copied into it.

use crate::array::{Array, BinaryArray, DecimalArray, PrimitiveArray, TimestampArray, Utf8Array};
use crate::error::{Error, ErrorContext, Result};
use crate::schema::{Column, LogicalType};

use super::budget::Budget;

/// Bytes charged for one value in the buffers decoding still needs.
pub(crate) const BOOLEAN_VALUE_BYTES: usize = size_of::<bool>();
pub(crate) const UNPACKED_VALUE_BYTES: usize = size_of::<u64>();
pub(crate) const LENGTH_VALUE_BYTES: usize = size_of::<u32>();
pub(crate) const VALUE_HANDLE_BYTES: usize = size_of::<Vec<u8>>();

/// The non-null values of one column, in row order and in the column's own
/// value type.
#[derive(Debug)]
pub(crate) enum DenseValues {
    Bool(Vec<bool>),
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Bytes(Vec<Vec<u8>>),
}

impl DenseValues {
    /// The empty dense form of a column, as an all-null block leaves it.
    pub(crate) fn empty(column: &Column) -> Self {
        match column.logical_type() {
            LogicalType::Bool => Self::Bool(Vec::new()),
            LogicalType::Int8 => Self::Int8(Vec::new()),
            LogicalType::Int16 => Self::Int16(Vec::new()),
            LogicalType::Int32 | LogicalType::Date32 => Self::Int32(Vec::new()),
            LogicalType::Int64 | LogicalType::Decimal { .. } | LogicalType::Timestamp { .. } => {
                Self::Int64(Vec::new())
            }
            LogicalType::UInt8 => Self::UInt8(Vec::new()),
            LogicalType::UInt16 => Self::UInt16(Vec::new()),
            LogicalType::UInt32 => Self::UInt32(Vec::new()),
            LogicalType::UInt64 => Self::UInt64(Vec::new()),
            LogicalType::Float32 => Self::Float32(Vec::new()),
            LogicalType::Float64 => Self::Float64(Vec::new()),
            LogicalType::Utf8
            | LogicalType::Categorical { .. }
            | LogicalType::Binary
            | LogicalType::FixedBinary { .. } => Self::Bytes(Vec::new()),
        }
    }

    /// Expand the single value of a constant column to `count` positions.
    pub(crate) fn repeated(
        self,
        count: usize,
        column: &Column,
        budget: &mut Budget,
    ) -> Result<Self> {
        budget.charge_elements(count, self.value_bytes())?;
        self.expand(&Repeat { count, column })
    }

    /// Expand a block-local dictionary through its index stream.
    pub(crate) fn expanded(
        self,
        indices: &[u64],
        column: &Column,
        budget: &mut Budget,
    ) -> Result<Self> {
        budget.charge_elements(indices.len(), self.value_bytes())?;
        self.expand(&Lookup { indices, column })
    }

    /// Expand run values through their run lengths.
    pub(crate) fn runs(
        self,
        lengths: &[usize],
        column: &Column,
        budget: &mut Budget,
    ) -> Result<Self> {
        let total = lengths
            .iter()
            .try_fold(0_usize, |total, length| total.checked_add(*length))
            .ok_or_else(|| corrupt(column, "run length sum overflow"))?;
        budget.charge_elements(total, self.value_bytes())?;
        self.expand(&Runs {
            lengths,
            total,
            column,
        })
    }

    /// Scatter the dense values into one logical position per row.
    pub(crate) fn into_array(
        self,
        column: &Column,
        validity: Option<Vec<bool>>,
        row_count: usize,
        budget: &mut Budget,
    ) -> Result<Array> {
        if !moves_into_place(column, validity.as_deref()) {
            budget.charge_elements(row_count, self.slot_bytes())?;
        }
        if validity
            .as_ref()
            .is_some_and(|bits| bits.len() != row_count)
        {
            return Err(corrupt(column, "validity bitmap has the wrong row count"));
        }
        build_array(column, self, validity, row_count)
    }

    /// An upper bound on the bytes one value costs when it is *copied* into
    /// another position, as every layout expansion copies it.
    fn value_bytes(&self) -> usize {
        match self {
            Self::Bytes(values) => values
                .iter()
                .map(Vec::len)
                .max()
                .unwrap_or_default()
                .saturating_add(VALUE_HANDLE_BYTES),
            fixed_width => fixed_width.slot_bytes(),
        }
    }

    /// The bytes one logical position costs when the value is *moved* into it.
    ///
    /// Scattering moves a byte-valued buffer rather than copying it, so a row
    /// costs one handle however long the value behind it is. Charging the
    /// longest value here instead would price a column by its largest string.
    fn slot_bytes(&self) -> usize {
        match self {
            Self::Bool(_) => BOOLEAN_VALUE_BYTES,
            Self::Int8(_) => size_of::<i8>(),
            Self::Int16(_) => size_of::<i16>(),
            Self::Int32(_) => size_of::<i32>(),
            Self::Int64(_) => size_of::<i64>(),
            Self::UInt8(_) => size_of::<u8>(),
            Self::UInt16(_) => size_of::<u16>(),
            Self::UInt32(_) => size_of::<u32>(),
            Self::UInt64(_) => size_of::<u64>(),
            Self::Float32(_) => size_of::<f32>(),
            Self::Float64(_) => size_of::<f64>(),
            Self::Bytes(_) => VALUE_HANDLE_BYTES,
        }
    }

    fn expand(self, expansion: &impl Expansion) -> Result<Self> {
        Ok(match self {
            Self::Bool(values) => Self::Bool(expansion.apply(values)?),
            Self::Int8(values) => Self::Int8(expansion.apply(values)?),
            Self::Int16(values) => Self::Int16(expansion.apply(values)?),
            Self::Int32(values) => Self::Int32(expansion.apply(values)?),
            Self::Int64(values) => Self::Int64(expansion.apply(values)?),
            Self::UInt8(values) => Self::UInt8(expansion.apply(values)?),
            Self::UInt16(values) => Self::UInt16(expansion.apply(values)?),
            Self::UInt32(values) => Self::UInt32(expansion.apply(values)?),
            Self::UInt64(values) => Self::UInt64(expansion.apply(values)?),
            Self::Float32(values) => Self::Float32(expansion.apply(values)?),
            Self::Float64(values) => Self::Float64(expansion.apply(values)?),
            Self::Bytes(values) => Self::Bytes(expansion.apply(values)?),
        })
    }
}

/// Whether an array can take the dense vector as it stands.
///
/// A column with no nulls already has one value per row, and only `utf8` and
/// `categorical` values change representation on the way into an array.
fn moves_into_place(column: &Column, validity: Option<&[bool]>) -> bool {
    validity.is_none()
        && !matches!(
            column.logical_type(),
            LogicalType::Utf8 | LogicalType::Categorical { .. }
        )
}

/// One way of expanding dense values, applied to whichever value type a column
/// stores. Rust cannot pass a generic function as a value, so each expansion is
/// a small type and [`DenseValues::expand`] matches the variants exactly once
/// for all of them.
trait Expansion {
    fn apply<T: Clone>(&self, values: Vec<T>) -> Result<Vec<T>>;
}

/// Constant layout: one stored value fills every dense position.
struct Repeat<'a> {
    count: usize,
    column: &'a Column,
}

impl Expansion for Repeat<'_> {
    fn apply<T: Clone>(&self, values: Vec<T>) -> Result<Vec<T>> {
        let [value] = &values[..] else {
            return Err(corrupt(
                self.column,
                "constant layout requires exactly one stored value",
            ));
        };
        let mut repeated = Vec::new();
        repeated
            .try_reserve_exact(self.count)
            .map_err(|_| exhausted(self.column, "constant values"))?;
        repeated.resize(self.count, value.clone());
        Ok(repeated)
    }
}

/// Dictionary layout: each index selects one block-local dictionary value.
struct Lookup<'a> {
    indices: &'a [u64],
    column: &'a Column,
}

impl Expansion for Lookup<'_> {
    fn apply<T: Clone>(&self, values: Vec<T>) -> Result<Vec<T>> {
        let mut expanded = Vec::new();
        expanded
            .try_reserve_exact(self.indices.len())
            .map_err(|_| exhausted(self.column, "dictionary values"))?;
        for index in self.indices {
            let value = usize::try_from(*index)
                .ok()
                .and_then(|index| values.get(index))
                .ok_or_else(|| corrupt(self.column, "dictionary index is out of range"))?;
            expanded.push(value.clone());
        }
        Ok(expanded)
    }
}

/// Run-length layout: each run value repeats for its run length.
struct Runs<'a> {
    lengths: &'a [usize],
    total: usize,
    column: &'a Column,
}

impl Expansion for Runs<'_> {
    fn apply<T: Clone>(&self, values: Vec<T>) -> Result<Vec<T>> {
        if values.len() != self.lengths.len() {
            return Err(corrupt(
                self.column,
                "run values and run lengths have different counts",
            ));
        }
        let mut expanded = Vec::new();
        expanded
            .try_reserve_exact(self.total)
            .map_err(|_| exhausted(self.column, "run values"))?;
        for (value, length) in values.iter().zip(self.lengths) {
            expanded.extend(std::iter::repeat_n(value.clone(), *length));
        }
        Ok(expanded)
    }
}

fn build_array(
    column: &Column,
    dense: DenseValues,
    validity: Option<Vec<bool>>,
    rows: usize,
) -> Result<Array> {
    let bits = validity.as_deref();
    match (column.logical_type(), dense) {
        (LogicalType::Bool, DenseValues::Bool(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::Bool(PrimitiveArray::new(values, validity)))
        }
        (LogicalType::Int8, DenseValues::Int8(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::Int8(PrimitiveArray::new(values, validity)))
        }
        (LogicalType::Int16, DenseValues::Int16(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::Int16(PrimitiveArray::new(values, validity)))
        }
        (LogicalType::Int32, DenseValues::Int32(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::Int32(PrimitiveArray::new(values, validity)))
        }
        (LogicalType::Int64, DenseValues::Int64(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::Int64(PrimitiveArray::new(values, validity)))
        }
        (LogicalType::UInt8, DenseValues::UInt8(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::UInt8(PrimitiveArray::new(values, validity)))
        }
        (LogicalType::UInt16, DenseValues::UInt16(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::UInt16(PrimitiveArray::new(values, validity)))
        }
        (LogicalType::UInt32, DenseValues::UInt32(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::UInt32(PrimitiveArray::new(values, validity)))
        }
        (LogicalType::UInt64, DenseValues::UInt64(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::UInt64(PrimitiveArray::new(values, validity)))
        }
        (LogicalType::Float32, DenseValues::Float32(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::Float32(PrimitiveArray::new(values, validity)))
        }
        (LogicalType::Float64, DenseValues::Float64(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::Float64(PrimitiveArray::new(values, validity)))
        }
        (LogicalType::Decimal { precision, scale }, DenseValues::Int64(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::Decimal(DecimalArray::new(
                values, validity, *precision, *scale,
            )))
        }
        (LogicalType::Timestamp { unit, timezone }, DenseValues::Int64(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::Timestamp(TimestampArray::new(
                values,
                validity,
                *unit,
                timezone.clone(),
            )))
        }
        (LogicalType::Date32, DenseValues::Int32(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::Date32(PrimitiveArray::new(values, validity)))
        }
        (LogicalType::Utf8, DenseValues::Bytes(values)) => {
            let values = into_text(values, bits, rows, column)?;
            Ok(Array::Utf8(Utf8Array::new(values, validity)))
        }
        (LogicalType::Categorical { .. }, DenseValues::Bytes(values)) => {
            let values = into_text(values, bits, rows, column)?;
            Ok(Array::Categorical(Utf8Array::new(values, validity)))
        }
        (LogicalType::Binary, DenseValues::Bytes(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::Binary(BinaryArray::new(values, validity)))
        }
        (LogicalType::FixedBinary { .. }, DenseValues::Bytes(values)) => {
            let values = scatter(values, bits, rows, column)?;
            Ok(Array::FixedBinary(BinaryArray::new(values, validity)))
        }
        (logical_type, _) => Err(Error::unsupported_frame(
            format!(
                "column {} decoded into a representation {logical_type:?} cannot hold",
                column.name()
            ),
            None,
        )
        .with_context(ErrorContext::Payload)),
    }
}

/// Place each dense value at its logical row, leaving null rows at the default.
///
/// A column with no nulls is already one value per row, so its vector is handed
/// through untouched.
fn scatter<T: Clone + Default>(
    dense: Vec<T>,
    validity: Option<&[bool]>,
    rows: usize,
    column: &Column,
) -> Result<Vec<T>> {
    let Some(bits) = validity else {
        if dense.len() != rows {
            return Err(corrupt(column, "dense values do not match the row count"));
        }
        return Ok(dense);
    };
    place(dense, bits, rows, column, Ok::<T, Error>)
}

/// Section 3 requires `utf8` and `categorical` values to be well-formed UTF-8,
/// which is settled here, on the way into the array.
fn into_text(
    dense: Vec<Vec<u8>>,
    validity: Option<&[bool]>,
    rows: usize,
    column: &Column,
) -> Result<Vec<String>> {
    match validity {
        Some(bits) => place(dense, bits, rows, column, String::from_utf8),
        None => {
            if dense.len() != rows {
                return Err(corrupt(column, "dense values do not match the row count"));
            }
            dense
                .into_iter()
                .map(|value| String::from_utf8(value).map_err(|error| not_valid(column, &error)))
                .collect()
        }
    }
}

/// Spread dense values across the rows a validity bitmap marks present.
fn place<T, U: Clone + Default, E: std::fmt::Display>(
    dense: Vec<T>,
    validity: &[bool],
    rows: usize,
    column: &Column,
    convert: impl Fn(T) -> std::result::Result<U, E>,
) -> Result<Vec<U>> {
    let present = validity.iter().filter(|bit| **bit).count();
    if dense.len() != present {
        return Err(corrupt(column, "dense values do not match validity"));
    }

    let mut values = Vec::new();
    values
        .try_reserve_exact(rows)
        .map_err(|_| exhausted(column, "logical values"))?;
    values.resize(rows, U::default());

    let mut dense = dense.into_iter();
    for (row, value) in values.iter_mut().enumerate() {
        if !validity[row] {
            continue;
        }
        let stored = dense
            .next()
            .ok_or_else(|| corrupt(column, "dense values ended before the valid rows did"))?;
        *value = convert(stored).map_err(|error| not_valid(column, &error))?;
    }
    Ok(values)
}

/// Whether a column's values arrive in a byte-values stream, either
/// variable-width alongside lengths or fixed-width without them.
pub(crate) fn is_byte_valued(column: &Column) -> bool {
    matches!(
        column.logical_type(),
        LogicalType::Utf8
            | LogicalType::Categorical { .. }
            | LogicalType::Binary
            | LogicalType::FixedBinary { .. }
    )
}

pub(crate) fn corrupt(column: &Column, message: impl std::fmt::Display) -> Error {
    Error::corruption(format!("column {}: {message}", column.name()), None)
        .with_context(ErrorContext::Payload)
}

fn not_valid(column: &Column, error: &impl std::fmt::Display) -> Error {
    corrupt(
        column,
        format!(
            "stored value is not a valid {:?}: {error}",
            column.logical_type()
        ),
    )
}

fn exhausted(column: &Column, what: &str) -> Error {
    Error::resource_limit(
        format!("unable to allocate {what} for column {}", column.name()),
        None,
    )
    .with_context(ErrorContext::Payload)
}
