//! Canonical v0.2 min/max statistics parsing and verification.
//!
//! Section 11 makes min/max statistics optional and, when present, exact: they
//! are the extrema of the column's non-null, non-NaN values in canonical form.
//! A reader that decodes a column therefore learns enough to falsify them, and
//! this module is where that check lives.
//!
//! Verification is per column, not per block. It runs immediately after one
//! column's logical values are reconstructed, so it never requires a column the
//! caller did not ask for and a projected read can verify exactly what it
//! decoded.

use std::cmp::Ordering;
use std::fs::File;

use crate::array::{Array, ScalarValue};
use crate::error::{Error, ErrorContext, Result};
use crate::format::constants::{COLUMN_HAS_STATS_FLAG, STATS_MIN_MAX, STATS_NONE};
use crate::format::data_frame::ColumnBlockDescriptor;
use crate::format::frame::FrameMetadata;
use crate::format::read_exact_at;
use crate::limits::Limits;
use crate::schema::{Column, LogicalType};

/// What one column descriptor claims about its optional statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatisticsClaim {
    /// The column stores no min/max statistic, and nothing needs reading.
    Absent,
    /// The column stores a canonical minimum followed by a canonical maximum,
    /// together occupying exactly `length` bytes.
    MinMax { length: u64 },
}

/// Verify one column's optional statistics against its decoded values.
///
/// `array` must be the fully reconstructed logical column that `descriptor`
/// describes. A column whose descriptor claims no statistics costs one flag
/// test and no I/O.
pub(crate) fn verify_column_statistics(
    file: &mut File,
    file_size: u64,
    frame: &FrameMetadata,
    descriptor: &ColumnBlockDescriptor,
    column: &Column,
    array: &Array,
    limits: Limits,
) -> Result<()> {
    let expected_length = match classify(descriptor, column, frame.header_offset)? {
        StatisticsClaim::Absent => return Ok(()),
        StatisticsClaim::MinMax { length } => length,
    };

    let actual_length = u64::from(descriptor.stats_length);
    if actual_length != expected_length {
        return Err(statistics_error(
            column,
            format!("min/max statistics require {expected_length} bytes, found {actual_length}"),
            Some(frame.header_offset),
        ));
    }

    let bytes = read_statistics(file, file_size, frame, descriptor, column, limits)?;
    let stored = parse_stored_statistics(&bytes, column)?;
    verify_stored_order(column, stored.0, stored.1)?;
    let Some(actual) = reduce_decoded_values(array, column)? else {
        return Err(statistics_error(
            column,
            "a present min/max statistic has no non-null, non-NaN value",
            Some(frame.header_offset),
        ));
    };
    verify_matches(column, stored, actual)
}

/// Decide what a descriptor claims, rejecting a descriptor that contradicts
/// itself.
///
/// # Defense in depth
///
/// Every branch below except `STATS_MIN_MAX` is currently unreachable through
/// the public API: the only path to [`verify_column_statistics`] runs through
/// [`crate::format::data_frame::read_layout`], whose descriptor validation
/// already rejects an unsupported statistics kind, a flag that disagrees with
/// the kind, and a nonzero offset or length on a column that claims nothing.
/// The checks are kept because this crate parses untrusted files and because
/// the two modules are free to be reordered or reused independently; a claim
/// this module acts on should be one this module has established. They are
/// covered by the unit tests below rather than through file fixtures, which
/// cannot reach them.
fn classify(
    descriptor: &ColumnBlockDescriptor,
    column: &Column,
    header_offset: u64,
) -> Result<StatisticsClaim> {
    let has_stats = descriptor.flags & COLUMN_HAS_STATS_FLAG != 0;
    match descriptor.stats_kind {
        STATS_NONE => {
            if has_stats || descriptor.stats_offset != 0 || descriptor.stats_length != 0 {
                return Err(statistics_error(
                    column,
                    "statistics flag, kind, offset, and length disagree",
                    Some(header_offset),
                ));
            }
            Ok(StatisticsClaim::Absent)
        }
        STATS_MIN_MAX => {
            if !has_stats {
                return Err(statistics_error(
                    column,
                    "min/max statistics are present without the statistics flag",
                    Some(header_offset),
                ));
            }
            let length = canonical_width(column)?.checked_mul(2).ok_or_else(|| {
                statistics_error(column, "statistics length overflows", Some(header_offset))
            })?;
            Ok(StatisticsClaim::MinMax { length })
        }
        kind => Err(Error::unsupported_frame(
            format!(
                "column {}: unsupported statistics kind {kind}",
                column.name()
            ),
            Some(header_offset),
        )
        .with_context(ErrorContext::Header)),
    }
}

/// Section 11: fixed-width statistics use the logical type's canonical width.
fn canonical_width(column: &Column) -> Result<u64> {
    let width = match column.logical_type() {
        // Section 11 stores a Boolean bound as one byte, not one bit.
        LogicalType::Bool => 1,
        LogicalType::Int8 | LogicalType::UInt8 => 1,
        LogicalType::Int16 | LogicalType::UInt16 => 2,
        LogicalType::Int32 | LogicalType::UInt32 | LogicalType::Float32 | LogicalType::Date32 => 4,
        LogicalType::Int64
        | LogicalType::UInt64
        | LogicalType::Float64
        | LogicalType::Decimal { .. }
        | LogicalType::Timestamp { .. } => 8,
        LogicalType::FixedBinary { byte_width } if *byte_width != 0 => u64::from(*byte_width),
        // Section 3.1 requires a nonzero width, and the schema frame enforces
        // it, so this is another defense-in-depth branch.
        LogicalType::FixedBinary { .. } => {
            return Err(statistics_error(
                column,
                "fixed_binary statistics have zero logical width",
                None,
            ));
        }
        LogicalType::Utf8 | LogicalType::Categorical { .. } | LogicalType::Binary => {
            return Err(statistics_error(
                column,
                "v0.2 min/max statistics are unsupported for this logical type",
                None,
            ));
        }
    };
    Ok(width)
}

/// Resolve a descriptor's statistics range to an absolute, in-snapshot byte
/// range, or explain why it is not one.
///
/// Section 8.1 places the range inside the statistics area, which the frame
/// parser has already checked. What is re-established here is the weaker but
/// independent property this module needs before it reads: the range lies in
/// the frame header, inside the captured file extent, and is small enough to
/// allocate under `limits`.
fn statistics_range(
    frame: &FrameMetadata,
    descriptor: &ColumnBlockDescriptor,
    column: &Column,
    file_size: u64,
    limits: Limits,
) -> Result<(u64, usize)> {
    let relative_offset = u64::from(descriptor.stats_offset);
    let length = u64::from(descriptor.stats_length);
    let header_end = frame
        .header_offset
        .checked_add(frame.header_length)
        .ok_or_else(|| statistics_error(column, "frame header offset overflows", None))?;
    let absolute_offset = frame
        .header_offset
        .checked_add(relative_offset)
        .ok_or_else(|| statistics_error(column, "statistics offset overflows", None))?;
    let absolute_end = absolute_offset.checked_add(length).ok_or_else(|| {
        statistics_error(column, "statistics range overflows", Some(absolute_offset))
    })?;

    if absolute_end > header_end {
        return Err(statistics_error(
            column,
            "statistics range leaves the frame header",
            Some(absolute_offset),
        ));
    }
    if absolute_end > file_size {
        return Err(statistics_error(
            column,
            "statistics range leaves the validation snapshot",
            Some(absolute_offset),
        ));
    }
    if length > limits.max_frame_header_length() {
        return Err(Error::resource_limit(
            format!(
                "column {}: statistics length {length} exceeds the {}-byte header limit",
                column.name(),
                limits.max_frame_header_length()
            ),
            Some(absolute_offset),
        )
        .with_context(ErrorContext::Header));
    }

    let length = usize::try_from(length).map_err(|_| {
        Error::resource_limit(
            format!(
                "column {}: statistics length does not fit this platform",
                column.name()
            ),
            Some(absolute_offset),
        )
        .with_context(ErrorContext::Header)
    })?;
    Ok((absolute_offset, length))
}

fn read_statistics(
    file: &mut File,
    file_size: u64,
    frame: &FrameMetadata,
    descriptor: &ColumnBlockDescriptor,
    column: &Column,
    limits: Limits,
) -> Result<Vec<u8>> {
    let (absolute_offset, length) = statistics_range(frame, descriptor, column, file_size, limits)?;

    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(|_| {
        Error::resource_limit(
            format!("column {}: unable to allocate statistics", column.name()),
            Some(absolute_offset),
        )
        .with_context(ErrorContext::Header)
    })?;
    bytes.resize(length, 0);
    read_exact_at(file, absolute_offset, &mut bytes)
        .map_err(|error| error.with_context(ErrorContext::Header))?;
    Ok(bytes)
}

/// One canonical statistic, in the value domain its logical type compares in.
#[derive(Debug, Clone, Copy)]
enum StatisticValue<'a> {
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float32(f32),
    Float64(f64),
    Bytes(&'a [u8]),
}

impl StatisticValue<'_> {
    fn is_nan(self) -> bool {
        match self {
            Self::Float32(value) => value.is_nan(),
            Self::Float64(value) => value.is_nan(),
            _ => false,
        }
    }

    /// Compare two statistics of the same logical type.
    ///
    /// Floats use `partial_cmp`, so `-0.0` and `0.0` compare equal: section 3
    /// requires stored bits to be preserved, but section 11 makes a bound a
    /// numeric claim, and both zero encodings are the same number. NaN never
    /// reaches here, so `None` means only that two different domains were
    /// compared, which is corruption.
    fn compare(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Bool(left), Self::Bool(right)) => Some(left.cmp(&right)),
            (Self::Signed(left), Self::Signed(right)) => Some(left.cmp(&right)),
            (Self::Unsigned(left), Self::Unsigned(right)) => Some(left.cmp(&right)),
            (Self::Float32(left), Self::Float32(right)) => left.partial_cmp(&right),
            (Self::Float64(left), Self::Float64(right)) => left.partial_cmp(&right),
            (Self::Bytes(left), Self::Bytes(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }
}

fn parse_stored_statistics<'a>(
    bytes: &'a [u8],
    column: &Column,
) -> Result<(StatisticValue<'a>, StatisticValue<'a>)> {
    let width = usize::try_from(canonical_width(column)?).map_err(|_| {
        Error::resource_limit(
            format!(
                "column {}: statistics width does not fit this platform",
                column.name()
            ),
            None,
        )
        .with_context(ErrorContext::Header)
    })?;
    let (minimum, maximum) = bytes
        .get(..width)
        .zip(bytes.get(width..width.saturating_mul(2)))
        .ok_or_else(|| statistics_error(column, "statistics bytes have the wrong length", None))?;

    let pair = match column.logical_type() {
        LogicalType::Bool => (
            StatisticValue::Bool(parse_bool(minimum, column)?),
            StatisticValue::Bool(parse_bool(maximum, column)?),
        ),
        LogicalType::Int8
        | LogicalType::Int16
        | LogicalType::Int32
        | LogicalType::Int64
        | LogicalType::Decimal { .. }
        | LogicalType::Timestamp { .. }
        | LogicalType::Date32 => (
            StatisticValue::Signed(parse_signed(minimum, column)?),
            StatisticValue::Signed(parse_signed(maximum, column)?),
        ),
        LogicalType::UInt8 | LogicalType::UInt16 | LogicalType::UInt32 | LogicalType::UInt64 => (
            StatisticValue::Unsigned(parse_unsigned(minimum, column)?),
            StatisticValue::Unsigned(parse_unsigned(maximum, column)?),
        ),
        LogicalType::Float32 => (
            StatisticValue::Float32(parse_float32(minimum, column)?),
            StatisticValue::Float32(parse_float32(maximum, column)?),
        ),
        LogicalType::Float64 => (
            StatisticValue::Float64(parse_float64(minimum, column)?),
            StatisticValue::Float64(parse_float64(maximum, column)?),
        ),
        LogicalType::FixedBinary { .. } => (
            StatisticValue::Bytes(minimum),
            StatisticValue::Bytes(maximum),
        ),
        LogicalType::Utf8 | LogicalType::Categorical { .. } | LogicalType::Binary => {
            return Err(statistics_error(
                column,
                "v0.2 min/max statistics are unsupported for this logical type",
                None,
            ));
        }
    };
    Ok(pair)
}

fn parse_bool(bytes: &[u8], column: &Column) -> Result<bool> {
    let [value] = bytes else {
        return Err(statistics_error(
            column,
            "Boolean statistics must be one byte",
            None,
        ));
    };
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(statistics_error(
            column,
            "Boolean statistics must contain only zero or one",
            None,
        )),
    }
}

fn parse_signed(bytes: &[u8], column: &Column) -> Result<i64> {
    match bytes {
        [value] => Ok(i64::from(i8::from_le_bytes([*value]))),
        [first, second] => Ok(i64::from(i16::from_le_bytes([*first, *second]))),
        [first, second, third, fourth] => Ok(i64::from(i32::from_le_bytes([
            *first, *second, *third, *fourth,
        ]))),
        [first, second, third, fourth, fifth, sixth, seventh, eighth] => Ok(i64::from_le_bytes([
            *first, *second, *third, *fourth, *fifth, *sixth, *seventh, *eighth,
        ])),
        _ => Err(statistics_error(
            column,
            "signed statistics have the wrong width",
            None,
        )),
    }
}

fn parse_unsigned(bytes: &[u8], column: &Column) -> Result<u64> {
    match bytes {
        [value] => Ok(u64::from(*value)),
        [first, second] => Ok(u64::from(u16::from_le_bytes([*first, *second]))),
        [first, second, third, fourth] => Ok(u64::from(u32::from_le_bytes([
            *first, *second, *third, *fourth,
        ]))),
        [first, second, third, fourth, fifth, sixth, seventh, eighth] => Ok(u64::from_le_bytes([
            *first, *second, *third, *fourth, *fifth, *sixth, *seventh, *eighth,
        ])),
        _ => Err(statistics_error(
            column,
            "unsigned statistics have the wrong width",
            None,
        )),
    }
}

fn parse_float32(bytes: &[u8], column: &Column) -> Result<f32> {
    let [first, second, third, fourth] = bytes else {
        return Err(statistics_error(
            column,
            "float32 statistics must be four bytes",
            None,
        ));
    };
    let value = f32::from_bits(u32::from_le_bytes([*first, *second, *third, *fourth]));
    if value.is_nan() {
        return Err(statistics_error(
            column,
            "a stored minimum or maximum is NaN",
            None,
        ));
    }
    Ok(value)
}

fn parse_float64(bytes: &[u8], column: &Column) -> Result<f64> {
    let [first, second, third, fourth, fifth, sixth, seventh, eighth] = bytes else {
        return Err(statistics_error(
            column,
            "float64 statistics must be eight bytes",
            None,
        ));
    };
    let value = f64::from_bits(u64::from_le_bytes([
        *first, *second, *third, *fourth, *fifth, *sixth, *seventh, *eighth,
    ]));
    if value.is_nan() {
        return Err(statistics_error(
            column,
            "a stored minimum or maximum is NaN",
            None,
        ));
    }
    Ok(value)
}

/// Section 11: nulls and floating NaNs are ignored, so a column of only those
/// has no extrema and returns `None`.
fn reduce_decoded_values<'a>(
    array: &'a Array,
    column: &Column,
) -> Result<Option<(StatisticValue<'a>, StatisticValue<'a>)>> {
    let mut minimum = None;
    let mut maximum = None;
    for row in 0..array.len() {
        let Some(value) = array.value_at(row) else {
            continue;
        };
        let value = scalar_value(value, column)?;
        if value.is_nan() {
            continue;
        }
        let replaces_minimum = match minimum {
            None => true,
            Some(current) => value.compare(current) == Some(Ordering::Less),
        };
        if replaces_minimum {
            minimum = Some(value);
        }
        let replaces_maximum = match maximum {
            None => true,
            Some(current) => value.compare(current) == Some(Ordering::Greater),
        };
        if replaces_maximum {
            maximum = Some(value);
        }
    }
    Ok(minimum.zip(maximum))
}

fn scalar_value<'a>(value: ScalarValue<'a>, column: &Column) -> Result<StatisticValue<'a>> {
    let value = match value {
        ScalarValue::Bool(value) => StatisticValue::Bool(value),
        ScalarValue::Int8(value) => StatisticValue::Signed(i64::from(value)),
        ScalarValue::Int16(value) => StatisticValue::Signed(i64::from(value)),
        ScalarValue::Int32(value) => StatisticValue::Signed(i64::from(value)),
        ScalarValue::Int64(value) => StatisticValue::Signed(value),
        ScalarValue::UInt8(value) => StatisticValue::Unsigned(u64::from(value)),
        ScalarValue::UInt16(value) => StatisticValue::Unsigned(u64::from(value)),
        ScalarValue::UInt32(value) => StatisticValue::Unsigned(u64::from(value)),
        ScalarValue::UInt64(value) => StatisticValue::Unsigned(value),
        ScalarValue::Float32(value) => StatisticValue::Float32(value),
        ScalarValue::Float64(value) => StatisticValue::Float64(value),
        // Scale is a column property, so comparing unscaled integers orders
        // decimal values the same way comparing the values would.
        ScalarValue::Decimal { unscaled, .. } => StatisticValue::Signed(unscaled),
        ScalarValue::Timestamp { value, .. } => StatisticValue::Signed(value),
        ScalarValue::FixedBinary(value) => StatisticValue::Bytes(value),
        ScalarValue::Date32(value) => StatisticValue::Signed(i64::from(value)),
        ScalarValue::Utf8(_) | ScalarValue::Categorical(_) | ScalarValue::Binary(_) => {
            return Err(statistics_error(
                column,
                "v0.2 min/max statistics are unsupported for this logical type",
                None,
            ));
        }
    };
    Ok(value)
}

fn verify_stored_order(
    column: &Column,
    minimum: StatisticValue<'_>,
    maximum: StatisticValue<'_>,
) -> Result<()> {
    let ordering = minimum.compare(maximum);
    if ordering == Some(Ordering::Greater) {
        return Err(statistics_error(
            column,
            "stored minimum is greater than stored maximum",
            None,
        ));
    }
    if ordering.is_none() {
        return Err(statistics_error(
            column,
            "stored statistics have incompatible types",
            None,
        ));
    }
    Ok(())
}

fn verify_matches(
    column: &Column,
    stored: (StatisticValue<'_>, StatisticValue<'_>),
    actual: (StatisticValue<'_>, StatisticValue<'_>),
) -> Result<()> {
    if stored.0.compare(actual.0) != Some(Ordering::Equal)
        || stored.1.compare(actual.1) != Some(Ordering::Equal)
    {
        return Err(statistics_error(
            column,
            "stored min/max statistics disagree with decoded values",
            None,
        ));
    }
    Ok(())
}

fn statistics_error(column: &Column, message: impl Into<String>, offset: Option<u64>) -> Error {
    Error::corruption(
        format!("column {}: {}", column.name(), message.into()),
        offset,
    )
    .with_context(ErrorContext::Header)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{BinaryArray, BooleanArray, PrimitiveArray};
    use crate::error::ErrorKind;
    use crate::schema::{TimeUnit, TimeZone};

    fn probe(logical_type: LogicalType) -> Column {
        Column::new(1, "probe", logical_type, true)
    }

    fn descriptor(kind: u16, flags: u16, offset: u32, length: u32) -> ColumnBlockDescriptor {
        ColumnBlockDescriptor {
            column_id: 1,
            layout: 0,
            flags,
            null_count: 0,
            dense_count: 0,
            first_stream: 0,
            stream_count: 0,
            stats_kind: kind,
            stats_offset: offset,
            stats_length: length,
        }
    }

    // ------------------------------------------------------------- classify

    #[test]
    fn an_absent_claim_needs_a_zero_kind_flag_offset_and_length() {
        let column = probe(LogicalType::Int64);
        assert_eq!(
            classify(&descriptor(STATS_NONE, 0, 0, 0), &column, 0).expect("a clean absent claim"),
            StatisticsClaim::Absent
        );
    }

    /// Defense in depth: the frame parser rejects these before this module
    /// sees them, so only a direct call can reach the branches.
    #[test]
    fn a_contradictory_absent_claim_is_corruption() {
        let column = probe(LogicalType::Int64);
        for descriptor in [
            descriptor(STATS_NONE, COLUMN_HAS_STATS_FLAG, 0, 0),
            descriptor(STATS_NONE, 0, 64, 0),
            descriptor(STATS_NONE, 0, 0, 16),
        ] {
            let error = classify(&descriptor, &column, 0)
                .expect_err("a contradictory absent claim is rejected");
            assert_eq!(error.kind(), ErrorKind::Corruption);
            assert!(error.to_string().contains("probe"), "{error}");
        }
    }

    #[test]
    fn min_max_without_the_flag_is_corruption() {
        let column = probe(LogicalType::Int64);
        let error = classify(&descriptor(STATS_MIN_MAX, 0, 0, 16), &column, 0)
            .expect_err("kind one without the flag is rejected");
        assert_eq!(error.kind(), ErrorKind::Corruption);
        assert!(error.to_string().contains("probe"), "{error}");
    }

    /// Defense in depth: the frame parser rejects an unknown kind first.
    #[test]
    fn an_unsupported_kind_is_unsupported_rather_than_corruption() {
        let column = probe(LogicalType::Int64);
        let error = classify(&descriptor(7, COLUMN_HAS_STATS_FLAG, 0, 16), &column, 0)
            .expect_err("kind seven is rejected");
        assert_eq!(error.kind(), ErrorKind::UnsupportedFrame);
        assert!(error.to_string().contains("probe"), "{error}");
    }

    #[test]
    fn every_supported_type_claims_twice_its_canonical_width() {
        let cases = [
            (LogicalType::Bool, 2),
            (LogicalType::Int8, 2),
            (LogicalType::UInt8, 2),
            (LogicalType::Int16, 4),
            (LogicalType::UInt16, 4),
            (LogicalType::Int32, 8),
            (LogicalType::UInt32, 8),
            (LogicalType::Float32, 8),
            (LogicalType::Date32, 8),
            (LogicalType::Int64, 16),
            (LogicalType::UInt64, 16),
            (LogicalType::Float64, 16),
            (
                LogicalType::Decimal {
                    precision: 18,
                    scale: 4,
                },
                16,
            ),
            (
                LogicalType::Timestamp {
                    unit: TimeUnit::Nanosecond,
                    timezone: TimeZone::Naive,
                },
                16,
            ),
            (LogicalType::FixedBinary { byte_width: 5 }, 10),
        ];
        for (logical_type, expected) in cases {
            let column = probe(logical_type.clone());
            assert_eq!(
                classify(
                    &descriptor(STATS_MIN_MAX, COLUMN_HAS_STATS_FLAG, 0, 0),
                    &column,
                    0
                )
                .unwrap_or_else(|error| panic!("{logical_type:?}: {error}")),
                StatisticsClaim::MinMax { length: expected },
                "{logical_type:?}"
            );
        }
    }

    #[test]
    fn variable_width_types_cannot_carry_min_max() {
        for logical_type in [
            LogicalType::Utf8,
            LogicalType::Categorical { ordered: false },
            LogicalType::Binary,
        ] {
            let column = probe(logical_type.clone());
            let error = classify(
                &descriptor(STATS_MIN_MAX, COLUMN_HAS_STATS_FLAG, 0, 0),
                &column,
                0,
            )
            .expect_err("v0.2 writes no variable-width statistics");
            assert_eq!(error.kind(), ErrorKind::Corruption, "{logical_type:?}");
        }
    }

    #[test]
    fn a_zero_width_fixed_binary_column_cannot_carry_min_max() {
        let column = probe(LogicalType::FixedBinary { byte_width: 0 });
        let error = classify(
            &descriptor(STATS_MIN_MAX, COLUMN_HAS_STATS_FLAG, 0, 0),
            &column,
            0,
        )
        .expect_err("a zero width has no canonical representation");
        assert_eq!(error.kind(), ErrorKind::Corruption);
    }

    // ------------------------------------------------------ statistics_range

    fn frame(header_offset: u64, header_length: u64) -> FrameMetadata {
        FrameMetadata {
            frame_type: crate::format::constants::DATA_FRAME_TYPE,
            sequence: 1,
            frame_offset: header_offset.saturating_sub(48),
            header_offset,
            header_length,
            payload_offset: header_offset.saturating_add(header_length),
            payload_length: 0,
            total_length: 80 + header_length,
        }
    }

    #[test]
    fn a_statistics_range_resolves_against_the_frame_header() {
        let column = probe(LogicalType::Int64);
        let (offset, length) = statistics_range(
            &frame(1_000, 256),
            &descriptor(STATS_MIN_MAX, COLUMN_HAS_STATS_FLAG, 128, 16),
            &column,
            u64::MAX,
            Limits::default(),
        )
        .expect("an in-header range resolves");
        assert_eq!((offset, length), (1_128, 16));
    }

    #[test]
    fn a_statistics_range_cannot_leave_the_header_snapshot_or_limit() {
        let column = probe(LogicalType::Int64);
        let cases = [
            (
                "one byte past the frame header",
                frame(1_000, 256),
                descriptor(STATS_MIN_MAX, COLUMN_HAS_STATS_FLAG, 248, 16),
                u64::MAX,
                Limits::default(),
                ErrorKind::Corruption,
            ),
            (
                "an offset that overflows the address space",
                frame(u64::MAX - 100, 0),
                descriptor(STATS_MIN_MAX, COLUMN_HAS_STATS_FLAG, u32::MAX, 16),
                u64::MAX,
                Limits::default(),
                ErrorKind::Corruption,
            ),
            (
                "inside the header but past the captured extent",
                frame(1_000, 256),
                descriptor(STATS_MIN_MAX, COLUMN_HAS_STATS_FLAG, 128, 16),
                1_130,
                Limits::default(),
                ErrorKind::Corruption,
            ),
            (
                "a length above the configured header limit",
                frame(1_000, 256),
                descriptor(STATS_MIN_MAX, COLUMN_HAS_STATS_FLAG, 0, 200),
                u64::MAX,
                Limits::default().with_max_frame_header_length(8),
                ErrorKind::ResourceLimit,
            ),
        ];
        for (label, frame, descriptor, file_size, limits, kind) in cases {
            let error =
                statistics_range(&frame, &descriptor, &column, file_size, limits).expect_err(label);
            assert_eq!(error.kind(), kind, "{label}: {error}");
            assert!(error.to_string().contains("probe"), "{label}: {error}");
        }
    }

    // ------------------------------------------------------ canonical parsing

    #[test]
    fn signed_and_unsigned_bytes_decode_at_every_canonical_width() {
        let column = probe(LogicalType::Int64);
        assert_eq!(parse_signed(&[0xff], &column).unwrap(), -1);
        assert_eq!(
            parse_signed(&[0x00, 0x80], &column).unwrap(),
            i64::from(i16::MIN)
        );
        assert_eq!(
            parse_signed(&i32::MIN.to_le_bytes(), &column).unwrap(),
            i64::from(i32::MIN)
        );
        assert_eq!(
            parse_signed(&i64::MIN.to_le_bytes(), &column).unwrap(),
            i64::MIN
        );
        assert!(parse_signed(&[0, 0, 0], &column).is_err());

        assert_eq!(parse_unsigned(&[0xff], &column).unwrap(), 255);
        assert_eq!(
            parse_unsigned(&u16::MAX.to_le_bytes(), &column).unwrap(),
            65_535
        );
        assert_eq!(
            parse_unsigned(&u32::MAX.to_le_bytes(), &column).unwrap(),
            u64::from(u32::MAX)
        );
        // A signed reading of this would be negative, so the two domains
        // cannot be confused silently.
        assert_eq!(
            parse_unsigned(&u64::MAX.to_le_bytes(), &column).unwrap(),
            u64::MAX
        );
        assert!(parse_unsigned(&[0, 0, 0], &column).is_err());
    }

    #[test]
    fn boolean_statistics_accept_only_zero_and_one() {
        let column = probe(LogicalType::Bool);
        assert!(!parse_bool(&[0], &column).unwrap());
        assert!(parse_bool(&[1], &column).unwrap());
        assert_eq!(
            parse_bool(&[2], &column)
                .expect_err("two is not a Boolean")
                .kind(),
            ErrorKind::Corruption
        );
        assert!(parse_bool(&[0, 1], &column).is_err());
    }

    #[test]
    fn a_stored_nan_bound_is_rejected_at_both_float_widths() {
        let single = probe(LogicalType::Float32);
        assert_eq!(
            parse_float32(&f32::NAN.to_le_bytes(), &single)
                .expect_err("NaN is never a bound")
                .kind(),
            ErrorKind::Corruption
        );
        assert_eq!(
            parse_float32(&f32::INFINITY.to_le_bytes(), &single).unwrap(),
            f32::INFINITY
        );

        let double = probe(LogicalType::Float64);
        assert_eq!(
            parse_float64(&f64::NAN.to_le_bytes(), &double)
                .expect_err("NaN is never a bound")
                .kind(),
            ErrorKind::Corruption
        );
        assert_eq!(
            parse_float64(&f64::NEG_INFINITY.to_le_bytes(), &double).unwrap(),
            f64::NEG_INFINITY
        );
    }

    #[test]
    fn stored_bytes_shorter_than_two_canonical_widths_are_rejected() {
        let column = probe(LogicalType::Int64);
        assert_eq!(
            parse_stored_statistics(&[0; 15], &column)
                .expect_err("int64 needs sixteen bytes")
                .kind(),
            ErrorKind::Corruption
        );
        assert!(parse_stored_statistics(&[0; 16], &column).is_ok());
    }

    // ------------------------------------------------------------- reduction

    #[test]
    fn nulls_and_nans_are_ignored_and_an_empty_reduction_is_none() {
        let column = probe(LogicalType::Int64);
        let array = Array::Int64(PrimitiveArray::new(
            vec![100, 5, 0, 20],
            Some(vec![false, true, false, true]),
        ));
        let (minimum, maximum) = reduce_decoded_values(&array, &column)
            .unwrap()
            .expect("two values remain");
        assert_eq!(
            minimum.compare(StatisticValue::Signed(5)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            maximum.compare(StatisticValue::Signed(20)),
            Some(Ordering::Equal)
        );

        let all_null = Array::Int64(PrimitiveArray::new(vec![1, 2], Some(vec![false, false])));
        assert!(reduce_decoded_values(&all_null, &column).unwrap().is_none());

        let floats = probe(LogicalType::Float64);
        let all_nan = Array::Float64(PrimitiveArray::new(vec![f64::NAN, f64::NAN], None));
        assert!(reduce_decoded_values(&all_nan, &floats).unwrap().is_none());

        let some_nan = Array::Float64(PrimitiveArray::new(
            vec![f64::NAN, -2.5, f64::INFINITY],
            None,
        ));
        let (minimum, maximum) = reduce_decoded_values(&some_nan, &floats)
            .unwrap()
            .expect("the NaN is skipped, not fatal");
        assert_eq!(
            minimum.compare(StatisticValue::Float64(-2.5)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            maximum.compare(StatisticValue::Float64(f64::INFINITY)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn fixed_binary_reduces_lexicographically_rather_than_by_leading_byte() {
        let column = probe(LogicalType::FixedBinary { byte_width: 3 });
        let array = Array::FixedBinary(BinaryArray::new(
            vec![
                vec![0x01, 0xff, 0xff],
                vec![0x02, 0x00, 0x00],
                vec![0x01, 0xff, 0xfe],
            ],
            None,
        ));
        let (minimum, maximum) = reduce_decoded_values(&array, &column)
            .unwrap()
            .expect("three values");
        assert_eq!(
            minimum.compare(StatisticValue::Bytes(&[0x01, 0xff, 0xfe])),
            Some(Ordering::Equal)
        );
        assert_eq!(
            maximum.compare(StatisticValue::Bytes(&[0x02, 0x00, 0x00])),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn booleans_reduce_with_false_below_true() {
        let column = probe(LogicalType::Bool);
        let array = Array::Bool(BooleanArray::new(vec![true, false, true], None));
        let (minimum, maximum) = reduce_decoded_values(&array, &column)
            .unwrap()
            .expect("three values");
        assert_eq!(
            minimum.compare(StatisticValue::Bool(false)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            maximum.compare(StatisticValue::Bool(true)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn variable_width_values_cannot_be_reduced() {
        let column = probe(LogicalType::Utf8);
        let array = Array::Utf8(crate::array::Utf8Array::new(vec!["a".into()], None));
        assert_eq!(
            reduce_decoded_values(&array, &column)
                .expect_err("utf8 has no v0.2 statistic")
                .kind(),
            ErrorKind::Corruption
        );
    }

    // --------------------------------------------------------- order and match

    #[test]
    fn a_stored_minimum_above_its_maximum_is_corruption() {
        let column = probe(LogicalType::Int64);
        assert!(
            verify_stored_order(
                &column,
                StatisticValue::Signed(1),
                StatisticValue::Signed(1)
            )
            .is_ok()
        );
        assert_eq!(
            verify_stored_order(
                &column,
                StatisticValue::Signed(2),
                StatisticValue::Signed(1)
            )
            .expect_err("two is not below one")
            .kind(),
            ErrorKind::Corruption
        );
        assert_eq!(
            verify_stored_order(
                &column,
                StatisticValue::Signed(1),
                StatisticValue::Unsigned(1)
            )
            .expect_err("mismatched domains cannot be ordered")
            .kind(),
            ErrorKind::Corruption
        );
    }

    #[test]
    fn signed_zero_bounds_match_either_encoding() {
        let column = probe(LogicalType::Float64);
        for stored in [0.0_f64, -0.0] {
            for actual in [0.0_f64, -0.0] {
                verify_matches(
                    &column,
                    (
                        StatisticValue::Float64(stored),
                        StatisticValue::Float64(stored),
                    ),
                    (
                        StatisticValue::Float64(actual),
                        StatisticValue::Float64(actual),
                    ),
                )
                .expect("both zero encodings are the same number");
            }
        }
    }

    #[test]
    fn a_bound_that_disagrees_with_the_decoded_extrema_is_corruption() {
        let column = probe(LogicalType::Int64);
        assert_eq!(
            verify_matches(
                &column,
                (StatisticValue::Signed(2), StatisticValue::Signed(3)),
                (StatisticValue::Signed(1), StatisticValue::Signed(3)),
            )
            .expect_err("the stored minimum is wrong")
            .kind(),
            ErrorKind::Corruption
        );
    }
}
