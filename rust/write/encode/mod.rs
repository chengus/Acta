//! Data-frame encoding, candidate materialization, and stream descriptors.

use std::cmp::Ordering;

use crate::array::{Array, ScalarValue};
use crate::crc32c::checksum;
use crate::error::{Error, ErrorContext, Result};
use crate::format::constants::{
    BLOCK_COLUMN_DESCRIPTOR_SIZE, BLOCK_STREAM_DESCRIPTOR_SIZE, CODEC_NONE, CODEC_ZSTD,
    COLUMN_HAS_STATS_FLAG, COLUMN_IMPLICIT_VALIDITY_FLAG, COLUMN_LAYOUT_PLAIN,
    DATA_BLOCK_HEADER_SIZE, DATA_FRAME_TYPE, FRAME_ALIGNMENT, ROW_IDS_BLOCK_FLAG, STATS_MIN_MAX,
    STATS_NONE, STREAM_KIND_VALIDITY, TRANSFORM_BOOLEAN_RLE, TRANSFORM_RAW, TS_SORTED_BLOCK_FLAG,
};
use crate::limits::Limits;
use crate::schema::{Column, LogicalType, Schema};

use self::candidates::{CandidateStream, PreparedValues, ValueEncoding, ValueProfile};
use self::raw::raw_column_candidate;
use super::api::{WriterCodec, WriterEncoding, WriterOptions, WriterStatistics, WriterTransform};
use super::buffer::{BlockRows, stream_payload_bytes};
use super::framing::{build_frame, pad_to_alignment, push_i64, push_u16, push_u32, push_u64};
use super::input::validate_column_parts;
use super::{internal, invalid_batch, invalid_option, resource};

mod candidates;
mod raw;

pub(super) struct EncodedStream {
    kind: u16,
    transform: u16,
    element_count: u64,
    payload_offset: u64,
    stored_length: u64,
    transformed_length: u64,
    crc: u32,
}

pub(super) struct EncodedColumn {
    column_id: u32,
    layout: u16,
    flags: u16,
    null_count: u32,
    dense_count: u32,
    first_stream: u32,
    streams: Vec<EncodedStream>,
    pending_streams: Vec<StoredStream>,
    statistics: Option<Vec<u8>>,
}

#[derive(Debug)]
pub(super) struct StoredStream {
    kind: u16,
    transform: u16,
    element_count: u64,
    transformed_length: u64,
    stored: Vec<u8>,
}

#[derive(Debug)]
pub(super) struct SelectedColumn {
    layout: u16,
    streams: Vec<StoredStream>,
}

#[cfg(test)]
pub(super) fn build_data_frame(
    schema: &Schema,
    rows: &BlockRows<'_>,
    row_ids: bool,
    base_row_id: u64,
    sequence: u64,
    codec: WriterCodec,
    encoding: WriterEncoding,
) -> Result<Vec<u8>> {
    build_data_frame_with_statistics(
        schema,
        rows,
        WriterOptions::default()
            .with_row_ids(row_ids)
            .with_codec(codec)
            .with_encoding(encoding),
        base_row_id,
        sequence,
    )
}

pub(super) fn build_data_frame_with_statistics(
    schema: &Schema,
    rows: &BlockRows<'_>,
    options: WriterOptions,
    base_row_id: u64,
    sequence: u64,
) -> Result<Vec<u8>> {
    let row_count = u32::try_from(rows.row_count)
        .map_err(|_| invalid_batch("the batch row count exceeds uint32::MAX"))?;
    if u64::from(row_count) > Limits::default().max_rows_per_block() {
        return Err(resource(
            "the batch row count exceeds the default block row limit",
        ));
    }
    let mut columns = Vec::new();
    columns
        .try_reserve_exact(schema.column_count())
        .map_err(|_| resource("unable to reserve encoded column descriptors"))?;

    let mut sorted_columns: Vec<(usize, &Column)> = schema.columns().iter().enumerate().collect();
    sorted_columns.sort_unstable_by_key(|(_, column)| column.id());

    let mut stream_count = 0_u32;
    let mut payload = Vec::new();
    for (schema_index, column) in sorted_columns {
        let mut encoded = encode_column(
            column,
            rows,
            schema_index,
            stream_count,
            options,
            schema.primary_column_id() == Some(column.id()),
        )?;
        for pending in encoded.pending_streams.drain(..) {
            encoded.streams.push(append_stream(pending, &mut payload)?);
        }
        stream_count = stream_count
            .checked_add(
                u32::try_from(encoded.streams.len())
                    .map_err(|_| invalid_batch("the stream count does not fit uint32"))?,
            )
            .ok_or_else(|| invalid_batch("the stream count overflows uint32"))?;
        columns.push(encoded);
    }

    let stream_count_usize = usize::try_from(stream_count)
        .map_err(|_| invalid_batch("the stream count does not fit this platform"))?;
    let stream_table_bytes = stream_count_usize
        .checked_mul(BLOCK_STREAM_DESCRIPTOR_SIZE as usize)
        .ok_or_else(|| invalid_batch("the stream table length overflows"))?;
    let column_table_bytes = schema
        .column_count()
        .checked_mul(BLOCK_COLUMN_DESCRIPTOR_SIZE as usize)
        .ok_or_else(|| invalid_batch("the column table length overflows"))?;
    let stream_table_offset = (DATA_BLOCK_HEADER_SIZE as usize)
        .checked_add(column_table_bytes)
        .ok_or_else(|| invalid_batch("the stream table offset overflows"))?;
    let statistics_offset = stream_table_offset
        .checked_add(stream_table_bytes)
        .ok_or_else(|| invalid_batch("the statistics offset overflows"))?;
    let mut statistics_area = Vec::new();
    let mut statistics_offsets = Vec::with_capacity(columns.len());
    for column in &columns {
        let Some(bytes) = column.statistics.as_ref() else {
            statistics_offsets.push(None);
            continue;
        };
        let relative_offset = statistics_offset
            .checked_add(statistics_area.len())
            .ok_or_else(|| invalid_batch("the statistics offset overflows"))?;
        statistics_offsets.push(Some(relative_offset));
        statistics_area.extend_from_slice(bytes);
    }
    pad_to_alignment(&mut statistics_area);
    let statistics_length = statistics_area.len();
    if u64::try_from(statistics_offset)
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(statistics_length).unwrap_or(u64::MAX))
        > Limits::default().max_frame_header_length()
    {
        return Err(resource(
            "the data frame header exceeds the default frame limit",
        ));
    }
    if u64::try_from(payload.len()).unwrap_or(u64::MAX)
        > Limits::default().max_frame_payload_length()
    {
        return Err(resource(
            "the data frame payload exceeds the default frame limit",
        ));
    }
    let (primary_min, primary_max, ts_sorted) = primary_metadata(schema, rows)?;
    let header_capacity = statistics_offset
        .checked_add(statistics_length)
        .ok_or_else(|| resource("the data frame header length overflows"))?;
    let mut header = Vec::with_capacity(header_capacity);
    push_u64(&mut header, schema.schema_id());
    push_u64(&mut header, base_row_id);
    push_u32(&mut header, row_count);
    push_u32(
        &mut header,
        u32::try_from(schema.column_count())
            .map_err(|_| invalid_batch("the schema column count exceeds uint32::MAX"))?,
    );
    push_i64(&mut header, primary_min);
    push_i64(&mut header, primary_max);
    push_u32(&mut header, DATA_BLOCK_HEADER_SIZE as u32);
    push_u32(
        &mut header,
        u32::try_from(stream_table_offset)
            .map_err(|_| resource("the stream table offset exceeds uint32::MAX"))?,
    );
    push_u32(
        &mut header,
        u32::try_from(statistics_offset)
            .map_err(|_| resource("the statistics offset exceeds uint32::MAX"))?,
    );
    push_u32(
        &mut header,
        u32::try_from(statistics_length)
            .map_err(|_| resource("the statistics area length exceeds uint32::MAX"))?,
    );
    let mut flags = if options.row_ids {
        ROW_IDS_BLOCK_FLAG
    } else {
        0
    };
    if ts_sorted {
        flags |= TS_SORTED_BLOCK_FLAG;
    }
    push_u32(&mut header, flags);
    push_u32(&mut header, 0);

    for (column, statistics_offset) in columns.iter().zip(statistics_offsets) {
        push_u32(&mut header, column.column_id);
        push_u16(&mut header, column.layout);
        push_u16(&mut header, column.flags);
        push_u32(&mut header, column.null_count);
        push_u32(&mut header, column.dense_count);
        push_u32(&mut header, column.first_stream);
        push_u16(
            &mut header,
            u16::try_from(column.streams.len())
                .map_err(|_| invalid_batch("a column has too many streams"))?,
        );
        push_u16(
            &mut header,
            if column.statistics.is_some() {
                STATS_MIN_MAX
            } else {
                STATS_NONE
            },
        );
        push_u32(
            &mut header,
            u32::try_from(statistics_offset.unwrap_or(0))
                .map_err(|_| resource("statistics offset exceeds uint32::MAX"))?,
        );
        push_u32(
            &mut header,
            u32::try_from(column.statistics.as_ref().map_or(0, Vec::len))
                .map_err(|_| resource("statistics length exceeds uint32::MAX"))?,
        );
    }

    for stream in columns.iter().flat_map(|column| &column.streams) {
        push_u16(&mut header, stream.kind);
        push_u16(&mut header, stream.transform);
        push_u16(&mut header, codec_id(options.codec));
        push_u16(&mut header, 0);
        push_u64(&mut header, stream.payload_offset);
        push_u64(&mut header, stream.stored_length);
        push_u64(&mut header, stream.transformed_length);
        push_u64(&mut header, stream.element_count);
        push_u32(&mut header, stream.crc);
        push_u32(&mut header, 0);
    }
    if header.len() != statistics_offset {
        return Err(internal(
            "the data frame header does not match its declared statistics offset",
        ));
    }
    header.extend_from_slice(&statistics_area);

    build_frame(DATA_FRAME_TYPE, sequence, &header, &payload)
}

/// Refuse a fixed policy this writer does not offer for one of the schema's
/// logical types, before the file exists.
///
/// The offered set is the section 12 candidate table [`WriterEncoding::Adaptive`]
/// prices, which is deliberately narrower than section 9's applicability table:
/// a dictionary `timestamp64` column is a legal v0.2 shape this writer does not
/// produce under either policy. The message says "not offered" rather than
/// "invalid" for exactly that reason.
pub(super) fn validate_encoding(schema: &Schema, encoding: WriterEncoding) -> Result<()> {
    let WriterEncoding::Fixed(transform) = encoding else {
        return Ok(());
    };
    let candidate = value_encoding(transform);
    for column in schema.columns() {
        if !candidates::supports(column.logical_type(), candidate) {
            return Err(invalid_option(format!(
                "fixed transform {transform:?} is not offered for column {} ({:?})",
                column.name(),
                column.logical_type()
            )));
        }
    }
    Ok(())
}

fn encode_column(
    column: &Column,
    rows: &BlockRows<'_>,
    index: usize,
    first_stream: u32,
    options: WriterOptions,
    is_primary: bool,
) -> Result<EncodedColumn> {
    validate_column_parts(column, rows, index)?;
    let row_count = rows.row_count;
    let valid_bits = validity_bits(rows, index)?;
    let null_count = valid_bits.iter().filter(|valid| !**valid).count();
    let dense_count = row_count - null_count;

    // Section 8.1 gives column flag bit 0 to a validity representation that
    // needs no stream. A non-nullable column has no validity representation for
    // the bit to describe, so it stays clear; a nullable all-valid or all-null
    // column keeps it, so a reader can tell that representation from a stream
    // it failed to find.
    let implicit = null_count == 0 || dense_count == 0;
    let statistics = generate_statistics(
        column,
        rows,
        index,
        &valid_bits,
        options.statistics,
        is_primary,
    )?;
    let mut flags = if column.is_nullable() && implicit {
        COLUMN_IMPLICIT_VALIDITY_FLAG
    } else {
        0
    };
    if statistics.is_some() {
        flags |= COLUMN_HAS_STATS_FLAG;
    }

    let selected = match options.encoding {
        WriterEncoding::Raw => raw_column_candidate(
            column,
            rows,
            index,
            &valid_bits,
            options.codec,
            options.zstd_level,
        )?,
        WriterEncoding::Adaptive => adaptive_column_candidate(
            column,
            rows,
            index,
            &valid_bits,
            options.codec,
            options.zstd_level,
        )?,
        WriterEncoding::Fixed(transform) => fixed_column_candidate(
            column,
            rows,
            index,
            &valid_bits,
            transform,
            options.codec,
            options.zstd_level,
        )?,
    };

    Ok(EncodedColumn {
        column_id: column.id(),
        layout: selected.layout,
        flags,
        null_count: u32::try_from(null_count)
            .map_err(|_| invalid_batch("the null count exceeds uint32::MAX"))?,
        dense_count: u32::try_from(dense_count)
            .map_err(|_| invalid_batch("the dense count exceeds uint32::MAX"))?,
        first_stream,
        streams: Vec::new(),
        pending_streams: selected.streams,
        statistics,
    })
}

/// The minimum number of non-null, non-NaN values in a block before automatic
/// statistics are considered. At smaller sizes the fixed header work and the
/// extra extrema pass dominate the likely pruning benefit.
const AUTOMATIC_MIN_DENSE_VALUES: usize = 64;

/// Require a column's raw dense value bytes to be at least this multiple of the
/// min/max pair describing them, so a statistic cannot materially inflate the
/// block it describes.
///
/// For every fixed-width type this reduces to `value_count >= 16`, which
/// [`AUTOMATIC_MIN_DENSE_VALUES`] already requires more strictly, so the ratio
/// binds only on `bool`, whose values cost one bit each rather than one byte.
/// There a two-byte pair needs sixteen bitmap bytes, which raises the effective
/// floor to 121 values. The test is kept in this general form rather than
/// folded into a per-type constant so that a narrower type added later is
/// priced correctly without revisiting the rule.
const AUTOMATIC_VALUE_TO_STATISTICS_RATIO: usize = 8;

/// One candidate extremum, borrowed from the array holding it.
///
/// Borrowing rather than owning keeps the scan allocation-free: a
/// `fixed_binary` column would otherwise copy every value it looked at, not
/// just the two it keeps.
#[derive(Debug, Clone, Copy)]
enum WriterStatisticValue<'a> {
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float32(f32),
    Float64(f64),
    Bytes(&'a [u8]),
}

impl WriterStatisticValue<'_> {
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

    fn is_nan(self) -> bool {
        match self {
            Self::Float32(value) => value.is_nan(),
            Self::Float64(value) => value.is_nan(),
            _ => false,
        }
    }
}

/// Generate one canonical min/max pair, or omit it when the selected policy or
/// logical type does not support one. The scan follows block row order and
/// keeps the first bit pattern on equal floating-point values, making signed
/// zero handling stable while preserving the format's exact float bits.
///
/// Every decision here reads only the block's values and its schema, so equal
/// blocks produce equal bytes however the rows were split across appends.
fn generate_statistics(
    column: &Column,
    rows: &BlockRows<'_>,
    index: usize,
    valid_bits: &[bool],
    policy: WriterStatistics,
    is_primary: bool,
) -> Result<Option<Vec<u8>>> {
    let Some(width) = statistic_width(column.logical_type()) else {
        return Ok(None);
    };
    match policy {
        WriterStatistics::None => return Ok(None),
        // Section 11: the block header already carries the primary column's
        // complete pruning bounds. Deciding that before the scan rather than
        // after keeps `Automatic` from paying for a pass it always discards,
        // on what is usually the largest column in the schema.
        WriterStatistics::Automatic if is_primary => return Ok(None),
        WriterStatistics::MinMax | WriterStatistics::Automatic => {}
    }

    let mut minimum: Option<WriterStatisticValue<'_>> = None;
    let mut maximum: Option<WriterStatisticValue<'_>> = None;
    let mut value_count = 0_usize;
    for ((array, row), valid) in rows.column(index).zip(valid_bits) {
        if !*valid {
            continue;
        }
        let Some(value) = array.value_at(row) else {
            return Err(invalid_batch(format!(
                "column {} has a missing valid value",
                column.name()
            )));
        };
        let value = statistic_value(column, value)?;
        if value.is_nan() {
            continue;
        }
        value_count += 1;
        if minimum.is_none_or(|current| value.compare(current) == Some(Ordering::Less)) {
            minimum = Some(value);
        }
        if maximum.is_none_or(|current| value.compare(current) == Some(Ordering::Greater)) {
            maximum = Some(value);
        }
    }

    let Some((minimum, maximum)) = minimum.zip(maximum) else {
        return Ok(None);
    };
    if matches!(policy, WriterStatistics::Automatic)
        && !automatic_statistics_are_worthwhile(column.logical_type(), value_count, width)
    {
        return Ok(None);
    }
    Ok(Some(encode_statistics(
        column.logical_type(),
        minimum,
        maximum,
    )?))
}

/// Section 11: the canonical statistic width of a type that has one.
///
/// `None` means this crate writes no v0.2 statistic for the type, which is the
/// single authority on the question: [`generate_statistics`] omits the pair,
/// [`crate::write::buffer`] reserves no header bytes for it, and
/// [`encode_statistics`] refuses to serialize one. A zero-width `fixed_binary`
/// column has no canonical representation to store, so it belongs here rather
/// than producing an empty pair; section 3.1 and
/// [`crate::write::framing::type_id_and_parameters`] both reject that width
/// before a file exists, which makes this the second of two guards.
pub(super) fn statistic_width(logical_type: &LogicalType) -> Option<usize> {
    match logical_type {
        LogicalType::Bool | LogicalType::Int8 | LogicalType::UInt8 => Some(1),
        LogicalType::Int16 | LogicalType::UInt16 => Some(2),
        LogicalType::Int32 | LogicalType::UInt32 | LogicalType::Float32 | LogicalType::Date32 => {
            Some(4)
        }
        LogicalType::Int64
        | LogicalType::UInt64
        | LogicalType::Float64
        | LogicalType::Decimal { .. }
        | LogicalType::Timestamp { .. } => Some(8),
        LogicalType::FixedBinary { byte_width: 0 } => None,
        LogicalType::FixedBinary { byte_width } => usize::try_from(*byte_width).ok(),
        LogicalType::Utf8 | LogicalType::Categorical { .. } | LogicalType::Binary => None,
    }
}

/// The size half of the `Automatic` rule, documented on its two constants.
///
/// `value_count` counts the values a statistic would actually describe, which
/// excludes nulls and NaNs, so a column that is mostly NaN is priced as the
/// small column it is.
fn automatic_statistics_are_worthwhile(
    logical_type: &LogicalType,
    value_count: usize,
    width: usize,
) -> bool {
    if value_count < AUTOMATIC_MIN_DENSE_VALUES {
        return false;
    }
    // A Boolean value costs one bit in its values stream, not one byte, so it
    // is the one type whose raw bytes are not `count × width`.
    let value_bytes = if matches!(logical_type, LogicalType::Bool) {
        value_count.div_ceil(8)
    } else {
        value_count.saturating_mul(width)
    };
    width
        .saturating_mul(2)
        .saturating_mul(AUTOMATIC_VALUE_TO_STATISTICS_RATIO)
        <= value_bytes
}

fn statistic_value<'a>(
    column: &Column,
    value: ScalarValue<'a>,
) -> Result<WriterStatisticValue<'a>> {
    let value = match (column.logical_type(), value) {
        (LogicalType::Bool, ScalarValue::Bool(value)) => WriterStatisticValue::Bool(value),
        (LogicalType::Int8, ScalarValue::Int8(value)) => {
            WriterStatisticValue::Signed(i64::from(value))
        }
        (LogicalType::Int16, ScalarValue::Int16(value)) => {
            WriterStatisticValue::Signed(i64::from(value))
        }
        (LogicalType::Int32, ScalarValue::Int32(value)) => {
            WriterStatisticValue::Signed(i64::from(value))
        }
        (LogicalType::Int64, ScalarValue::Int64(value)) => WriterStatisticValue::Signed(value),
        (LogicalType::UInt8, ScalarValue::UInt8(value)) => {
            WriterStatisticValue::Unsigned(u64::from(value))
        }
        (LogicalType::UInt16, ScalarValue::UInt16(value)) => {
            WriterStatisticValue::Unsigned(u64::from(value))
        }
        (LogicalType::UInt32, ScalarValue::UInt32(value)) => {
            WriterStatisticValue::Unsigned(u64::from(value))
        }
        (LogicalType::UInt64, ScalarValue::UInt64(value)) => WriterStatisticValue::Unsigned(value),
        (LogicalType::Float32, ScalarValue::Float32(value)) => WriterStatisticValue::Float32(value),
        (LogicalType::Float64, ScalarValue::Float64(value)) => WriterStatisticValue::Float64(value),
        (LogicalType::Decimal { .. }, ScalarValue::Decimal { unscaled, .. }) => {
            WriterStatisticValue::Signed(unscaled)
        }
        (LogicalType::Timestamp { .. }, ScalarValue::Timestamp { value, .. }) => {
            WriterStatisticValue::Signed(value)
        }
        (LogicalType::Date32, ScalarValue::Date32(value)) => {
            WriterStatisticValue::Signed(i64::from(value))
        }
        (LogicalType::FixedBinary { byte_width }, ScalarValue::FixedBinary(value)) => {
            if value.len() != *byte_width as usize {
                return Err(invalid_batch(format!(
                    "column {} has a {}-byte value in a fixed_binary({byte_width}) column",
                    column.name(),
                    value.len()
                )));
            }
            WriterStatisticValue::Bytes(value)
        }
        _ => {
            return Err(invalid_batch(format!(
                "column {} contains a value with the wrong logical type",
                column.name()
            )));
        }
    };
    Ok(value)
}

/// Section 11: the canonical minimum, then the canonical maximum, each in the
/// logical type's canonical width and little-endian byte order.
fn encode_statistics(
    logical_type: &LogicalType,
    minimum: WriterStatisticValue<'_>,
    maximum: WriterStatisticValue<'_>,
) -> Result<Vec<u8>> {
    let width = statistic_width(logical_type)
        .ok_or_else(|| invalid_batch("statistics are unsupported for this logical type"))?;
    let mut bytes = Vec::with_capacity(width * 2);
    append_statistic_value(&mut bytes, logical_type, minimum)?;
    append_statistic_value(&mut bytes, logical_type, maximum)?;
    debug_assert_eq!(bytes.len(), width * 2);
    Ok(bytes)
}

fn append_statistic_value(
    bytes: &mut Vec<u8>,
    logical_type: &LogicalType,
    value: WriterStatisticValue<'_>,
) -> Result<()> {
    match (logical_type, value) {
        (LogicalType::Bool, WriterStatisticValue::Bool(value)) => bytes.push(u8::from(value)),
        (LogicalType::Int8, WriterStatisticValue::Signed(value)) => {
            bytes.extend_from_slice(&(value as i8).to_le_bytes())
        }
        (LogicalType::Int16, WriterStatisticValue::Signed(value)) => {
            bytes.extend_from_slice(&(value as i16).to_le_bytes())
        }
        (LogicalType::Int32, WriterStatisticValue::Signed(value))
        | (LogicalType::Date32, WriterStatisticValue::Signed(value)) => {
            bytes.extend_from_slice(&(value as i32).to_le_bytes())
        }
        (LogicalType::Int64, WriterStatisticValue::Signed(value))
        | (LogicalType::Decimal { .. }, WriterStatisticValue::Signed(value))
        | (LogicalType::Timestamp { .. }, WriterStatisticValue::Signed(value)) => {
            bytes.extend_from_slice(&value.to_le_bytes())
        }
        (LogicalType::UInt8, WriterStatisticValue::Unsigned(value)) => bytes.push(value as u8),
        (LogicalType::UInt16, WriterStatisticValue::Unsigned(value)) => {
            bytes.extend_from_slice(&(value as u16).to_le_bytes())
        }
        (LogicalType::UInt32, WriterStatisticValue::Unsigned(value)) => {
            bytes.extend_from_slice(&(value as u32).to_le_bytes())
        }
        (LogicalType::UInt64, WriterStatisticValue::Unsigned(value)) => {
            bytes.extend_from_slice(&value.to_le_bytes())
        }
        (LogicalType::Float32, WriterStatisticValue::Float32(value)) => {
            bytes.extend_from_slice(&value.to_bits().to_le_bytes())
        }
        (LogicalType::Float64, WriterStatisticValue::Float64(value)) => {
            bytes.extend_from_slice(&value.to_bits().to_le_bytes())
        }
        (LogicalType::FixedBinary { byte_width }, WriterStatisticValue::Bytes(value))
            if value.len() == *byte_width as usize =>
        {
            bytes.extend_from_slice(value)
        }
        _ => {
            return Err(invalid_batch(
                "statistics value type does not match its column",
            ));
        }
    }
    Ok(())
}

/// Apply one requested transform to this column, or fail.
///
/// Nothing here prices anything and nothing falls back to raw, which is the
/// whole point of the policy: a block whose values the transform cannot
/// describe is an error the caller asked for by choosing it.
fn fixed_column_candidate(
    column: &Column,
    rows: &BlockRows<'_>,
    index: usize,
    valid_bits: &[bool],
    transform: WriterTransform,
    codec: WriterCodec,
    zstd_level: i32,
) -> Result<SelectedColumn> {
    // Section 8.1 gives an all-null column no value stream to transform, so
    // there is nothing here for the policy to apply and nothing to refuse.
    if valid_bits.iter().all(|valid| !*valid) {
        return materialize_selected(COLUMN_LAYOUT_PLAIN, Vec::new(), codec, zstd_level);
    }
    if transform == WriterTransform::Raw {
        return raw_column_candidate(column, rows, index, valid_bits, codec, zstd_level);
    }

    let dense = dense_values(column, rows, index, valid_bits);
    let (prepared, profile) = candidates::prepare(column.logical_type(), dense)
        .map_err(|error| fixed_refusal(transform, column, error))?;
    let value = candidates::encode(
        column.logical_type(),
        &prepared,
        &profile,
        value_encoding(transform),
    )
    .map_err(|error| fixed_refusal(transform, column, error))?;
    // The requested transform describes the dense values. Validity is an
    // independent boolean stream that it says nothing about, so it stays raw
    // rather than acquiring a representation the caller did not ask for.
    let streams = Validity::raw(valid_bits).prepend_to(value.streams)?;
    materialize_selected(value.layout, streams, codec, zstd_level)
}

/// Name the fixed policy and the column in an encoder error, and tag it like
/// every other block-level writer failure.
fn fixed_refusal(transform: WriterTransform, column: &Column, error: Error) -> Error {
    error
        .with_message_prefix(format!(
            "fixed transform {transform:?} on {}",
            column.name()
        ))
        .with_context(ErrorContext::Payload)
}

/// One column's dense values in block order, skipping the rows validity marks
/// absent. The values are streamed straight into the flat profiling buffers,
/// so no intermediate vector of scalars exists alongside them.
fn dense_values<'a>(
    column: &'a Column,
    rows: &'a BlockRows<'a>,
    index: usize,
    valid_bits: &'a [bool],
) -> impl Iterator<Item = Result<ScalarValue<'a>>> + 'a {
    rows.column(index)
        .zip(valid_bits)
        .filter(|(_, valid)| **valid)
        .map(move |((array, row), _)| {
            array.value_at(row).ok_or_else(|| {
                invalid_batch(format!(
                    "column {} has a missing valid value",
                    column.name()
                ))
            })
        })
}

fn value_encoding(transform: WriterTransform) -> ValueEncoding {
    match transform {
        WriterTransform::Raw => ValueEncoding::Raw,
        WriterTransform::BitPacked => ValueEncoding::BitPacked,
        WriterTransform::BooleanRle => ValueEncoding::BooleanRle,
        WriterTransform::FrameOfReference => ValueEncoding::FrameOfReference,
        WriterTransform::Delta => ValueEncoding::Delta,
        WriterTransform::DeltaOfDelta => ValueEncoding::DeltaOfDelta,
        WriterTransform::ByteStreamSplit => ValueEncoding::ByteStreamSplit,
        WriterTransform::Constant => ValueEncoding::Constant,
        WriterTransform::Dictionary => ValueEncoding::Dictionary,
        WriterTransform::RunLength => ValueEncoding::RunLength,
    }
}

fn codec_id(codec: WriterCodec) -> u16 {
    match codec {
        WriterCodec::None => CODEC_NONE,
        WriterCodec::Zstandard => CODEC_ZSTD,
    }
}

/// Store one transformed stream, taking its bytes so that the `none` codec
/// costs no copy of a buffer that is already exactly what will be written.
fn compress_stream(codec: WriterCodec, zstd_level: i32, bytes: Vec<u8>) -> Result<Vec<u8>> {
    match codec {
        WriterCodec::None => Ok(bytes),
        WriterCodec::Zstandard => compress_zstd(&bytes, zstd_level),
    }
}

#[cfg(feature = "zstd")]
fn compress_zstd(bytes: &[u8], level: i32) -> Result<Vec<u8>> {
    zstd::bulk::compress(bytes, level).map_err(|error| {
        Error::internal(format!("Zstandard compression failed: {error}"))
            .with_context(ErrorContext::Payload)
    })
}

/// Selecting Zstandard without the feature is refused when the writer is
/// created, so this exists only to keep [`compress_stream`] total.
#[cfg(not(feature = "zstd"))]
fn compress_zstd(_bytes: &[u8], _level: i32) -> Result<Vec<u8>> {
    Err(internal("Zstandard output requires the zstd feature"))
}

fn adaptive_column_candidate(
    column: &Column,
    rows: &BlockRows<'_>,
    index: usize,
    valid_bits: &[bool],
    codec: WriterCodec,
    zstd_level: i32,
) -> Result<SelectedColumn> {
    let null_count = valid_bits.iter().filter(|valid| !**valid).count();
    if null_count == valid_bits.len() {
        return materialize_selected(COLUMN_LAYOUT_PLAIN, Vec::new(), codec, zstd_level);
    }

    let dense = dense_values(column, rows, index, valid_bits);
    let (prepared, profile) = candidates::prepare(column.logical_type(), dense)?;
    let validity = Validity::plan(valid_bits, codec)?;

    let mut estimates = Vec::new();
    for candidate in candidates::candidates(column.logical_type(), &prepared, &profile) {
        // Raw values under a raw validity representation is the baseline this
        // shortlist is measured against, so it is never one of its entries.
        if candidate == ValueEncoding::Raw && validity.transform == TRANSFORM_RAW {
            continue;
        }
        if let Some(cost) = estimate_column_candidate(
            column.logical_type(),
            &prepared,
            &profile,
            candidate,
            &validity,
            codec,
        ) {
            estimates.push((cost, candidate));
        }
    }
    // The rank is unique per candidate, so this order is total and does not
    // depend on the order the candidates were offered in.
    estimates.sort_unstable_by_key(|(cost, candidate)| (*cost, candidate.rank()));

    let raw = raw_column_candidate(column, rows, index, valid_bits, codec, zstd_level)?;
    let raw_cost = stored_column_cost(&raw);
    let mut selected_cost = raw_cost;
    let mut selected = raw;
    let margin = raw_cost.div_ceil(100).max(64);

    // Only the two best cheap estimates are fully materialized. A failed
    // candidate is discarded; it can never affect the healthy raw fallback.
    for (_, candidate) in estimates.into_iter().take(2) {
        let Ok(value) = candidates::encode(column.logical_type(), &prepared, &profile, candidate)
        else {
            continue;
        };
        let Ok(streams) = validity.prepend_to(value.streams) else {
            continue;
        };
        let Ok(materialized) = materialize_selected(value.layout, streams, codec, zstd_level)
        else {
            continue;
        };
        let cost = stored_column_cost(&materialized);
        if cost.saturating_add(margin) <= raw_cost
            && (cost < selected_cost
                || (cost == selected_cost && materialized.layout < selected.layout))
        {
            selected_cost = cost;
            selected = materialized;
        }
    }
    Ok(selected)
}

/// The validity representation chosen for one adaptive column, materialized
/// once so that neither estimation nor candidate encoding rebuilds it.
///
/// Section 8.1 gives all-valid and all-null columns no validity stream at all.
/// Otherwise the choice is between section 9.1 raw bits and section 9.7
/// boolean RLE; bit packing is never offered, because a packed bitmap carries
/// a width byte the raw bitmap does not and so is always the larger of the two.
struct Validity {
    transform: u16,
    element_count: usize,
    bytes: Option<Vec<u8>>,
}

impl Validity {
    fn raw(valid_bits: &[bool]) -> Self {
        let implicit = valid_bits.is_empty()
            || valid_bits.iter().all(|valid| *valid)
            || valid_bits.iter().all(|valid| !*valid);
        Self {
            transform: TRANSFORM_RAW,
            element_count: valid_bits.len(),
            bytes: (!implicit).then(|| candidates::pack_booleans(valid_bits)),
        }
    }

    fn plan(valid_bits: &[bool], codec: WriterCodec) -> Result<Self> {
        let raw = Self::raw(valid_bits);
        let Some(raw_bytes) = raw.bytes.as_deref() else {
            return Ok(raw);
        };
        let rle = candidates::boolean_rle(valid_bits)?;
        let raw_cost = estimated_stream_cost(raw_bytes.len() as u64, codec);
        let rle_cost = estimated_stream_cost(rle.len() as u64, codec);
        if rle_cost.saturating_add(raw_cost.div_ceil(100).max(64)) <= raw_cost {
            return Ok(Self {
                transform: TRANSFORM_BOOLEAN_RLE,
                element_count: valid_bits.len(),
                bytes: Some(rle),
            });
        }
        Ok(raw)
    }

    /// The stored cost of the validity stream, or zero when there is none.
    fn estimated_cost(&self, codec: WriterCodec) -> Option<u64> {
        let Some(bytes) = self.bytes.as_deref() else {
            return Some(0);
        };
        estimated_stream_cost(bytes.len() as u64, codec).checked_add(BLOCK_STREAM_DESCRIPTOR_SIZE)
    }

    /// Put the validity stream ahead of the value streams.
    ///
    /// Section 8.2 fixes each stream's meaning by its kind rather than its
    /// position, and this reader finds streams by kind, so the order is a
    /// convention rather than a requirement. It is the order the raw writer has
    /// always used, and keeping it means a column's streams read the same way
    /// whichever policy produced them.
    fn prepend_to(&self, mut streams: Vec<CandidateStream>) -> Result<Vec<CandidateStream>> {
        let Some(bytes) = self.bytes.as_deref() else {
            return Ok(streams);
        };
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(bytes.len())
            .map_err(|_| resource("a validity stream"))?;
        owned.extend_from_slice(bytes);
        streams.insert(
            0,
            candidates::candidate_stream(
                STREAM_KIND_VALIDITY,
                self.transform,
                self.element_count,
                owned,
            )?,
        );
        Ok(streams)
    }
}

/// Price one candidate's complete stored column: every stream it emits, the
/// validity stream beside them, their padding, and their descriptors.
fn estimate_column_candidate(
    logical_type: &LogicalType,
    values: &PreparedValues,
    profile: &ValueProfile,
    candidate: ValueEncoding,
    validity: &Validity,
    codec: WriterCodec,
) -> Option<u64> {
    let lengths = candidates::estimate(logical_type, values, profile, candidate)?;
    let mut cost = u64::try_from(lengths.len())
        .ok()?
        .checked_mul(BLOCK_STREAM_DESCRIPTOR_SIZE)?;
    for length in lengths {
        cost = cost.checked_add(estimated_stream_cost(length, codec))?;
    }
    cost.checked_add(validity.estimated_cost(codec)?)
}

fn estimated_stream_cost(transformed_length: u64, codec: WriterCodec) -> u64 {
    // Codec framing has a small deterministic overhead. The actual codec size
    // is measured for raw and the two shortlisted candidates below.
    let codec_overhead = if matches!(codec, WriterCodec::Zstandard) {
        16
    } else {
        0
    };
    stream_payload_bytes(transformed_length.saturating_add(codec_overhead))
}

fn materialize_selected(
    layout: u16,
    streams: Vec<CandidateStream>,
    codec: WriterCodec,
    zstd_level: i32,
) -> Result<SelectedColumn> {
    let mut stored = Vec::new();
    stored
        .try_reserve_exact(streams.len())
        .map_err(|_| resource("stored candidate streams"))?;
    for stream in streams {
        let transformed_length = u64::try_from(stream.bytes.len())
            .map_err(|_| resource("transformed stream length does not fit uint64"))?;
        let compressed = compress_stream(codec, zstd_level, stream.bytes)?;
        stored.push(StoredStream {
            kind: stream.kind,
            transform: stream.transform,
            element_count: stream.element_count,
            transformed_length,
            stored: compressed,
        });
    }
    Ok(SelectedColumn {
        layout,
        streams: stored,
    })
}

fn stored_column_cost(column: &SelectedColumn) -> u64 {
    let descriptors = (column.streams.len() as u64).saturating_mul(BLOCK_STREAM_DESCRIPTOR_SIZE);
    column.streams.iter().fold(descriptors, |total, stream| {
        total.saturating_add(stream_payload_bytes(stream.stored.len() as u64))
    })
}

/// One validity bit per row, taken from the array's own answer about nullity so
/// that the bitmap written and the values encoded cannot disagree.
fn validity_bits(rows: &BlockRows<'_>, index: usize) -> Result<Vec<bool>> {
    let mut valid_bits = Vec::new();
    valid_bits
        .try_reserve_exact(rows.row_count)
        .map_err(|_| resource("unable to reserve a batch validity bitmap"))?;
    valid_bits.extend(rows.column(index).map(|(array, row)| !array.is_null(row)));
    Ok(valid_bits)
}

fn append_stream(pending: StoredStream, payload: &mut Vec<u8>) -> Result<EncodedStream> {
    let payload_offset = u64::try_from(payload.len())
        .map_err(|_| resource("the data payload offset does not fit uint64"))?;
    let stored_length = u64::try_from(pending.stored.len())
        .map_err(|_| resource("a stored stream length does not fit uint64"))?;
    let crc = checksum(&pending.stored);
    payload.extend_from_slice(&pending.stored);
    if pending.stored.is_empty() {
        // An empty stream would otherwise share its offset with the stream
        // after it. Section 8.2 forbids overlapping stream ranges without
        // saying whether an empty range can overlap, so every stream is given
        // an offset of its own rather than rest on a reader's reading of that.
        payload.resize(payload.len() + FRAME_ALIGNMENT as usize, 0);
    } else {
        pad_to_alignment(payload);
    }
    Ok(EncodedStream {
        kind: pending.kind,
        transform: pending.transform,
        element_count: pending.element_count,
        payload_offset,
        stored_length,
        transformed_length: pending.transformed_length,
        crc,
    })
}
/// The block's primary timestamp minimum, maximum, and `TS_SORTED` claim.
///
/// Section 8 requires both bounds to be zero and the flag clear when the schema
/// has no primary timestamp column.
fn primary_metadata(schema: &Schema, rows: &BlockRows<'_>) -> Result<(i64, i64, bool)> {
    let Some(primary_id) = schema.primary_column_id() else {
        return Ok((0, 0, false));
    };
    let (index, column) = schema
        .columns()
        .iter()
        .enumerate()
        .find(|(_, column)| column.id() == primary_id)
        .ok_or_else(|| invalid_batch("the primary column is not present in the schema"))?;

    // The first row seeds every answer, so the empty block is the only case the
    // fold below cannot state, and it is settled before the fold begins.
    let mut values = rows
        .column(index)
        .map(|(array, row)| primary_value(column, array, row));
    let first = values
        .next()
        .transpose()?
        .ok_or_else(|| invalid_batch("the primary column is empty"))?;
    let (mut minimum, mut maximum, mut previous, mut sorted) = (first, first, first, true);
    for value in values {
        let value = value?;
        minimum = minimum.min(value);
        maximum = maximum.max(value);
        sorted &= previous <= value;
        previous = value;
    }
    Ok((minimum, maximum, sorted))
}

/// One primary timestamp value as the signed count section 8 stores.
fn primary_value(column: &Column, array: &Array, row: usize) -> Result<i64> {
    match array.value_at(row) {
        Some(ScalarValue::Timestamp { value, .. })
            if matches!(column.logical_type(), LogicalType::Timestamp { .. }) =>
        {
            Ok(value)
        }
        Some(ScalarValue::Date32(value))
            if matches!(column.logical_type(), LogicalType::Date32) =>
        {
            Ok(i64::from(value))
        }
        None => Err(invalid_batch("the primary column contains a null")),
        _ => Err(invalid_batch(
            "the primary column has the wrong logical type",
        )),
    }
}
