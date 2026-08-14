//! Bounded block buffering and raw frame-size accounting.

use crate::array::{Array, ScalarValue};
use crate::batch::RecordBatch;
use crate::format::constants::{
    BLOCK_COLUMN_DESCRIPTOR_SIZE, BLOCK_STREAM_DESCRIPTOR_SIZE, DATA_BLOCK_HEADER_SIZE,
    FRAME_ALIGNMENT, PREFIX_SIZE, TRAILER_SIZE,
};
use crate::limits::Limits;
use crate::schema::{Column, LogicalType, Schema};

use super::api::WriterStatistics;

/// The bytes one variable-width value contributes to its lengths stream.
const LENGTH_ENTRY_BYTES: u64 = 4;

/// What one column's rows cost in a raw block, accumulated as rows arrive.
///
/// Section 8.2 derives every stream length from these counters and the column's
/// logical type, so a block can be priced without being serialized.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ColumnFootprint {
    dense_count: u64,
    null_count: u64,
    variable_bytes: u64,
}

impl ColumnFootprint {
    /// Add one row, which is either null or contributes a stored value.
    pub(super) fn add_row(&mut self, array: &Array, row: usize) {
        match array.value_at(row) {
            None => self.null_count += 1,
            Some(value) => {
                self.dense_count += 1;
                self.variable_bytes += variable_byte_length(value);
            }
        }
    }

    /// Whether this column's validity needs no stream of its own, which
    /// [`encode_column`] decides the same way.
    pub(super) fn has_implicit_validity(&self) -> bool {
        self.null_count == 0 || self.dense_count == 0
    }
}

/// The raw serialized geometry of one block.
///
/// Arithmetic here saturates rather than wrapping. Saturation can only make a
/// block look larger, and every size this large is already past a limit
/// [`Self::within_format_limits`] refuses, so it cannot pass off an
/// unwritable block as a writable one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct BlockSize {
    pub(super) header_bytes: u64,
    pub(super) payload_bytes: u64,
    pub(super) frame_bytes: u64,
}

impl BlockSize {
    /// Whether a block this large can be written at all.
    ///
    /// [`Limits`] bounds what a reader accepts, so a buffer that crosses one
    /// could never be published. The assembler stops before accepting such a
    /// row rather than discovering the problem at publication, when the rows
    /// are already committed to a block.
    pub(super) fn within_format_limits(&self, rows: u64) -> bool {
        let limits = Limits::default();
        rows <= limits.max_rows_per_block()
            && rows <= u64::from(u32::MAX)
            && self.header_bytes <= limits.max_frame_header_length()
            && self.payload_bytes <= limits.max_frame_payload_length()
    }
}

/// Price the raw frame while reserving the possible header statistics selected
/// by the writer policy. The value scan can still omit a pair for an all-NaN
/// column, so this is a conservative estimate for the buffered block.
pub(super) fn block_size_with_statistics(
    schema: &Schema,
    footprints: &[ColumnFootprint],
    rows: u64,
    statistics: WriterStatistics,
) -> BlockSize {
    let mut payload_bytes = 0_u64;
    let mut stream_count = 0_u64;
    let mut statistics_bytes = 0_u64;
    for (column, footprint) in schema.columns().iter().zip(footprints) {
        let (bytes, streams) = column_size(column, footprint, rows);
        payload_bytes = payload_bytes.saturating_add(bytes);
        stream_count = stream_count.saturating_add(streams);
        if should_reserve_statistics(schema, column, footprint, statistics) {
            statistics_bytes = statistics_bytes
                .saturating_add(statistics_width(column.logical_type()).saturating_mul(2));
        }
    }
    if statistics_bytes != 0 {
        statistics_bytes =
            statistics_bytes.saturating_add(FRAME_ALIGNMENT - 1) & !(FRAME_ALIGNMENT - 1);
    }

    let column_count = schema.column_count() as u64;
    let header_bytes = DATA_BLOCK_HEADER_SIZE
        .saturating_add(column_count.saturating_mul(BLOCK_COLUMN_DESCRIPTOR_SIZE))
        .saturating_add(stream_count.saturating_mul(BLOCK_STREAM_DESCRIPTOR_SIZE))
        .saturating_add(statistics_bytes);
    let frame_bytes = (PREFIX_SIZE as u64)
        .saturating_add(header_bytes)
        .saturating_add(payload_bytes)
        .saturating_add(TRAILER_SIZE as u64);
    BlockSize {
        header_bytes,
        payload_bytes,
        frame_bytes,
    }
}

fn should_reserve_statistics(
    schema: &Schema,
    column: &Column,
    footprint: &ColumnFootprint,
    statistics: WriterStatistics,
) -> bool {
    if footprint.dense_count == 0 || !statistics_width_supported(column.logical_type()) {
        return false;
    }
    if matches!(statistics, WriterStatistics::MinMax) {
        return true;
    }
    if !matches!(statistics, WriterStatistics::Automatic)
        || schema.primary_column_id() == Some(column.id())
        || footprint.dense_count < 64
    {
        return false;
    }
    let width = statistics_width(column.logical_type());
    let value_bytes = if matches!(column.logical_type(), LogicalType::Bool) {
        bitmap_bytes(footprint.dense_count)
    } else {
        width.saturating_mul(footprint.dense_count)
    };
    width.saturating_mul(2).saturating_mul(8) <= value_bytes
}

fn statistics_width_supported(logical_type: &LogicalType) -> bool {
    statistics_width(logical_type) != 0
}

/// Section 11 canonical widths, as [`super::encode::statistic_width`] answers
/// them, with zero standing for the types that carry no v0.2 statistic.
///
/// The two must agree. This one may only ever be the larger of the two: it
/// reserves header bytes that the value scan then decides whether to use, so
/// reserving for a column that ends up without a statistic wastes an estimate,
/// while failing to reserve for one that gets a statistic would let the writer
/// assemble a block it cannot publish.
fn statistics_width(logical_type: &LogicalType) -> u64 {
    match logical_type {
        LogicalType::Bool | LogicalType::Int8 | LogicalType::UInt8 => 1,
        LogicalType::Int16 | LogicalType::UInt16 => 2,
        LogicalType::Int32 | LogicalType::UInt32 | LogicalType::Float32 | LogicalType::Date32 => 4,
        LogicalType::Int64
        | LogicalType::UInt64
        | LogicalType::Float64
        | LogicalType::Decimal { .. }
        | LogicalType::Timestamp { .. } => 8,
        // A zero width has no canonical representation to store, so it reads as
        // unsupported here exactly as it does in the encoder.
        LogicalType::FixedBinary { byte_width } => u64::from(*byte_width),
        LogicalType::Utf8 | LogicalType::Categorical { .. } | LogicalType::Binary => 0,
    }
}

/// The payload bytes and stream count one column contributes to a raw block.
fn column_size(column: &Column, footprint: &ColumnFootprint, rows: u64) -> (u64, u64) {
    let mut bytes = 0_u64;
    let mut streams = 0_u64;
    if !footprint.has_implicit_validity() {
        bytes = bytes.saturating_add(stream_payload_bytes(bitmap_bytes(rows)));
        streams += 1;
    }
    if footprint.dense_count != 0 {
        let values = values_bytes(column.logical_type(), footprint);
        bytes = bytes.saturating_add(stream_payload_bytes(values));
        streams += 1;
        if is_variable_width(column.logical_type()) {
            let lengths = LENGTH_ENTRY_BYTES.saturating_mul(footprint.dense_count);
            bytes = bytes.saturating_add(stream_payload_bytes(lengths));
            streams += 1;
        }
    }
    (bytes, streams)
}

/// The raw bytes a column's values stream holds for its dense rows.
fn values_bytes(logical_type: &LogicalType, footprint: &ColumnFootprint) -> u64 {
    let value_width = match logical_type {
        LogicalType::Bool => return bitmap_bytes(footprint.dense_count),
        LogicalType::Utf8 | LogicalType::Categorical { .. } | LogicalType::Binary => {
            return footprint.variable_bytes;
        }
        LogicalType::Int8 | LogicalType::UInt8 => 1,
        LogicalType::Int16 | LogicalType::UInt16 => 2,
        LogicalType::Int32 | LogicalType::UInt32 | LogicalType::Float32 | LogicalType::Date32 => 4,
        LogicalType::Int64
        | LogicalType::UInt64
        | LogicalType::Float64
        | LogicalType::Decimal { .. }
        | LogicalType::Timestamp { .. } => 8,
        LogicalType::FixedBinary { byte_width } => u64::from(*byte_width),
    };
    value_width.saturating_mul(footprint.dense_count)
}

/// The stored bytes of a variable-width value. Fixed-width types are priced
/// from their row count instead, so they contribute nothing here.
fn variable_byte_length(value: ScalarValue<'_>) -> u64 {
    match value {
        ScalarValue::Utf8(value) | ScalarValue::Categorical(value) => value.len() as u64,
        ScalarValue::Binary(value) => value.len() as u64,
        _ => 0,
    }
}

/// Whether a logical type stores its values alongside a lengths stream.
pub(super) fn is_variable_width(logical_type: &LogicalType) -> bool {
    matches!(
        logical_type,
        LogicalType::Utf8 | LogicalType::Categorical { .. } | LogicalType::Binary
    )
}

/// The payload bytes one stream occupies, including its alignment padding.
///
/// This mirrors [`append_stream`], where an empty stream is still given a whole
/// alignment unit so that no two streams share a payload offset.
pub(super) fn stream_payload_bytes(length: u64) -> u64 {
    if length == 0 {
        return FRAME_ALIGNMENT;
    }
    length.saturating_add(FRAME_ALIGNMENT - 1) & !(FRAME_ALIGNMENT - 1)
}

/// The bytes a packed bitmap of `bits` bits occupies.
pub(super) fn bitmap_bytes(bits: u64) -> u64 {
    bits.div_ceil(8)
}

/// The rows accepted for the next block, held as the batches they arrived in.
///
/// Keeping the appended batches side by side rather than concatenating them is
/// what makes filling a block cost its rows instead of its rows squared: an
/// appended batch is copied at most once, and only when it straddles a block
/// boundary. The footprints alongside them price the block as it grows.
#[derive(Debug)]
pub(super) struct BlockBuffer {
    chunks: Vec<RecordBatch>,
    rows: usize,
    footprints: Vec<ColumnFootprint>,
    size: BlockSize,
    statistics: WriterStatistics,
}

impl BlockBuffer {
    pub(super) fn new_with_statistics(column_count: usize, statistics: WriterStatistics) -> Self {
        Self {
            chunks: Vec::new(),
            rows: 0,
            footprints: vec![ColumnFootprint::default(); column_count],
            size: BlockSize::default(),
            statistics,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub(super) fn row_count(&self) -> u64 {
        self.rows as u64
    }

    /// The raw frame length these rows would serialize to.
    pub(super) fn frame_bytes(&self) -> u64 {
        self.size.frame_bytes
    }

    pub(super) fn footprints(&self) -> &[ColumnFootprint] {
        &self.footprints
    }

    /// The buffered rows in file order, as one block's worth of columns.
    pub(super) fn rows(&self) -> BlockRows<'_> {
        BlockRows {
            chunks: &self.chunks,
            row_count: self.rows,
        }
    }

    /// Accept a batch whose rows the assembler has already priced.
    pub(super) fn push(&mut self, schema: &Schema, chunk: RecordBatch) {
        for (footprint, array) in self.footprints.iter_mut().zip(chunk.columns()) {
            for row in 0..chunk.row_count() {
                footprint.add_row(array, row);
            }
        }
        self.rows += chunk.row_count();
        self.size =
            block_size_with_statistics(schema, &self.footprints, self.row_count(), self.statistics);
        self.chunks.push(chunk);
    }

    /// Drop every buffered row, which the caller has just published.
    pub(super) fn clear(&mut self) {
        self.chunks.clear();
        self.rows = 0;
        self.footprints.fill(ColumnFootprint::default());
        self.size = BlockSize::default();
    }
}

/// One block's rows, in file order across the batches they were appended in.
pub(super) struct BlockRows<'a> {
    pub(super) chunks: &'a [RecordBatch],
    pub(super) row_count: usize,
}

impl<'a> BlockRows<'a> {
    /// One column's rows, in file order, each as the array holding it.
    pub(super) fn column(&self, index: usize) -> impl Iterator<Item = (&'a Array, usize)> {
        self.chunks
            .iter()
            .filter_map(move |chunk| chunk.column(index))
            .flat_map(|array| (0..array.len()).map(move |row| (array, row)))
    }
}
