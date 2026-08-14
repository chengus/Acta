//! Checked parsing of the fixed metadata portion of a v0.2 data frame.

use std::fs::File;

use crate::error::{Error, ErrorContext, Result};
use crate::limits::Limits;
use crate::schema::{Column, Schema};

use super::constants::{
    BLOCK_BASE_ROW_ID_FIELD, BLOCK_COLUMN_COUNT_FIELD, BLOCK_COLUMN_DESCRIPTOR_SIZE,
    BLOCK_COLUMN_TABLE_OFFSET_FIELD, BLOCK_FLAGS_FIELD, BLOCK_PRIMARY_MIN_FIELD,
    BLOCK_RESERVED_SIZE, BLOCK_ROW_COUNT_FIELD, BLOCK_SCHEMA_ID_FIELD,
    BLOCK_STATISTICS_LENGTH_FIELD, BLOCK_STATISTICS_OFFSET_FIELD, BLOCK_STREAM_DESCRIPTOR_SIZE,
    BLOCK_STREAM_TABLE_OFFSET_FIELD, CODEC_NONE, CODEC_ZSTD, COLUMN_HAS_STATS_FLAG,
    COLUMN_IMPLICIT_VALIDITY_FLAG, COLUMN_LAYOUT_RUN_LENGTH, DATA_BLOCK_HEADER_SIZE,
    FIRST_BASE_ROW_ID, ROW_IDS_BLOCK_FLAG, ROW_IDS_FEATURE, STATS_MIN_MAX, STATS_NONE,
    STREAM_KIND_RUN_LENGTHS, STREAM_KIND_VALIDITY, TRANSFORM_BOOLEAN_RLE, TS_SORTED_BLOCK_FLAG,
    UNAVAILABLE_BASE_ROW_ID,
};
use super::cursor::Cursor;
use super::frame::FrameMetadata;
use super::{field_offset, read_exact_at};

/// Metadata extracted from the fixed 64-byte block header.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DataFrameMetadata {
    pub(crate) row_count: u64,
    pub(crate) base_row_id: Option<u64>,
    pub(crate) primary_bounds: Option<(i64, i64)>,
    pub(crate) ts_sorted: bool,
}

/// A validated logical-column descriptor and its contiguous stream range.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ColumnBlockDescriptor {
    pub(crate) column_id: u32,
    pub(crate) layout: u16,
    pub(crate) flags: u16,
    pub(crate) null_count: u32,
    pub(crate) dense_count: u32,
    pub(crate) first_stream: u32,
    pub(crate) stream_count: u16,
    pub(crate) stats_kind: u16,
    pub(crate) stats_offset: u32,
    pub(crate) stats_length: u32,
}

/// A validated physical stream descriptor. Its bytes remain in the file and
/// are read only when a block is decoded.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StreamDescriptor {
    pub(crate) kind: u16,
    pub(crate) transform: u16,
    pub(crate) codec: u16,
    pub(crate) payload_offset: u64,
    pub(crate) stored_length: u64,
    pub(crate) transformed_length: u64,
    pub(crate) element_count: u64,
    pub(crate) crc: u32,
}

/// The checked descriptor tables needed by the codec for one block.
#[derive(Debug, Clone)]
pub(crate) struct BlockLayout {
    pub(crate) metadata: DataFrameMetadata,
    pub(crate) columns: Vec<ColumnBlockDescriptor>,
    pub(crate) streams: Vec<StreamDescriptor>,
}

/// Parse the fixed block header after generic frame validation has completed.
pub(crate) fn parse(
    file: &mut File,
    frame: &FrameMetadata,
    schema: &Schema,
    feature_flags: u64,
    expected_base_row_id: Option<u64>,
) -> Result<DataFrameMetadata> {
    check_frame_type(frame)?;

    let fields = read_header(file, frame)?;
    let schema_column_count = check_schema_agreement(&fields, schema, frame)?;
    validate_layout(
        &fields,
        frame.header_length,
        schema_column_count,
        frame.sequence,
        frame.header_offset,
    )?;
    check_flags(&fields, frame)?;

    let base_row_id = resolve_base_row_id(&fields, feature_flags, expected_base_row_id, frame)?;
    let ts_sorted = fields.flags & TS_SORTED_BLOCK_FLAG != 0;
    let primary_bounds = resolve_primary_bounds(&fields, schema, ts_sorted, frame)?;

    Ok(DataFrameMetadata {
        row_count: u64::from(fields.row_count),
        base_row_id,
        primary_bounds,
        ts_sorted,
    })
}

/// Read and validate the complete data-frame header for a logical decode.
pub(crate) fn read_layout(
    file: &mut File,
    frame: &FrameMetadata,
    schema: &Schema,
    feature_flags: u64,
    expected_base_row_id: Option<u64>,
    limits: Limits,
) -> Result<BlockLayout> {
    read_block_layout(
        file,
        frame,
        schema,
        feature_flags,
        expected_base_row_id,
        limits,
        None,
    )
}

/// Read a block layout while deferring unsupported transform/codec handling for
/// columns that the caller will not decode. Structural descriptor, range,
/// length, and envelope checks still cover the entire block.
pub(crate) fn read_selected_layout(
    file: &mut File,
    frame: &FrameMetadata,
    schema: &Schema,
    feature_flags: u64,
    expected_base_row_id: Option<u64>,
    limits: Limits,
    selected_column_ids: &[u32],
) -> Result<BlockLayout> {
    read_block_layout(
        file,
        frame,
        schema,
        feature_flags,
        expected_base_row_id,
        limits,
        Some(selected_column_ids),
    )
}

/// Read the block header and parse it. `selected_column_ids` is `None` for a
/// decode that will read every column, which is the only difference between
/// the two entry points above.
fn read_block_layout(
    file: &mut File,
    frame: &FrameMetadata,
    schema: &Schema,
    feature_flags: u64,
    expected_base_row_id: Option<u64>,
    limits: Limits,
    selected_column_ids: Option<&[u32]>,
) -> Result<BlockLayout> {
    let header_length = usize::try_from(frame.header_length).map_err(|_| {
        data_error(
            Error::resource_limit("data frame header does not fit this platform", None),
            frame.sequence,
            ErrorContext::Header,
        )
    })?;
    let mut header = Vec::new();
    header.try_reserve_exact(header_length).map_err(|_| {
        data_error(
            Error::resource_limit("unable to allocate the data frame header", None),
            frame.sequence,
            ErrorContext::Header,
        )
    })?;
    header.resize(header_length, 0);
    read_exact_at(file, frame.header_offset, &mut header)
        .map_err(|error| data_error(error, frame.sequence, ErrorContext::Header))?;

    parse_layout(
        &header,
        frame,
        schema,
        feature_flags,
        expected_base_row_id,
        limits,
        selected_column_ids,
    )
}

fn parse_layout(
    header: &[u8],
    frame: &FrameMetadata,
    schema: &Schema,
    feature_flags: u64,
    expected_base_row_id: Option<u64>,
    limits: Limits,
    selected_column_ids: Option<&[u32]>,
) -> Result<BlockLayout> {
    check_frame_type(frame)?;

    let fixed_header = header
        .get(..DATA_BLOCK_HEADER_SIZE as usize)
        .ok_or_else(|| {
            data_error(
                Error::corruption("data frame header is shorter than 64 bytes", None),
                frame.sequence,
                ErrorContext::Header,
            )
        })?;
    let fixed_header: &[u8; DATA_BLOCK_HEADER_SIZE as usize] =
        fixed_header.try_into().map_err(|_| {
            data_error(
                Error::corruption("invalid fixed data frame header", None),
                frame.sequence,
                ErrorContext::Header,
            )
        })?;
    let fields = parse_header(fixed_header, frame.header_offset)
        .map_err(|error| data_error(error, frame.sequence, ErrorContext::Header))?;
    let schema_column_count = check_schema_agreement(&fields, schema, frame)?;
    validate_layout(
        &fields,
        frame.header_length,
        schema_column_count,
        frame.sequence,
        frame.header_offset,
    )?;
    check_flags(&fields, frame)?;
    let base_row_id = resolve_base_row_id(&fields, feature_flags, expected_base_row_id, frame)?;
    let ts_sorted = fields.flags & TS_SORTED_BLOCK_FLAG != 0;
    let primary_bounds = resolve_primary_bounds(&fields, schema, ts_sorted, frame)?;
    let metadata = DataFrameMetadata {
        row_count: u64::from(fields.row_count),
        base_row_id,
        primary_bounds,
        ts_sorted,
    };

    // Columns are parsed first only so that a stream-descriptor rejection can
    // name the column that owns the stream. Section 8.1 gives each column a
    // contiguous stream range, so the ownership is a lookup once the column
    // table is in hand.
    let stream_count = stream_count(&fields, frame)?;
    let columns = parse_columns(header, &fields, schema, stream_count, frame, limits)?;
    let owners = StreamOwners {
        columns: &columns,
        schema,
        selected_column_ids,
    };
    let streams = parse_streams(header, &fields, stream_count, frame, limits, &owners)?;
    validate_stream_ranges(
        &streams,
        frame.payload_length,
        frame.sequence,
        frame.header_offset,
    )?;

    Ok(BlockLayout {
        metadata,
        columns,
        streams,
    })
}

fn check_frame_type(frame: &FrameMetadata) -> Result<()> {
    if frame.frame_type != super::constants::DATA_FRAME_TYPE {
        return Err(
            Error::corruption("expected a data frame", Some(frame.frame_offset)).with_context(
                ErrorContext::Frame {
                    sequence: frame.sequence,
                },
            ),
        );
    }
    Ok(())
}

fn read_header(file: &mut File, frame: &FrameMetadata) -> Result<BlockHeaderFields> {
    let mut bytes = [0_u8; DATA_BLOCK_HEADER_SIZE as usize];
    read_exact_at(file, frame.header_offset, &mut bytes)
        .map_err(|error| data_error(error, frame.sequence, ErrorContext::Header))?;
    parse_header(&bytes, frame.header_offset)
        .map_err(|error| data_error(error, frame.sequence, ErrorContext::Header))
}

/// Check the block against the one schema every frame in the file shares, and
/// return the schema's column count for the layout arithmetic that follows.
fn check_schema_agreement(
    fields: &BlockHeaderFields,
    schema: &Schema,
    frame: &FrameMetadata,
) -> Result<u64> {
    let header_error = |error| data_error(error, frame.sequence, ErrorContext::Header);
    let at = |field| field_offset(frame.header_offset, field);

    if fields.schema_id != schema.schema_id() {
        return Err(header_error(Error::corruption(
            format!(
                "data frame schema ID {} does not match schema {}",
                fields.schema_id,
                schema.schema_id()
            ),
            at(BLOCK_SCHEMA_ID_FIELD),
        )));
    }
    let schema_column_count = u64::try_from(schema.column_count()).map_err(|_| {
        header_error(Error::resource_limit(
            "schema column count does not fit this platform",
            None,
        ))
    })?;
    if u64::from(fields.column_count) != schema_column_count {
        return Err(header_error(Error::corruption(
            format!(
                "data frame column count {} does not match schema {schema_column_count}",
                fields.column_count
            ),
            at(BLOCK_COLUMN_COUNT_FIELD),
        )));
    }
    if fields.row_count == 0 {
        return Err(header_error(Error::corruption(
            "data frame row count must be nonzero",
            at(BLOCK_ROW_COUNT_FIELD),
        )));
    }
    Ok(schema_column_count)
}

fn check_flags(fields: &BlockHeaderFields, frame: &FrameMetadata) -> Result<()> {
    if fields.flags & !(ROW_IDS_BLOCK_FLAG | TS_SORTED_BLOCK_FLAG) != 0 {
        return Err(data_error(
            Error::corruption(
                format!("unknown data block flags 0x{:x}", fields.flags),
                field_offset(frame.header_offset, BLOCK_FLAGS_FIELD),
            ),
            frame.sequence,
            ErrorContext::Header,
        ));
    }
    Ok(())
}

/// Section 8: with `ROW_IDS` enabled the first block starts at zero and every
/// later base continues the previous block; with it disabled every base is the
/// unavailable sentinel.
fn resolve_base_row_id(
    fields: &BlockHeaderFields,
    feature_flags: u64,
    expected_base_row_id: Option<u64>,
    frame: &FrameMetadata,
) -> Result<Option<u64>> {
    let header_error = |error| data_error(error, frame.sequence, ErrorContext::Header);
    let at = |field| field_offset(frame.header_offset, field);

    let row_ids_enabled = feature_flags & ROW_IDS_FEATURE != 0;
    if row_ids_enabled != (fields.flags & ROW_IDS_BLOCK_FLAG != 0) {
        return Err(header_error(Error::corruption(
            "data block ROW_IDS flag does not match the file feature flags",
            at(BLOCK_FLAGS_FIELD),
        )));
    }

    if !row_ids_enabled {
        if fields.base_row_id != UNAVAILABLE_BASE_ROW_ID {
            return Err(header_error(Error::corruption(
                "data block base row ID must be UINT64_MAX when ROW_IDS is disabled",
                at(BLOCK_BASE_ROW_ID_FIELD),
            )));
        }
        return Ok(None);
    }

    let expected = expected_base_row_id.unwrap_or(FIRST_BASE_ROW_ID);
    if fields.base_row_id != expected {
        return Err(header_error(Error::corruption(
            format!(
                "data block base row ID {} does not match expected {expected}",
                fields.base_row_id
            ),
            at(BLOCK_BASE_ROW_ID_FIELD),
        )));
    }
    Ok(Some(fields.base_row_id))
}

/// Section 8: bounds are meaningful only with a primary column. Without one
/// they are absence sentinels that MUST be zero, and `TS_SORTED` MUST be clear.
fn resolve_primary_bounds(
    fields: &BlockHeaderFields,
    schema: &Schema,
    ts_sorted: bool,
    frame: &FrameMetadata,
) -> Result<Option<(i64, i64)>> {
    let header_error = |error| data_error(error, frame.sequence, ErrorContext::Header);
    let at = |field| field_offset(frame.header_offset, field);

    if schema.primary_column_id().is_some() {
        if fields.primary_min > fields.primary_max {
            return Err(header_error(Error::corruption(
                "primary bounds have minimum greater than maximum",
                at(BLOCK_PRIMARY_MIN_FIELD),
            )));
        }
        return Ok(Some((fields.primary_min, fields.primary_max)));
    }

    if fields.primary_min != 0 || fields.primary_max != 0 {
        return Err(header_error(Error::corruption(
            "a schema without a primary column must have zero bounds",
            at(BLOCK_PRIMARY_MIN_FIELD),
        )));
    }
    if ts_sorted {
        return Err(header_error(Error::corruption(
            "TS_SORTED requires a primary timestamp/date column",
            at(BLOCK_FLAGS_FIELD),
        )));
    }
    Ok(None)
}

fn stream_count(fields: &BlockHeaderFields, frame: &FrameMetadata) -> Result<usize> {
    let length = fields
        .statistics_offset
        .checked_sub(fields.stream_table_offset)
        .ok_or_else(|| {
            data_error(
                Error::corruption("statistics area precedes the stream table", None),
                frame.sequence,
                ErrorContext::Header,
            )
        })?;
    let count = length / super::constants::BLOCK_STREAM_DESCRIPTOR_SIZE;
    usize::try_from(count).map_err(|_| {
        data_error(
            Error::resource_limit("stream descriptor count does not fit this platform", None),
            frame.sequence,
            ErrorContext::Header,
        )
    })
}

/// Which column owns each stream in a block, and which of those columns the
/// caller intends to decode.
///
/// Section 8.1 gives every column a contiguous stream range, so ownership is a
/// lookup once the column table is parsed. Both the error messages and the
/// decision to defer a transform or codec check need that same lookup.
struct StreamOwners<'a> {
    columns: &'a [ColumnBlockDescriptor],
    schema: &'a Schema,
    /// `None` when the caller will decode every column, so every stream is
    /// checked in full.
    selected_column_ids: Option<&'a [u32]>,
}

impl StreamOwners<'_> {
    /// The column whose stream range covers `index`, if any does.
    fn owner_of(&self, index: usize) -> Option<&ColumnBlockDescriptor> {
        let index = index as u64;
        self.columns.iter().find(|column| {
            let first = u64::from(column.first_stream);
            index >= first && index < first + u64::from(column.stream_count)
        })
    }

    /// Name the stream for an error message.
    fn describe(&self, index: usize) -> String {
        match self
            .owner_of(index)
            .and_then(|column| self.schema.column_by_id(column.column_id))
            .map(Column::name)
        {
            Some(name) => format!("column {name}, stream {index}"),
            None => format!("stream {index} (owned by no column)"),
        }
    }

    /// Whether this stream belongs to a column the caller will decode.
    ///
    /// A stream owned by no column is treated as selected: it is not one this
    /// read can legitimately skip, so it keeps its full validation.
    fn is_selected(&self, index: usize) -> bool {
        let Some(selected) = self.selected_column_ids else {
            return true;
        };
        match self.owner_of(index) {
            Some(column) => selected.contains(&column.column_id),
            None => true,
        }
    }
}

fn parse_streams(
    header: &[u8],
    fields: &BlockHeaderFields,
    stream_count: usize,
    frame: &FrameMetadata,
    limits: Limits,
    owners: &StreamOwners<'_>,
) -> Result<Vec<StreamDescriptor>> {
    // Only ever called to build an error message, so the linear search costs
    // nothing on the path that matters.
    let owner = |index: usize| -> String { owners.describe(index) };
    let table_offset = usize::try_from(fields.stream_table_offset).map_err(|_| {
        data_error(
            Error::resource_limit("stream table offset does not fit this platform", None),
            frame.sequence,
            ErrorContext::Header,
        )
    })?;
    let table_bytes = stream_count.checked_mul(48).ok_or_else(|| {
        data_error(
            Error::corruption("stream table length overflow", None),
            frame.sequence,
            ErrorContext::Header,
        )
    })?;
    let table_end = table_offset.checked_add(table_bytes).ok_or_else(|| {
        data_error(
            Error::corruption("stream table end overflow", None),
            frame.sequence,
            ErrorContext::Header,
        )
    })?;
    if header.get(table_offset..table_end).is_none() {
        return Err(data_error(
            Error::corruption("stream table exceeds the frame header", None),
            frame.sequence,
            ErrorContext::Header,
        ));
    }

    let mut streams = Vec::new();
    streams.try_reserve_exact(stream_count).map_err(|_| {
        data_error(
            Error::resource_limit("unable to allocate stream descriptors", None),
            frame.sequence,
            ErrorContext::Header,
        )
    })?;
    for index in 0..stream_count {
        let offset = table_offset + index * 48;
        let bytes = &header[offset..offset + 48];
        let mut cursor = Cursor::new(bytes, frame.header_offset + offset as u64);
        let kind = cursor.read_u16("stream kind")?;
        let transform = cursor.read_u16("stream transform")?;
        let codec = cursor.read_u16("stream compression codec")?;
        let flags = cursor.read_u16("stream flags")?;
        let payload_offset = cursor.read_u64("stream payload offset")?;
        let stored_length = cursor.read_u64("stream stored length")?;
        let transformed_length = cursor.read_u64("stream transformed length")?;
        let element_count = cursor.read_u64("stream element count")?;
        let crc = cursor.read_u32("stream CRC32C")?;
        cursor.skip(4, "stream reserved field")?;
        if flags != 0 {
            return Err(data_error(
                Error::corruption(
                    format!("{}: unknown stream flags 0x{flags:x}", owner(index)),
                    field_offset(frame.header_offset + offset as u64, 6),
                ),
                frame.sequence,
                ErrorContext::Header,
            ));
        }
        ensure_stream_kind(kind, &owner, index, frame)?;
        // A transform or codec this build cannot apply is only a problem for a
        // stream this read will decode. Deferring it lets a projection succeed
        // beside a column it never touches, while a read of that column still
        // fails here.
        if owners.is_selected(index) {
            ensure_transform(transform, &owner, index, frame)?;
            ensure_codec(codec, &owner, index, frame)?;
        }
        if payload_offset % super::constants::FRAME_ALIGNMENT != 0 {
            return Err(data_error(
                Error::corruption(
                    format!("{}: payload offset is not eight-byte aligned", owner(index)),
                    field_offset(frame.header_offset + offset as u64, 8),
                ),
                frame.sequence,
                ErrorContext::Header,
            ));
        }
        if stored_length > limits.max_frame_payload_length()
            || transformed_length > limits.max_frame_payload_length()
        {
            return Err(data_error(
                Error::resource_limit(
                    format!(
                        "{}: length exceeds the configured payload limit",
                        owner(index)
                    ),
                    field_offset(frame.header_offset + offset as u64, 16),
                ),
                frame.sequence,
                ErrorContext::Header,
            ));
        }
        if codec == CODEC_NONE && stored_length != transformed_length {
            return Err(data_error(
                Error::corruption(
                    format!("{}: unequal uncompressed lengths", owner(index)),
                    field_offset(frame.header_offset + offset as u64, 16),
                ),
                frame.sequence,
                ErrorContext::Header,
            ));
        }
        streams.push(StreamDescriptor {
            kind,
            transform,
            codec,
            payload_offset,
            stored_length,
            transformed_length,
            element_count,
            crc,
        });
    }
    Ok(streams)
}

fn parse_columns(
    header: &[u8],
    fields: &BlockHeaderFields,
    schema: &Schema,
    stream_count: usize,
    frame: &FrameMetadata,
    limits: Limits,
) -> Result<Vec<ColumnBlockDescriptor>> {
    let column_count = usize::try_from(fields.column_count).map_err(|_| {
        data_error(
            Error::resource_limit("column count does not fit this platform", None),
            frame.sequence,
            ErrorContext::Header,
        )
    })?;
    if u64::from(fields.column_count) > limits.max_schema_columns() {
        return Err(data_error(
            Error::resource_limit("data frame column count exceeds the configured limit", None),
            frame.sequence,
            ErrorContext::Header,
        ));
    }
    let table_offset = usize::try_from(fields.column_table_offset).map_err(|_| {
        data_error(
            Error::resource_limit("column table offset does not fit this platform", None),
            frame.sequence,
            ErrorContext::Header,
        )
    })?;
    let table_end = table_offset
        .checked_add(column_count.checked_mul(32).ok_or_else(|| {
            data_error(
                Error::corruption("column table length overflow", None),
                frame.sequence,
                ErrorContext::Header,
            )
        })?)
        .ok_or_else(|| {
            data_error(
                Error::corruption("column table end overflow", None),
                frame.sequence,
                ErrorContext::Header,
            )
        })?;
    if header.get(table_offset..table_end).is_none() {
        return Err(data_error(
            Error::corruption("column table exceeds the frame header", None),
            frame.sequence,
            ErrorContext::Header,
        ));
    }

    let mut schema_columns: Vec<(u32, usize)> = Vec::new();
    schema_columns
        .try_reserve_exact(schema.column_count())
        .map_err(|_| {
            data_error(
                Error::resource_limit("unable to allocate schema column index", None),
                frame.sequence,
                ErrorContext::Header,
            )
        })?;
    for (index, column) in schema.columns().iter().enumerate() {
        schema_columns.push((column.id(), index));
    }
    schema_columns.sort_unstable_by_key(|(id, _)| *id);

    let mut columns = Vec::new();
    columns.try_reserve_exact(column_count).map_err(|_| {
        data_error(
            Error::resource_limit("unable to allocate column descriptors", None),
            frame.sequence,
            ErrorContext::Header,
        )
    })?;
    for (position, (expected_id, schema_index)) in schema_columns.iter().enumerate() {
        let offset = table_offset + position * 32;
        let bytes = &header[offset..offset + 32];
        let mut cursor = Cursor::new(bytes, frame.header_offset + offset as u64);
        let descriptor = ColumnBlockDescriptor {
            column_id: cursor.read_u32("data column ID")?,
            layout: cursor.read_u16("column layout")?,
            flags: cursor.read_u16("column flags")?,
            null_count: cursor.read_u32("column null count")?,
            dense_count: cursor.read_u32("column dense count")?,
            first_stream: cursor.read_u32("column first stream")?,
            stream_count: cursor.read_u16("column stream count")?,
            stats_kind: cursor.read_u16("column statistics kind")?,
            stats_offset: cursor.read_u32("column statistics offset")?,
            stats_length: cursor.read_u32("column statistics length")?,
        };
        if descriptor.column_id != *expected_id {
            return Err(data_error(
                Error::corruption(
                    format!("data column descriptor order does not match schema ID {expected_id}"),
                    field_offset(frame.header_offset + offset as u64, 0),
                ),
                frame.sequence,
                ErrorContext::Header,
            ));
        }
        validate_column_descriptor(
            &descriptor,
            &schema.columns()[*schema_index],
            u64::from(fields.row_count),
            fields.statistics_offset,
            fields.statistics_length,
            stream_count,
            frame,
        )?;
        columns.push(descriptor);
    }
    Ok(columns)
}

/// Check one column's descriptor against its schema column.
///
/// Every rejection here names the column: a descriptor table is a list of
/// near-identical 32-byte records, so an operator reading the error needs to
/// know which one of them is wrong.
fn validate_column_descriptor(
    descriptor: &ColumnBlockDescriptor,
    column: &Column,
    row_count: u64,
    statistics_offset: u64,
    statistics_length: u64,
    stream_count: usize,
    frame: &FrameMetadata,
) -> Result<()> {
    let offset = frame.header_offset;
    let nullable = column.is_nullable();
    let named = |message: String| format!("column {}: {message}", column.name());
    if descriptor.flags & !(COLUMN_IMPLICIT_VALIDITY_FLAG | COLUMN_HAS_STATS_FLAG) != 0 {
        return Err(data_error(
            Error::corruption(
                named(format!("unknown column flags 0x{:x}", descriptor.flags)),
                Some(offset),
            ),
            frame.sequence,
            ErrorContext::Header,
        ));
    }
    if descriptor.layout > COLUMN_LAYOUT_RUN_LENGTH {
        return Err(data_error(
            Error::unsupported_frame(
                named(format!("unsupported column layout {}", descriptor.layout)),
                Some(offset),
            ),
            frame.sequence,
            ErrorContext::Header,
        ));
    }
    if u64::from(descriptor.null_count) > row_count
        || u64::from(descriptor.dense_count) != row_count - u64::from(descriptor.null_count)
    {
        return Err(data_error(
            Error::corruption(
                named("dense count does not equal rows minus nulls".to_owned()),
                Some(offset),
            ),
            frame.sequence,
            ErrorContext::Header,
        ));
    }
    if !nullable && descriptor.null_count != 0 {
        return Err(data_error(
            Error::corruption(
                named("a non-nullable column has nulls".to_owned()),
                Some(offset),
            ),
            frame.sequence,
            ErrorContext::Header,
        ));
    }
    let stream_end = u64::from(descriptor.first_stream)
        .checked_add(u64::from(descriptor.stream_count))
        .ok_or_else(|| {
            data_error(
                Error::corruption(named("stream range overflow".to_owned()), Some(offset)),
                frame.sequence,
                ErrorContext::Header,
            )
        })?;
    let stream_count = u64::try_from(stream_count).map_err(|_| {
        data_error(
            Error::resource_limit(
                named("stream descriptor count does not fit this format".to_owned()),
                None,
            ),
            frame.sequence,
            ErrorContext::Header,
        )
    })?;
    if stream_end > stream_count {
        return Err(data_error(
            Error::corruption(
                named("stream range exceeds the stream table".to_owned()),
                Some(offset),
            ),
            frame.sequence,
            ErrorContext::Header,
        ));
    }
    if descriptor.stats_kind > STATS_MIN_MAX {
        return Err(data_error(
            Error::unsupported_frame(
                named(format!(
                    "unsupported statistics kind {}",
                    descriptor.stats_kind
                )),
                Some(offset),
            ),
            frame.sequence,
            ErrorContext::Header,
        ));
    }
    let has_stats = descriptor.flags & COLUMN_HAS_STATS_FLAG != 0;
    if has_stats != (descriptor.stats_kind != STATS_NONE) {
        return Err(data_error(
            Error::corruption(
                named("statistics flag and kind disagree".to_owned()),
                Some(offset),
            ),
            frame.sequence,
            ErrorContext::Header,
        ));
    }
    if !has_stats && (descriptor.stats_offset != 0 || descriptor.stats_length != 0) {
        return Err(data_error(
            Error::corruption(
                named(
                    "a column without statistics must have zero statistics offset and length"
                        .to_owned(),
                ),
                Some(offset),
            ),
            frame.sequence,
            ErrorContext::Header,
        ));
    }
    if has_stats {
        let stats_end = u64::from(descriptor.stats_offset)
            .checked_add(u64::from(descriptor.stats_length))
            .ok_or_else(|| {
                data_error(
                    Error::corruption(named("statistics range overflow".to_owned()), Some(offset)),
                    frame.sequence,
                    ErrorContext::Header,
                )
            })?;
        let area_end = statistics_offset
            .checked_add(statistics_length)
            .ok_or_else(|| {
                data_error(
                    Error::corruption(
                        named("statistics area range overflow".to_owned()),
                        Some(offset),
                    ),
                    frame.sequence,
                    ErrorContext::Header,
                )
            })?;
        if u64::from(descriptor.stats_offset) < statistics_offset || stats_end > area_end {
            return Err(data_error(
                Error::corruption(
                    named("statistics lie outside the statistics area".to_owned()),
                    Some(offset),
                ),
                frame.sequence,
                ErrorContext::Header,
            ));
        }
    }
    Ok(())
}

fn validate_stream_ranges(
    streams: &[StreamDescriptor],
    payload_length: u64,
    sequence: u64,
    header_offset: u64,
) -> Result<()> {
    let mut order: Vec<usize> = Vec::new();
    order.try_reserve(streams.len()).map_err(|_| {
        data_error(
            Error::resource_limit("unable to allocate stream range index", None),
            sequence,
            ErrorContext::Header,
        )
    })?;
    order.extend(0..streams.len());
    order.sort_unstable_by_key(|index| streams[*index].payload_offset);
    for pair in order.windows(2) {
        let earlier = streams[pair[0]];
        let later = streams[pair[1]];
        let earlier_end = earlier
            .payload_offset
            .checked_add(earlier.stored_length)
            .ok_or_else(|| {
                data_error(
                    Error::corruption("stream range overflow", Some(header_offset)),
                    sequence,
                    ErrorContext::Header,
                )
            })?;
        if earlier_end > later.payload_offset {
            return Err(data_error(
                Error::corruption("stream ranges overlap", Some(header_offset)),
                sequence,
                ErrorContext::Header,
            ));
        }
    }
    for stream in streams {
        let end = stream
            .payload_offset
            .checked_add(stream.stored_length)
            .ok_or_else(|| {
                data_error(
                    Error::corruption("stream range overflow", Some(header_offset)),
                    sequence,
                    ErrorContext::Header,
                )
            })?;
        if end > payload_length {
            return Err(data_error(
                Error::corruption(
                    "stream range exceeds the frame payload",
                    Some(header_offset),
                ),
                sequence,
                ErrorContext::Header,
            ));
        }
    }
    Ok(())
}

fn ensure_stream_kind(
    kind: u16,
    owner: &dyn Fn(usize) -> String,
    index: usize,
    frame: &FrameMetadata,
) -> Result<()> {
    if (STREAM_KIND_VALIDITY..=STREAM_KIND_RUN_LENGTHS).contains(&kind) {
        return Ok(());
    }
    Err(data_error(
        Error::unsupported_frame(format!("{}: unsupported kind {kind}", owner(index)), None),
        frame.sequence,
        ErrorContext::Header,
    ))
}

fn ensure_transform(
    transform: u16,
    owner: &dyn Fn(usize) -> String,
    index: usize,
    frame: &FrameMetadata,
) -> Result<()> {
    if transform <= TRANSFORM_BOOLEAN_RLE {
        return Ok(());
    }
    Err(data_error(
        Error::unsupported_frame(
            format!("{}: unsupported transform {transform}", owner(index)),
            None,
        ),
        frame.sequence,
        ErrorContext::Header,
    ))
}

fn ensure_codec(
    codec: u16,
    owner: &dyn Fn(usize) -> String,
    index: usize,
    frame: &FrameMetadata,
) -> Result<()> {
    if codec == CODEC_NONE || codec == CODEC_ZSTD {
        return Ok(());
    }
    Err(data_error(
        Error::unsupported_frame(format!("{}: unsupported codec {codec}", owner(index)), None),
        frame.sequence,
        ErrorContext::Header,
    ))
}

#[derive(Debug, Clone, Copy)]
struct BlockHeaderFields {
    schema_id: u64,
    base_row_id: u64,
    row_count: u32,
    column_count: u32,
    primary_min: i64,
    primary_max: i64,
    column_table_offset: u64,
    stream_table_offset: u64,
    statistics_offset: u64,
    statistics_length: u64,
    flags: u32,
}

fn parse_header(bytes: &[u8; 64], offset: u64) -> Result<BlockHeaderFields> {
    let mut cursor = Cursor::new(bytes, offset);
    let fields = BlockHeaderFields {
        schema_id: cursor.read_u64("data frame schema ID")?,
        base_row_id: cursor.read_u64("data frame base row ID")?,
        row_count: cursor.read_u32("data frame row count")?,
        column_count: cursor.read_u32("data frame column count")?,
        primary_min: i64::from_le_bytes(
            cursor.read_u64("primary timestamp minimum")?.to_le_bytes(),
        ),
        primary_max: i64::from_le_bytes(
            cursor.read_u64("primary timestamp maximum")?.to_le_bytes(),
        ),
        column_table_offset: u64::from(cursor.read_u32("column table offset")?),
        stream_table_offset: u64::from(cursor.read_u32("stream table offset")?),
        statistics_offset: u64::from(cursor.read_u32("statistics area offset")?),
        statistics_length: u64::from(cursor.read_u32("statistics area length")?),
        flags: cursor.read_u32("data block flags")?,
    };
    cursor.skip(BLOCK_RESERVED_SIZE, "data block reserved field")?;
    Ok(fields)
}

fn validate_layout(
    fields: &BlockHeaderFields,
    header_length: u64,
    schema_column_count: u64,
    sequence: u64,
    header_offset: u64,
) -> Result<()> {
    if fields.column_table_offset != DATA_BLOCK_HEADER_SIZE {
        return Err(data_error(
            Error::corruption(
                format!(
                    "column table offset must be {}, found {}",
                    DATA_BLOCK_HEADER_SIZE, fields.column_table_offset
                ),
                field_offset(header_offset, BLOCK_COLUMN_TABLE_OFFSET_FIELD),
            ),
            sequence,
            ErrorContext::Header,
        ));
    }
    let descriptor_bytes = schema_column_count
        .checked_mul(BLOCK_COLUMN_DESCRIPTOR_SIZE)
        .ok_or_else(|| {
            data_error(
                Error::corruption("column table length overflow", Some(header_offset)),
                sequence,
                ErrorContext::Header,
            )
        })?;
    let expected_stream_offset = fields
        .column_table_offset
        .checked_add(descriptor_bytes)
        .ok_or_else(|| {
            data_error(
                Error::corruption("stream table offset overflow", Some(header_offset)),
                sequence,
                ErrorContext::Header,
            )
        })?;
    if fields.stream_table_offset != expected_stream_offset {
        return Err(data_error(
            Error::corruption(
                format!(
                    "stream table offset must be {expected_stream_offset}, found {}",
                    fields.stream_table_offset
                ),
                field_offset(header_offset, BLOCK_STREAM_TABLE_OFFSET_FIELD),
            ),
            sequence,
            ErrorContext::Header,
        ));
    }
    if fields.statistics_offset < fields.stream_table_offset {
        return Err(data_error(
            Error::corruption(
                "statistics area begins before the stream table ends",
                field_offset(header_offset, BLOCK_STATISTICS_OFFSET_FIELD),
            ),
            sequence,
            ErrorContext::Header,
        ));
    }
    if (fields.statistics_offset - fields.stream_table_offset) % BLOCK_STREAM_DESCRIPTOR_SIZE != 0 {
        return Err(data_error(
            Error::corruption(
                "stream table length is not a multiple of 48 bytes",
                field_offset(header_offset, BLOCK_STATISTICS_OFFSET_FIELD),
            ),
            sequence,
            ErrorContext::Header,
        ));
    }
    let statistics_end = fields
        .statistics_offset
        .checked_add(fields.statistics_length)
        .ok_or_else(|| {
            data_error(
                Error::corruption("statistics area offset overflow", Some(header_offset)),
                sequence,
                ErrorContext::Header,
            )
        })?;
    if statistics_end > header_length {
        return Err(data_error(
            Error::corruption(
                "statistics area extends beyond the padded frame header",
                field_offset(header_offset, BLOCK_STATISTICS_LENGTH_FIELD),
            ),
            sequence,
            ErrorContext::Header,
        ));
    }
    Ok(())
}

fn data_error(error: Error, sequence: u64, region: ErrorContext) -> Error {
    error
        .with_context(ErrorContext::Frame { sequence })
        .with_context(region)
}
