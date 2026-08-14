//! Decoding one committed data block into logical arrays.
//!
//! The transforms and codecs themselves live in [`crate::codec`]; this module
//! is the reader's orchestration around them. It turns the checked descriptor
//! tables of a data frame into schema-order arrays, spending one shared memory
//! allowance so the block as a whole stays within its configured bound.

use std::borrow::Cow;
use std::fs::File;
use std::sync::Arc;

use crate::array::{Array, ScalarValue};
use crate::batch::RecordBatch;
use crate::codec::transform;
use crate::crc32c::checksum;
use crate::error::{Error, ErrorContext, Result};
use crate::format::constants::{
    CODEC_NONE, COLUMN_IMPLICIT_VALIDITY_FLAG, COLUMN_LAYOUT_CONSTANT, COLUMN_LAYOUT_DICTIONARY,
    COLUMN_LAYOUT_PLAIN, COLUMN_LAYOUT_RUN_LENGTH, STREAM_KIND_DICTIONARY_LENGTHS,
    STREAM_KIND_DICTIONARY_VALUES, STREAM_KIND_INDICES, STREAM_KIND_LENGTHS,
    STREAM_KIND_RUN_LENGTHS, STREAM_KIND_RUN_VALUES, STREAM_KIND_VALIDITY, STREAM_KIND_VALUES,
    TRANSFORM_BIT_PACKED, TRANSFORM_BYTE_STREAM_SPLIT, TRANSFORM_RAW,
};
use crate::format::data_frame::{BlockLayout, ColumnBlockDescriptor, StreamDescriptor};
use crate::format::frame::FrameMetadata;
use crate::format::read_exact_at;
use crate::limits::Limits;
use crate::schema::{Column, LogicalType, Schema};
use crate::validate::statistics::verify_column_statistics;

use super::budget::{Budget, ScanBudget};
use super::values::{
    BOOLEAN_VALUE_BYTES, DenseValues, LENGTH_VALUE_BYTES, UNPACKED_VALUE_BYTES, VALUE_HANDLE_BYTES,
    corrupt, is_byte_valued,
};

/// How section 3 interprets a stored integer, spelled out at the call sites
/// that choose it.
const SIGNED: bool = true;
const UNSIGNED: bool = false;

/// The columns one block decode materializes, and the schema its batch carries.
///
/// `primary` names the one column a decode may materialize without projecting
/// it. It is a single field rather than a pair of "decode it" and "verify it"
/// flags on purpose: section 8's bounds and `TS_SORTED` are claims about the
/// primary column's values, so any decode that reconstructs those values owes
/// the file that check, and no caller should be able to ask for one without
/// the other.
pub(crate) struct Selection<'a> {
    pub(crate) source_schema: Arc<Schema>,
    pub(crate) output_schema: Arc<Schema>,
    pub(crate) selected: &'a [usize],
    pub(crate) primary: Option<usize>,
}

impl<'a> Selection<'a> {
    /// Every column in schema order, which is what a full block decode reads.
    pub(crate) fn all(schema: Arc<Schema>, selected: &'a [usize]) -> Self {
        Self {
            primary: primary_index(&schema),
            source_schema: Arc::clone(&schema),
            output_schema: schema,
            selected,
        }
    }
}

/// The schema position of the primary column, if the schema designates one.
pub(crate) fn primary_index(schema: &Schema) -> Option<usize> {
    let id = schema.primary_column_id()?;
    schema.columns().iter().position(|column| column.id() == id)
}

/// Decode one complete data frame into schema-order logical arrays.
pub(crate) fn decode_block(
    file: &mut File,
    file_size: u64,
    frame: &FrameMetadata,
    layout: &BlockLayout,
    schema: Arc<Schema>,
    limits: Limits,
) -> Result<RecordBatch> {
    let selected: Vec<usize> = (0..schema.column_count()).collect();
    decode_selected_block(
        file,
        file_size,
        frame,
        layout,
        &Selection::all(schema, &selected),
        None,
        limits,
    )
    .map(|decoded| decoded.batch)
}

/// Decode only the selected output columns and, when requested, the primary
/// column needed to filter rows. The latter is retained only as internal
/// values and never enters the output batch unless it was selected explicitly.
pub(crate) fn decode_selected_block(
    file: &mut File,
    file_size: u64,
    frame: &FrameMetadata,
    layout: &BlockLayout,
    selection: &Selection<'_>,
    scan_budget: Option<&mut ScanBudget>,
    limits: Limits,
) -> Result<DecodedBlock> {
    let mut budget = Budget::new(limits.max_decoded_block_bytes());
    if let Some(scan_budget) = scan_budget {
        budget = budget.with_scan(scan_budget);
    }
    BlockDecoder {
        file,
        file_size,
        frame,
        layout,
        limits,
        budget,
    }
    .block(selection)
    .map_err(|error| {
        error.with_context(ErrorContext::Frame {
            sequence: frame.sequence,
        })
    })
}

/// The result of a decode. `primary_values` exists whenever the selection
/// named a primary column, which is required for range scans and for any
/// projection that includes the primary column itself.
#[derive(Debug)]
pub(crate) struct DecodedBlock {
    pub(crate) batch: RecordBatch,
    pub(crate) primary_values: Option<Vec<i64>>,
    /// Whether these primary values were verified to be nondecreasing.
    ///
    /// A caller that binary-searches them needs this, and needs it from the
    /// decode rather than from block metadata captured earlier: the two come
    /// from different reads of the file, and only this one was established
    /// against the values in hand.
    pub(crate) primary_sorted: bool,
}

/// One block decode in progress, and the allowance its columns share.
struct BlockDecoder<'a, 'scan> {
    file: &'a mut File,
    file_size: u64,
    frame: &'a FrameMetadata,
    layout: &'a BlockLayout,
    limits: Limits,
    budget: Budget<'scan>,
}

impl<'a, 'scan> BlockDecoder<'a, 'scan> {
    fn block(mut self, selection: &Selection<'_>) -> Result<DecodedBlock> {
        let row_count = self.row_count()?;
        let schema = &selection.source_schema;
        if selection
            .selected
            .iter()
            .any(|&index| index >= schema.column_count())
        {
            return Err(Error::internal(
                "selected column index is outside the schema",
            ));
        }
        if selection
            .primary
            .is_some_and(|index| index >= schema.column_count())
        {
            return Err(Error::internal(
                "primary column index is outside the schema",
            ));
        }

        let mut arrays: Vec<Option<Array>> = Vec::new();
        arrays
            .try_reserve_exact(schema.column_count())
            .map_err(|_| Error::resource_limit("unable to allocate the decoded columns", None))?;
        arrays.resize_with(schema.column_count(), || None);

        for &index in selection.selected {
            arrays[index] = Some(self.decoded_column(schema, index, row_count)?);
        }
        // The primary is decoded even when it was not projected, because the
        // block's bounds cannot be confirmed without its values.
        let internal_primary = selection
            .primary
            .filter(|index| !selection.selected.contains(index));
        if let Some(index) = internal_primary {
            arrays[index] = Some(self.decoded_column(schema, index, row_count)?);
        }

        let primary_values = match selection.primary {
            Some(index) => {
                let column = schema
                    .columns()
                    .get(index)
                    .ok_or_else(|| Error::internal("primary column index is outside the schema"))?;
                let array = arrays[index]
                    .as_ref()
                    .ok_or_else(|| Error::corruption("the primary column was not decoded", None))?;
                Some(self.verify_primary_column(column, array)?)
            }
            None => None,
        };

        let mut output_arrays = Vec::new();
        output_arrays
            .try_reserve_exact(selection.selected.len())
            .map_err(|_| Error::resource_limit("unable to allocate the projected columns", None))?;
        for &index in selection.selected {
            output_arrays.push(
                arrays[index]
                    .take()
                    .ok_or_else(|| Error::corruption("a selected column was not decoded", None))?,
            );
        }
        // A primary used only for filtering is deliberately dropped here. Its
        // values have already been reduced to the private verification vector.
        let _internal_primary = internal_primary.and_then(|index| arrays[index].take());

        Ok(DecodedBlock {
            // `verify_primary_column` returns only after establishing that
            // `TS_SORTED` describes the values it read, so the block's own
            // claim is a fact by the time it is reported here.
            primary_sorted: primary_values.is_some() && self.layout.metadata.ts_sorted,
            batch: RecordBatch::try_new(
                Arc::clone(&selection.output_schema),
                output_arrays,
                row_count,
            )?,
            primary_values,
        })
    }

    /// Decode the schema column at `index` from this block.
    fn decoded_column(&mut self, schema: &Schema, index: usize, row_count: usize) -> Result<Array> {
        let column = schema
            .columns()
            .get(index)
            .ok_or_else(|| Error::internal("column index is outside the schema"))?;
        let descriptor = self.descriptor_for(column)?;
        self.column(column, &descriptor, row_count)
    }

    fn row_count(&self) -> Result<usize> {
        let declared = self.layout.metadata.row_count;
        if declared > self.limits.max_rows_per_block() {
            return Err(Error::resource_limit(
                format!(
                    "row count {declared} exceeds the {}-row decode limit",
                    self.limits.max_rows_per_block()
                ),
                None,
            )
            .with_context(ErrorContext::Payload));
        }
        usize::try_from(declared).map_err(|_| {
            Error::resource_limit("row count does not fit this platform", None)
                .with_context(ErrorContext::Payload)
        })
    }

    /// The block's descriptor for a schema column. Section 8.1 sorts the column
    /// table by column ID, which the frame parser has already verified.
    fn descriptor_for(&self, column: &Column) -> Result<ColumnBlockDescriptor> {
        self.layout
            .columns
            .binary_search_by_key(&column.id(), |descriptor| descriptor.column_id)
            .ok()
            .and_then(|index| self.layout.columns.get(index).copied())
            .ok_or_else(|| corrupt(column, "the block has no descriptor for this column"))
    }

    fn column(
        &mut self,
        column: &Column,
        descriptor: &ColumnBlockDescriptor,
        row_count: usize,
    ) -> Result<Array> {
        let streams = self.streams_for(descriptor, column)?;
        check_stream_kinds(&streams, descriptor, column)?;
        let validity = self.validity(&streams, column, descriptor, row_count)?;

        let dense_count = usize::try_from(descriptor.dense_count).map_err(|_| {
            Error::resource_limit("dense value count does not fit this platform", None)
                .with_context(ErrorContext::Payload)
        })?;
        let dense = if dense_count == 0 {
            all_null(&streams, descriptor, column)?
        } else {
            self.dense_values(&streams, column, descriptor, dense_count)?
        };

        let array = dense.into_array(column, validity, row_count, &mut self.budget)?;
        // Section 11 statistics are a claim about this column's values alone,
        // so they are settled here rather than after the whole block: a
        // projected read verifies exactly the columns it decoded, and never
        // has to materialize a column it was not asked for.
        verify_column_statistics(
            self.file,
            self.file_size,
            self.frame,
            descriptor,
            column,
            &array,
            self.limits,
        )?;
        // Verification reads exactly the descriptor's statistics range, which
        // is zero bytes for a column that claims none.
        self.budget
            .record_bytes_read(u64::from(descriptor.stats_length))?;
        Ok(array)
    }

    fn dense_values(
        &mut self,
        streams: &[&StreamDescriptor],
        column: &Column,
        descriptor: &ColumnBlockDescriptor,
        count: usize,
    ) -> Result<DenseValues> {
        match descriptor.layout {
            COLUMN_LAYOUT_PLAIN => self.plain(streams, column, count),
            COLUMN_LAYOUT_CONSTANT => self.constant(streams, column, count),
            COLUMN_LAYOUT_DICTIONARY => self.dictionary(streams, column, count),
            COLUMN_LAYOUT_RUN_LENGTH => self.run_length(streams, column, count),
            layout => Err(Error::unsupported_frame(
                format!("unsupported column layout {layout}"),
                None,
            )
            .with_context(ErrorContext::Payload)),
        }
    }

    // -------------------------------------------------------------- validity

    fn validity(
        &mut self,
        streams: &[&StreamDescriptor],
        column: &Column,
        descriptor: &ColumnBlockDescriptor,
        row_count: usize,
    ) -> Result<Option<Vec<bool>>> {
        let stream = find_stream(streams, STREAM_KIND_VALIDITY, column)?;

        if !column.is_nullable() {
            if descriptor.null_count != 0 || stream.is_some() {
                return Err(corrupt(
                    column,
                    "a non-nullable column carries validity data",
                ));
            }
            return Ok(None);
        }
        if descriptor.flags & COLUMN_IMPLICIT_VALIDITY_FLAG != 0 {
            return self.implicit_validity(column, descriptor, stream, row_count);
        }

        let Some(stream) = stream else {
            if descriptor.null_count != 0 {
                return Err(corrupt(
                    column,
                    "a column with nulls has no validity stream",
                ));
            }
            return Ok(None);
        };
        expect_elements(stream, row_count, column)?;
        let payload = self.stream_bytes(stream, column)?;
        self.charge_booleans(row_count)?;
        let bits = transform::booleans(
            &payload,
            stream.element_count,
            stream.transform,
            &description(column, "validity"),
        )?;

        let nulls = bits.iter().filter(|bit| !**bit).count();
        if u64::try_from(nulls) != Ok(u64::from(descriptor.null_count)) {
            return Err(corrupt(
                column,
                "the validity stream does not match the declared null count",
            ));
        }
        Ok(Some(bits))
    }

    /// Section 8.1: an implicit validity representation is all-valid when the
    /// null count is zero and all-null when it equals the row count. Neither
    /// stores a validity stream.
    fn implicit_validity(
        &mut self,
        column: &Column,
        descriptor: &ColumnBlockDescriptor,
        stream: Option<&StreamDescriptor>,
        row_count: usize,
    ) -> Result<Option<Vec<bool>>> {
        if stream.is_some() {
            return Err(corrupt(
                column,
                "implicit validity does not use a validity stream",
            ));
        }
        if descriptor.null_count == 0 {
            return Ok(None);
        }
        if usize::try_from(descriptor.null_count).ok() != Some(row_count) {
            return Err(corrupt(
                column,
                "implicit validity is neither all-valid nor all-null",
            ));
        }

        self.budget
            .charge_elements(row_count, BOOLEAN_VALUE_BYTES)?;
        let mut nulls = Vec::new();
        nulls
            .try_reserve_exact(row_count)
            .map_err(|_| limited(column, "validity bits"))?;
        nulls.resize(row_count, false);
        Ok(Some(nulls))
    }

    // --------------------------------------------------------------- layouts

    fn plain(
        &mut self,
        streams: &[&StreamDescriptor],
        column: &Column,
        count: usize,
    ) -> Result<DenseValues> {
        if is_byte_valued(column) {
            return self.byte_values(
                streams,
                column,
                STREAM_KIND_VALUES,
                STREAM_KIND_LENGTHS,
                count,
            );
        }
        let stream = required_stream(streams, STREAM_KIND_VALUES, column)?;
        expect_elements(stream, count, column)?;
        self.stored_values(stream, column)
    }

    /// Section 8.1: a constant column stores one value, and one length when the
    /// logical type is variable-width, exactly as a one-row plain column does.
    fn constant(
        &mut self,
        streams: &[&StreamDescriptor],
        column: &Column,
        count: usize,
    ) -> Result<DenseValues> {
        self.plain(streams, column, 1)?
            .repeated(count, column, &mut self.budget)
    }

    fn dictionary(
        &mut self,
        streams: &[&StreamDescriptor],
        column: &Column,
        count: usize,
    ) -> Result<DenseValues> {
        let indices = self.indices(streams, column, count)?;
        let stream = required_stream(streams, STREAM_KIND_DICTIONARY_VALUES, column)?;
        if stream.element_count == 0 {
            return Err(corrupt(
                column,
                "dictionary layout stores no dictionary values",
            ));
        }

        let dictionary = if is_byte_valued(column) {
            let dictionary_count = element_count(stream, column)?;
            self.byte_values(
                streams,
                column,
                STREAM_KIND_DICTIONARY_VALUES,
                STREAM_KIND_DICTIONARY_LENGTHS,
                dictionary_count,
            )?
        } else {
            self.stored_values(stream, column)?
        };
        dictionary.expanded(&indices, column, &mut self.budget)
    }

    fn run_length(
        &mut self,
        streams: &[&StreamDescriptor],
        column: &Column,
        count: usize,
    ) -> Result<DenseValues> {
        let lengths = self.run_lengths(streams, column, count)?;
        let stream = required_stream(streams, STREAM_KIND_RUN_VALUES, column)?;
        expect_elements(stream, lengths.len(), column)?;

        let values = if is_byte_valued(column) {
            self.byte_values(
                streams,
                column,
                STREAM_KIND_RUN_VALUES,
                STREAM_KIND_LENGTHS,
                lengths.len(),
            )?
        } else {
            self.stored_values(stream, column)?
        };
        values.runs(&lengths, column, &mut self.budget)
    }

    // --------------------------------------------------------------- streams

    /// Decode one stream of stored values into the column's own value type.
    ///
    /// Section 3 gives each logical type one canonical stored width, and the
    /// transform narrows to the type as it decodes, so the vector this returns
    /// is the only one the values ever occupy.
    fn stored_values(&mut self, stream: &StreamDescriptor, column: &Column) -> Result<DenseValues> {
        match column.logical_type() {
            LogicalType::Bool => self.booleans(stream, column),
            LogicalType::Int8 => self
                .integers(stream, column, 1, SIGNED)
                .map(DenseValues::Int8),
            LogicalType::Int16 => self
                .integers(stream, column, 2, SIGNED)
                .map(DenseValues::Int16),
            LogicalType::Int32 | LogicalType::Date32 => self
                .integers(stream, column, 4, SIGNED)
                .map(DenseValues::Int32),
            LogicalType::Int64 | LogicalType::Decimal { .. } | LogicalType::Timestamp { .. } => {
                self.integers(stream, column, 8, SIGNED)
                    .map(DenseValues::Int64)
            }
            LogicalType::UInt8 => self
                .integers(stream, column, 1, UNSIGNED)
                .map(DenseValues::UInt8),
            LogicalType::UInt16 => self
                .integers(stream, column, 2, UNSIGNED)
                .map(DenseValues::UInt16),
            LogicalType::UInt32 => self
                .integers(stream, column, 4, UNSIGNED)
                .map(DenseValues::UInt32),
            LogicalType::UInt64 => self
                .integers(stream, column, 8, UNSIGNED)
                .map(DenseValues::UInt64),
            LogicalType::Float32 => self.floats(stream, column, Float::Single),
            LogicalType::Float64 => self.floats(stream, column, Float::Double),
            LogicalType::FixedBinary { byte_width } => {
                self.fixed_binary(stream, column, *byte_width)
            }
            // Byte-valued columns arrive through `byte_values` instead.
            LogicalType::Utf8 | LogicalType::Categorical { .. } | LogicalType::Binary => {
                Err(unsupported_representation(column))
            }
        }
    }

    fn booleans(&mut self, stream: &StreamDescriptor, column: &Column) -> Result<DenseValues> {
        let count = element_count(stream, column)?;
        let payload = self.stream_bytes(stream, column)?;
        self.charge_booleans(count)?;
        transform::booleans(
            &payload,
            stream.element_count,
            stream.transform,
            &description(column, "values"),
        )
        .map(DenseValues::Bool)
    }

    fn integers<T: TryFrom<i128>>(
        &mut self,
        stream: &StreamDescriptor,
        column: &Column,
        width: usize,
        signed: bool,
    ) -> Result<Vec<T>> {
        let count = element_count(stream, column)?;
        let payload = self.stream_bytes(stream, column)?;
        self.budget.charge_elements(count, size_of::<T>())?;
        transform::integer(
            &payload,
            stream.element_count,
            width,
            signed,
            stream.transform,
            &description(column, "values"),
        )
    }

    fn floats(
        &mut self,
        stream: &StreamDescriptor,
        column: &Column,
        float: Float,
    ) -> Result<DenseValues> {
        let count = element_count(stream, column)?;
        let payload = self.stream_bytes(stream, column)?;
        let canonical = canonical_bytes(&payload, stream, column, float.width(), &mut self.budget)?;
        self.budget.charge_elements(count, float.width())?;
        float.values(&canonical, column)
    }

    fn fixed_binary(
        &mut self,
        stream: &StreamDescriptor,
        column: &Column,
        byte_width: u32,
    ) -> Result<DenseValues> {
        let width = usize::try_from(byte_width).map_err(|_| {
            Error::resource_limit("fixed_binary width does not fit this platform", None)
                .with_context(ErrorContext::Payload)
        })?;
        let count = element_count(stream, column)?;
        let payload = self.stream_bytes(stream, column)?;
        let canonical = canonical_bytes(&payload, stream, column, width, &mut self.budget)?;
        self.budget
            .charge_elements(count, width.saturating_add(VALUE_HANDLE_BYTES))?;

        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|_| limited(column, "fixed_binary values"))?;
        for value in canonical.chunks_exact(width) {
            values.push(value.to_vec());
        }
        Ok(DenseValues::Bytes(values))
    }

    /// Decode a byte-values stream, with its lengths stream when the logical
    /// type is variable-width.
    fn byte_values(
        &mut self,
        streams: &[&StreamDescriptor],
        column: &Column,
        values_kind: u16,
        lengths_kind: u16,
        count: usize,
    ) -> Result<DenseValues> {
        let stream = required_stream(streams, values_kind, column)?;
        expect_elements(stream, count, column)?;
        if let LogicalType::FixedBinary { byte_width } = column.logical_type() {
            return self.fixed_binary(stream, column, *byte_width);
        }

        // Section 9.1 concatenates variable-width values as stored; no other
        // transform describes a byte-values stream.
        if stream.transform != TRANSFORM_RAW {
            return Err(corrupt(
                column,
                "variable-width values must use the raw transform",
            ));
        }
        let lengths = self.lengths(streams, column, lengths_kind, count)?;
        let payload = self.stream_bytes(stream, column)?;
        self.budget.charge(payload.len())?;
        self.budget.charge_elements(count, VALUE_HANDLE_BYTES)?;
        split_values(&payload, &lengths, column)
    }

    fn lengths(
        &mut self,
        streams: &[&StreamDescriptor],
        column: &Column,
        kind: u16,
        count: usize,
    ) -> Result<Vec<u32>> {
        let stream = required_stream(streams, kind, column)?;
        expect_elements(stream, count, column)?;
        let payload = self.stream_bytes(stream, column)?;
        self.budget.charge_elements(count, LENGTH_VALUE_BYTES)?;
        transform::lengths(
            &payload,
            stream.element_count,
            stream.transform,
            &description(column, "lengths"),
        )
    }

    fn indices(
        &mut self,
        streams: &[&StreamDescriptor],
        column: &Column,
        count: usize,
    ) -> Result<Vec<u64>> {
        let stream = required_stream(streams, STREAM_KIND_INDICES, column)?;
        expect_elements(stream, count, column)?;
        self.unpack(stream, column, count, "dictionary indices")
    }

    fn run_lengths(
        &mut self,
        streams: &[&StreamDescriptor],
        column: &Column,
        dense_count: usize,
    ) -> Result<Vec<usize>> {
        let stream = required_stream(streams, STREAM_KIND_RUN_LENGTHS, column)?;
        let run_count = element_count(stream, column)?;
        let packed = self.unpack(stream, column, run_count, "run lengths")?;

        self.budget.charge_elements(run_count, size_of::<usize>())?;
        let mut lengths = Vec::new();
        lengths
            .try_reserve_exact(run_count)
            .map_err(|_| limited(column, "run lengths"))?;
        let mut total = 0_usize;
        for length in packed {
            let length = usize::try_from(length)
                .ok()
                .filter(|length| *length != 0)
                .ok_or_else(|| corrupt(column, "a run length is zero or unrepresentable"))?;
            total = total
                .checked_add(length)
                .filter(|total| *total <= dense_count)
                .ok_or_else(|| corrupt(column, "run lengths exceed the dense value count"))?;
            lengths.push(length);
        }
        if total != dense_count {
            return Err(corrupt(
                column,
                "run lengths do not sum to the dense value count",
            ));
        }
        Ok(lengths)
    }

    /// Section 9.2 defines bit packing as the representation of dictionary
    /// indices and run lengths.
    fn unpack(
        &mut self,
        stream: &StreamDescriptor,
        column: &Column,
        count: usize,
        what: &str,
    ) -> Result<Vec<u64>> {
        if stream.transform != TRANSFORM_BIT_PACKED {
            return Err(Error::unsupported_frame(
                format!(
                    "column {}: {what} use transform {}, and only bit packing is defined for them",
                    column.name(),
                    stream.transform
                ),
                None,
            )
            .with_context(ErrorContext::Payload));
        }
        let payload = self.stream_bytes(stream, column)?;
        self.budget.charge_elements(count, UNPACKED_VALUE_BYTES)?;
        transform::bit_unpack(&payload, count, &description(column, what))
    }

    /// Read one stream's stored bytes, verify its CRC, and decompress them.
    ///
    /// Section 8.2 checksums the bytes exactly as stored, so the CRC is checked
    /// before the codec sees them. The frame parser has already checked every
    /// stream range against the frame payload and both declared lengths against
    /// the configured payload limit; what remains is the snapshot extent, the
    /// decode allowance, and the checksum.
    fn stream_bytes(&mut self, stream: &StreamDescriptor, column: &Column) -> Result<Vec<u8>> {
        if stream.element_count > self.limits.max_rows_per_block() {
            return Err(Error::resource_limit(
                format!(
                    "column {}: stream element count {} exceeds the decode limit",
                    column.name(),
                    stream.element_count
                ),
                None,
            )
            .with_context(ErrorContext::Payload));
        }

        let offset = self
            .frame
            .payload_offset
            .checked_add(stream.payload_offset)
            .ok_or_else(|| corrupt(column, "the stream offset overflows"))?;
        let end = offset
            .checked_add(stream.stored_length)
            .ok_or_else(|| corrupt(column, "the stream range overflows"))?;
        if end > self.file_size {
            return Err(corrupt(column, "the stream range leaves the snapshot"));
        }
        let stored_length = usize::try_from(stream.stored_length).map_err(|_| {
            Error::resource_limit("stored stream length does not fit this platform", None)
                .with_context(ErrorContext::Payload)
        })?;

        self.budget.charge(stored_length)?;
        let mut stored = Vec::new();
        stored
            .try_reserve_exact(stored_length)
            .map_err(|_| limited(column, "the stored stream"))?;
        stored.resize(stored_length, 0);
        read_exact_at(self.file, offset, &mut stored)?;

        if checksum(&stored) != stream.crc {
            return Err(Error::corruption(
                format!("bad stream CRC32C for column {}", column.name()),
                Some(offset),
            )
            .with_context(ErrorContext::Payload));
        }
        if stream.codec == CODEC_NONE {
            self.budget.record_stream(stream.stored_length)?;
            return Ok(stored);
        }

        let transformed_length = usize::try_from(stream.transformed_length).map_err(|_| {
            Error::resource_limit("transformed stream length does not fit this platform", None)
                .with_context(ErrorContext::Payload)
        })?;
        self.budget.charge(transformed_length)?;
        // The codec module is column-agnostic by design, so the column is
        // attached here, where it is known.
        let transformed = decompress(stream.codec, &stored, transformed_length)
            .map_err(|error| error.with_message_prefix(format!("column {}", column.name())))?;
        self.budget.record_stream(stream.stored_length)?;
        Ok(transformed)
    }

    /// Boolean streams may arrive run-length encoded, which unpacks its run
    /// lengths before expanding them.
    fn charge_booleans(&mut self, count: usize) -> Result<()> {
        self.budget
            .charge_elements(count, BOOLEAN_VALUE_BYTES + UNPACKED_VALUE_BYTES)
    }

    // -------------------------------------------------------- primary column

    /// Section 8: the block's primary bounds are calculated over the primary
    /// column's values, and `TS_SORTED` claims those values do not decrease. A
    /// reader that decodes the column must confirm both.
    fn verify_primary_column(&mut self, _column: &Column, array: &Array) -> Result<Vec<i64>> {
        let row_count = array.len();
        self.budget.charge_elements(row_count, size_of::<i64>())?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(row_count)
            .map_err(|_| Error::resource_limit("unable to allocate primary values", None))?;
        for row in 0..row_count {
            values.push(match array.value_at(row) {
                Some(ScalarValue::Timestamp { value, .. }) => value,
                Some(ScalarValue::Date32(value)) => i64::from(value),
                Some(_) => {
                    return Err(Error::corruption(
                        "the primary column is not a timestamp or date",
                        None,
                    ));
                }
                None => {
                    return Err(Error::corruption(
                        "the primary column contains a null value",
                        None,
                    ));
                }
            });
        }

        let bounds = self.layout.metadata.primary_bounds;
        if values.iter().copied().min() != bounds.map(|bounds| bounds.0)
            || values.iter().copied().max() != bounds.map(|bounds| bounds.1)
        {
            return Err(Error::corruption(
                "the block's primary bounds do not match its decoded values",
                None,
            ));
        }
        if !self.layout.metadata.ts_sorted {
            return Ok(values);
        }
        if values.windows(2).any(|pair| pair[0] > pair[1]) {
            return Err(Error::corruption(
                "TS_SORTED is set but the primary values decrease",
                None,
            ));
        }
        if values.first().copied() != bounds.map(|bounds| bounds.0)
            || values.last().copied() != bounds.map(|bounds| bounds.1)
        {
            return Err(Error::corruption(
                "TS_SORTED requires bounds equal to the first and last values",
                None,
            ));
        }
        Ok(values)
    }

    /// The contiguous stream-table range section 8.1 assigns to one column.
    fn streams_for(
        &self,
        descriptor: &ColumnBlockDescriptor,
        column: &Column,
    ) -> Result<Vec<&'a StreamDescriptor>> {
        let start = usize::try_from(descriptor.first_stream)
            .map_err(|_| corrupt(column, "the stream index does not fit this platform"))?;
        let end = start
            .checked_add(usize::from(descriptor.stream_count))
            .ok_or_else(|| corrupt(column, "the stream range overflows"))?;
        self.layout
            .streams
            .get(start..end)
            .map(|streams| streams.iter().collect())
            .ok_or_else(|| corrupt(column, "the stream range leaves the stream table"))
    }
}

/// The IEEE 754 binary formats v0.2 stores.
#[derive(Debug, Clone, Copy)]
enum Float {
    Single,
    Double,
}

impl Float {
    fn width(self) -> usize {
        match self {
            Self::Single => size_of::<f32>(),
            Self::Double => size_of::<f64>(),
        }
    }

    /// Reinterpret canonical little-endian bytes, preserving every stored bit
    /// including signed zero and NaN payloads.
    fn values(self, canonical: &[u8], column: &Column) -> Result<DenseValues> {
        match self {
            Self::Single => {
                reinterpret(canonical, column, f32::from_le_bytes).map(DenseValues::Float32)
            }
            Self::Double => {
                reinterpret(canonical, column, f64::from_le_bytes).map(DenseValues::Float64)
            }
        }
    }
}

fn reinterpret<const WIDTH: usize, T>(
    canonical: &[u8],
    column: &Column,
    convert: fn([u8; WIDTH]) -> T,
) -> Result<Vec<T>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(canonical.len() / WIDTH)
        .map_err(|_| limited(column, "float values"))?;
    for bytes in canonical.chunks_exact(WIDTH) {
        let mut raw = [0_u8; WIDTH];
        raw.copy_from_slice(bytes);
        values.push(convert(raw));
    }
    Ok(values)
}

/// Restore the canonical little-endian bytes of a fixed-width stream.
///
/// The raw transform already stores them that way, so only a byte-stream split
/// needs a buffer of its own.
fn canonical_bytes<'p>(
    payload: &'p [u8],
    stream: &StreamDescriptor,
    column: &Column,
    width: usize,
    budget: &mut Budget,
) -> Result<Cow<'p, [u8]>> {
    let count = element_count(stream, column)?;
    let expected = count
        .checked_mul(width)
        .ok_or_else(|| corrupt(column, "the value byte count overflows"))?;

    let canonical = match stream.transform {
        TRANSFORM_RAW => Cow::Borrowed(payload),
        TRANSFORM_BYTE_STREAM_SPLIT => {
            budget.charge(expected)?;
            Cow::Owned(transform::byte_stream_split(
                payload,
                stream.element_count,
                width,
                &description(column, "values"),
            )?)
        }
        transform => {
            return Err(corrupt(
                column,
                format!("transform {transform} does not apply to these values"),
            ));
        }
    };
    if canonical.len() != expected {
        return Err(corrupt(
            column,
            format!(
                "the stream holds {} value bytes, expected {expected}",
                canonical.len()
            ),
        ));
    }
    Ok(canonical)
}

/// Split a concatenated byte-values stream into its individual values.
fn split_values(payload: &[u8], lengths: &[u32], column: &Column) -> Result<DenseValues> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(lengths.len())
        .map_err(|_| limited(column, "variable-width values"))?;

    let mut offset = 0_usize;
    for length in lengths {
        let end = usize::try_from(*length)
            .ok()
            .and_then(|length| offset.checked_add(length))
            .ok_or_else(|| corrupt(column, "a value length does not fit this platform"))?;
        let value = payload
            .get(offset..end)
            .ok_or_else(|| corrupt(column, "the value lengths exceed the values stream"))?;
        values.push(value.to_vec());
        offset = end;
    }
    if offset != payload.len() {
        return Err(corrupt(
            column,
            "the value lengths do not consume the values stream",
        ));
    }
    Ok(DenseValues::Bytes(values))
}

/// Section 8.1: an all-null column uses plain layout and stores no value
/// streams, so only its validity representation remains.
fn all_null(
    streams: &[&StreamDescriptor],
    descriptor: &ColumnBlockDescriptor,
    column: &Column,
) -> Result<DenseValues> {
    if descriptor.layout != COLUMN_LAYOUT_PLAIN {
        return Err(corrupt(column, "an all-null column must use plain layout"));
    }
    if streams
        .iter()
        .any(|stream| stream.kind != STREAM_KIND_VALIDITY)
    {
        return Err(corrupt(
            column,
            "an all-null column stores no value streams",
        ));
    }
    Ok(DenseValues::empty(column))
}

#[cfg(feature = "zstd")]
fn decompress(codec: u16, stored: &[u8], transformed_length: usize) -> Result<Vec<u8>> {
    if codec == crate::format::constants::CODEC_ZSTD {
        return crate::codec::compression::zstd(stored, transformed_length);
    }
    Err(unsupported_codec(codec))
}

#[cfg(not(feature = "zstd"))]
fn decompress(codec: u16, _stored: &[u8], _transformed_length: usize) -> Result<Vec<u8>> {
    Err(unsupported_codec(codec))
}

fn unsupported_codec(codec: u16) -> Error {
    Error::unsupported_frame(
        format!("stream codec {codec} is not available in this build"),
        None,
    )
    .with_context(ErrorContext::Payload)
}

/// Section 8.1 lists the streams each layout uses. A column carrying any other
/// kind is describing a representation its layout does not have.
fn check_stream_kinds(
    streams: &[&StreamDescriptor],
    descriptor: &ColumnBlockDescriptor,
    column: &Column,
) -> Result<()> {
    let allowed: &[u16] = match descriptor.layout {
        COLUMN_LAYOUT_PLAIN | COLUMN_LAYOUT_CONSTANT => &[
            STREAM_KIND_VALIDITY,
            STREAM_KIND_VALUES,
            STREAM_KIND_LENGTHS,
        ],
        COLUMN_LAYOUT_DICTIONARY => &[
            STREAM_KIND_VALIDITY,
            STREAM_KIND_DICTIONARY_VALUES,
            STREAM_KIND_DICTIONARY_LENGTHS,
            STREAM_KIND_INDICES,
        ],
        COLUMN_LAYOUT_RUN_LENGTH => &[
            STREAM_KIND_VALIDITY,
            STREAM_KIND_RUN_VALUES,
            STREAM_KIND_RUN_LENGTHS,
            STREAM_KIND_LENGTHS,
        ],
        _ => &[],
    };
    for stream in streams {
        if !allowed.contains(&stream.kind) {
            return Err(corrupt(
                column,
                format!(
                    "stream kind {} does not belong to column layout {}",
                    stream.kind, descriptor.layout
                ),
            ));
        }
    }
    Ok(())
}

fn find_stream<'a>(
    streams: &[&'a StreamDescriptor],
    kind: u16,
    column: &Column,
) -> Result<Option<&'a StreamDescriptor>> {
    let mut found = None;
    for stream in streams {
        if stream.kind != kind {
            continue;
        }
        if found.is_some() {
            return Err(corrupt(column, format!("duplicate stream kind {kind}")));
        }
        found = Some(*stream);
    }
    Ok(found)
}

fn required_stream<'a>(
    streams: &[&'a StreamDescriptor],
    kind: u16,
    column: &Column,
) -> Result<&'a StreamDescriptor> {
    find_stream(streams, kind, column)?
        .ok_or_else(|| corrupt(column, format!("missing stream kind {kind}")))
}

fn expect_elements(stream: &StreamDescriptor, expected: usize, column: &Column) -> Result<()> {
    if u64::try_from(expected) != Ok(stream.element_count) {
        return Err(corrupt(
            column,
            format!(
                "stream kind {} declares {} elements, expected {expected}",
                stream.kind, stream.element_count
            ),
        ));
    }
    Ok(())
}

fn element_count(stream: &StreamDescriptor, column: &Column) -> Result<usize> {
    usize::try_from(stream.element_count).map_err(|_| {
        Error::resource_limit(
            format!(
                "column {}: element count does not fit this platform",
                column.name()
            ),
            None,
        )
        .with_context(ErrorContext::Payload)
    })
}

fn unsupported_representation(column: &Column) -> Error {
    Error::unsupported_frame(
        format!("no fixed-width decoder for column {}", column.name()),
        None,
    )
    .with_context(ErrorContext::Payload)
}

fn description(column: &Column, part: &str) -> String {
    format!("column {} {part}", column.name())
}

fn limited(column: &Column, what: &str) -> Error {
    Error::resource_limit(
        format!("unable to allocate {what} for column {}", column.name()),
        None,
    )
    .with_context(ErrorContext::Payload)
}
