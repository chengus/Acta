//! Buffered writing of Acta v0.2 files.
//!
//! The private writer hierarchy keeps configuration, lifecycle, buffering,
//! validation, framing, and encoding responsibilities separate while preserving
//! the crate-root writer API and wire behavior.

mod api;
mod buffer;
mod encode;
mod framing;
mod input;
mod writer;

pub use api::{
    DEFAULT_BYTE_BLOCK_TARGET, DEFAULT_ROW_BLOCK_TARGET, DEFAULT_ZSTD_LEVEL, WriteAccounting,
    WriteSummary, WriterCodec, WriterEncoding, WriterOptions, WriterStatistics, WriterTransform,
};
pub use writer::Writer;

use crate::error::{Error, ErrorContext};

fn invalid_schema(message: impl Into<String>) -> Error {
    Error::invalid_argument(message).with_context(ErrorContext::Header)
}

/// A [`WriterOptions`] value this build or this format version cannot honor.
fn invalid_option(message: impl Into<String>) -> Error {
    Error::invalid_argument(message).with_context(ErrorContext::File)
}

fn invalid_batch(message: impl Into<String>) -> Error {
    Error::invalid_argument(message).with_context(ErrorContext::Payload)
}

fn resource(message: impl Into<String>) -> Error {
    Error::resource_limit(message, None).with_context(ErrorContext::Payload)
}

/// An invariant of this module that no input can violate, so reaching one is a
/// bug here rather than anything a caller or a file did.
fn internal(message: impl Into<String>) -> Error {
    Error::internal(message)
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::ops::Range;
    use std::sync::Mutex;

    use super::api::*;
    use super::buffer::*;
    use super::encode::*;
    use super::input::*;
    use super::writer::*;
    use crate::array::Array;
    use crate::array::{
        BinaryArray, BooleanArray, DecimalArray, PrimitiveArray, TimestampArray, Utf8Array,
    };
    use crate::batch::RecordBatch;
    use crate::error::ErrorKind;
    use crate::format::constants::{
        FIRST_DATA_FRAME_SEQUENCE, FRAME_ALIGNMENT, PROLOGUE_SIZE, UNAVAILABLE_BASE_ROW_ID,
    };
    use crate::limits::Limits;
    use crate::schema::{Column, LogicalType, Schema, TimeUnit, TimeZone};
    use std::sync::Arc;
    /// What a writer asked of its sink, and whether the sink refuses.
    #[derive(Default)]
    struct Record {
        writes: usize,
        flushes: usize,
        syncs: usize,
        refusing: bool,
    }

    /// A sink that records every request and can be told to refuse them, which
    /// is how a partial I/O failure is reached without an unwritable disk.
    #[derive(Clone, Default)]
    struct Recorder(Arc<Mutex<Record>>);

    impl Recorder {
        fn refuse(&self) {
            self.0.lock().expect("recorder lock").refusing = true;
        }

        fn record(&self, count: impl Fn(&mut Record)) -> io::Result<()> {
            let mut record = self.0.lock().expect("recorder lock");
            count(&mut record);
            if record.refusing {
                return Err(io::Error::other("the sink refuses this request"));
            }
            Ok(())
        }

        fn flushes(&self) -> usize {
            self.0.lock().expect("recorder lock").flushes
        }

        fn syncs(&self) -> usize {
            self.0.lock().expect("recorder lock").syncs
        }
    }

    impl Write for Recorder {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.record(|record| record.writes += 1)?;
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.record(|record| record.flushes += 1)
        }
    }

    impl Sink for Recorder {
        fn sync(&mut self) -> io::Result<()> {
            self.record(|record| record.syncs += 1)
        }
    }

    fn schema() -> Schema {
        Schema::new(
            1,
            vec![Column::new(1, "value", LogicalType::Int64, false)],
            None,
        )
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema()),
            vec![Array::Int64(PrimitiveArray::new(vec![1, 2, 3], None))],
            3,
        )
        .expect("a well-formed batch")
    }

    fn writer_over(recorder: &Recorder) -> Writer {
        Writer::new(
            Box::new(recorder.clone()),
            schema(),
            WriterOptions::default().with_row_block_target(1),
            AppendState::new(PROLOGUE_SIZE as u64),
        )
    }

    /// A writer entering the shared append engine the way `Writer::open` does:
    /// mid-file, mid-sequence, and mid-row-ID chain. A real reopened session
    /// cannot reach a refusing sink, so this is how the two entry points are
    /// shown to poison and to account identically.
    fn reopened_writer_over(recorder: &Recorder, options: WriterOptions) -> Writer {
        Writer::new(
            Box::new(recorder.clone()),
            schema(),
            options,
            AppendState {
                file_length: 4_096,
                next_sequence: 7,
                next_row_id: 100,
            },
        )
    }

    #[test]
    fn a_refused_write_poisons_a_reopened_writer_the_same_way() {
        let recorder = Recorder::default();
        let mut writer =
            reopened_writer_over(&recorder, WriterOptions::default().with_row_block_target(1));
        recorder.refuse();

        assert_eq!(writer.append(batch()).unwrap_err().kind(), ErrorKind::Io);
        assert_eq!(
            writer.append(batch()).unwrap_err().kind(),
            ErrorKind::Poisoned
        );
        assert_eq!(writer.flush().unwrap_err().kind(), ErrorKind::Poisoned);
        assert_eq!(writer.finish().unwrap_err().kind(), ErrorKind::Poisoned);
    }

    #[test]
    fn a_reopened_session_continues_the_file_and_counts_only_its_own_work() {
        let recorder = Recorder::default();
        let mut writer =
            reopened_writer_over(&recorder, WriterOptions::default().with_row_ids(true));
        writer.append(batch()).expect("the sink accepts the frame");

        let summary = writer.finish().expect("the sink accepts the sync");

        assert_eq!(summary.rows_written(), 3);
        assert_eq!(summary.blocks_written(), 1);
        assert_eq!(summary.last_sequence(), Some(7));
        assert!(summary.bytes_written() > 4_096);
        assert_eq!(summary.accounting().published_rows(), 3);
        assert_eq!(summary.accounting().buffered_rows(), 0);
    }

    #[test]
    fn a_reopened_session_that_publishes_nothing_reports_no_sequence() {
        let recorder = Recorder::default();
        let writer = reopened_writer_over(&recorder, WriterOptions::default());

        let summary = writer.finish().expect("the sink accepts the sync");

        assert_eq!(summary.last_sequence(), None);
        assert_eq!(summary.rows_written(), 0);
        assert_eq!(summary.bytes_written(), 4_096);
    }

    #[test]
    fn a_refused_write_poisons_the_writer() {
        let recorder = Recorder::default();
        let mut writer = writer_over(&recorder);
        recorder.refuse();

        assert_eq!(writer.append(batch()).unwrap_err().kind(), ErrorKind::Io);
        assert_eq!(
            writer.append(batch()).unwrap_err().kind(),
            ErrorKind::Poisoned
        );
    }

    #[test]
    fn a_poisoned_writer_refuses_to_flush() {
        let recorder = Recorder::default();
        let mut writer = writer_over(&recorder);
        recorder.refuse();
        let _ = writer.append(batch());

        assert_eq!(writer.flush().unwrap_err().kind(), ErrorKind::Poisoned);
    }

    #[test]
    fn a_poisoned_writer_refuses_to_finish() {
        let recorder = Recorder::default();
        let mut writer = writer_over(&recorder);
        recorder.refuse();
        let _ = writer.append(batch());

        assert_eq!(writer.finish().unwrap_err().kind(), ErrorKind::Poisoned);
    }

    #[test]
    fn a_refused_sync_poisons_the_writer() {
        let recorder = Recorder::default();
        let mut writer = writer_over(&recorder);
        writer.append(batch()).expect("the sink accepts the frame");
        recorder.refuse();

        assert_eq!(writer.sync().unwrap_err().kind(), ErrorKind::Io);
        assert_eq!(
            writer.append(batch()).unwrap_err().kind(),
            ErrorKind::Poisoned
        );
    }

    #[test]
    fn a_rejected_batch_leaves_the_writer_usable() {
        let recorder = Recorder::default();
        let mut writer = writer_over(&recorder);
        let empty = RecordBatch::try_new(
            Arc::new(schema()),
            vec![Array::Int64(PrimitiveArray::new(Vec::new(), None))],
            0,
        )
        .expect("an empty batch is a valid batch");

        assert_eq!(
            writer.append(empty).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        writer
            .append(batch())
            .expect("the writer still accepts work");
    }

    #[test]
    fn dropping_a_writer_neither_flushes_nor_synchronizes() {
        let recorder = Recorder::default();
        let mut writer = writer_over(&recorder);
        writer.append(batch()).expect("the sink accepts the frame");

        drop(writer);

        assert_eq!((recorder.flushes(), recorder.syncs()), (0, 0));
    }

    #[test]
    fn finishing_a_writer_synchronizes_once() {
        let recorder = Recorder::default();
        let mut writer = writer_over(&recorder);
        writer.append(batch()).expect("the sink accepts the frame");

        let _ = writer.finish().expect("the sink accepts the sync");

        assert_eq!(recorder.syncs(), 1);
    }

    /// Every logical type, both validity representations, and the all-null and
    /// all-valid special cases, so a sizing mistake in any arm shows up.
    fn every_type_schema() -> Schema {
        Schema::new(
            7,
            vec![
                Column::new(
                    1,
                    "time",
                    LogicalType::Timestamp {
                        unit: TimeUnit::Microsecond,
                        timezone: TimeZone::Utc,
                    },
                    false,
                ),
                Column::new(2, "bool", LogicalType::Bool, true),
                Column::new(3, "i8", LogicalType::Int8, true),
                Column::new(4, "i16", LogicalType::Int16, false),
                Column::new(5, "i32", LogicalType::Int32, true),
                Column::new(6, "i64", LogicalType::Int64, false),
                Column::new(7, "u8", LogicalType::UInt8, true),
                Column::new(8, "u16", LogicalType::UInt16, false),
                Column::new(9, "u32", LogicalType::UInt32, true),
                Column::new(10, "u64", LogicalType::UInt64, false),
                Column::new(11, "f32", LogicalType::Float32, true),
                Column::new(12, "f64", LogicalType::Float64, false),
                Column::new(
                    13,
                    "decimal",
                    LogicalType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                    true,
                ),
                Column::new(14, "text", LogicalType::Utf8, true),
                Column::new(
                    15,
                    "category",
                    LogicalType::Categorical { ordered: false },
                    true,
                ),
                Column::new(16, "binary", LogicalType::Binary, true),
                Column::new(
                    17,
                    "fixed",
                    LogicalType::FixedBinary { byte_width: 3 },
                    true,
                ),
                Column::new(18, "date", LogicalType::Date32, false),
                Column::new(19, "all_null", LogicalType::Int64, true),
                Column::new(20, "all_valid", LogicalType::Utf8, true),
            ],
            Some(1),
        )
    }

    fn every_type_batch(schema: &Schema, range: Range<usize>) -> RecordBatch {
        let rows: Vec<usize> = range.collect();
        let count = rows.len();
        // A null on every third row leaves mixed columns genuinely mixed.
        let mixed: Vec<bool> = rows.iter().map(|row| row % 3 != 1).collect();
        let present = |bits: &Vec<bool>| Some(bits.clone());
        let columns = vec![
            Array::Timestamp(TimestampArray::new(
                rows.iter().map(|row| *row as i64 * 10).collect(),
                None,
                TimeUnit::Microsecond,
                TimeZone::Utc,
            )),
            Array::Bool(BooleanArray::new(
                rows.iter().map(|row| row % 2 == 0).collect(),
                present(&mixed),
            )),
            Array::Int8(PrimitiveArray::new(
                rows.iter().map(|row| *row as i8).collect(),
                present(&mixed),
            )),
            Array::Int16(PrimitiveArray::new(
                rows.iter().map(|row| *row as i16).collect(),
                None,
            )),
            Array::Int32(PrimitiveArray::new(
                rows.iter().map(|row| *row as i32).collect(),
                present(&mixed),
            )),
            Array::Int64(PrimitiveArray::new(
                rows.iter().map(|row| *row as i64).collect(),
                None,
            )),
            Array::UInt8(PrimitiveArray::new(
                rows.iter().map(|row| *row as u8).collect(),
                present(&mixed),
            )),
            Array::UInt16(PrimitiveArray::new(
                rows.iter().map(|row| *row as u16).collect(),
                None,
            )),
            Array::UInt32(PrimitiveArray::new(
                rows.iter().map(|row| *row as u32).collect(),
                present(&mixed),
            )),
            Array::UInt64(PrimitiveArray::new(
                rows.iter().map(|row| *row as u64).collect(),
                None,
            )),
            Array::Float32(PrimitiveArray::new(
                rows.iter().map(|row| *row as f32).collect(),
                present(&mixed),
            )),
            Array::Float64(PrimitiveArray::new(
                rows.iter().map(|row| *row as f64).collect(),
                None,
            )),
            Array::Decimal(DecimalArray::new(
                rows.iter().map(|row| *row as i64).collect(),
                present(&mixed),
                10,
                2,
            )),
            // Varying byte lengths keep the variable-width arithmetic honest.
            Array::Utf8(Utf8Array::new(
                rows.iter().map(|row| "t".repeat(row % 5)).collect(),
                present(&mixed),
            )),
            Array::Categorical(Utf8Array::new(
                rows.iter().map(|row| format!("c{}", row % 3)).collect(),
                present(&mixed),
            )),
            Array::Binary(BinaryArray::new(
                rows.iter().map(|row| vec![*row as u8; row % 7]).collect(),
                present(&mixed),
            )),
            Array::FixedBinary(BinaryArray::new(
                rows.iter().map(|row| vec![*row as u8; 3]).collect(),
                present(&mixed),
            )),
            Array::Date32(PrimitiveArray::new(
                rows.iter().map(|row| *row as i32).collect(),
                None,
            )),
            Array::Int64(PrimitiveArray::new(
                vec![0; count],
                Some(vec![false; count]),
            )),
            Array::Utf8(Utf8Array::new(
                rows.iter().map(|row| format!("v{row}")).collect(),
                Some(vec![true; count]),
            )),
        ];
        RecordBatch::try_new(Arc::new(schema.clone()), columns, count).expect("a well-formed batch")
    }

    /// A buffer holding one appended batch.
    fn buffer_of(schema: &Schema, rows: Range<usize>) -> BlockBuffer {
        buffer_over(schema, &[rows])
    }

    /// A buffer holding the same rows, appended as several batches.
    fn buffer_over(schema: &Schema, chunks: &[Range<usize>]) -> BlockBuffer {
        let mut buffer =
            BlockBuffer::new_with_statistics(schema.column_count(), WriterStatistics::None);
        for chunk in chunks {
            buffer.push(schema, every_type_batch(schema, chunk.clone()));
        }
        buffer
    }

    fn serialize(schema: &Schema, buffer: &BlockBuffer) -> Vec<u8> {
        build_data_frame(
            schema,
            &buffer.rows(),
            false,
            UNAVAILABLE_BASE_ROW_ID,
            FIRST_DATA_FRAME_SEQUENCE,
            WriterCodec::None,
            WriterEncoding::Raw,
        )
        .expect("the buffered block serializes")
    }

    #[test]
    fn the_estimated_block_size_matches_the_serialized_frame() {
        let schema = every_type_schema();

        for rows in 1..=9 {
            let buffer = buffer_of(&schema, 0..rows);
            assert_eq!(
                buffer.frame_bytes(),
                serialize(&schema, &buffer).len() as u64,
                "the estimate disagrees with the serializer at {rows} rows"
            );
        }
    }

    #[test]
    fn the_estimated_block_size_ignores_how_the_rows_were_appended() {
        let schema = every_type_schema();

        assert_eq!(
            buffer_of(&schema, 0..6).frame_bytes(),
            buffer_over(&schema, &[0..1, 1..4, 4..6]).frame_bytes()
        );
    }

    #[test]
    fn a_block_serializes_the_same_however_its_rows_were_appended() {
        let schema = every_type_schema();

        assert_eq!(
            serialize(&schema, &buffer_of(&schema, 0..6)),
            serialize(&schema, &buffer_over(&schema, &[0..1, 1..4, 4..6]))
        );
    }

    #[test]
    fn a_published_buffer_reports_no_rows() {
        let schema = every_type_schema();
        let mut buffer = buffer_of(&schema, 0..4);

        buffer.clear();

        assert!(buffer.is_empty());
    }

    #[test]
    fn a_published_buffer_reports_no_bytes() {
        let schema = every_type_schema();
        let mut buffer = buffer_of(&schema, 0..4);

        buffer.clear();

        assert_eq!(buffer.frame_bytes(), 0);
    }

    #[test]
    fn a_cleared_buffer_prices_the_same_as_a_fresh_one() {
        let schema = every_type_schema();
        let mut reused = buffer_of(&schema, 0..4);
        reused.clear();

        reused.push(&schema, every_type_batch(&schema, 0..3));

        assert_eq!(reused.frame_bytes(), buffer_of(&schema, 0..3).frame_bytes());
    }

    #[test]
    fn an_empty_stream_is_still_given_an_alignment_unit() {
        assert_eq!(stream_payload_bytes(0), FRAME_ALIGNMENT);
    }

    #[test]
    fn a_stream_is_padded_up_to_the_frame_alignment() {
        assert_eq!(
            stream_payload_bytes(FRAME_ALIGNMENT + 1),
            FRAME_ALIGNMENT * 2
        );
    }

    #[test]
    fn an_aligned_stream_is_not_padded_further() {
        assert_eq!(stream_payload_bytes(FRAME_ALIGNMENT), FRAME_ALIGNMENT);
    }

    #[test]
    fn a_bitmap_rounds_up_to_whole_bytes() {
        assert_eq!(
            (bitmap_bytes(0), bitmap_bytes(1), bitmap_bytes(9)),
            (0, 1, 2)
        );
    }

    #[test]
    fn a_byte_target_beyond_the_frame_limit_is_refused() {
        let options = WriterOptions::default()
            .with_byte_block_target(Limits::default().max_frame_payload_length() + 1);

        assert_eq!(
            validate_options(options).unwrap_err().kind(),
            ErrorKind::ResourceLimit
        );
    }

    #[test]
    fn a_zero_byte_target_is_refused() {
        let options = WriterOptions::default().with_byte_block_target(0);

        assert_eq!(
            validate_options(options).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
    }

    #[test]
    fn a_zero_row_target_is_refused() {
        let options = WriterOptions::default().with_row_block_target(0);

        assert_eq!(
            validate_options(options).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
    }
}
