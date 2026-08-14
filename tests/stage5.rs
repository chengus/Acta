//! Stage 5 projection, primary-range, and cumulative scan-limit coverage.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use acta::{
    Array, BinaryArray, BooleanArray, Column, DecimalArray, ErrorKind, Limits, LogicalType,
    PrimaryRange, PrimitiveArray, Reader, RecordBatch, ScalarValue, Schema, TimeUnit, TimeZone,
    TimestampArray, Utf8Array, ValidationLevel, ValidationOptions, Writer, WriterEncoding,
    WriterOptions,
};
use common::{
    BLOCK_HEADER_SIZE, BLOCK_STATISTICS_LENGTH, BLOCK_STATISTICS_OFFSET, PREFIX_HEADER_LENGTH,
    PREFIX_SIZE, STREAM_PAYLOAD_OFFSET, STREAM_TRANSFORM, TemporaryFile, data_frame_offset,
    frame_end, payload_offset, put_u16, put_u32, read_u16, read_u32, read_u64, repair_frame,
    stream_descriptor_offset,
};

struct TempPath(PathBuf);

impl TempPath {
    fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(std::env::temp_dir().join(format!(
            "acta-stage5-{label}-{}-{}.acta",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn time_schema() -> Schema {
    Schema::new(
        50,
        vec![
            Column::new(
                20,
                "timestamp",
                LogicalType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: TimeZone::Utc,
                },
                false,
            ),
            Column::new(3, "value", LogicalType::Int64, false),
            Column::new(9, "label", LogicalType::Utf8, false),
        ],
        Some(20),
    )
}

fn time_batch(schema: &Schema, timestamps: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Timestamp(TimestampArray::new(
                timestamps.to_vec(),
                None,
                TimeUnit::Microsecond,
                TimeZone::Utc,
            )),
            Array::Int64(PrimitiveArray::new(
                timestamps.iter().map(|value| value * 10).collect(),
                None,
            )),
            Array::Utf8(Utf8Array::new(
                timestamps.iter().map(|value| format!("v{value}")).collect(),
                None,
            )),
        ],
        timestamps.len(),
    )
    .expect("well-formed timestamp batch")
}

fn write(schema: Schema, batch: RecordBatch, rows_per_block: u64) -> TempPath {
    write_with_options(
        schema,
        batch,
        WriterOptions::default().with_row_block_target(rows_per_block),
    )
}

fn write_with_options(schema: Schema, batch: RecordBatch, options: WriterOptions) -> TempPath {
    let path = TempPath::new("file");
    let mut writer = Writer::create(path.path(), schema, options).expect("create writer");
    writer.append(batch).expect("append batch");
    let _ = writer.finish().expect("finish writer");
    path
}

fn values(batch: &RecordBatch, column: usize) -> Vec<i64> {
    (0..batch.row_count())
        .map(
            |row| match batch.column(column).unwrap().value_at(row).unwrap() {
                acta::ScalarValue::Int64(value) => value,
                other => panic!("expected int64, found {other:?}"),
            },
        )
        .collect()
}

fn timestamps(batch: &RecordBatch, column: usize) -> Vec<i64> {
    (0..batch.row_count())
        .map(
            |row| match batch.column(column).unwrap().value_at(row).unwrap() {
                acta::ScalarValue::Timestamp { value, .. } => value,
                other => panic!("expected timestamp, found {other:?}"),
            },
        )
        .collect()
}

/// The same three columns as [`time_schema`], but with ascending column IDs so
/// that a test which reaches into the block header can name a column by its
/// position: section 8.1 orders the column table by column ID.
fn wire_schema() -> Schema {
    Schema::new(
        54,
        vec![
            Column::new(
                1,
                "timestamp",
                LogicalType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: TimeZone::Utc,
                },
                false,
            ),
            Column::new(2, "value", LogicalType::Int64, false),
            Column::new(3, "label", LogicalType::Utf8, false),
        ],
        Some(1),
    )
}

/// The stream table of a [`wire_schema`] block written by the raw writer: one
/// values stream per fixed-width column, then values and lengths for the UTF-8
/// column.
const TIMESTAMP_STREAM: usize = 0;
const VALUE_STREAM: usize = 1;
const LABEL_STREAM: usize = 2;
const WIRE_STREAM_COUNT: usize = 4;

/// Write a file, then hand its bytes to `patch` before writing them somewhere
/// else. Nothing here rebuilds the writer's output; the point is to damage one
/// field of a file this crate really produced.
fn patched(label: &str, source: &TempPath, patch: impl FnOnce(&mut Vec<u8>)) -> TemporaryFile {
    let mut bytes = std::fs::read(source.path()).expect("read the written file");
    patch(&mut bytes);
    TemporaryFile::new(label, &bytes)
}

/// The offset of the data frame holding block `index`.
fn block_frame(bytes: &[u8], index: usize) -> usize {
    let mut frame = data_frame_offset(bytes);
    for _ in 0..index {
        frame = frame_end(bytes, frame);
    }
    frame
}

fn stream_count(bytes: &[u8], frame: usize) -> usize {
    let header = frame + PREFIX_SIZE;
    let table = read_u32(bytes, header + common::BLOCK_STREAM_TABLE_OFFSET) as usize;
    let statistics = read_u32(bytes, header + BLOCK_STATISTICS_OFFSET) as usize;
    (statistics - table) / common::STREAM_DESCRIPTOR_SIZE
}

/// Declare a transform no v0.2 build implements for one stream of one block.
fn break_stream_transform(bytes: &mut [u8], block: usize, stream: usize) {
    let frame = block_frame(bytes, block);
    assert_eq!(stream_count(bytes, frame), WIRE_STREAM_COUNT);
    let descriptor = stream_descriptor_offset(bytes, frame, stream);
    put_u16(bytes, descriptor + STREAM_TRANSFORM, 99);
    repair_frame(bytes, frame);
}

/// Flip a byte inside one stream's stored payload, leaving every frame
/// checksum correct so that only that stream's own CRC disagrees.
fn corrupt_stream_payload(bytes: &mut [u8], block: usize, stream: usize) {
    let frame = block_frame(bytes, block);
    let descriptor = stream_descriptor_offset(bytes, frame, stream);
    let offset = read_u64(bytes, descriptor + STREAM_PAYLOAD_OFFSET) as usize;
    let target = payload_offset(bytes, frame) + offset;
    bytes[target] ^= 0xff;
    repair_frame(bytes, frame);
}

/// Splice a min/max statistics area into block zero and point the descriptor
/// of the column at `column_index` at it. The index is the position in the
/// column table, which section 8.1 sorts by column ID.
fn with_statistics(bytes: &mut Vec<u8>, column_index: usize, statistics: &[u8]) {
    const COLUMN_HAS_STATS: u16 = 2;
    const STATS_MIN_MAX: u16 = 1;
    const COLUMN_DESCRIPTOR_SIZE: usize = 32;

    let frame = data_frame_offset(bytes);
    let header_length_offset = frame + PREFIX_HEADER_LENGTH;
    let header_length = read_u32(bytes, header_length_offset) as usize;
    let padded = statistics.len().next_multiple_of(8);
    let mut stored = statistics.to_vec();
    stored.resize(padded, 0);
    let payload = payload_offset(bytes, frame);
    bytes.splice(payload..payload, stored);

    let header = frame + PREFIX_SIZE;
    put_u32(bytes, header_length_offset, (header_length + padded) as u32);
    put_u32(
        bytes,
        header + BLOCK_STATISTICS_OFFSET,
        header_length as u32,
    );
    put_u32(bytes, header + BLOCK_STATISTICS_LENGTH, padded as u32);

    let descriptor = header + BLOCK_HEADER_SIZE + column_index * COLUMN_DESCRIPTOR_SIZE;
    let flags = read_u16(bytes, descriptor + 6) | COLUMN_HAS_STATS;
    put_u16(bytes, descriptor + 6, flags);
    put_u16(bytes, descriptor + 22, STATS_MIN_MAX);
    put_u32(bytes, descriptor + 24, header_length as u32);
    put_u32(bytes, descriptor + 28, statistics.len() as u32);
    repair_frame(bytes, frame);
}

fn full_validation(path: &Path) -> acta::Result<acta::ValidationReport> {
    acta::validate_with_options(
        path,
        ValidationOptions::default().with_level(ValidationLevel::Full),
    )
}

/// Drive a scan to exhaustion and return the first failure, or the rows it
/// produced. Errors are items, so a caller that wants the first one has to
/// look for it.
fn drain(scan: impl Iterator<Item = acta::Result<RecordBatch>>) -> acta::Result<Vec<RecordBatch>> {
    scan.collect()
}

fn collect_one(scan: impl Iterator<Item = acta::Result<RecordBatch>>) -> RecordBatch {
    let mut batches: Vec<RecordBatch> = scan.map(|batch| batch.expect("scan batch")).collect();
    assert_eq!(batches.len(), 1, "the test expected one output batch");
    batches.pop().unwrap()
}

#[test]
fn default_and_reordered_projection_preserve_schema_order_ids_and_primary_designation() {
    let schema = time_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2]), 65_536);
    let reader = Reader::open(path.path()).expect("open");

    let all = collect_one(reader.scan());
    assert_eq!(
        all.schema()
            .columns()
            .iter()
            .map(Column::name)
            .collect::<Vec<_>>(),
        vec!["timestamp", "value", "label"]
    );
    assert_eq!(
        all.schema()
            .columns()
            .iter()
            .map(Column::id)
            .collect::<Vec<_>>(),
        vec![20, 3, 9]
    );
    assert_eq!(all.schema().primary_column_id(), Some(20));

    let reordered = collect_one(
        reader
            .scan()
            .project(["label", "timestamp"])
            .expect("projection is valid"),
    );
    assert_eq!(
        reordered
            .schema()
            .columns()
            .iter()
            .map(Column::name)
            .collect::<Vec<_>>(),
        vec!["label", "timestamp"]
    );
    assert_eq!(reordered.schema().primary_column_id(), Some(20));
    assert_eq!(
        reordered.column(1).unwrap().value_at(2),
        all.column(0).unwrap().value_at(2)
    );

    let value_only = collect_one(reader.scan().project(["value"]).unwrap());
    assert_eq!(value_only.schema().primary_column_id(), None);
    assert_eq!(values(&value_only, 0), vec![0, 10, 20]);
}

#[test]
fn empty_projection_preserves_rows_and_unknown_or_duplicate_names_fail_immediately() {
    let schema = time_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2, 3]), 2);
    let reader = Reader::open(path.path()).expect("open");

    let batches: Vec<RecordBatch> = reader
        .scan()
        .project([] as [&str; 0])
        .unwrap()
        .map(|batch| batch.expect("empty projection decodes"))
        .collect();
    assert_eq!(batches.iter().map(RecordBatch::row_count).sum::<usize>(), 4);
    assert!(
        batches
            .iter()
            .all(|batch| batch.schema().column_count() == 0)
    );

    assert_eq!(
        reader.scan().project(["missing"]).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        reader
            .scan()
            .project(["value", "value"])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
}

#[test]
fn selected_streams_are_the_only_streams_decoded() {
    let schema = time_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2]), 65_536);
    let reader = Reader::open(path.path()).expect("open");

    let mut scan = reader.scan().project(["value"]).unwrap();
    let _batch = scan.next().unwrap().unwrap();
    let metrics = scan.metrics();
    assert_eq!(metrics.blocks_considered(), 1);
    assert_eq!(metrics.blocks_pruned(), 0);
    assert_eq!(metrics.streams_decoded(), 1);
    assert!(metrics.bytes_read() > 0);
    assert_eq!(metrics.rows_returned(), 3);
}

#[test]
fn timestamp_ranges_are_half_open_pruned_and_file_ordered() {
    let schema = time_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2, 3, 4, 5]), 2);
    let reader = Reader::open(path.path()).expect("open");

    let mut scan = reader
        .scan()
        .project(["timestamp", "value"])
        .unwrap()
        .primary_range(PrimaryRange::timestamp(2, 4))
        .unwrap();
    let batches: Vec<RecordBatch> = scan.by_ref().map(|batch| batch.unwrap()).collect();
    assert_eq!(batches.len(), 1);
    assert_eq!(timestamps(&batches[0], 0), vec![2, 3]);
    assert_eq!(values(&batches[0], 1), vec![20, 30]);
    assert_eq!(batches[0].schema().primary_column_id(), Some(20));
    assert_eq!(scan.metrics().blocks_considered(), 3);
    assert_eq!(scan.metrics().blocks_pruned(), 2);
    assert_eq!(scan.metrics().streams_decoded(), 2);

    let empty: Vec<RecordBatch> = reader
        .scan()
        .primary_range(PrimaryRange::timestamp(3, 3))
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect();
    assert!(empty.is_empty());

    let empty_projection: Vec<RecordBatch> = reader
        .scan()
        .project([] as [&str; 0])
        .unwrap()
        .primary_range(PrimaryRange::timestamp(2, 4))
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect();
    assert_eq!(
        empty_projection
            .iter()
            .map(RecordBatch::row_count)
            .sum::<usize>(),
        2
    );
    assert!(
        empty_projection
            .iter()
            .all(|batch| batch.schema().column_count() == 0)
    );
}

#[test]
fn unsorted_ranges_keep_matching_rows_in_block_order_and_date32_is_typed() {
    let schema = time_schema();
    let path = write(
        schema.clone(),
        time_batch(&schema, &[3, 1, 4, 2, 5]),
        65_536,
    );
    let reader = Reader::open(path.path()).expect("open");
    let batch = collect_one(
        reader
            .scan()
            .project(["value"])
            .unwrap()
            .primary_range(PrimaryRange::timestamp(2, 5))
            .unwrap(),
    );
    assert_eq!(values(&batch, 0), vec![30, 40, 20]);

    let date_schema = Schema::new(
        51,
        vec![Column::new(1, "day", LogicalType::Date32, false)],
        Some(1),
    );
    let date_batch = RecordBatch::try_new(
        Arc::new(date_schema.clone()),
        vec![Array::Date32(PrimitiveArray::new(
            vec![-2, -1, 0, 1, 2],
            None,
        ))],
        5,
    )
    .unwrap();
    let date_path = write(date_schema, date_batch, 65_536);
    let date_reader = Reader::open(date_path.path()).unwrap();
    let date = collect_one(
        date_reader
            .scan()
            .primary_range(PrimaryRange::date32(-1, 2))
            .unwrap(),
    );
    assert_eq!(date.row_count(), 3);
    assert_eq!(
        date.column(0).unwrap().value_at(0),
        Some(acta::ScalarValue::Date32(-1))
    );
    assert_eq!(
        date.column(0).unwrap().value_at(2),
        Some(acta::ScalarValue::Date32(1))
    );
}

#[test]
fn range_configuration_rejects_invalid_or_incompatible_schema_requests() {
    let schema = time_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0]), 65_536);
    let reader = Reader::open(path.path()).unwrap();
    assert_eq!(
        reader
            .scan()
            .primary_range(PrimaryRange::timestamp(2, 1))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        reader
            .scan()
            .primary_range(PrimaryRange::date32(0, 1))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );

    let no_primary = Schema::new(
        52,
        vec![Column::new(1, "value", LogicalType::Int64, false)],
        None,
    );
    let no_primary_batch = RecordBatch::try_new(
        Arc::new(no_primary.clone()),
        vec![Array::Int64(PrimitiveArray::new(vec![1], None))],
        1,
    )
    .unwrap();
    let no_primary_path = write(no_primary, no_primary_batch, 65_536);
    let no_primary_reader = Reader::open(no_primary_path.path()).unwrap();
    assert_eq!(
        no_primary_reader
            .scan()
            .primary_range(PrimaryRange::timestamp(0, 1))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
}

#[test]
fn cumulative_limits_charge_candidate_blocks_and_selected_bytes() {
    let schema = time_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2, 3]), 2);
    let limited =
        Reader::open_with_limits(path.path(), Limits::default().with_max_rows_per_scan(2)).unwrap();
    let mut scan = limited.scan();
    assert_eq!(scan.next().unwrap().unwrap().row_count(), 2);
    assert_eq!(
        scan.next().unwrap().unwrap_err().kind(),
        ErrorKind::ResourceLimit
    );

    let no_bytes = Reader::open_with_limits(
        path.path(),
        Limits::default().with_max_decoded_scan_bytes(0),
    )
    .unwrap();
    assert_eq!(
        no_bytes.scan().next().unwrap().unwrap_err().kind(),
        ErrorKind::ResourceLimit
    );

    let pruned =
        Reader::open_with_limits(path.path(), Limits::default().with_max_rows_per_scan(0)).unwrap();
    let mut scan = pruned
        .scan()
        .primary_range(PrimaryRange::timestamp(100, 101))
        .unwrap();
    assert!(scan.next().is_none());
    assert_eq!(scan.metrics().blocks_pruned(), 2);
}

#[test]
fn projection_and_ranges_work_on_adaptive_and_zstandard_blocks() {
    let schema = time_schema();
    let options = WriterOptions::default()
        .with_row_block_target(2)
        .with_encoding(WriterEncoding::Adaptive);
    let path = write_with_options(schema.clone(), time_batch(&schema, &[0, 1, 2, 3]), options);
    let reader = Reader::open(path.path()).unwrap();
    let batches: Vec<RecordBatch> = reader
        .scan()
        .project(["value"])
        .unwrap()
        .primary_range(PrimaryRange::timestamp(1, 3))
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect();
    assert_eq!(
        batches
            .iter()
            .flat_map(|batch| values(batch, 0))
            .collect::<Vec<_>>(),
        vec![10, 20]
    );

    #[cfg(feature = "zstd")]
    {
        let path = write_with_options(
            schema.clone(),
            time_batch(&schema, &[0, 1, 2, 3]),
            options.with_codec(acta::WriterCodec::Zstandard),
        );
        let reader = Reader::open(path.path()).unwrap();
        let batches: Vec<RecordBatch> = reader
            .scan()
            .project(["label"])
            .unwrap()
            .map(|batch| batch.unwrap())
            .collect();
        assert!(
            batches
                .iter()
                .all(|batch| batch.schema().columns()[0].name() == "label")
        );
        assert_eq!(batches.iter().map(RecordBatch::row_count).sum::<usize>(), 4);
    }
}

#[test]
fn every_native_logical_type_can_be_projected() {
    let schema = Schema::new(
        53,
        vec![
            Column::new(
                1,
                "timestamp",
                LogicalType::Timestamp {
                    unit: TimeUnit::Nanosecond,
                    timezone: TimeZone::Naive,
                },
                false,
            ),
            Column::new(2, "bool", LogicalType::Bool, false),
            Column::new(3, "i8", LogicalType::Int8, false),
            Column::new(4, "i16", LogicalType::Int16, false),
            Column::new(5, "i32", LogicalType::Int32, false),
            Column::new(6, "i64", LogicalType::Int64, false),
            Column::new(7, "u8", LogicalType::UInt8, false),
            Column::new(8, "u16", LogicalType::UInt16, false),
            Column::new(9, "u32", LogicalType::UInt32, false),
            Column::new(10, "u64", LogicalType::UInt64, false),
            Column::new(11, "f32", LogicalType::Float32, false),
            Column::new(12, "f64", LogicalType::Float64, false),
            Column::new(
                13,
                "decimal",
                LogicalType::Decimal {
                    precision: 9,
                    scale: 2,
                },
                false,
            ),
            Column::new(14, "text", LogicalType::Utf8, false),
            Column::new(15, "cat", LogicalType::Categorical { ordered: true }, false),
            Column::new(16, "binary", LogicalType::Binary, false),
            Column::new(
                17,
                "fixed",
                LogicalType::FixedBinary { byte_width: 2 },
                false,
            ),
            Column::new(18, "date", LogicalType::Date32, false),
        ],
        Some(1),
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Timestamp(TimestampArray::new(
                vec![1, 2],
                None,
                TimeUnit::Nanosecond,
                TimeZone::Naive,
            )),
            Array::Bool(BooleanArray::new(vec![true, false], None)),
            Array::Int8(PrimitiveArray::new(vec![-1, 1], None)),
            Array::Int16(PrimitiveArray::new(vec![-2, 2], None)),
            Array::Int32(PrimitiveArray::new(vec![-3, 3], None)),
            Array::Int64(PrimitiveArray::new(vec![-4, 4], None)),
            Array::UInt8(PrimitiveArray::new(vec![5, 6], None)),
            Array::UInt16(PrimitiveArray::new(vec![7, 8], None)),
            Array::UInt32(PrimitiveArray::new(vec![9, 10], None)),
            Array::UInt64(PrimitiveArray::new(vec![11, 12], None)),
            Array::Float32(PrimitiveArray::new(vec![1.0, 2.0], None)),
            Array::Float64(PrimitiveArray::new(vec![3.0, 4.0], None)),
            Array::Decimal(DecimalArray::new(vec![500, 600], None, 9, 2)),
            Array::Utf8(Utf8Array::new(vec!["a".into(), "b".into()], None)),
            Array::Categorical(Utf8Array::new(vec!["x".into(), "y".into()], None)),
            Array::Binary(BinaryArray::new(vec![vec![1], vec![2]], None)),
            Array::FixedBinary(BinaryArray::new(vec![vec![3, 4], vec![5, 6]], None)),
            Array::Date32(PrimitiveArray::new(vec![7, 8], None)),
        ],
        2,
    )
    .unwrap();
    let path = write(schema.clone(), batch, 65_536);
    let reader = Reader::open(path.path()).unwrap();
    for column in schema.columns() {
        let projected = collect_one(reader.scan().project([column.name()]).unwrap());
        assert_eq!(projected.schema().columns()[0].id(), column.id());
        assert_eq!(projected.row_count(), 2);
    }
}

// ------------------------------------------------------------ range edges

/// Block bounds are inclusive and the range is half-open, so the two meet at
/// exactly one place in each direction: a block whose maximum equals the start
/// is kept, and a block whose minimum equals the end is not.
#[test]
fn inclusive_block_bounds_are_pruned_at_exactly_the_range_edges() {
    let schema = time_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2, 3, 4, 5]), 2);
    let reader = Reader::open(path.path()).expect("open");

    // Blocks bound [0, 1], [2, 3] and [4, 5].
    for (start, end, expected, pruned) in [
        // The first block's maximum is the start: kept, and it contributes.
        (1, 2, vec![1_i64], 2),
        // The middle block's minimum is the end: pruned.
        (0, 2, vec![0, 1], 2),
        // The last block's maximum is the start: kept.
        (5, 6, vec![5], 2),
        // Wholly inside one block.
        (3, 4, vec![3], 2),
        // Every block overlaps.
        (1, 5, vec![1, 2, 3, 4], 0),
    ] {
        let mut scan = reader
            .scan()
            .project(["timestamp"])
            .unwrap()
            .primary_range(PrimaryRange::timestamp(start, end))
            .unwrap();
        let rows: Vec<i64> = scan
            .by_ref()
            .flat_map(|batch| timestamps(&batch.expect("range scan"), 0))
            .collect();
        assert_eq!(rows, expected, "range [{start}, {end})");
        assert_eq!(
            scan.metrics().blocks_pruned(),
            pruned,
            "range [{start}, {end}) pruned the wrong number of blocks"
        );
        assert_eq!(scan.metrics().blocks_considered(), 3);
        assert_eq!(scan.metrics().rows_returned(), expected.len() as u64);
    }
}

/// A block whose bounds straddle the range can still hold no row inside it.
/// Inclusive bounds cannot see the gap, so the block is decoded and then
/// contributes nothing; it must not surface as an empty batch.
#[test]
fn an_overlapping_block_with_no_matching_row_yields_no_batch() {
    let schema = time_schema();
    for timestamps in [[0_i64, 10, 20, 30], [10, 0, 30, 20]] {
        let path = write(schema.clone(), time_batch(&schema, &timestamps), 2);
        let reader = Reader::open(path.path()).expect("open");
        let mut scan = reader
            .scan()
            .project(["value"])
            .unwrap()
            .primary_range(PrimaryRange::timestamp(1, 10))
            .unwrap();
        let batches: Vec<RecordBatch> = scan.by_ref().map(|batch| batch.expect("scan")).collect();

        assert!(batches.is_empty(), "{timestamps:?} produced {batches:?}");
        let metrics = scan.metrics();
        assert_eq!(metrics.blocks_considered(), 2);
        // The second block is pruned; the first has to be decoded to find out.
        assert_eq!(metrics.blocks_pruned(), 1);
        assert_eq!(metrics.rows_returned(), 0);
        assert!(metrics.streams_decoded() > 0);
    }
}

// ------------------------------------------------------- damaged files

/// Pruning happens before any byte of a block is read, so a block the range
/// excludes cannot fail a scan however damaged it is.
#[test]
fn a_malformed_block_that_pruning_skips_never_fails_the_scan() {
    let schema = wire_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2, 3]), 2);
    let file = patched("stage5-pruned-malformed", &path, |bytes| {
        break_stream_transform(bytes, 1, TIMESTAMP_STREAM);
    });
    let reader = Reader::open(file.path()).expect("the damage is inside a frame, not its framing");

    let mut scan = reader
        .scan()
        .project(["value"])
        .unwrap()
        .primary_range(PrimaryRange::timestamp(0, 2))
        .unwrap();
    let batches = drain(scan.by_ref()).expect("the damaged block is pruned");
    assert_eq!(values(&batches[0], 0), vec![0, 10]);
    assert_eq!(scan.metrics().blocks_pruned(), 1);

    // A range that reaches the block decodes the column it damaged, and only
    // then does the scan fail.
    let mut scan = reader
        .scan()
        .project(["value"])
        .unwrap()
        .primary_range(PrimaryRange::timestamp(0, 4))
        .unwrap();
    assert_eq!(scan.next().unwrap().unwrap().row_count(), 2);
    assert_eq!(
        scan.next().unwrap().unwrap_err().kind(),
        ErrorKind::UnsupportedFrame
    );
    assert_eq!(
        drain(reader.scan())
            .expect_err("the default projection reads it too")
            .kind(),
        ErrorKind::UnsupportedFrame
    );
}

/// A transform this build cannot apply is only a problem for a column the read
/// actually decodes. Validation is deferred, not skipped: every path that does
/// decode the column still fails.
#[test]
fn an_unsupported_transform_in_an_unprojected_column_does_not_fail_a_projected_read() {
    let schema = wire_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2]), 65_536);
    let file = patched("stage5-unprojected-transform", &path, |bytes| {
        break_stream_transform(bytes, 0, LABEL_STREAM);
    });
    let reader = Reader::open(file.path()).expect("open");

    let projected = collect_one(reader.scan().project(["timestamp", "value"]).unwrap());
    assert_eq!(values(&projected, 1), vec![0, 10, 20]);

    let error = drain(reader.scan().project(["label"]).unwrap())
        .expect_err("decoding the column reaches its transform");
    assert_eq!(error.kind(), ErrorKind::UnsupportedFrame);
    assert!(error.to_string().contains("label"), "{error}");

    assert_eq!(
        drain(reader.scan())
            .expect_err("the default projection reads every column")
            .kind(),
        ErrorKind::UnsupportedFrame
    );
    assert_eq!(
        reader.read_block(0).unwrap_err().kind(),
        ErrorKind::UnsupportedFrame
    );
    assert_eq!(
        full_validation(file.path()).unwrap_err().kind(),
        ErrorKind::UnsupportedFrame
    );
}

/// Damage inside a column the scan selected is reported, and the report names
/// both the frame and the column.
#[test]
fn corruption_in_a_selected_column_fails_with_frame_and_column_context() {
    let schema = wire_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2]), 65_536);
    let file = patched("stage5-selected-corruption", &path, |bytes| {
        corrupt_stream_payload(bytes, 0, VALUE_STREAM);
    });
    let reader = Reader::open(file.path()).expect("open");

    let error =
        drain(reader.scan().project(["value"]).unwrap()).expect_err("the stream CRC disagrees");
    assert_eq!(error.kind(), ErrorKind::Corruption);
    assert!(error.to_string().contains("value"), "{error}");
    assert!(error.to_string().contains("sequence"), "{error}");

    // A different column of the same block is untouched.
    let intact = collect_one(reader.scan().project(["label"]).unwrap());
    assert_eq!(intact.row_count(), 3);
}

/// Section 11 statistics are a claim about one column's values, so a scan
/// settles exactly the claims of the columns it decoded. Full validation
/// decodes every column and so settles all of them.
#[test]
fn false_statistics_fail_only_for_the_columns_a_scan_decodes() {
    let schema = wire_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2]), 65_536);
    // Column table position 1 is column ID 2, `value`, whose real values are
    // 0, 10 and 20.
    let statistics = [1_i64.to_le_bytes(), 2_i64.to_le_bytes()].concat();
    let file = patched("stage5-false-statistics", &path, |bytes| {
        with_statistics(bytes, 1, &statistics);
    });
    let reader = Reader::open(file.path()).expect("open");

    let deferred = collect_one(reader.scan().project(["label"]).unwrap());
    assert_eq!(deferred.row_count(), 3);

    let error = drain(reader.scan().project(["value"]).unwrap())
        .expect_err("the projected column's statistics are false");
    assert_eq!(error.kind(), ErrorKind::Corruption);
    assert!(error.to_string().contains("value"), "{error}");

    assert_eq!(
        reader.read_block(0).unwrap_err().kind(),
        ErrorKind::Corruption
    );
    assert_eq!(
        full_validation(file.path()).unwrap_err().kind(),
        ErrorKind::Corruption
    );
}

/// The primary column a range decodes internally is a decoded column like any
/// other: its section 8 bounds are checked even though it never reaches the
/// output batch.
#[test]
fn false_primary_bounds_fail_a_range_scan_that_does_not_project_the_primary() {
    let schema = wire_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2]), 65_536);
    let file = patched("stage5-false-bounds", &path, |bytes| {
        let frame = data_frame_offset(bytes);
        let header = frame + PREFIX_SIZE;
        common::put_u64(bytes, header + common::BLOCK_PRIMARY_MAX, 999);
        repair_frame(bytes, frame);
    });
    let reader = Reader::open(file.path()).expect("open");
    assert_eq!(
        reader.blocks()[0].primary_bounds().map(|b| b.max()),
        Some(999)
    );

    let error = drain(
        reader
            .scan()
            .project(["value"])
            .unwrap()
            .primary_range(PrimaryRange::timestamp(0, 1_000))
            .unwrap(),
    )
    .expect_err("the internally decoded primary contradicts the header");
    assert_eq!(error.kind(), ErrorKind::Corruption);

    // The same check is owed by every path that decodes the column.
    assert_eq!(
        reader.read_block(0).unwrap_err().kind(),
        ErrorKind::Corruption
    );
    assert_eq!(
        drain(reader.scan()).unwrap_err().kind(),
        ErrorKind::Corruption
    );
    assert_eq!(
        full_validation(file.path()).unwrap_err().kind(),
        ErrorKind::Corruption
    );

    // A projection that decodes neither the primary nor a false claim is
    // unaffected: nothing it read disagrees with anything.
    assert_eq!(
        collect_one(reader.scan().project(["value"]).unwrap()).row_count(),
        3
    );
}

// ------------------------------------------------------------ null patterns

/// Filtering rebuilds each selected array from chosen positions, so validity
/// has to travel with the values rather than be rebuilt from a count.
#[test]
fn null_patterns_survive_projection_and_range_filtering() {
    let schema = Schema::new(
        55,
        vec![
            Column::new(
                1,
                "timestamp",
                LogicalType::Timestamp {
                    unit: TimeUnit::Millisecond,
                    timezone: TimeZone::Naive,
                },
                false,
            ),
            Column::new(2, "mixed", LogicalType::Int64, true),
            Column::new(3, "all_null", LogicalType::Utf8, true),
            Column::new(4, "never_null", LogicalType::Bool, true),
        ],
        Some(1),
    );
    let rows = 6;
    let mixed_validity: Vec<bool> = (0..rows).map(|row| row % 2 == 0).collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Timestamp(TimestampArray::new(
                (0..rows as i64).collect(),
                None,
                TimeUnit::Millisecond,
                TimeZone::Naive,
            )),
            Array::Int64(PrimitiveArray::new(
                (0..rows as i64).map(|row| row * 100).collect(),
                Some(mixed_validity.clone()),
            )),
            Array::Utf8(Utf8Array::new(
                vec![String::new(); rows],
                Some(vec![false; rows]),
            )),
            Array::Bool(BooleanArray::new(
                (0..rows).map(|row| row % 3 == 0).collect(),
                Some(vec![true; rows]),
            )),
        ],
        rows,
    )
    .expect("well-formed nullable batch");
    let path = write(schema, batch, 2);
    let reader = Reader::open(path.path()).expect("open");

    let batches: Vec<RecordBatch> = drain(
        reader
            .scan()
            .project(["all_null", "never_null", "mixed"])
            .unwrap()
            .primary_range(PrimaryRange::timestamp(1, 5))
            .unwrap(),
    )
    .expect("nullable range scan");
    assert_eq!(batches.iter().map(RecordBatch::row_count).sum::<usize>(), 4);

    let mut row_index = 1;
    for batch in &batches {
        for row in 0..batch.row_count() {
            assert_eq!(
                batch.column(0).unwrap().value_at(row),
                None,
                "row {row_index}"
            );
            assert_eq!(
                batch.column(1).unwrap().value_at(row),
                Some(ScalarValue::Bool(row_index % 3 == 0)),
                "row {row_index}"
            );
            let expected =
                mixed_validity[row_index].then(|| ScalarValue::Int64(row_index as i64 * 100));
            assert_eq!(
                batch.column(2).unwrap().value_at(row),
                expected,
                "row {row_index}"
            );
            row_index += 1;
        }
    }
}

// --------------------------------------------------------------- accounting

/// A snapshot is taken at open time. A projection changes which columns a scan
/// reads, not which blocks it can see.
#[test]
fn a_later_append_is_not_visible_to_a_projected_scan() {
    let schema = time_schema();
    let path = TempPath::new("append-isolation");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default().with_row_block_target(2),
    )
    .expect("create writer");
    writer
        .append(time_batch(&schema, &[0, 1]))
        .expect("first append");
    writer.flush().expect("publish the first block");

    let reader = Reader::open(path.path()).expect("open");
    writer
        .append(time_batch(&schema, &[2, 3]))
        .expect("second append");
    let _ = writer.finish().expect("finish writer");

    assert_eq!(reader.blocks().len(), 1);
    let batches = drain(reader.scan().project(["value"]).unwrap()).expect("scan the snapshot");
    assert_eq!(batches.iter().map(RecordBatch::row_count).sum::<usize>(), 2);
    assert_eq!(values(&batches[0], 0), vec![0, 10]);

    // Reopening the path is how a caller asks for the newer file.
    let reopened = Reader::open(path.path()).expect("reopen");
    assert_eq!(reopened.blocks().len(), 2);
}

/// The two byte counters answer different questions: one measures the work
/// projection removes, the other what the scan cost the file system.
#[test]
fn metrics_separate_decoded_stream_bytes_from_bytes_read() {
    let schema = time_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2, 3]), 2);
    let reader = Reader::open(path.path()).expect("open");

    let mut full = reader.scan();
    drain(full.by_ref()).expect("full scan");
    let mut sparse = reader.scan().project(["value"]).unwrap();
    drain(sparse.by_ref()).expect("sparse scan");

    let full = full.metrics();
    let sparse = sparse.metrics();
    assert!(
        sparse.stream_bytes_decoded() < full.stream_bytes_decoded(),
        "projection did not reduce decoded stream bytes: {sparse:?} against {full:?}"
    );
    assert_eq!(sparse.streams_decoded(), 2);
    assert_eq!(full.streams_decoded(), 8);
    // Reading a block verifies its whole frame body, so the bytes a scan reads
    // are always more than the streams it decoded.
    assert!(sparse.bytes_read() > sparse.stream_bytes_decoded());
    assert_eq!(sparse.rows_returned(), 4);

    // A pruned block reads nothing at all.
    let mut pruned = reader
        .scan()
        .primary_range(PrimaryRange::timestamp(100, 200))
        .unwrap();
    assert!(pruned.by_ref().next().is_none());
    let pruned = pruned.metrics();
    assert_eq!(pruned.blocks_pruned(), 2);
    assert_eq!(pruned.bytes_read(), 0);
    assert_eq!(pruned.stream_bytes_decoded(), 0);
    assert_eq!(pruned.streams_decoded(), 0);
}

/// A cumulative allowance belongs to one scan. It is not a property of the
/// snapshot, so a single-block read never inherits what a scan has spent.
#[test]
fn read_block_does_not_inherit_cumulative_scan_state() {
    let schema = time_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2, 3]), 2);
    let reader = Reader::open_with_limits(
        path.path(),
        Limits::default()
            .with_max_rows_per_scan(0)
            .with_max_decoded_scan_bytes(0),
    )
    .expect("open");

    assert_eq!(
        reader.scan().next().unwrap().unwrap_err().kind(),
        ErrorKind::ResourceLimit
    );
    assert_eq!(reader.read_block(0).expect("a block decode").row_count(), 2);
    assert_eq!(reader.read_block(1).expect("a block decode").row_count(), 2);

    // The per-block allowance still applies on its own.
    let tiny = Reader::open_with_limits(
        path.path(),
        Limits::default().with_max_decoded_block_bytes(8),
    )
    .expect("open");
    assert_eq!(
        tiny.read_block(0).unwrap_err().kind(),
        ErrorKind::ResourceLimit
    );
}

/// An exhausted allowance stays exhausted, and a failure is an item rather
/// than the end of the scan, so every later block reports it too.
#[test]
fn an_exhausted_cumulative_allowance_fails_each_remaining_block() {
    let schema = time_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 1, 2, 3, 4, 5]), 2);
    let reader =
        Reader::open_with_limits(path.path(), Limits::default().with_max_rows_per_scan(2)).unwrap();

    let mut scan = reader.scan();
    assert_eq!(
        scan.next()
            .unwrap()
            .expect("the first block fits")
            .row_count(),
        2
    );
    let kinds: Vec<ErrorKind> = scan
        .by_ref()
        .map(|item| item.expect_err("the allowance is spent").kind())
        .collect();
    assert_eq!(
        kinds,
        vec![ErrorKind::ResourceLimit, ErrorKind::ResourceLimit]
    );
    assert!(
        scan.next().is_none(),
        "the scan is fused once blocks run out"
    );
    assert_eq!(scan.metrics().rows_returned(), 2);
}

/// The snapshot's `TS_SORTED` and the block's own are two separate reads of
/// the file, and a file can be replaced between them. Filtering therefore
/// binary-searches only what the decode established about the values it read:
/// searching values that are not really ordered would return arbitrary
/// boundaries, and a boundary pair in the wrong order would be a panic rather
/// than an error.
#[test]
fn filtering_follows_the_decoded_order_rather_than_the_captured_flag() {
    let schema = Schema::new(
        56,
        vec![
            Column::new(
                1,
                "timestamp",
                LogicalType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: TimeZone::Utc,
                },
                false,
            ),
            Column::new(2, "value", LogicalType::Int64, false),
        ],
        Some(1),
    );
    let two_columns = |timestamps: &[i64]| {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Array::Timestamp(TimestampArray::new(
                    timestamps.to_vec(),
                    None,
                    TimeUnit::Microsecond,
                    TimeZone::Utc,
                )),
                Array::Int64(PrimitiveArray::new(
                    timestamps.iter().map(|value| value * 10).collect(),
                    None,
                )),
            ],
            timestamps.len(),
        )
        .expect("well-formed batch")
    };

    // Two files of identical shape: same schema, same row count, same block
    // geometry, so every frame of the second lands exactly where the first
    // one's did. Only the order of the rows differs, and with it TS_SORTED.
    let sorted = write_with_options(
        schema.clone(),
        two_columns(&[0, 1, 2, 3, 4, 5, 6, 7]),
        WriterOptions::default().with_row_block_target(8),
    );
    let shuffled = write_with_options(
        schema.clone(),
        two_columns(&[5, 0, 7, 2, 6, 1, 4, 3]),
        WriterOptions::default().with_row_block_target(8),
    );
    let sorted_bytes = std::fs::read(sorted.path()).expect("the sorted file");
    let shuffled_bytes = std::fs::read(shuffled.path()).expect("the shuffled file");
    assert_eq!(
        sorted_bytes.len(),
        shuffled_bytes.len(),
        "the two files must have the same shape for this test to mean anything"
    );

    let reader = Reader::open(sorted.path()).expect("open the sorted file");
    assert!(reader.blocks()[0].ts_sorted(), "the snapshot claims order");
    std::fs::write(sorted.path(), &shuffled_bytes).expect("replace the file under the reader");

    // Any verdict is acceptable except a panic or an invented row. What the
    // reader must not do is trust the flag it captured earlier.
    let batches = drain(
        reader
            .scan()
            .project(["value"])
            .unwrap()
            .primary_range(PrimaryRange::timestamp(2, 6))
            .unwrap(),
    );
    if let Ok(batches) = batches {
        let returned: Vec<i64> = batches.iter().flat_map(|batch| values(batch, 0)).collect();
        assert_eq!(
            returned,
            vec![50, 20, 40, 30],
            "the rows must be the ones the file on disk really holds, in its order"
        );
    }
}

/// `remaining_candidate_blocks` is an upper bound the planner can defend, not
/// a promise about how many batches follow.
#[test]
fn remaining_candidate_blocks_never_understates_what_the_scan_yields() {
    let schema = time_schema();
    let path = write(schema.clone(), time_batch(&schema, &[0, 10, 20, 30]), 2);
    let reader = Reader::open(path.path()).expect("open");

    let mut scan = reader
        .scan()
        .primary_range(PrimaryRange::timestamp(1, 10))
        .unwrap();
    // One block overlaps the range but holds no matching row.
    assert_eq!(scan.remaining_candidate_blocks(), 1);
    assert_eq!(scan.size_hint(), (0, Some(1)));
    assert!(scan.next().is_none());
    assert_eq!(scan.remaining_candidate_blocks(), 0);
    assert_eq!(scan.size_hint(), (0, Some(0)));

    let mut plain = reader.scan();
    assert_eq!(plain.remaining_candidate_blocks(), 2);
    let _ = plain.next();
    assert_eq!(plain.remaining_candidate_blocks(), 1);
}
