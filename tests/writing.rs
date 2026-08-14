//! Stage 6 compatibility and Stage 7 buffered-writer coverage.

mod common;

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use acta::{
    Array, BinaryArray, BooleanArray, Column, DecimalArray, Error, ErrorKind, Limits, LogicalType,
    PrimitiveArray, Reader, RecordBatch, Schema, TimeUnit, TimeZone, TimestampArray, Utf8Array,
    Writer, WriterCodec, WriterEncoding, WriterOptions, WriterStatistics, WriterTransform,
};

/// Every data frame's sequence number and stored base row ID, read straight
/// from the wire with the transcribed field offsets in `common` rather than
/// through a `Reader`.
fn wire_sequences_and_row_ids(bytes: &[u8]) -> (Vec<u64>, Vec<u64>) {
    let mut sequences = Vec::new();
    let mut row_ids = Vec::new();
    let mut offset = common::data_frame_offset(bytes);
    while offset < bytes.len() {
        assert_eq!(
            &bytes[offset..offset + common::FRAME_MAGIC.len()],
            &common::FRAME_MAGIC,
            "a frame must begin at every computed boundary"
        );
        sequences.push(common::read_u64(bytes, offset + common::PREFIX_SEQUENCE));
        row_ids.push(common::read_u64(
            bytes,
            offset + common::PREFIX_SIZE + common::BLOCK_BASE_ROW_ID,
        ));
        offset = common::frame_end(bytes, offset);
    }
    assert_eq!(offset, bytes.len(), "the file must end on a frame boundary");
    (sequences, row_ids)
}

struct TempPath(PathBuf);

impl TempPath {
    fn new(label: &str) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        Self(std::env::temp_dir().join(format!(
            "acta-writer-{label}-{}-{}.acta",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        )))
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn write_batch(
    label: &str,
    schema: Schema,
    batch: RecordBatch,
    options: WriterOptions,
) -> TempPath {
    let path = TempPath::new(label);
    let mut writer = Writer::create(path.path(), schema, options).expect("create writer");
    writer.append(batch).expect("append batch");
    let _summary = writer.finish().expect("finish writer");
    path
}

/// The error a writer gives for a batch its schema cannot describe.
fn rejected(label: &str, schema: Schema, batch: RecordBatch) -> Error {
    let path = TempPath::new(label);
    let mut writer = Writer::create(path.path(), schema, WriterOptions::default()).expect("create");
    writer.append(batch).expect_err("the batch is rejected")
}

/// Every checked-in v0.2 fixture, so a new one is covered the day it lands.
fn fixture_paths() -> Vec<PathBuf> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("spec/v0.2/fixtures");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&root)
        .expect("the fixture directory is readable")
        .filter_map(|entry| std::fs::read_dir(entry.expect("a fixture entry").path()).ok())
        .filter_map(|mut files| {
            files.find_map(|file| {
                let path = file.ok()?.path();
                path.extension()
                    .is_some_and(|kind| kind == "acta")
                    .then_some(path)
            })
        })
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no v0.2 fixtures were found");
    paths
}

#[test]
fn every_logical_type_round_trips_through_raw_writer() {
    let schema = Schema::new(
        11,
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
                    precision: 10,
                    scale: 2,
                },
                false,
            ),
            Column::new(14, "text", LogicalType::Utf8, false),
            Column::new(
                15,
                "category",
                LogicalType::Categorical { ordered: true },
                false,
            ),
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
                vec![10, 20, 20],
                None,
                TimeUnit::Microsecond,
                TimeZone::Utc,
            )),
            Array::Bool(BooleanArray::new(vec![true, false, true], None)),
            Array::Int8(PrimitiveArray::new(vec![-1, 0, 127], None)),
            Array::Int16(PrimitiveArray::new(vec![-2, 0, 32000], None)),
            Array::Int32(PrimitiveArray::new(vec![-3, 0, 2_000_000], None)),
            Array::Int64(PrimitiveArray::new(vec![-4, 0, 9_000_000], None)),
            Array::UInt8(PrimitiveArray::new(vec![0, 1, 255], None)),
            Array::UInt16(PrimitiveArray::new(vec![0, 1, 65_535], None)),
            Array::UInt32(PrimitiveArray::new(vec![0, 1, 4_000_000_000], None)),
            Array::UInt64(PrimitiveArray::new(vec![0, 1, u64::MAX], None)),
            Array::Float32(PrimitiveArray::new(vec![0.0, -1.5, 2.25], None)),
            Array::Float64(PrimitiveArray::new(vec![0.0, -1.5, 2.25], None)),
            Array::Decimal(DecimalArray::new(vec![-100, 0, 225], None, 10, 2)),
            Array::Utf8(Utf8Array::new(
                vec!["a".into(), "".into(), "hé".into()],
                None,
            )),
            Array::Categorical(Utf8Array::new(
                vec!["red".into(), "blue".into(), "red".into()],
                None,
            )),
            Array::Binary(BinaryArray::new(vec![vec![1], vec![], vec![2, 3]], None)),
            Array::FixedBinary(BinaryArray::new(
                vec![vec![0, 1], vec![2, 3], vec![4, 5]],
                None,
            )),
            Array::Date32(PrimitiveArray::new(vec![-1, 0, 1], None)),
        ],
        3,
    )
    .expect("valid complete batch");

    let path = write_batch("all-types", schema, batch.clone(), WriterOptions::default());
    common::expect_valid("writer-all-types", &std::fs::read(path.path()).unwrap());
    let reader = Reader::open(path.path()).expect("writer file opens");
    let decoded = reader.read_block(0).expect("writer file decodes");
    assert_eq!(decoded, batch);
}

#[test]
fn nullable_all_null_all_valid_and_mixed_columns_round_trip() {
    let schema = Schema::new(
        12,
        vec![
            Column::new(1, "all_null", LogicalType::Int32, true),
            Column::new(2, "all_valid", LogicalType::Utf8, true),
            Column::new(3, "mixed", LogicalType::UInt16, true),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Int32(PrimitiveArray::new(
                vec![0, 0, 0],
                Some(vec![false, false, false]),
            )),
            Array::Utf8(Utf8Array::new(
                vec!["one".into(), "two".into(), "three".into()],
                Some(vec![true, true, true]),
            )),
            Array::UInt16(PrimitiveArray::new(
                vec![10, 20, 30],
                Some(vec![true, false, true]),
            )),
        ],
        3,
    )
    .expect("valid nullable batch");
    let path = write_batch("nullable", schema, batch.clone(), WriterOptions::default());
    let reader = Reader::open(path.path()).expect("nullable writer file opens");
    let decoded = reader.read_block(0).unwrap();
    assert_eq!(decoded.column(0).unwrap().value_at(0), None);
    assert_eq!(
        decoded.column(1).unwrap().value_at(2),
        batch.column(1).unwrap().value_at(2)
    );
    assert_eq!(
        decoded.column(2).unwrap().value_at(0),
        batch.column(2).unwrap().value_at(0)
    );
    assert_eq!(decoded.column(2).unwrap().value_at(1), None);
}

#[test]
fn multiple_blocks_preserve_order_bounds_and_row_ids() {
    let schema = Schema::new(
        13,
        vec![Column::new(7, "date", LogicalType::Date32, false)],
        Some(7),
    );
    let first = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Date32(PrimitiveArray::new(vec![1, 2], None))],
        2,
    )
    .unwrap();
    let second = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Date32(PrimitiveArray::new(vec![3, 4], None))],
        2,
    )
    .unwrap();
    let path = TempPath::new("blocks");
    let mut writer = Writer::create(
        path.path(),
        schema,
        WriterOptions::default().with_row_ids(true),
    )
    .unwrap();
    writer.append(first).unwrap();
    writer.flush().unwrap();
    writer.append(second).unwrap();
    writer.sync().unwrap();
    let summary = writer.finish().unwrap();
    assert_eq!(summary.rows_written(), 4);
    assert_eq!(summary.blocks_written(), 2);
    assert_eq!(summary.last_sequence(), Some(2));

    let reader = Reader::open(path.path()).unwrap();
    assert_eq!(reader.blocks().len(), 2);
    assert_eq!(reader.blocks()[0].base_row_id(), Some(0));
    assert_eq!(reader.blocks()[1].base_row_id(), Some(2));
    assert_eq!(reader.blocks()[0].primary_bounds().unwrap().min(), 1);
    assert_eq!(reader.blocks()[1].primary_bounds().unwrap().max(), 4);
    assert!(reader.blocks().iter().all(|block| block.ts_sorted()));
    let dates: Vec<Vec<i32>> = reader
        .scan()
        .map(|batch| match batch.unwrap().column(0).unwrap() {
            Array::Date32(values) => values.values().to_vec(),
            other => panic!("unexpected array: {other:?}"),
        })
        .collect();
    assert_eq!(dates, vec![vec![1, 2], vec![3, 4]]);
}

#[test]
fn reopening_continues_sequences_row_ids_and_the_shared_append_engine() {
    let schema = int64_schema(37);
    let path = TempPath::new("reopen");
    let direct_path = TempPath::new("reopen-direct");
    let options = WriterOptions::default()
        .with_row_ids(true)
        .with_row_block_target(2);

    let mut direct = Writer::create(direct_path.path(), schema.clone(), options).unwrap();
    direct
        .append(int64_batch(&schema, vec![1, 2, 3, 4]))
        .expect("direct append");
    let _ = direct.finish().expect("direct finish");

    let mut writer = Writer::create(path.path(), schema.clone(), options).unwrap();
    writer
        .append(int64_batch(&schema, vec![1, 2]))
        .expect("first append");
    let first_summary = writer.finish().expect("first finish");
    assert_eq!(first_summary.last_sequence(), Some(1));

    let before_mismatch = std::fs::read(path.path()).unwrap();
    let mismatch = int64_schema(40);
    let error = Writer::open_with_schema(path.path(), &mismatch, options).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::SchemaMismatch);
    assert_eq!(std::fs::read(path.path()).unwrap(), before_mismatch);

    let mut reopened =
        Writer::open_with_schema(path.path(), &schema, options).expect("reopen for append");
    reopened
        .append(int64_batch(&schema, vec![3, 4]))
        .expect("second append");
    let second_summary = reopened.finish().expect("second finish");
    assert_eq!(second_summary.rows_written(), 2);
    assert_eq!(second_summary.blocks_written(), 1);
    assert_eq!(second_summary.last_sequence(), Some(2));

    let reader = Reader::open(path.path()).expect("reopened file is readable");
    assert_eq!(reader.schema(), &schema);
    assert_eq!(
        reader
            .blocks()
            .iter()
            .map(|block| block.sequence())
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        reader
            .blocks()
            .iter()
            .map(|block| block.base_row_id())
            .collect::<Vec<_>>(),
        [Some(0), Some(2)]
    );
    assert_eq!(scan_int64(path.path()), vec![1, 2, 3, 4]);
    assert_eq!(
        std::fs::read(path.path()).unwrap(),
        std::fs::read(direct_path.path()).unwrap()
    );
}

#[test]
fn open_requires_an_existing_path_and_rejects_an_incomplete_tail() {
    let missing = TempPath::new("reopen-missing");
    let error = Writer::open(missing.path(), WriterOptions::default()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert!(!missing.path().exists());

    let schema = int64_schema(38);
    let path = TempPath::new("reopen-tail");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer
        .append(int64_batch(&schema, vec![1]))
        .expect("write a complete block");
    let _ = writer.finish().expect("finish the complete block");

    let mut tail = std::fs::OpenOptions::new()
        .append(true)
        .open(path.path())
        .expect("open the file to make an interrupted tail");
    tail.write_all(&[0]).expect("write interrupted-tail bytes");
    drop(tail);

    let error = Writer::open(path.path(), WriterOptions::default()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::IncompleteTail);
}

/// Both entry points exclude each other in both directions, and readers are
/// never held off by the lock either writer holds.
#[test]
fn a_second_writer_cannot_acquire_the_append_lock() {
    let schema = int64_schema(39);
    let path = TempPath::new("reopen-lock");
    let creating = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();

    let error = Writer::open(path.path(), WriterOptions::default()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriterLocked);
    // A reader shares the file with a live writer rather than waiting for it.
    let snapshot = Reader::open(path.path()).expect("readers never take the writer lock");
    assert_eq!(snapshot.blocks().len(), 0);
    let _ = creating.finish().expect("finish the creating writer");

    // A reopened writer excludes another reopened writer just as a creating one
    // does, and both public reopen entry points contend for the same lock.
    let reopened = Writer::open(path.path(), WriterOptions::default()).unwrap();
    assert_eq!(
        Writer::open(path.path(), WriterOptions::default())
            .unwrap_err()
            .kind(),
        ErrorKind::WriterLocked
    );
    assert_eq!(
        Writer::open_with_schema(path.path(), &schema, WriterOptions::default())
            .unwrap_err()
            .kind(),
        ErrorKind::WriterLocked
    );
    assert!(Reader::open(path.path()).is_ok());
    drop(reopened);

    let _ = Writer::open(path.path(), WriterOptions::default())
        .expect("the lock is released when the first writer drops")
        .finish()
        .expect("finish after lock release");
    // `finish` consumes the writer, so it releases the lock as drop does.
    let _ = Writer::open(path.path(), WriterOptions::default())
        .expect("the lock is released when a writer finishes")
        .finish()
        .expect("finish after the previous session finished");
}

/// `Writer::open` is only usable if the schema it reconstructs is reachable,
/// because `append` demands an exactly equal batch schema.
#[test]
fn a_reopened_writer_exposes_the_schema_it_reconstructed() {
    let schema = int64_schema(41);
    let path = TempPath::new("reopen-schema");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer.append(int64_batch(&schema, vec![1])).unwrap();
    let _ = writer.finish().unwrap();

    // Nothing but the path is carried over from the creating session.
    let mut reopened = Writer::open(path.path(), WriterOptions::default()).unwrap();
    assert_eq!(reopened.schema().as_ref(), &schema);

    let reconstructed = Arc::clone(reopened.schema());
    let batch = RecordBatch::try_new(
        reconstructed,
        vec![Array::Int64(PrimitiveArray::new(vec![2, 3], None))],
        2,
    )
    .expect("a batch built from the reconstructed schema");
    reopened
        .append(batch)
        .expect("a batch built from the reconstructed schema is accepted");
    let _ = reopened.finish().unwrap();

    assert_eq!(scan_int64(path.path()), vec![1, 2, 3]);
}

#[test]
fn reopening_a_schema_only_file_starts_at_the_first_sequence() {
    let schema = int64_schema(42);
    let path = TempPath::new("reopen-schema-only");
    let summary = Writer::create(path.path(), schema.clone(), WriterOptions::default())
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(summary.last_sequence(), None);
    let header_only = std::fs::read(path.path()).unwrap();
    assert_eq!(summary.bytes_written(), header_only.len() as u64);

    let mut reopened = Writer::open(path.path(), WriterOptions::default()).unwrap();
    reopened.append(int64_batch(&schema, vec![7])).unwrap();
    let summary = reopened.finish().unwrap();

    assert_eq!(summary.last_sequence(), Some(1));
    let bytes = std::fs::read(path.path()).unwrap();
    assert_eq!(&bytes[..header_only.len()], &header_only[..]);
    assert_eq!(
        common::read_u64(&bytes, header_only.len() + common::PREFIX_SEQUENCE),
        1
    );
}

#[test]
fn a_reopened_session_that_appends_nothing_writes_nothing() {
    let schema = int64_schema(43);
    let path = TempPath::new("reopen-noop");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer.append(int64_batch(&schema, vec![1, 2])).unwrap();
    let _ = writer.finish().unwrap();
    let before = std::fs::read(path.path()).unwrap();

    let summary = Writer::open(path.path(), WriterOptions::default())
        .unwrap()
        .finish()
        .unwrap();

    assert_eq!(summary.rows_written(), 0);
    assert_eq!(summary.blocks_written(), 0);
    assert_eq!(summary.last_sequence(), None);
    assert_eq!(summary.bytes_written(), before.len() as u64);
    assert_eq!(std::fs::read(path.path()).unwrap(), before);
}

/// Sequences and the implicit row-ID chain are read straight off the wire, so
/// the continuation is checked against the bytes rather than against a reader.
#[test]
fn repeated_reopening_keeps_sequences_and_row_ids_contiguous() {
    let schema = int64_schema(44);
    let path = TempPath::new("reopen-repeated");
    let options = WriterOptions::default()
        .with_row_ids(true)
        .with_row_block_target(2);

    let mut writer = Writer::create(path.path(), schema.clone(), options).unwrap();
    writer.append(int64_batch(&schema, vec![0, 1, 2])).unwrap();
    let _ = writer.finish().unwrap();
    for round in 1..4_i64 {
        let mut writer = Writer::open(path.path(), options).unwrap();
        let base = round * 10;
        writer
            .append(int64_batch(&schema, vec![base, base + 1, base + 2]))
            .unwrap();
        let _ = writer.finish().unwrap();
    }

    let bytes = std::fs::read(path.path()).unwrap();
    let (sequences, row_ids) = wire_sequences_and_row_ids(&bytes);
    // Four sessions of three rows at a two-row block target: 2 + 1 each time.
    assert_eq!(sequences, (1..=8).collect::<Vec<u64>>());
    assert_eq!(row_ids, vec![0, 2, 3, 5, 6, 8, 9, 11]);
    assert_eq!(
        common::read_u64(&bytes, common::PROLOGUE_FEATURE_FLAGS),
        1,
        "the row-ID feature is untouched by reopening"
    );
}

#[test]
fn a_file_without_row_ids_keeps_storing_the_unavailable_sentinel() {
    let schema = int64_schema(45);
    let path = TempPath::new("reopen-no-row-ids");
    let options = WriterOptions::default().with_row_block_target(2);

    let mut writer = Writer::create(path.path(), schema.clone(), options).unwrap();
    writer.append(int64_batch(&schema, vec![1, 2, 3])).unwrap();
    let _ = writer.finish().unwrap();
    let mut writer = Writer::open(path.path(), options).unwrap();
    writer.append(int64_batch(&schema, vec![4, 5])).unwrap();
    let _ = writer.finish().unwrap();

    let bytes = std::fs::read(path.path()).unwrap();
    let (sequences, row_ids) = wire_sequences_and_row_ids(&bytes);
    assert_eq!(sequences, vec![1, 2, 3]);
    assert_eq!(row_ids, vec![common::UINT64_MAX; 3]);
    assert_eq!(common::read_u64(&bytes, common::PROLOGUE_FEATURE_FLAGS), 0);
}

/// Reopening cannot turn the row-ID feature on or off, in either direction.
#[test]
fn the_row_id_feature_cannot_be_changed_by_reopening() {
    let schema = int64_schema(46);
    let with_ids = TempPath::new("reopen-ids-on");
    let without_ids = TempPath::new("reopen-ids-off");
    let enabled = WriterOptions::default().with_row_ids(true);

    for (path, options) in [
        (with_ids.path(), enabled),
        (without_ids.path(), WriterOptions::default()),
    ] {
        let mut writer = Writer::create(path, schema.clone(), options).unwrap();
        writer.append(int64_batch(&schema, vec![1])).unwrap();
        let _ = writer.finish().unwrap();
    }

    let before = std::fs::read(with_ids.path()).unwrap();
    let error = Writer::open(with_ids.path(), WriterOptions::default()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert_eq!(std::fs::read(with_ids.path()).unwrap(), before);

    let before = std::fs::read(without_ids.path()).unwrap();
    let error = Writer::open(without_ids.path(), enabled).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert_eq!(std::fs::read(without_ids.path()).unwrap(), before);
}

/// The guard is only useful if it is sensitive to every field a schema carries.
#[test]
fn the_expected_schema_guard_covers_every_schema_field() {
    let timestamp = |unit, timezone| LogicalType::Timestamp { unit, timezone };
    let schema = Schema::new(
        47,
        vec![
            Column::new(
                1,
                "ts",
                timestamp(TimeUnit::Microsecond, TimeZone::Utc),
                false,
            ),
            Column::new(2, "value", LogicalType::Int64, true),
        ],
        Some(1),
    );
    let path = TempPath::new("reopen-guard");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer
        .append(
            RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Array::Timestamp(TimestampArray::new(
                        vec![1],
                        None,
                        TimeUnit::Microsecond,
                        TimeZone::Utc,
                    )),
                    Array::Int64(PrimitiveArray::new(vec![9], Some(vec![true]))),
                ],
                1,
            )
            .unwrap(),
        )
        .unwrap();
    let _ = writer.finish().unwrap();
    let committed = std::fs::read(path.path()).unwrap();

    let columns = schema.columns();
    let variants = [
        ("schema ID", Schema::new(48, columns.to_vec(), Some(1))),
        (
            "column order",
            Schema::new(47, vec![columns[1].clone(), columns[0].clone()], Some(1)),
        ),
        (
            "column count",
            Schema::new(47, vec![columns[0].clone()], Some(1)),
        ),
        (
            "column ID",
            Schema::new(
                47,
                vec![
                    Column::new(3, "ts", columns[0].logical_type().clone(), false),
                    columns[1].clone(),
                ],
                Some(3),
            ),
        ),
        (
            "column name",
            Schema::new(
                47,
                vec![
                    Column::new(1, "time", columns[0].logical_type().clone(), false),
                    columns[1].clone(),
                ],
                Some(1),
            ),
        ),
        (
            "logical type parameter",
            Schema::new(
                47,
                vec![
                    Column::new(
                        1,
                        "ts",
                        timestamp(TimeUnit::Nanosecond, TimeZone::Utc),
                        false,
                    ),
                    columns[1].clone(),
                ],
                Some(1),
            ),
        ),
        (
            "timezone annotation",
            Schema::new(
                47,
                vec![
                    Column::new(
                        1,
                        "ts",
                        timestamp(TimeUnit::Microsecond, TimeZone::Naive),
                        false,
                    ),
                    columns[1].clone(),
                ],
                Some(1),
            ),
        ),
        (
            "nullability",
            Schema::new(
                47,
                vec![
                    columns[0].clone(),
                    Column::new(2, "value", LogicalType::Int64, false),
                ],
                Some(1),
            ),
        ),
        ("primary selection", Schema::new(47, columns.to_vec(), None)),
    ];

    for (field, variant) in variants {
        let error = Writer::open_with_schema(path.path(), &variant, WriterOptions::default())
            .err()
            .unwrap_or_else(|| panic!("a schema differing only in its {field} was accepted"));
        assert_eq!(error.kind(), ErrorKind::SchemaMismatch, "{field}");
        assert_eq!(
            std::fs::read(path.path()).unwrap(),
            committed,
            "the rejected {field} guard wrote bytes"
        );
    }

    let _ = Writer::open_with_schema(path.path(), &schema, WriterOptions::default())
        .expect("the exact schema is accepted")
        .finish()
        .unwrap();
}

/// Every way reopening can fail must leave the file byte-identical, so the
/// bytes are compared directly rather than inferred from a reader.
#[test]
fn every_reopen_failure_leaves_the_file_byte_identical() {
    let schema = int64_schema(49);
    let path = TempPath::new("reopen-no-writes");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer.append(int64_batch(&schema, vec![1, 2])).unwrap();
    let _ = writer.finish().unwrap();
    let committed = std::fs::read(path.path()).unwrap();
    let unchanged = |label: &str| {
        assert_eq!(
            std::fs::read(path.path()).unwrap(),
            committed,
            "{label} changed the file"
        );
    };

    let error = Writer::open_with_schema(path.path(), &int64_schema(50), WriterOptions::default())
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::SchemaMismatch);
    unchanged("a schema mismatch");

    let error = Writer::open(path.path(), WriterOptions::default().with_row_ids(true)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    unchanged("a row-ID option mismatch");

    let held = Writer::open(path.path(), WriterOptions::default()).unwrap();
    let error = Writer::open(path.path(), WriterOptions::default()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriterLocked);
    unchanged("a lock conflict");
    drop(held);
    unchanged("releasing the lock");

    let error = Writer::create(path.path(), schema, WriterOptions::default()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Io);
    unchanged("creating over an existing path");

    // A path typo must not bring a file into existence.
    let missing = TempPath::new("reopen-typo");
    for kind in [
        Writer::open(missing.path(), WriterOptions::default())
            .unwrap_err()
            .kind(),
        Writer::open_with_schema(missing.path(), &int64_schema(49), WriterOptions::default())
            .unwrap_err()
            .kind(),
    ] {
        assert_eq!(kind, ErrorKind::Io);
    }
    assert!(!missing.path().exists(), "open created a missing path");
}

/// Every genuine partial frame is a recoverable tail, wherever the file was cut.
#[test]
fn a_cut_anywhere_in_an_appended_frame_is_an_incomplete_tail() {
    let schema = int64_schema(51);
    let path = TempPath::new("reopen-tail-sweep");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer.append(int64_batch(&schema, vec![1, 2])).unwrap();
    let _ = writer.finish().unwrap();
    let one_block = std::fs::read(path.path()).unwrap();

    let mut writer = Writer::open(path.path(), WriterOptions::default()).unwrap();
    writer.append(int64_batch(&schema, vec![3, 4])).unwrap();
    let _ = writer.finish().unwrap();
    let two_blocks = std::fs::read(path.path()).unwrap();

    let appended = one_block.len();
    let header = common::read_u32(&two_blocks, appended + common::PREFIX_HEADER_LENGTH) as usize;
    let payload = common::read_u64(&two_blocks, appended + common::PREFIX_PAYLOAD_LENGTH) as usize;
    let frame = common::frame_length(&two_blocks, appended);
    // One cut inside each region of the appended frame, and both sides of every
    // boundary between them.
    let mut cuts = vec![
        1,
        common::PREFIX_SIZE - 1,
        common::PREFIX_SIZE,
        common::PREFIX_SIZE + 1,
        common::PREFIX_SIZE + header / 2,
        common::PREFIX_SIZE + header,
        common::PREFIX_SIZE + header + 1,
        common::PREFIX_SIZE + header + payload / 2,
        common::PREFIX_SIZE + header + payload,
        frame - common::TRAILER_SIZE + 1,
        frame - 1,
    ];
    cuts.retain(|cut| (1..frame).contains(cut));
    cuts.sort_unstable();
    cuts.dedup();
    assert!(cuts.len() >= 8, "the sweep must cover every frame region");

    for cut in cuts {
        let truncated = TempPath::new("reopen-cut");
        std::fs::write(truncated.path(), &two_blocks[..appended + cut]).unwrap();
        let error = Writer::open(truncated.path(), WriterOptions::default()).unwrap_err();
        assert_eq!(
            error.kind(),
            ErrorKind::IncompleteTail,
            "a cut {cut} bytes into the appended frame reported {error}"
        );
        assert_eq!(
            std::fs::read(truncated.path()).unwrap().len(),
            appended + cut,
            "refusing a tail must not resize the file"
        );
    }

    // A file that ends inside its schema frame declares no columns at all and
    // is refused the same way rather than being treated as appendable.
    for cut in [
        1,
        common::PREFIX_SIZE,
        common::frame_length(&two_blocks, common::PROLOGUE_SIZE) - 1,
    ] {
        let truncated = TempPath::new("reopen-schema-cut");
        std::fs::write(truncated.path(), &two_blocks[..common::PROLOGUE_SIZE + cut]).unwrap();
        let error = Writer::open(truncated.path(), WriterOptions::default()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::IncompleteTail, "schema cut {cut}");
    }
}

/// Damage to a frame that is present in full is corruption, never a tail a
/// later append could complete.
#[test]
fn complete_damage_to_the_last_frame_stays_corruption() {
    let schema = int64_schema(52);
    let path = TempPath::new("reopen-damage");
    let options = WriterOptions::default()
        .with_row_ids(true)
        .with_row_block_target(2);
    let mut writer = Writer::create(path.path(), schema.clone(), options).unwrap();
    writer
        .append(int64_batch(&schema, vec![1, 2, 3, 4]))
        .unwrap();
    let _ = writer.finish().unwrap();

    let clean = std::fs::read(path.path()).unwrap();
    let last = common::frame_end(&clean, common::data_frame_offset(&clean));
    let trailer = common::trailer_offset(&clean, last);
    /// Damage the frame beginning at the second offset, whose trailer begins
    /// at the third.
    type Damage = fn(&mut Vec<u8>, usize, usize);

    let damage: [(&str, Damage); 8] = [
        ("prefix CRC", |bytes, frame, _| {
            bytes[frame + common::PREFIX_CRC] ^= 0xff;
        }),
        ("header CRC", |bytes, frame, _| {
            bytes[frame + common::PREFIX_HEADER_CRC] ^= 0xff;
            common::repair_prefix(bytes, frame);
        }),
        ("payload byte", |bytes, frame, _| {
            let payload = common::payload_offset(bytes, frame);
            bytes[payload] ^= 0xff;
        }),
        ("trailer CRC", |bytes, _, trailer| {
            bytes[trailer + common::TRAILER_CRC] ^= 0xff;
        }),
        ("commit magic", |bytes, _, trailer| {
            bytes[trailer + common::TRAILER_COMMIT_MAGIC] ^= 0xff;
            common::repair_trailer_crc(bytes, trailer);
        }),
        ("frame sequence", |bytes, frame, _| {
            common::put_u64(bytes, frame + common::PREFIX_SEQUENCE, 99);
            common::repair_frame(bytes, frame);
        }),
        ("block schema ID", |bytes, frame, _| {
            common::put_u64(
                bytes,
                frame + common::PREFIX_SIZE + common::BLOCK_SCHEMA_ID,
                4_242,
            );
            common::repair_frame(bytes, frame);
        }),
        ("row-ID chain", |bytes, frame, _| {
            common::put_u64(
                bytes,
                frame + common::PREFIX_SIZE + common::BLOCK_BASE_ROW_ID,
                500,
            );
            common::repair_frame(bytes, frame);
        }),
    ];

    for (label, mutate) in damage {
        let damaged = TempPath::new("reopen-damaged");
        let mut bytes = clean.clone();
        mutate(&mut bytes, last, trailer);
        assert_ne!(bytes, clean, "the {label} mutation changed nothing");
        std::fs::write(damaged.path(), &bytes).unwrap();

        let error = Writer::open(damaged.path(), options).unwrap_err();
        assert_eq!(
            error.kind(),
            ErrorKind::Corruption,
            "damaged {label} reported {error}"
        );
        assert_eq!(
            std::fs::read(damaged.path()).unwrap(),
            bytes,
            "refusing corruption must not repair the file"
        );
    }
}

/// A reopened session may choose any block policy it likes without disturbing
/// the blocks or file-level features already committed.
#[test]
fn changing_block_policy_on_reopen_leaves_history_and_features_intact() {
    let schema = Schema::new(
        53,
        vec![
            Column::new(
                1,
                "ts",
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
    let batch = |start: i64| {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Array::Timestamp(TimestampArray::new(
                    (start..start + 8).collect(),
                    None,
                    TimeUnit::Microsecond,
                    TimeZone::Utc,
                )),
                Array::Int64(PrimitiveArray::new(
                    (start..start + 8).map(|value| value * 3).collect(),
                    None,
                )),
            ],
            8,
        )
        .unwrap()
    };
    let session = WriterOptions::default()
        .with_row_ids(true)
        .with_row_block_target(4);

    let path = TempPath::new("reopen-policy");
    let mut writer = Writer::create(path.path(), schema.clone(), session).unwrap();
    writer.append(batch(0)).unwrap();
    let _ = writer.finish().unwrap();
    let history = std::fs::read(path.path()).unwrap();

    let mut adaptive = session
        .with_encoding(WriterEncoding::Adaptive)
        .with_statistics(WriterStatistics::Automatic);
    if cfg!(feature = "zstd") {
        adaptive = adaptive.with_codec(WriterCodec::Zstandard);
    }
    let mut writer = Writer::open(path.path(), adaptive).unwrap();
    writer.append(batch(8)).unwrap();
    let _ = writer.finish().unwrap();

    let fixed = session
        .with_encoding(WriterEncoding::Fixed(WriterTransform::Delta))
        .with_statistics(WriterStatistics::MinMax);
    let mut writer = Writer::open(path.path(), fixed).unwrap();
    writer.append(batch(16)).unwrap();
    let _ = writer.finish().unwrap();

    let bytes = std::fs::read(path.path()).unwrap();
    assert_eq!(
        &bytes[..history.len()],
        &history[..],
        "history was rewritten"
    );
    assert_eq!(
        common::read_u64(&bytes, common::PROLOGUE_FEATURE_FLAGS),
        1,
        "a policy change must not alter file-level features"
    );
    let (sequences, row_ids) = wire_sequences_and_row_ids(&bytes);
    assert_eq!(sequences, (1..=6).collect::<Vec<u64>>());
    assert_eq!(row_ids, vec![0, 4, 8, 12, 16, 20]);

    // Three writer policies in one file survive both validation levels and
    // every read path.
    common::expect_valid("reopen-policy", &bytes);
    let full = acta::validate_with_options(
        path.path(),
        acta::ValidationOptions::default().with_level(acta::ValidationLevel::Full),
    )
    .expect("full validation accepts a file written by three policies");
    assert_eq!(full.frame_count(), 7);

    let reader = Reader::open(path.path()).unwrap();
    assert_eq!(reader.blocks().len(), 6);
    for index in 0..reader.blocks().len() {
        assert_eq!(reader.read_block(index).unwrap().row_count(), 4);
    }
    let scanned: Vec<i64> = reader
        .scan()
        .flat_map(|batch| match batch.unwrap().column(1).unwrap() {
            Array::Int64(values) => values.values().to_vec(),
            other => panic!("unexpected array: {other:?}"),
        })
        .collect();
    assert_eq!(
        scanned,
        (0..24).map(|value| value * 3).collect::<Vec<i64>>()
    );

    let projected: usize = reader
        .scan()
        .project(["value"])
        .unwrap()
        .map(|batch| batch.unwrap().row_count())
        .sum();
    assert_eq!(projected, 24);

    // Stage 5 pruning still selects across the boundary between the sessions.
    let pruned: usize = reader
        .scan()
        .primary_range(acta::PrimaryRange::timestamp(6, 17))
        .unwrap()
        .map(|batch| batch.unwrap().row_count())
        .sum();
    assert!(
        (1..24).contains(&pruned),
        "pruning selected {pruned} of 24 rows"
    );
}

/// Guards answerable from the prologue and schema frame must be answered
/// before the walk reads the rest of the file, so a caller who named the wrong
/// schema does not pay for a checksum pass over the whole thing. A file whose
/// data frame is corrupt shows the ordering: a walk would report that damage.
#[test]
fn the_cheap_reopen_guards_run_before_the_structural_walk() {
    let schema = int64_schema(55);
    let path = TempPath::new("reopen-guard-order");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer.append(int64_batch(&schema, vec![1, 2])).unwrap();
    let _ = writer.finish().unwrap();

    let mut bytes = std::fs::read(path.path()).unwrap();
    let damaged_crc = common::data_frame_offset(&bytes) + common::PREFIX_CRC;
    bytes[damaged_crc] ^= 0xff;
    std::fs::write(path.path(), &bytes).unwrap();

    // What the walk reports for this file.
    assert_eq!(
        Writer::open(path.path(), WriterOptions::default())
            .unwrap_err()
            .kind(),
        ErrorKind::Corruption
    );
    // Both guards answer ahead of it.
    assert_eq!(
        Writer::open_with_schema(path.path(), &int64_schema(56), WriterOptions::default())
            .unwrap_err()
            .kind(),
        ErrorKind::SchemaMismatch
    );
    assert_eq!(
        Writer::open(path.path(), WriterOptions::default().with_row_ids(true))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(std::fs::read(path.path()).unwrap(), bytes);
}

/// A file whose frames exceed the default bounds is readable under explicit
/// limits, so it must be appendable under them too.
#[test]
fn reopening_accepts_explicit_limits() {
    let schema = int64_schema(57);
    let path = TempPath::new("reopen-limits");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer.append(int64_batch(&schema, vec![1, 2])).unwrap();
    let _ = writer.finish().unwrap();

    let error = Writer::open_with_limits(
        path.path(),
        Limits::default().with_max_frame_payload_length(8),
        WriterOptions::default(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ResourceLimit);

    let mut reopened = Writer::open_with_limits(
        path.path(),
        Limits::default().with_max_frame_payload_length(1 << 40),
        WriterOptions::default(),
    )
    .expect("a bound above the file's frames accepts it");
    reopened.append(int64_batch(&schema, vec![3])).unwrap();
    let summary = reopened.finish().unwrap();

    assert_eq!(summary.last_sequence(), Some(2));
    assert_eq!(scan_int64(path.path()), vec![1, 2, 3]);
}

/// A reader holds the snapshot it opened, and a later reader sees the append.
#[test]
fn a_reader_keeps_its_snapshot_across_an_append() {
    let schema = int64_schema(54);
    let path = TempPath::new("reopen-snapshot");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer.append(int64_batch(&schema, vec![1])).unwrap();
    let _ = writer.finish().unwrap();

    let before = Reader::open(path.path()).unwrap();
    let mut writer = Writer::open(path.path(), WriterOptions::default()).unwrap();
    writer.append(int64_batch(&schema, vec![2])).unwrap();
    let _ = writer.finish().unwrap();
    let after = Reader::open(path.path()).unwrap();

    assert_eq!(before.blocks().len(), 1);
    assert_eq!(after.blocks().len(), 2);
}

#[test]
fn raw_serialization_is_deterministic_and_validates() {
    let schema = Schema::new(
        14,
        vec![Column::new(1, "value", LogicalType::Int64, false)],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Int64(PrimitiveArray::new(vec![1, 2, 3], None))],
        3,
    )
    .unwrap();
    let first = write_batch(
        "deterministic-a",
        schema.clone(),
        batch.clone(),
        WriterOptions::default(),
    );
    let second = write_batch("deterministic-b", schema, batch, WriterOptions::default());
    let first_bytes = std::fs::read(first.path()).unwrap();
    let second_bytes = std::fs::read(second.path()).unwrap();
    assert_eq!(first_bytes, second_bytes);
    common::expect_valid("writer-deterministic", &first_bytes);
}

#[test]
fn invalid_batches_are_rejected_without_poisoning_and_create_is_exclusive() {
    let path = TempPath::new("exclusive");
    std::fs::write(path.path(), b"existing").unwrap();
    let schema = Schema::new(
        15,
        vec![Column::new(1, "value", LogicalType::Int64, false)],
        None,
    );
    let error = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(std::fs::read(path.path()).unwrap(), b"existing");

    let output = TempPath::new("invalid-batch");
    let mut writer =
        Writer::create(output.path(), schema.clone(), WriterOptions::default()).unwrap();
    let wrong_schema = Schema::new(
        16,
        vec![Column::new(1, "other", LogicalType::Int64, false)],
        None,
    );
    let wrong_batch = RecordBatch::try_new(
        Arc::new(wrong_schema),
        vec![Array::Int64(PrimitiveArray::new(vec![1], None))],
        1,
    )
    .unwrap();
    let error = writer.append(wrong_batch).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);

    let valid_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Array::Int64(PrimitiveArray::new(vec![9], None))],
        1,
    )
    .unwrap();
    writer.append(valid_batch).unwrap();
    let _summary = writer.finish().unwrap();
    assert_eq!(Reader::open(output.path()).unwrap().total_rows(), 1);
}

#[test]
fn every_v0_2_fixture_survives_a_writer_rewrite() {
    for fixture in fixture_paths() {
        let label = fixture
            .file_stem()
            .expect("a fixture file name")
            .to_string_lossy()
            .into_owned();
        let reader = Reader::open(&fixture).unwrap_or_else(|error| panic!("{label}: {error}"));
        let schema = reader.schema().clone();
        let row_ids = reader
            .blocks()
            .first()
            .is_some_and(|block| block.base_row_id().is_some());
        let batches: Vec<RecordBatch> = reader
            .scan()
            .collect::<Result<_, _>>()
            .unwrap_or_else(|error| panic!("{label}: {error}"));

        let path = TempPath::new(&label);
        let mut writer = Writer::create(
            path.path(),
            schema,
            WriterOptions::default().with_row_ids(row_ids),
        )
        .unwrap_or_else(|error| panic!("{label}: {error}"));
        for batch in &batches {
            writer
                .append(batch.clone())
                .unwrap_or_else(|error| panic!("{label}: {error}"));
        }
        let _summary = writer
            .finish()
            .unwrap_or_else(|error| panic!("{label}: {error}"));

        common::expect_valid(&label, &std::fs::read(path.path()).unwrap());
        let rewritten: Vec<RecordBatch> = Reader::open(path.path())
            .and_then(|reader| reader.scan().collect())
            .unwrap_or_else(|error| panic!("{label}: {error}"));
        assert_eq!(rewritten, batches, "{label} did not survive a rewrite");
    }
}

#[test]
fn a_nullable_fixed_binary_column_round_trips_through_its_null_slots() {
    let schema = Schema::new(
        20,
        vec![Column::new(
            1,
            "fixed",
            LogicalType::FixedBinary { byte_width: 2 },
            true,
        )],
        None,
    );
    // A decoded array leaves the slot behind a null empty, so that is the shape
    // a reader hands back and therefore the shape a writer has to accept.
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::FixedBinary(BinaryArray::new(
            vec![vec![0, 1], Vec::new(), vec![4, 5]],
            Some(vec![true, false, true]),
        ))],
        3,
    )
    .expect("valid fixed binary batch");

    let path = write_batch(
        "fixed-binary-nulls",
        schema,
        batch.clone(),
        WriterOptions::default(),
    );
    let decoded = Reader::open(path.path()).unwrap().read_block(0).unwrap();
    assert_eq!(decoded, batch);
}

#[test]
fn a_fixed_binary_value_of_the_wrong_width_is_rejected() {
    let schema = Schema::new(
        21,
        vec![Column::new(
            1,
            "fixed",
            LogicalType::FixedBinary { byte_width: 2 },
            false,
        )],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::FixedBinary(BinaryArray::new(
            vec![vec![1, 2], vec![3]],
            None,
        ))],
        2,
    )
    .expect("a batch may carry a value the column cannot");

    let error = rejected("fixed-binary-width", schema, batch);
    assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{error}");
}

#[test]
fn a_decimal_array_that_contradicts_its_column_is_rejected() {
    let schema = Schema::new(
        22,
        vec![Column::new(
            1,
            "amount",
            LogicalType::Decimal {
                precision: 10,
                scale: 2,
            },
            false,
        )],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Decimal(DecimalArray::new(vec![1], None, 9, 2))],
        1,
    )
    .expect("a batch may carry a mismatched precision");

    let error = rejected("decimal-precision", schema, batch);
    assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{error}");
}

#[test]
fn a_timestamp_array_that_contradicts_its_column_is_rejected() {
    let schema = Schema::new(
        23,
        vec![Column::new(
            1,
            "time",
            LogicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: TimeZone::Utc,
            },
            false,
        )],
        Some(1),
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Timestamp(TimestampArray::new(
            vec![1],
            None,
            TimeUnit::Nanosecond,
            TimeZone::Utc,
        ))],
        1,
    )
    .expect("a batch may carry a mismatched unit");

    let error = rejected("timestamp-unit", schema, batch);
    assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{error}");
}

#[test]
fn invalid_schemas_are_rejected_before_any_file_exists() {
    let naive = LogicalType::Timestamp {
        unit: TimeUnit::Second,
        timezone: TimeZone::Naive,
    };
    let int64 = |id, name| Column::new(id, name, LogicalType::Int64, false);
    let cases = [
        (
            "a zero schema ID",
            Schema::new(0, vec![int64(1, "a")], None),
        ),
        ("no columns", Schema::new(1, Vec::new(), None)),
        (
            "a zero column ID",
            Schema::new(1, vec![int64(0, "a")], None),
        ),
        (
            "a duplicate column ID",
            Schema::new(1, vec![int64(1, "a"), int64(1, "b")], None),
        ),
        (
            "a duplicate column name",
            Schema::new(1, vec![int64(1, "a"), int64(2, "a")], None),
        ),
        (
            "an undeclared primary column",
            Schema::new(1, vec![Column::new(1, "a", naive.clone(), false)], Some(2)),
        ),
        (
            "a nullable primary column",
            Schema::new(1, vec![Column::new(1, "a", naive, true)], Some(1)),
        ),
        (
            "a non-temporal primary column",
            Schema::new(1, vec![int64(1, "a")], Some(1)),
        ),
        (
            "a decimal precision above 18",
            Schema::new(
                1,
                vec![Column::new(
                    1,
                    "a",
                    LogicalType::Decimal {
                        precision: 19,
                        scale: 0,
                    },
                    false,
                )],
                None,
            ),
        ),
        (
            "a decimal precision below 1",
            Schema::new(
                1,
                vec![Column::new(
                    1,
                    "a",
                    LogicalType::Decimal {
                        precision: 0,
                        scale: 0,
                    },
                    false,
                )],
                None,
            ),
        ),
        (
            "a zero fixed_binary width",
            Schema::new(
                1,
                vec![Column::new(
                    1,
                    "a",
                    LogicalType::FixedBinary { byte_width: 0 },
                    false,
                )],
                None,
            ),
        ),
        (
            "an empty IANA timezone name",
            Schema::new(
                1,
                vec![Column::new(
                    1,
                    "a",
                    LogicalType::Timestamp {
                        unit: TimeUnit::Second,
                        timezone: TimeZone::Iana(String::new()),
                    },
                    false,
                )],
                None,
            ),
        ),
    ];

    for (label, schema) in cases {
        let path = TempPath::new("invalid-schema");
        let error = Writer::create(path.path(), schema, WriterOptions::default())
            .expect_err(&format!("{label} is rejected"));
        assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{label}: {error}");
        assert!(!path.path().exists(), "{label} left a file behind");
    }
}

#[test]
fn blocks_have_no_base_row_id_unless_row_ids_are_enabled() {
    let path = date_file("no-row-ids", vec![1, 2]);
    let reader = Reader::open(path.path()).unwrap();
    assert_eq!(reader.blocks()[0].base_row_id(), None);
}

#[test]
fn unsorted_primary_values_claim_no_order() {
    let path = date_file("unsorted", vec![5, 1, 3]);
    let reader = Reader::open(path.path()).unwrap();
    assert!(!reader.blocks()[0].ts_sorted());
}

#[test]
fn unsorted_primary_values_still_bound_the_block() {
    let path = date_file("unsorted-bounds", vec![5, 1, 3]);
    let reader = Reader::open(path.path()).unwrap();
    let bounds = reader.blocks()[0].primary_bounds().expect("primary bounds");
    assert_eq!((bounds.min(), bounds.max()), (1, 5));
}

#[test]
fn equal_adjacent_primary_values_still_claim_order() {
    let path = date_file("equal-adjacent", vec![1, 1, 2]);
    let reader = Reader::open(path.path()).unwrap();
    assert!(reader.blocks()[0].ts_sorted());
}

#[test]
fn a_schema_without_a_primary_column_has_no_bounds() {
    let path = write_batch(
        "no-primary-bounds",
        mixed_schema(false),
        mixed_batch(false),
        WriterOptions::default(),
    );
    let reader = Reader::open(path.path()).unwrap();
    assert!(reader.blocks()[0].primary_bounds().is_none());
}

#[test]
fn a_schema_without_a_primary_column_claims_no_order() {
    let path = write_batch(
        "no-primary-order",
        mixed_schema(false),
        mixed_batch(false),
        WriterOptions::default(),
    );
    let reader = Reader::open(path.path()).unwrap();
    assert!(!reader.blocks()[0].ts_sorted());
}

#[test]
fn every_validity_representation_round_trips() {
    let batch = mixed_batch(true);
    let path = write_batch(
        "mixed-validity",
        mixed_schema(true),
        batch.clone(),
        WriterOptions::default().with_row_ids(true),
    );
    common::expect_valid("mixed-validity", &std::fs::read(path.path()).unwrap());
    let decoded = Reader::open(path.path()).unwrap().read_block(0).unwrap();
    assert_same_rows(&decoded, &batch);
}

#[test]
fn an_explicit_all_valid_bitmap_comes_back_without_one() {
    let path = write_batch(
        "all-valid-bitmap",
        mixed_schema(true),
        mixed_batch(true),
        WriterOptions::default(),
    );
    let decoded = Reader::open(path.path()).unwrap().read_block(0).unwrap();
    match decoded.column_by_name("all_valid").expect("the column") {
        Array::Bool(array) => assert_eq!(array.validity(), None),
        other => panic!("unexpected array: {other:?}"),
    }
}

#[test]
fn a_decoded_block_rewrites_to_an_identical_block() {
    let first = write_batch(
        "rewrite-once",
        mixed_schema(true),
        mixed_batch(true),
        WriterOptions::default(),
    );
    let decoded = Reader::open(first.path()).unwrap().read_block(0).unwrap();

    let second = write_batch(
        "rewrite-twice",
        mixed_schema(true),
        decoded.clone(),
        WriterOptions::default(),
    );
    let rewritten = Reader::open(second.path()).unwrap().read_block(0).unwrap();

    assert_eq!(rewritten, decoded);
}

#[test]
fn every_representation_serializes_deterministically() {
    let options = WriterOptions::default().with_row_ids(true);
    let first = write_batch(
        "deterministic-mixed-a",
        mixed_schema(true),
        mixed_batch(true),
        options,
    );
    let second = write_batch(
        "deterministic-mixed-b",
        mixed_schema(true),
        mixed_batch(true),
        options,
    );
    assert_eq!(
        std::fs::read(first.path()).unwrap(),
        std::fs::read(second.path()).unwrap()
    );
}

#[test]
fn a_writer_with_no_appended_batch_reports_no_sequence() {
    let path = TempPath::new("schema-only-summary");
    let schema = Schema::new(
        26,
        vec![Column::new(1, "value", LogicalType::Int64, false)],
        None,
    );
    let writer = Writer::create(path.path(), schema, WriterOptions::default()).unwrap();
    assert_eq!(writer.finish().unwrap().last_sequence(), None);
}

#[test]
fn a_writer_with_no_appended_batch_leaves_a_valid_schema_only_file() {
    let path = TempPath::new("schema-only-file");
    let schema = Schema::new(
        27,
        vec![Column::new(1, "value", LogicalType::Int64, false)],
        None,
    );
    let writer = Writer::create(path.path(), schema, WriterOptions::default()).unwrap();
    let _summary = writer.finish().unwrap();

    common::expect_valid("schema-only", &std::fs::read(path.path()).unwrap());
    assert!(Reader::open(path.path()).unwrap().blocks().is_empty());
}

#[test]
fn a_column_of_empty_strings_round_trips() {
    let batch = empty_string_batch();
    let path = write_batch(
        "empty-strings",
        empty_string_schema(),
        batch.clone(),
        WriterOptions::default(),
    );
    common::expect_valid("empty-strings", &std::fs::read(path.path()).unwrap());
    let decoded = Reader::open(path.path()).unwrap().read_block(0).unwrap();
    assert_eq!(decoded, batch);
}

#[test]
fn an_empty_values_stream_gets_a_payload_offset_of_its_own() {
    let path = write_batch(
        "empty-stream-offsets",
        empty_string_schema(),
        empty_string_batch(),
        WriterOptions::default(),
    );
    let bytes = std::fs::read(path.path()).unwrap();
    let frame = common::data_frame_offset(&bytes);
    let values = common::stream_descriptor_offset(&bytes, frame, 0);
    let lengths = common::stream_descriptor_offset(&bytes, frame, 1);

    assert_eq!(
        common::read_u64(&bytes, values + common::STREAM_STORED_LENGTH),
        0,
        "the premise of this test is an empty values stream"
    );
    assert_ne!(
        common::read_u64(&bytes, values + common::STREAM_PAYLOAD_OFFSET),
        common::read_u64(&bytes, lengths + common::STREAM_PAYLOAD_OFFSET),
    );
}

/// Compare two batches by what each row means rather than by the validity
/// representation a block chose.
///
/// Section 3 stores values densely and section 8.1 lets an all-valid nullable
/// column carry no validity stream, so a column written with an all-true bitmap
/// comes back without one. The rows are unchanged either way.
fn assert_same_rows(decoded: &RecordBatch, expected: &RecordBatch) {
    assert_eq!(decoded.schema(), expected.schema());
    assert_eq!(decoded.row_count(), expected.row_count());
    for index in 0..expected.schema().column_count() {
        let decoded = decoded.column(index).expect("a decoded column");
        let expected = expected.column(index).expect("an expected column");
        for row in 0..expected.len() {
            assert_eq!(
                decoded.value_at(row),
                expected.value_at(row),
                "column {index} row {row}"
            );
        }
    }
}

/// A one-column `date32` file whose primary values arrive in the given order.
fn date_file(label: &str, days: Vec<i32>) -> TempPath {
    let schema = Schema::new(
        24,
        vec![Column::new(1, "day", LogicalType::Date32, false)],
        Some(1),
    );
    let row_count = days.len();
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Date32(PrimitiveArray::new(days, None))],
        row_count,
    )
    .expect("valid date batch");
    write_batch(label, schema, batch, WriterOptions::default())
}

/// A schema covering every validity representation: no bitmap, all-null,
/// mixed, and all-valid, with and without a primary timestamp column.
fn mixed_schema(primary: bool) -> Schema {
    Schema::new(
        25,
        vec![
            Column::new(
                1,
                "time",
                LogicalType::Timestamp {
                    unit: TimeUnit::Nanosecond,
                    timezone: TimeZone::Iana("Europe/Zurich".into()),
                },
                false,
            ),
            Column::new(2, "all_null", LogicalType::Utf8, true),
            Column::new(3, "mixed", LogicalType::Binary, true),
            Column::new(4, "all_valid", LogicalType::Bool, true),
        ],
        primary.then_some(1),
    )
}

/// Values behind null positions are the type default, which is what a decoded
/// block holds, so a round trip can compare batches directly.
fn mixed_batch(primary: bool) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(mixed_schema(primary)),
        vec![
            Array::Timestamp(TimestampArray::new(
                vec![10, 20, 30],
                None,
                TimeUnit::Nanosecond,
                TimeZone::Iana("Europe/Zurich".into()),
            )),
            Array::Utf8(Utf8Array::new(
                vec![String::new(), String::new(), String::new()],
                Some(vec![false, false, false]),
            )),
            Array::Binary(BinaryArray::new(
                vec![vec![1], Vec::new(), vec![2, 3]],
                Some(vec![true, false, true]),
            )),
            Array::Bool(BooleanArray::new(
                vec![true, false, true],
                Some(vec![true, true, true]),
            )),
        ],
        3,
    )
    .expect("valid mixed batch")
}

/// A variable-width column whose values are all empty, which is the only way a
/// nonempty column produces a zero-length values stream.
fn empty_string_schema() -> Schema {
    Schema::new(
        28,
        vec![Column::new(1, "text", LogicalType::Utf8, false)],
        None,
    )
}

fn int64_schema(id: u64) -> Schema {
    Schema::new(
        id,
        vec![Column::new(1, "value", LogicalType::Int64, false)],
        None,
    )
}

/// Every row of a single-Int64-column file, in file order across its blocks.
fn scan_int64(path: &std::path::Path) -> Vec<i64> {
    Reader::open(path)
        .unwrap()
        .scan()
        .flat_map(|batch| match batch.unwrap().column(0).unwrap() {
            Array::Int64(values) => values.values().to_vec(),
            other => panic!("unexpected array: {other:?}"),
        })
        .collect()
}

fn int64_batch(schema: &Schema, values: Vec<i64>) -> RecordBatch {
    let row_count = values.len();
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Int64(PrimitiveArray::new(values, None))],
        row_count,
    )
    .expect("valid int64 batch")
}

#[test]
fn the_row_target_publishes_complete_blocks_automatically() {
    let schema = int64_schema(29);
    let path = TempPath::new("row-threshold");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default()
            .with_row_block_target(2)
            .with_byte_block_target(1_000_000),
    )
    .unwrap();

    writer
        .append(int64_batch(&schema, vec![1, 2, 3, 4, 5]))
        .unwrap();
    let accounting = writer.accounting();
    assert_eq!(accounting.published_rows(), 4);
    assert_eq!(accounting.buffered_rows(), 1);
    assert_eq!(Reader::open(path.path()).unwrap().blocks().len(), 2);

    let summary = writer.finish().unwrap();
    assert_eq!(summary.blocks_written(), 3);
    assert_eq!(summary.rows_written(), 5);
}

#[test]
fn the_byte_target_publishes_complete_blocks_automatically() {
    let schema = int64_schema(30);
    let path = TempPath::new("byte-threshold");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default()
            .with_row_block_target(100)
            .with_byte_block_target(240),
    )
    .unwrap();

    writer
        .append(int64_batch(&schema, vec![1, 2, 3, 4, 5]))
        .unwrap();
    let accounting = writer.accounting();
    assert_eq!(accounting.published_rows(), 4);
    assert_eq!(accounting.buffered_rows(), 1);
    assert!(accounting.buffered_bytes() <= 240);
    assert_eq!(Reader::open(path.path()).unwrap().blocks().len(), 2);
}

#[test]
fn multiple_published_blocks_preserve_file_order() {
    let schema = int64_schema(31);
    let path = TempPath::new("block-order");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default()
            .with_row_block_target(2)
            .with_byte_block_target(1_000_000),
    )
    .unwrap();
    writer
        .append(int64_batch(&schema, (1..=7).collect()))
        .unwrap();
    let _ = writer.finish().unwrap();

    assert_eq!(scan_int64(path.path()), (1..=7).collect::<Vec<i64>>());
}

#[test]
fn accounting_distinguishes_buffered_published_and_durable_data() {
    let schema = int64_schema(32);
    let path = TempPath::new("accounting");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default()
            .with_row_block_target(2)
            .with_byte_block_target(1_000_000),
    )
    .unwrap();
    writer.append(int64_batch(&schema, vec![1, 2, 3])).unwrap();

    let accounting = writer.accounting();
    assert_eq!(accounting.buffered_rows(), 1);
    assert_eq!(accounting.published_rows(), 2);
    assert_eq!(accounting.durable_rows(), 0);
    assert_eq!(accounting.total_rows(), 3);
    assert_eq!(
        accounting.total_rows(),
        accounting.buffered_rows() + accounting.published_rows()
    );
    assert_eq!(
        accounting.total_bytes(),
        accounting.buffered_bytes() + accounting.published_bytes()
    );

    writer.flush().unwrap();
    let flushed = writer.accounting();
    assert_eq!(flushed.buffered_rows(), 0);
    assert_eq!(flushed.published_rows(), 3);
    assert_eq!(flushed.durable_rows(), 0);

    writer.sync().unwrap();
    let synced = writer.accounting();
    assert_eq!(synced.durable_rows(), 3);
    assert_eq!(synced.durable_bytes(), synced.published_bytes());
}

#[test]
fn raw_stage_7_output_round_trips_through_validator_and_reader() {
    let schema = int64_schema(33);
    let path = TempPath::new("raw-round-trip");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default().with_row_block_target(2),
    )
    .unwrap();
    writer
        .append(int64_batch(&schema, vec![-2, -1, 0, 1, 2]))
        .unwrap();
    let _ = writer.finish().unwrap();

    let bytes = std::fs::read(path.path()).unwrap();
    common::expect_valid("stage-7-raw", &bytes);
    assert_eq!(scan_int64(path.path()), vec![-2, -1, 0, 1, 2]);
}

#[cfg(feature = "zstd")]
#[test]
fn zstandard_output_round_trips_through_validator_and_reader() {
    let schema = int64_schema(34);
    let path = TempPath::new("zstd-round-trip");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default()
            .with_row_block_target(4)
            .with_codec(WriterCodec::Zstandard),
    )
    .unwrap();
    writer
        .append(int64_batch(&schema, vec![7, 7, 7, 7, 7, 7, 7, 7]))
        .unwrap();
    let _ = writer.finish().unwrap();

    let bytes = std::fs::read(path.path()).unwrap();
    common::expect_valid("stage-7-zstd", &bytes);
    let frame = common::data_frame_offset(&bytes);
    let descriptor = common::stream_descriptor_offset(&bytes, frame, 0);
    assert_eq!(
        common::read_u16(&bytes, descriptor + common::STREAM_CODEC),
        common::CODEC_ZSTD
    );
    assert_eq!(
        common::read_u16(&bytes, descriptor + common::STREAM_TRANSFORM),
        common::TRANSFORM_RAW
    );

    assert_eq!(scan_int64(path.path()), vec![7; 8]);
}

#[cfg(feature = "zstd")]
#[test]
fn zstandard_level_is_configurable_and_default_level_is_stable() {
    let schema = int64_schema(340);
    let values: Vec<i64> = (0..8192)
        .map(|index| {
            let block = (index / 128) as i64;
            (index % 64) as i64 + (block % 11) * 10_000
        })
        .collect();

    let write = |label: &str, level: Option<i32>| {
        let path = TempPath::new(label);
        let mut options = WriterOptions::default()
            .with_row_block_target(8192)
            .with_codec(WriterCodec::Zstandard);
        if let Some(level) = level {
            options = options.with_zstd_level(level);
        }
        let mut writer = Writer::create(path.path(), schema.clone(), options).unwrap();
        writer.append(int64_batch(&schema, values.clone())).unwrap();
        let _ = writer.finish().unwrap();

        let bytes = std::fs::read(path.path()).unwrap();
        common::expect_valid(label, &bytes);
        assert_eq!(scan_int64(path.path()), values);
        bytes
    };

    let default_bytes = write("zstd-level-default", None);
    let explicit_default_bytes = write("zstd-level-three", Some(3));
    assert_eq!(default_bytes, explicit_default_bytes);

    let range = zstd::compression_level_range();
    let minimum_bytes = write("zstd-level-minimum", Some(*range.start()));
    let maximum_bytes = write("zstd-level-maximum", Some(*range.end()));
    assert_ne!(minimum_bytes.len(), maximum_bytes.len());
}

#[test]
fn zstandard_level_is_ignored_for_the_raw_codec() {
    let schema = int64_schema(342);
    let path = TempPath::new("zstd-level-ignored");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default().with_zstd_level(i32::MAX),
    )
    .unwrap();
    writer.append(int64_batch(&schema, vec![1, 2, 3])).unwrap();
    let _ = writer.finish().unwrap();

    assert_eq!(scan_int64(path.path()), vec![1, 2, 3]);
}

#[cfg(feature = "zstd")]
#[test]
fn invalid_zstandard_level_is_rejected_before_file_creation() {
    let schema = int64_schema(341);
    let path = TempPath::new("zstd-invalid-level");
    let error = Writer::create(
        path.path(),
        schema,
        WriterOptions::default()
            .with_codec(WriterCodec::Zstandard)
            .with_zstd_level(i32::MAX),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert!(!path.path().exists());
}

#[cfg(not(feature = "zstd"))]
#[test]
fn zstandard_writer_selection_is_unavailable_without_the_feature() {
    let schema = int64_schema(35);
    let path = TempPath::new("zstd-without-feature");
    let error = Writer::create(
        path.path(),
        schema,
        WriterOptions::default().with_codec(WriterCodec::Zstandard),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert!(!path.path().exists());
}

#[cfg(feature = "zstd")]
#[test]
fn compressed_stream_metadata_crc_and_alignment_are_consistent() {
    let schema = Schema::new(
        36,
        vec![
            Column::new(1, "first", LogicalType::Int64, false),
            Column::new(2, "second", LogicalType::Int64, false),
        ],
        None,
    );
    let path = TempPath::new("zstd-metadata");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default().with_codec(WriterCodec::Zstandard),
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Int64(PrimitiveArray::new(vec![11; 32], None)),
            Array::Int64(PrimitiveArray::new(vec![12; 32], None)),
        ],
        32,
    )
    .unwrap();
    writer.append(batch).unwrap();
    let _ = writer.finish().unwrap();

    let bytes = std::fs::read(path.path()).unwrap();
    common::expect_valid("zstd-metadata", &bytes);
    let frame = common::data_frame_offset(&bytes);
    let payload = common::payload_offset(&bytes, frame);
    let payload_length = common::read_u64(&bytes, frame + common::PREFIX_PAYLOAD_LENGTH);
    let header = frame + common::PREFIX_SIZE;
    let stream_table =
        common::read_u32(&bytes, header + common::BLOCK_STREAM_TABLE_OFFSET) as usize;
    let statistics = common::read_u32(&bytes, header + common::BLOCK_STATISTICS_OFFSET) as usize;
    let stream_count = (statistics - stream_table) / common::STREAM_DESCRIPTOR_SIZE;
    assert_eq!(stream_count, 2);
    for index in 0..stream_count {
        let descriptor = common::stream_descriptor_offset(&bytes, frame, index);
        let offset = common::read_u64(&bytes, descriptor + common::STREAM_PAYLOAD_OFFSET);
        let stored = common::read_u64(&bytes, descriptor + common::STREAM_STORED_LENGTH);
        let transformed = common::read_u64(&bytes, descriptor + common::STREAM_TRANSFORMED_LENGTH);
        let crc = common::read_u32(&bytes, descriptor + common::STREAM_CRC);
        assert_eq!(
            common::read_u16(&bytes, descriptor + common::STREAM_CODEC),
            common::CODEC_ZSTD
        );
        assert_eq!(
            common::read_u16(&bytes, descriptor + common::STREAM_TRANSFORM),
            common::TRANSFORM_RAW
        );
        assert_eq!(offset % 8, 0);
        assert!(stored > 0);
        assert_eq!(transformed, 32 * std::mem::size_of::<i64>() as u64);
        assert!(offset + stored <= payload_length);
        let stored_start = payload + offset as usize;
        let stored_end = stored_start + stored as usize;
        assert_eq!(common::crc32c(&bytes[stored_start..stored_end]), crc);
    }
}

#[test]
fn buffered_rows_never_exceed_the_configured_row_target() {
    let schema = int64_schema(37);
    let path = TempPath::new("bounded-buffer");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default()
            .with_row_block_target(3)
            .with_byte_block_target(1_000_000),
    )
    .unwrap();
    writer
        .append(int64_batch(&schema, (0..100).map(i64::from).collect()))
        .unwrap();
    let accounting = writer.accounting();
    assert!(accounting.buffered_rows() <= 3);
    assert!(accounting.buffered_bytes() <= 1_000_000);
    assert_eq!(accounting.total_rows(), 100);
}

#[test]
fn several_appends_accumulate_into_one_block() {
    let schema = int64_schema(38);
    let path = TempPath::new("coalescing");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default().with_row_block_target(10),
    )
    .unwrap();

    for value in 1..=6 {
        writer.append(int64_batch(&schema, vec![value])).unwrap();
    }
    let summary = writer.finish().unwrap();

    assert_eq!(summary.blocks_written(), 1);
}

#[test]
fn rows_buffered_across_appends_keep_their_order() {
    let schema = int64_schema(39);
    let path = TempPath::new("coalesced-order");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default().with_row_block_target(10),
    )
    .unwrap();
    for value in 1..=6 {
        writer.append(int64_batch(&schema, vec![value])).unwrap();
    }
    let _ = writer.finish().unwrap();

    assert_eq!(scan_int64(path.path()), (1..=6).collect::<Vec<i64>>());
}

/// The exact raw frame length these rows would serialize to, taken from the
/// writer's own accounting for a buffer it has not published yet. Deriving a
/// byte target this way keeps the threshold tests off hard-coded frame sizes.
fn buffered_frame_bytes(schema: &Schema, batch: RecordBatch) -> u64 {
    let path = TempPath::new("measure");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer.append(batch).unwrap();
    writer.accounting().buffered_bytes()
}

#[test]
fn a_byte_target_at_a_block_boundary_publishes_that_block() {
    let schema = int64_schema(40);
    let target = buffered_frame_bytes(&schema, int64_batch(&schema, vec![0, 1, 2]));
    let path = TempPath::new("byte-boundary-at");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default().with_byte_block_target(target),
    )
    .unwrap();

    writer
        .append(int64_batch(&schema, (0..6).collect()))
        .unwrap();

    assert_eq!(block_row_counts(&mut writer, &path), vec![3, 3]);
}

#[test]
fn a_byte_target_one_below_a_boundary_publishes_a_smaller_block() {
    let schema = int64_schema(41);
    let target = buffered_frame_bytes(&schema, int64_batch(&schema, vec![0, 1, 2])) - 1;
    let path = TempPath::new("byte-boundary-below");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default().with_byte_block_target(target),
    )
    .unwrap();

    writer
        .append(int64_batch(&schema, (0..6).collect()))
        .unwrap();

    assert_eq!(block_row_counts(&mut writer, &path), vec![2, 2, 2]);
}

/// Publish everything buffered and report the rows each block ended up with.
fn block_row_counts(writer: &mut Writer, path: &TempPath) -> Vec<usize> {
    writer.flush().unwrap();
    Reader::open(path.path())
        .unwrap()
        .scan()
        .map(|batch| batch.unwrap().row_count())
        .collect()
}

#[test]
fn a_byte_target_below_the_frame_overhead_still_writes_every_row() {
    let schema = int64_schema(42);
    let path = TempPath::new("byte-target-tiny");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default().with_byte_block_target(1),
    )
    .unwrap();
    writer
        .append(int64_batch(&schema, (1..=5).collect()))
        .unwrap();
    let summary = writer.finish().unwrap();

    common::expect_valid("tiny-byte-target", &std::fs::read(path.path()).unwrap());
    assert_eq!(
        (summary.blocks_written(), scan_int64(path.path())),
        (5, (1..=5).collect::<Vec<i64>>()),
        "each oversize row becomes a block of its own"
    );
}

#[test]
fn a_row_wider_than_the_byte_target_becomes_a_block_of_its_own() {
    let schema = Schema::new(
        43,
        vec![Column::new(1, "blob", LogicalType::Binary, false)],
        None,
    );
    let path = TempPath::new("oversize-row");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default().with_byte_block_target(64),
    )
    .unwrap();
    writer
        .append(binary_batch(
            &schema,
            vec![vec![1; 4096], vec![2; 4096], vec![3; 4096]],
        ))
        .unwrap();
    let summary = writer.finish().unwrap();

    common::expect_valid("oversize-row", &std::fs::read(path.path()).unwrap());
    assert_eq!((summary.blocks_written(), summary.rows_written()), (3, 3));
}

fn binary_batch(schema: &Schema, values: Vec<Vec<u8>>) -> RecordBatch {
    let row_count = values.len();
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Binary(BinaryArray::new(values, None))],
        row_count,
    )
    .expect("valid binary batch")
}

#[test]
fn a_full_buffer_is_published_before_a_row_that_cannot_join_it() {
    let schema = Schema::new(
        44,
        vec![Column::new(1, "blob", LogicalType::Binary, false)],
        None,
    );
    // Sized so the two narrow rows exactly fill a block. The wide row that
    // follows cannot join them, so the buffer must be published before it is
    // accepted rather than the wide row being split or refused.
    let target = buffered_frame_bytes(&schema, binary_batch(&schema, vec![vec![1; 8], vec![2; 8]]));
    let path = TempPath::new("publish-before-oversize");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default().with_byte_block_target(target),
    )
    .unwrap();

    writer
        .append(binary_batch(
            &schema,
            vec![vec![1; 8], vec![2; 8], vec![3; 4096]],
        ))
        .unwrap();
    let _ = writer.finish().unwrap();

    common::expect_valid(
        "publish-before-oversize",
        &std::fs::read(path.path()).unwrap(),
    );
    let blocks: Vec<usize> = Reader::open(path.path())
        .unwrap()
        .scan()
        .map(|batch| batch.unwrap().row_count())
        .collect();
    assert_eq!(blocks, vec![2, 1]);
}

/// A schema covering every logical type, both validity representations, and
/// the all-null and all-valid special cases, so a block boundary that mishandles
/// any one of them is visible.
fn every_type_split_schema() -> Schema {
    Schema::new(
        45,
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
            Column::new(3, "i32", LogicalType::Int32, true),
            Column::new(4, "f64", LogicalType::Float64, false),
            Column::new(
                5,
                "decimal",
                LogicalType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                true,
            ),
            Column::new(6, "text", LogicalType::Utf8, true),
            Column::new(
                7,
                "category",
                LogicalType::Categorical { ordered: false },
                true,
            ),
            Column::new(8, "binary", LogicalType::Binary, true),
            Column::new(9, "fixed", LogicalType::FixedBinary { byte_width: 3 }, true),
            Column::new(10, "date", LogicalType::Date32, false),
            Column::new(11, "all_null", LogicalType::Int64, true),
            Column::new(12, "all_valid", LogicalType::UInt16, true),
        ],
        Some(1),
    )
}

fn every_type_split_batch(schema: &Schema, range: std::ops::Range<i64>) -> RecordBatch {
    let rows: Vec<i64> = range.collect();
    let count = rows.len();
    // A null on every third row leaves the mixed columns genuinely mixed.
    let mixed: Vec<bool> = rows.iter().map(|row| row % 3 != 1).collect();
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Timestamp(TimestampArray::new(
                rows.iter().map(|row| row * 10).collect(),
                None,
                TimeUnit::Microsecond,
                TimeZone::Utc,
            )),
            Array::Bool(BooleanArray::new(
                rows.iter().map(|row| row % 2 == 0).collect(),
                Some(mixed.clone()),
            )),
            Array::Int32(PrimitiveArray::new(
                rows.iter().map(|row| *row as i32).collect(),
                Some(mixed.clone()),
            )),
            Array::Float64(PrimitiveArray::new(
                rows.iter().map(|row| *row as f64).collect(),
                None,
            )),
            Array::Decimal(DecimalArray::new(
                rows.iter().map(|row| row * 7).collect(),
                Some(mixed.clone()),
                10,
                2,
            )),
            Array::Utf8(Utf8Array::new(
                rows.iter()
                    .map(|row| "t".repeat(*row as usize % 5))
                    .collect(),
                Some(mixed.clone()),
            )),
            Array::Categorical(Utf8Array::new(
                rows.iter().map(|row| format!("c{}", row % 3)).collect(),
                Some(mixed.clone()),
            )),
            Array::Binary(BinaryArray::new(
                rows.iter()
                    .map(|row| vec![*row as u8; *row as usize % 7])
                    .collect(),
                Some(mixed.clone()),
            )),
            Array::FixedBinary(BinaryArray::new(
                rows.iter().map(|row| vec![*row as u8; 3]).collect(),
                Some(mixed.clone()),
            )),
            Array::Date32(PrimitiveArray::new(
                rows.iter().map(|row| *row as i32).collect(),
                None,
            )),
            Array::Int64(PrimitiveArray::new(
                vec![0; count],
                Some(vec![false; count]),
            )),
            Array::UInt16(PrimitiveArray::new(
                rows.iter().map(|row| *row as u16).collect(),
                Some(vec![true; count]),
            )),
        ],
        count,
    )
    .expect("a well-formed batch")
}

/// Write the same twelve rows with the given block target and append chunking,
/// then read every block back as one list of rows.
fn write_split(label: &str, row_target: u64, chunk: i64, codec: WriterCodec) -> Vec<RecordBatch> {
    let schema = every_type_split_schema();
    let path = TempPath::new(label);
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        WriterOptions::default()
            .with_row_block_target(row_target)
            .with_codec(codec),
    )
    .unwrap();
    let mut start = 0;
    while start < 12 {
        let end = (start + chunk).min(12);
        writer
            .append(every_type_split_batch(&schema, start..end))
            .unwrap();
        start = end;
    }
    let _ = writer.finish().unwrap();

    common::expect_valid(label, &std::fs::read(path.path()).unwrap());
    Reader::open(path.path())
        .unwrap()
        .scan()
        .map(|batch| batch.unwrap())
        .collect()
}

/// Every row of a scan, rendered so rows from differently shaped blocks compare.
fn rendered_rows(blocks: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for block in blocks {
        for row in 0..block.row_count() {
            let cells: Vec<String> = block
                .columns()
                .iter()
                .map(|column| format!("{:?}", column.value_at(row)))
                .collect();
            rows.push(cells.join("|"));
        }
    }
    rows
}

#[test]
fn a_block_boundary_preserves_every_type_and_null_pattern() {
    let whole = rendered_rows(&write_split("split-whole", 12, 12, WriterCodec::None));

    for row_target in 1..=12 {
        for chunk in 1..=5 {
            let split = write_split("split-part", row_target, chunk, WriterCodec::None);
            assert_eq!(
                rendered_rows(&split),
                whole,
                "row_target={row_target} chunk={chunk} did not survive its block boundaries"
            );
        }
    }
}

#[cfg(feature = "zstd")]
#[test]
fn zstandard_output_survives_block_boundaries_like_raw_output() {
    let raw = rendered_rows(&write_split("split-raw", 12, 12, WriterCodec::None));

    for row_target in [1_u64, 5, 12] {
        let compressed = write_split("split-zstd", row_target, 3, WriterCodec::Zstandard);
        assert_eq!(rendered_rows(&compressed), raw, "row_target={row_target}");
    }
}

#[test]
fn a_block_is_serialized_the_same_however_its_rows_were_appended() {
    let schema = every_type_split_schema();
    let one_append = TempPath::new("chunking-one");
    let many_appends = TempPath::new("chunking-many");

    let mut writer =
        Writer::create(one_append.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer
        .append(every_type_split_batch(&schema, 0..12))
        .unwrap();
    let _ = writer.finish().unwrap();

    let mut writer = Writer::create(
        many_appends.path(),
        schema.clone(),
        WriterOptions::default(),
    )
    .unwrap();
    for start in (0..12).step_by(5) {
        writer
            .append(every_type_split_batch(&schema, start..(start + 5).min(12)))
            .unwrap();
    }
    let _ = writer.finish().unwrap();

    assert_eq!(
        std::fs::read(one_append.path()).unwrap(),
        std::fs::read(many_appends.path()).unwrap()
    );
}

fn empty_string_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(empty_string_schema()),
        vec![Array::Utf8(Utf8Array::new(
            vec![String::new(), String::new(), String::new()],
            None,
        ))],
        3,
    )
    .expect("valid empty-string batch")
}
