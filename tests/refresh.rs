//! Stage 9 reader refresh coverage.
//!
//! Refresh must extend a snapshot with exactly the frames committed after its
//! own boundary, validate them exactly as initial opening validates a whole
//! file, and leave every field and block vector unchanged when it fails. These
//! tests exercise the report counts, the incomplete-tail lifecycle, repair,
//! corruption, truncation, replacement, snapshot isolation, and the coexistence
//! of a refresh with an active writer.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use acta::{
    Array, Column, ErrorKind, Limits, LogicalType, PrimaryRange, PrimitiveArray, Reader,
    RecordBatch, Schema, TimeUnit, TimeZone, TimestampArray, ValidationLevel, ValidationOptions,
    Writer, WriterOptions,
};
use common::{
    DATA_FRAME_TYPE, PREFIX_SIZE, block_header, build_file, data_frame_offset, frame, frame_length,
    int64_column, schema_header, with_appended_frame,
};

struct TempPath(PathBuf);

impl TempPath {
    fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(std::env::temp_dir().join(format!(
            "acta-refresh-{label}-{}-{}.acta",
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

fn int64_schema(id: u64) -> Schema {
    Schema::new(
        id,
        vec![Column::new(1, "value", LogicalType::Int64, false)],
        None,
    )
}

fn int64_batch(schema: &Schema, values: Vec<i64>) -> RecordBatch {
    let row_count = values.len();
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Int64(PrimitiveArray::new(values, None))],
        row_count,
    )
    .expect("well-formed int64 batch")
}

fn rows_of(batch: &RecordBatch) -> Vec<i64> {
    match &batch.columns()[0] {
        Array::Int64(array) => (0..batch.row_count())
            .map(|row| array.values()[row])
            .collect(),
        other => panic!("unexpected column {other:?}"),
    }
}

/// Create a file whose rows land in one block per row, so an append of `rows`
/// contributes exactly `rows.len()` blocks.
fn write_blocks(path: &Path, schema: &Schema, rows: &[i64], options: WriterOptions) {
    let mut writer = Writer::create(path, schema.clone(), options).unwrap();
    writer.append(int64_batch(schema, rows.to_vec())).unwrap();
    let _ = writer.finish().unwrap();
}

fn append_blocks(path: &Path, schema: &Schema, rows: &[i64], options: WriterOptions) {
    let mut writer = Writer::open(path, options).unwrap();
    writer.append(int64_batch(schema, rows.to_vec())).unwrap();
    let _ = writer.finish().unwrap();
}

fn one_block_options() -> WriterOptions {
    WriterOptions::default().with_row_block_target(1)
}

fn time_schema() -> Schema {
    Schema::new(
        60,
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
        ],
        timestamps.len(),
    )
    .expect("well-formed timestamp batch")
}

/// A file with a primary timestamp column and one block per row.
fn write_time_blocks(path: &Path, schema: &Schema, timestamps: &[i64], options: WriterOptions) {
    let mut writer = Writer::create(path, schema.clone(), options).unwrap();
    writer.append(time_batch(schema, timestamps)).unwrap();
    let _ = writer.finish().unwrap();
}

/// The first `cut` bytes of a frame copied after `base` with `sequence`.
///
/// The copied frame is a duplicate of `base`'s first data frame, so a cut
/// inside its prefix never exposes a wrong sequence to the reader; the frame
/// is simply incomplete.
fn with_partial_frame(base: &[u8], sequence: u64, cut: usize) -> Vec<u8> {
    let appended = with_appended_frame(base, data_frame_offset(base), sequence);
    let frame_length = appended.len() - base.len();
    let cut = cut.min(frame_length.saturating_sub(1));
    appended[..base.len() + cut].to_vec()
}

// ------------------------------------------------------------------ report

#[test]
fn refresh_on_schema_only_one_block_and_multi_block_files_adds_nothing() {
    let schema = int64_schema(1);
    let options = one_block_options();

    let schema_only = TempPath::new("schema-only");
    {
        let writer = Writer::create(schema_only.path(), schema.clone(), options).unwrap();
        let _ = writer;
        let mut reader = Reader::open(schema_only.path()).unwrap();
        let report = reader.refresh().unwrap();
        assert_eq!(report.blocks_added(), 0);
        assert_eq!(report.rows_added(), 0);
        assert_eq!(report.previous_file_size(), report.observed_file_size());
        assert!(!report.incomplete_tail());
    }

    let one_block = TempPath::new("one-block");
    write_blocks(one_block.path(), &schema, &[1], options);
    let mut reader = Reader::open(one_block.path()).unwrap();
    let report = reader.refresh().unwrap();
    assert_eq!(report.blocks_added(), 0);
    assert_eq!(reader.blocks().len(), 1);

    let multi_block = TempPath::new("multi-block");
    write_blocks(multi_block.path(), &schema, &[1, 2, 3], options);
    let mut reader = Reader::open(multi_block.path()).unwrap();
    assert_eq!(reader.blocks().len(), 3);
    let report = reader.refresh().unwrap();
    assert_eq!(report.blocks_added(), 0);
    assert_eq!(reader.blocks().len(), 3);
}

#[test]
fn new_blocks_are_added_with_exact_report_counts() {
    let schema = int64_schema(2);
    let options = one_block_options();
    let path = TempPath::new("new-blocks");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let before = reader.file_metadata().file_size();

    append_blocks(path.path(), &schema, &[2, 3], options);
    let report = reader.refresh().unwrap();

    assert_eq!(report.blocks_added(), 2);
    assert_eq!(report.rows_added(), 2);
    assert_eq!(report.previous_file_size(), before);
    assert!(report.observed_file_size() > before);
    assert!(!report.incomplete_tail());
    assert_eq!(reader.blocks().len(), 3);
    assert_eq!(reader.total_rows(), 3);
    assert_eq!(reader.blocks()[2].sequence(), 3);
    assert_eq!(
        reader.file_metadata().last_good_offset(),
        report.observed_file_size()
    );
}

#[test]
fn several_new_blocks_are_added_in_one_refresh() {
    let schema = int64_schema(3);
    let options = one_block_options();
    let path = TempPath::new("several-new");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();

    append_blocks(path.path(), &schema, &[2], options);
    append_blocks(path.path(), &schema, &[3], options);
    append_blocks(path.path(), &schema, &[4], options);

    let report = reader.refresh().unwrap();
    assert_eq!(report.blocks_added(), 3);
    assert_eq!(report.rows_added(), 3);
    assert_eq!(reader.blocks().len(), 4);
    assert_eq!(
        reader
            .blocks()
            .iter()
            .map(|block| block.sequence())
            .collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
}

#[test]
fn several_refresh_cycles_add_each_block_exactly_once() {
    let schema = int64_schema(4);
    let options = one_block_options();
    let path = TempPath::new("cycles");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let mut added_total = 0;

    for (cycle, rows) in [(1, vec![2, 3]), (2, vec![4]), (3, vec![])] {
        if !rows.is_empty() {
            append_blocks(path.path(), &schema, &rows, options);
        }
        let report = reader.refresh().unwrap();
        let expected = rows.len() as u64;
        assert_eq!(report.blocks_added(), expected, "cycle {cycle}");
        added_total += expected;
        assert_eq!(
            reader.blocks().len() as u64,
            1 + added_total,
            "cycle {cycle}"
        );
    }

    assert_eq!(reader.blocks().len(), 4);
    let sequences: Vec<u64> = reader
        .blocks()
        .iter()
        .map(|block| block.sequence())
        .collect();
    assert_eq!(sequences, [1, 2, 3, 4], "no frame is ever added twice");
}

// ---------------------------------------------------------- writer coexistence

#[test]
fn buffered_unpublished_rows_produce_no_refresh_addition() {
    let schema = int64_schema(5);
    let path = TempPath::new("buffered");
    // A single batch is far below the default 65,536-row target, so it stays
    // buffered and the file still ends after the schema frame.
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer.append(int64_batch(&schema, vec![1, 2, 3])).unwrap();

    let mut reader = Reader::open(path.path()).unwrap();
    assert_eq!(reader.blocks().len(), 0);

    let report = reader.refresh().unwrap();
    assert_eq!(report.blocks_added(), 0);
    assert_eq!(report.rows_added(), 0);
    assert!(!report.incomplete_tail());

    drop(writer);
}

#[test]
fn writer_flush_publishes_a_frame_visible_to_refresh_while_the_lock_is_held() {
    let schema = int64_schema(6);
    let path = TempPath::new("flush");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer.append(int64_batch(&schema, vec![1, 2, 3])).unwrap();

    let mut reader = Reader::open(path.path()).unwrap();
    assert_eq!(reader.refresh().unwrap().blocks_added(), 0);

    writer.flush().unwrap();
    let report = reader.refresh().unwrap();
    assert_eq!(report.blocks_added(), 1);
    assert_eq!(report.rows_added(), 3);
    assert_eq!(reader.total_rows(), 3);

    // The writer is still alive and holds the cooperative exclusive lock;
    // refresh never took it, so the reader and writer coexist.
    let _ = writer.finish().unwrap();
}

#[test]
fn reader_and_writer_coexist_without_reader_lock_acquisition() {
    let schema = int64_schema(7);
    let path = TempPath::new("coexist");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer.append(int64_batch(&schema, vec![1])).unwrap();

    let mut reader = Reader::open(path.path()).unwrap();
    // If refresh acquired the writer lock, this would fail with WriterLocked
    // while the writer is alive.
    let report = reader.refresh().unwrap();
    assert_eq!(report.blocks_added(), 0);

    writer.flush().unwrap();
    assert_eq!(reader.refresh().unwrap().blocks_added(), 1);
    let _ = writer.finish().unwrap();
}

// -------------------------------------------------------- incomplete-tail cuts

#[test]
fn an_incomplete_prefix_header_payload_or_trailer_is_never_exposed() {
    let schema = int64_schema(8);
    let options = one_block_options();
    let path = TempPath::new("cuts");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let base = fs::read(path.path()).unwrap();

    let cuts = [
        1,
        PREFIX_SIZE - 1,
        PREFIX_SIZE,
        PREFIX_SIZE + 1,
        100,
        frame_length(&base, data_frame_offset(&base)) - 1,
    ];
    for cut in cuts {
        fs::write(path.path(), with_partial_frame(&base, 2, cut)).unwrap();
        let report = reader.refresh().unwrap();
        assert_eq!(report.blocks_added(), 0, "cut {cut}");
        assert!(report.incomplete_tail(), "cut {cut}");
        assert_eq!(reader.blocks().len(), 1, "cut {cut}");
        assert_eq!(reader.file_metadata().last_good_offset(), base.len() as u64);
    }
}

#[test]
fn every_cut_across_one_appended_frame_is_reported_as_an_incomplete_tail() {
    let schema = int64_schema(9);
    let options = one_block_options();
    let path = TempPath::new("sweep");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let base = fs::read(path.path()).unwrap();
    let appended = with_appended_frame(&base, data_frame_offset(&base), 2);
    let added = appended.len() - base.len();

    for cut in 1..added {
        fs::write(path.path(), &appended[..base.len() + cut]).unwrap();
        let report = reader.refresh().unwrap();
        assert_eq!(report.blocks_added(), 0, "cut {cut}");
        assert!(report.incomplete_tail(), "cut {cut}");
        assert_eq!(reader.blocks().len(), 1);
    }
}

#[test]
fn an_incomplete_tail_later_becomes_a_complete_frame() {
    let schema = int64_schema(10);
    let options = one_block_options();
    let path = TempPath::new("completes");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let base = fs::read(path.path()).unwrap();

    fs::write(path.path(), with_partial_frame(&base, 2, 7)).unwrap();
    let pending = reader.refresh().unwrap();
    assert_eq!(pending.blocks_added(), 0);
    assert!(pending.incomplete_tail());

    fs::write(
        path.path(),
        with_appended_frame(&base, data_frame_offset(&base), 2),
    )
    .unwrap();
    let complete = reader.refresh().unwrap();
    assert_eq!(complete.blocks_added(), 1);
    assert!(!complete.incomplete_tail());
    assert_eq!(reader.blocks().len(), 2);
}

#[test]
fn complete_frames_followed_by_a_new_incomplete_frame_are_all_added() {
    let path = TempPath::new("complete-then-tail");
    // A base file with one committed block, then two complete frames and one
    // incomplete frame cut near its end, all built from the field tables.
    let base = build_file(
        0,
        &schema_header(1, 1, 0, 0),
        &[int64_column(1, "value")],
        &[block_header(1, 2, u64::MAX, 0)],
    );
    fs::write(path.path(), &base).unwrap();
    let mut reader = Reader::open(path.path()).unwrap();

    // Two complete frames and one incomplete frame cut near its end, appended
    // after the reader already holds its one-block snapshot.
    let mut bytes = base;
    let second = frame(DATA_FRAME_TYPE, 2, &block_header(1, 2, u64::MAX, 0), &[]);
    let third = frame(DATA_FRAME_TYPE, 3, &block_header(1, 2, u64::MAX, 0), &[]);
    let fourth = frame(DATA_FRAME_TYPE, 4, &block_header(1, 2, u64::MAX, 0), &[]);
    bytes.extend_from_slice(&second);
    bytes.extend_from_slice(&third);
    bytes.extend_from_slice(&fourth[..fourth.len() - 1]);
    fs::write(path.path(), &bytes).unwrap();

    let report = reader.refresh().unwrap();
    assert_eq!(report.blocks_added(), 2);
    assert!(report.incomplete_tail());
    assert_eq!(reader.blocks().len(), 3);
    assert_eq!(
        reader
            .blocks()
            .iter()
            .map(|block| block.sequence())
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
}

// --------------------------------------------------------------------- repair

#[test]
fn refresh_accepts_a_repair_that_removes_the_incomplete_tail() {
    let schema = int64_schema(12);
    let options = one_block_options();
    let path = TempPath::new("repair-accept");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let base = fs::read(path.path()).unwrap();
    fs::write(path.path(), with_partial_frame(&base, 2, 40)).unwrap();
    let pending = reader.refresh().unwrap();
    assert_eq!(pending.blocks_added(), 0);
    assert!(pending.incomplete_tail());

    acta::repair_incomplete_tail(path.path()).unwrap();
    let report = reader.refresh().unwrap();
    assert_eq!(report.blocks_added(), 0);
    assert!(!report.incomplete_tail());
    assert_eq!(report.observed_file_size(), base.len() as u64);
    assert_eq!(reader.file_metadata().file_size(), base.len() as u64);
    assert_eq!(reader.file_metadata().last_good_offset(), base.len() as u64);
    assert_eq!(reader.blocks().len(), 1);
}

#[test]
fn append_after_repair_is_discovered_with_the_correct_sequence() {
    let schema = int64_schema(13);
    let options = one_block_options();
    let path = TempPath::new("repair-append");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let base = fs::read(path.path()).unwrap();

    fs::write(path.path(), with_partial_frame(&base, 2, 40)).unwrap();
    assert!(reader.refresh().unwrap().incomplete_tail());

    acta::repair_incomplete_tail(path.path()).unwrap();
    reader.refresh().unwrap();
    append_blocks(path.path(), &schema, &[2], options);

    let report = reader.refresh().unwrap();
    assert_eq!(report.blocks_added(), 1);
    assert_eq!(reader.blocks()[1].sequence(), 2);
    assert_eq!(reader.total_rows(), 2);
}

#[test]
fn row_ids_continue_across_refresh_and_repair() {
    let schema = int64_schema(14);
    let options = one_block_options().with_row_ids(true);
    let path = TempPath::new("row-ids");
    write_blocks(path.path(), &schema, &[1, 2], options);
    let mut reader = Reader::open(path.path()).unwrap();

    append_blocks(path.path(), &schema, &[3], options);
    let report = reader.refresh().unwrap();
    assert_eq!(report.blocks_added(), 1);
    assert_eq!(reader.total_rows(), 3);
    assert_eq!(reader.blocks()[2].base_row_id(), Some(2));

    // Interrupt a next append (a copy of the first frame's prefix, cut inside
    // the prefix so no sequence is ever checked), repair it, then continue.
    let current = fs::read(path.path()).unwrap();
    let first_frame = data_frame_offset(&current);
    let mut grown = current.clone();
    grown.extend_from_within(first_frame..first_frame + 7);
    fs::write(path.path(), &grown).unwrap();
    let pending = reader.refresh().unwrap();
    assert!(pending.incomplete_tail());
    assert_eq!(reader.blocks().len(), 3);

    acta::repair_incomplete_tail(path.path()).unwrap();
    let cleared = reader.refresh().unwrap();
    assert!(!cleared.incomplete_tail());
    assert_eq!(reader.blocks().len(), 3);

    append_blocks(path.path(), &schema, &[4, 5], options);
    let report = reader.refresh().unwrap();
    assert_eq!(report.blocks_added(), 2);
    assert_eq!(reader.blocks()[3].sequence(), 4);
    assert_eq!(reader.blocks()[3].base_row_id(), Some(3));
    assert_eq!(reader.blocks()[4].sequence(), 5);
    assert_eq!(reader.blocks()[4].base_row_id(), Some(4));
    assert_eq!(reader.total_rows(), 5);
}

// ----------------------------------------------------- corruption and failures

#[test]
fn a_corrupt_newly_complete_frame_leaves_the_snapshot_unchanged() {
    let schema = int64_schema(15);
    let options = one_block_options();
    let path = TempPath::new("corrupt-new");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let base = fs::read(path.path()).unwrap();

    let mut bytes = with_appended_frame(&base, data_frame_offset(&base), 2);
    // A byte inside the appended frame's body: the prefix, header, and body
    // checksums all cover it, so any flip is corruption.
    let body = bytes.len() - 8;
    bytes[body] ^= 0xff;
    fs::write(path.path(), &bytes).unwrap();

    let before_blocks = reader.blocks().len();
    let before_size = reader.file_metadata().file_size();
    let error = reader.refresh().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Corruption);
    assert_eq!(reader.blocks().len(), before_blocks);
    assert_eq!(reader.file_metadata().file_size(), before_size);
    assert_eq!(reader.total_rows(), 1);
}

#[test]
fn sequence_schema_and_row_id_failures_leave_the_snapshot_unchanged() {
    // Each case opens a reader over the good one-block base, appends a bad
    // frame to the file, and refreshes: the open must still succeed (the base
    // is valid), and only the refresh discovers the damaged frame.

    // A wrong sequence number is caught at the prefix, before the body.
    let sequence_path = TempPath::new("bad-sequence");
    let sequence_base = build_file(
        0,
        &schema_header(1, 1, 0, 0),
        &[int64_column(1, "value")],
        &[block_header(1, 2, u64::MAX, 0)],
    );
    fs::write(sequence_path.path(), &sequence_base).unwrap();
    let mut reader = Reader::open(sequence_path.path()).unwrap();
    let mut sequence_bytes = sequence_base;
    sequence_bytes.extend_from_slice(&frame(
        DATA_FRAME_TYPE,
        99,
        &block_header(1, 2, u64::MAX, 0),
        &[],
    ));
    fs::write(sequence_path.path(), &sequence_bytes).unwrap();
    let error = reader.refresh().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Corruption, "{error}");
    assert!(
        error.message().contains("expected frame sequence 2"),
        "{error}"
    );
    assert_eq!(reader.blocks().len(), 1);

    // A wrong schema ID is caught when the block header is parsed.
    let schema_path = TempPath::new("bad-schema-id");
    let schema_base = build_file(
        0,
        &schema_header(1, 1, 0, 0),
        &[int64_column(1, "value")],
        &[block_header(1, 2, u64::MAX, 0)],
    );
    fs::write(schema_path.path(), &schema_base).unwrap();
    let mut reader = Reader::open(schema_path.path()).unwrap();
    let mut schema_bytes = schema_base;
    let mut bad_header = block_header(1, 2, u64::MAX, 0);
    common::put_u64(&mut bad_header, 0, 999);
    schema_bytes.extend_from_slice(&frame(DATA_FRAME_TYPE, 2, &bad_header, &[]));
    fs::write(schema_path.path(), &schema_bytes).unwrap();
    let error = reader.refresh().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Corruption, "{error}");
    assert!(error.message().contains("schema ID 999"), "{error}");
    assert_eq!(reader.blocks().len(), 1);

    // A wrong base row ID is caught against the row-ID chain.
    let row_id_path = TempPath::new("bad-row-id");
    let row_id_base = build_file(
        1,
        &schema_header(1, 1, 0, 0),
        &[int64_column(1, "value")],
        &[block_header(1, 2, 0, 1)],
    );
    fs::write(row_id_path.path(), &row_id_base).unwrap();
    let mut reader = Reader::open(row_id_path.path()).unwrap();
    let mut row_id_bytes = row_id_base;
    row_id_bytes.extend_from_slice(&frame(DATA_FRAME_TYPE, 2, &block_header(1, 2, 7, 1), &[]));
    fs::write(row_id_path.path(), &row_id_bytes).unwrap();
    let error = reader.refresh().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Corruption, "{error}");
    assert!(
        error.message().contains("does not match expected 2"),
        "{error}"
    );
    assert_eq!(reader.blocks().len(), 1);
}

#[test]
fn a_resource_limit_failure_leaves_the_snapshot_unchanged() {
    // A one-block limit lets the snapshot open with its single block, and the
    // refresh visitor rejects a second block before any metadata is staged.
    let mut bytes = build_file(
        0,
        &schema_header(1, 1, 0, 0),
        &[int64_column(1, "value")],
        &[block_header(1, 2, u64::MAX, 0)],
    );
    let limits = Limits::default().with_max_blocks(1);
    let path = TempPath::new("limits");
    fs::write(path.path(), &bytes).unwrap();
    let mut reader = Reader::open_with_limits(path.path(), limits).unwrap();

    bytes.extend_from_slice(&frame(
        DATA_FRAME_TYPE,
        2,
        &block_header(1, 2, u64::MAX, 0),
        &[],
    ));
    fs::write(path.path(), &bytes).unwrap();

    let before_blocks = reader.blocks().len();
    let error = reader.refresh().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ResourceLimit);
    assert_eq!(reader.blocks().len(), before_blocks);
    assert_eq!(reader.total_rows(), 2);
}

#[test]
fn committed_prefix_truncation_is_refused() {
    let schema = int64_schema(17);
    let options = one_block_options();
    let path = TempPath::new("truncate-committed");
    write_blocks(path.path(), &schema, &[1], options);
    append_blocks(path.path(), &schema, &[2], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let before_size = reader.file_metadata().file_size();
    assert_eq!(reader.blocks().len(), 2);

    let bytes = fs::read(path.path()).unwrap();
    let cut = data_frame_offset(&bytes) + frame_length(&bytes, data_frame_offset(&bytes)) / 2;
    fs::write(path.path(), &bytes[..cut]).unwrap();

    let error = reader.refresh().unwrap_err();
    // A committed-prefix truncation has its own kind, so a caller can tell it
    // from a real I/O failure without matching on the message.
    assert_eq!(error.kind(), ErrorKind::FileTruncated, "{error}");
    assert_eq!(reader.blocks().len(), 2);
    assert_eq!(reader.file_metadata().file_size(), before_size);

    // Emptying the file entirely is the same failure, not corruption.
    fs::write(path.path(), b"").unwrap();
    assert_eq!(
        reader.refresh().unwrap_err().kind(),
        ErrorKind::FileTruncated
    );
    assert_eq!(reader.blocks().len(), 2);
    assert_eq!(reader.file_metadata().file_size(), before_size);
}

// --------------------------------------------------------------- replacement

fn replace_with_blocks(
    path: &Path,
    label: &str,
    schema: &Schema,
    rows: &[i64],
    options: WriterOptions,
) {
    // Create the replacement while the current file is still linked. This
    // guarantees that the two files have distinct identities even on a file
    // system that immediately reuses an inode after unlinking.
    let replacement = TempPath::new(label);
    write_blocks(replacement.path(), schema, rows, options);
    fs::remove_file(path).unwrap();
    fs::rename(replacement.path(), path).unwrap();
}

#[test]
fn a_replaced_path_is_refused_regardless_of_the_replacement() {
    let schema = int64_schema(18);
    let options = one_block_options();
    let path = TempPath::new("replaced");
    write_blocks(path.path(), &schema, &[1], options);
    let original = fs::read(path.path()).unwrap();
    let mut reader = Reader::open(path.path()).unwrap();
    let before_size = reader.file_metadata().file_size();

    // Every replacement below is built beside the current file before being
    // moved onto its path, so file-system identity alone is enough to refuse
    // it. Replacements that keep the identity get their own test.
    //
    // (a) A different schema: same column count and type, different column
    // ID and name, so a content check alone could not distinguish it from the
    // original file; only the file identity can.
    let other_schema = Schema::new(
        999,
        vec![Column::new(2, "other", LogicalType::Int64, false)],
        None,
    );
    replace_with_blocks(
        path.path(),
        "different-schema-replacement",
        &other_schema,
        &[7, 8],
        options,
    );
    let error = reader.refresh().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::FileReplaced, "{error}");

    // (b) An identical schema, so the bytes could have passed a content check.
    replace_with_blocks(
        path.path(),
        "identical-schema-replacement",
        &schema,
        &[1, 2],
        options,
    );
    assert_eq!(
        reader.refresh().unwrap_err().kind(),
        ErrorKind::FileReplaced
    );

    // (c) A different valid file of exactly the same length.
    replace_with_blocks(
        path.path(),
        "same-length-replacement",
        &schema,
        &[3, 4],
        options,
    );
    let mut same_length = fs::read(path.path()).unwrap();
    let delta = same_length.len() as i64 - original.len() as i64;
    if delta > 0 {
        same_length.truncate(original.len());
    } else {
        same_length.extend(std::iter::repeat_n(0, (-delta) as usize));
    }
    assert_eq!(same_length.len(), original.len());
    fs::write(path.path(), &same_length).unwrap();
    assert_eq!(
        reader.refresh().unwrap_err().kind(),
        ErrorKind::FileReplaced
    );

    // (d) Another valid Acta file entirely.
    let replacement = common::reference_fixture();
    fs::remove_file(path.path()).unwrap();
    fs::write(path.path(), &replacement).unwrap();
    assert_eq!(
        reader.refresh().unwrap_err().kind(),
        ErrorKind::FileReplaced
    );

    // The snapshot never changed on any of the attempts.
    assert_eq!(reader.blocks().len(), 1);
    assert_eq!(reader.file_metadata().file_size(), before_size);
}

/// A truncate-and-rewrite keeps the path's inode, so file-system identity
/// cannot see it. The last committed frame's commit trailer can, and must.
#[test]
fn an_in_place_rewrite_that_keeps_the_file_identity_is_refused() {
    let schema = int64_schema(30);
    let options = one_block_options();
    let path = TempPath::new("in-place");
    let other = TempPath::new("in-place-other");
    write_blocks(path.path(), &schema, &[1, 2], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let before_size = reader.file_metadata().file_size();
    let before_rows: Vec<i64> = reader
        .scan()
        .flat_map(|batch| rows_of(&batch.unwrap()))
        .collect();
    assert_eq!(before_rows, [1, 2]);

    // (a) A longer file: the replacement's own third frame sits exactly at
    // this snapshot's committed boundary and carries the sequence number the
    // walk expects, so only the committed bytes themselves separate them.
    write_blocks(other.path(), &schema, &[7, 8, 9], options);
    // `fs::write` truncates in place rather than unlinking, so the inode,
    // device, schema, prologue, and file ID are all unchanged.
    let identity_before = file_identity(path.path());
    fs::write(path.path(), fs::read(other.path()).unwrap()).unwrap();
    assert_eq!(file_identity(path.path()), identity_before);
    let error = reader.refresh().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::FileReplaced, "{error}");

    // (b) A file of exactly the same length, so the refresh takes its
    // no-growth path and must still refuse rather than report success.
    let same_length = TempPath::new("in-place-same-length");
    write_blocks(same_length.path(), &schema, &[8, 9], options);
    fs::write(path.path(), fs::read(same_length.path()).unwrap()).unwrap();
    assert_eq!(
        fs::metadata(path.path()).unwrap().len(),
        before_size,
        "the replacement must be the same length for this case to mean anything"
    );
    assert_eq!(
        reader.refresh().unwrap_err().kind(),
        ErrorKind::FileReplaced
    );

    // (c) A different schema of the same shape. Refusing here keeps the
    // reader's own schema and its reported schema ID from disagreeing.
    let other_schema = Schema::new(
        999,
        vec![Column::new(1, "value", LogicalType::Int64, false)],
        None,
    );
    let other_schema_file = TempPath::new("in-place-schema");
    write_blocks(other_schema_file.path(), &other_schema, &[1, 2, 3], options);
    fs::write(path.path(), fs::read(other_schema_file.path()).unwrap()).unwrap();
    assert_eq!(
        reader.refresh().unwrap_err().kind(),
        ErrorKind::FileReplaced
    );
    assert_eq!(reader.schema().schema_id(), 30);
    assert_eq!(reader.file_metadata().schema_id(), 30);

    // No attempt moved the snapshot, and the reader still follows its own
    // bytes once they are back: this refuses replacement, not change.
    assert_eq!(reader.blocks().len(), 2);
    assert_eq!(reader.file_metadata().file_size(), before_size);
    let original = TempPath::new("in-place-original");
    write_blocks(original.path(), &schema, &[1, 2], options);
    fs::write(path.path(), fs::read(original.path()).unwrap()).unwrap();
    let report = reader.refresh().unwrap();
    assert_eq!(report.blocks_added(), 0);
    append_blocks(path.path(), &schema, &[3], options);
    assert_eq!(reader.refresh().unwrap().blocks_added(), 1);
    assert_eq!(reader.file_metadata().schema_id(), 30);
    assert_eq!(
        reader
            .scan()
            .flat_map(|batch| rows_of(&batch.unwrap()))
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
}

#[cfg(unix)]
fn file_identity(path: &Path) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;

    let metadata = fs::metadata(path).unwrap();
    (metadata.dev(), metadata.ino())
}

#[cfg(not(unix))]
fn file_identity(_path: &Path) -> (u64, u64) {
    // The assertion this feeds only has to hold where it can be observed; the
    // refusal it guards is checked on every target.
    (0, 0)
}

// ------------------------------------------------------------ snapshot safety

#[test]
fn refreshing_one_clone_does_not_mutate_another() {
    let schema = int64_schema(19);
    let options = one_block_options();
    let path = TempPath::new("clone");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let clone = reader.clone();
    let before_size = clone.file_metadata().file_size();

    append_blocks(path.path(), &schema, &[2], options);
    let report = reader.refresh().unwrap();
    assert_eq!(report.blocks_added(), 1);
    assert_eq!(reader.blocks().len(), 2);

    assert_eq!(clone.blocks().len(), 1);
    assert_eq!(clone.total_rows(), 1);
    assert_eq!(clone.file_metadata().file_size(), before_size);
}

#[test]
fn old_snapshot_scans_remain_isolated_from_later_refreshes() {
    let schema = int64_schema(20);
    let options = one_block_options();
    let path = TempPath::new("isolated");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();

    // Collecting consumes the scan, so its borrow of the reader ends before
    // the refresh below can take the mutable borrow.
    let old_rows: Vec<i64> = reader
        .scan()
        .flat_map(|item| rows_of(&item.unwrap()))
        .collect();
    assert_eq!(old_rows, [1]);

    append_blocks(path.path(), &schema, &[2, 3], options);
    reader.refresh().unwrap();
    assert_eq!(reader.blocks().len(), 3);

    let new_rows: Vec<i64> = reader
        .scan()
        .flat_map(|item| rows_of(&item.unwrap()))
        .collect();
    assert_eq!(new_rows, [1, 2, 3]);
}

// ------------------------------------------------- projection, ranges, counts

#[test]
fn projection_and_primary_ranges_span_pre_and_post_refresh_blocks() {
    let schema = time_schema();
    let options = WriterOptions::default().with_row_block_target(1);
    let path = TempPath::new("ranges");
    write_time_blocks(path.path(), &schema, &[1, 2, 3], options);
    let mut reader = Reader::open(path.path()).unwrap();

    let mut writer = Writer::open(path.path(), options).unwrap();
    writer.append(time_batch(&schema, &[4, 5])).unwrap();
    let _ = writer.finish().unwrap();
    reader.refresh().unwrap();

    let range = PrimaryRange::timestamp(2, 5);
    let batches: Vec<RecordBatch> = reader
        .scan()
        .project(["value"])
        .unwrap()
        .primary_range(range)
        .unwrap()
        .collect::<acta::Result<Vec<_>>>()
        .unwrap();
    let rows: Vec<i64> = batches.iter().flat_map(rows_of).collect();
    // The range spans a pre-refresh block (2, 3) and a post-refresh block
    // (4), so both must be present through one scan over the refreshed reader.
    assert_eq!(rows, [20, 30, 40]);
}

#[test]
fn zero_column_scans_retain_correct_row_counts_after_refresh() {
    let schema = int64_schema(21);
    let options = one_block_options();
    let path = TempPath::new("zero-column");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    append_blocks(path.path(), &schema, &[2, 3], options);
    reader.refresh().unwrap();

    let batches: Vec<RecordBatch> = reader
        .scan()
        .project(std::iter::empty::<&str>())
        .unwrap()
        .collect::<acta::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(batches.len(), 3);
    let total: usize = batches.iter().map(|batch| batch.row_count()).sum();
    assert_eq!(total, 3);
}

#[test]
fn structural_and_full_validation_agree_with_the_refreshed_snapshot() {
    let schema = int64_schema(22);
    let options = one_block_options();
    let path = TempPath::new("validation");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    append_blocks(path.path(), &schema, &[2], options);
    append_blocks(path.path(), &schema, &[3], options);
    reader.refresh().unwrap();

    let structural = acta::validate(path.path()).unwrap();
    let full = acta::validate_with_options(
        path.path(),
        ValidationOptions::default().with_level(ValidationLevel::Full),
    )
    .unwrap();

    let metadata = reader.file_metadata();
    let expected_frames = reader.blocks().len() as u64 + 1;
    let expected_size = metadata.file_size();
    assert_eq!(structural.frame_count(), expected_frames);
    assert_eq!(full.frame_count(), expected_frames);
    assert_eq!(structural.file_size(), expected_size);
    assert_eq!(full.file_size(), expected_size);
    assert!(!structural.incomplete_tail());
    assert!(!full.incomplete_tail());
    assert_eq!(reader.blocks().len(), 3);
}
