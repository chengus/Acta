//! Stage 9 tail coverage.
//!
//! A tail is a synchronous, non-blocking poll over a reader: it yields each
//! newly committed matching block exactly once in file order, returns
//! [`Ok(None)`] when nothing is committed yet without ever fusing or ending,
//! never exposes a partial frame, shares Scan's projection/range/filter and
//! per-block decode-error semantics, and can be cancelled by being dropped.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use acta::{
    Array, Column, ErrorKind, LogicalType, PrimaryRange, PrimitiveArray, Reader, RecordBatch,
    Schema, TimeUnit, TimeZone, TimestampArray, Writer, WriterOptions,
};
use common::{
    BLOCK_STREAM_TABLE_OFFSET, PREFIX_SIZE, STREAM_PAYLOAD_OFFSET, data_frame_offset, frame_end,
    payload_offset, read_u32, read_u64, repair_frame,
};

struct TempPath(PathBuf);

impl TempPath {
    fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(std::env::temp_dir().join(format!(
            "acta-tail-{label}-{}-{}.acta",
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

/// Create a file whose rows land in one block per row.
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
        70,
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

fn write_time_blocks(path: &Path, schema: &Schema, timestamps: &[i64], options: WriterOptions) {
    let mut writer = Writer::create(path, schema.clone(), options).unwrap();
    writer.append(time_batch(schema, timestamps)).unwrap();
    let _ = writer.finish().unwrap();
}

fn append_time_blocks(path: &Path, schema: &Schema, timestamps: &[i64], options: WriterOptions) {
    let mut writer = Writer::open(path, options).unwrap();
    writer.append(time_batch(schema, timestamps)).unwrap();
    let _ = writer.finish().unwrap();
}

// --------------------------------------------------------------------- pending

#[test]
fn a_tail_initially_polls_pending() {
    let schema = int64_schema(1);
    let path = TempPath::new("initially-pending");
    write_blocks(path.path(), &schema, &[1], WriterOptions::default());
    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader.tail();

    // The snapshot already holds the committed block; the tail starts after
    // it and has nothing new to return.
    assert_eq!(tail.poll_next().unwrap(), None);
    assert_eq!(tail.poll_next().unwrap(), None);
}

#[test]
fn one_committed_append_becomes_one_batch() {
    let schema = int64_schema(2);
    let options = one_block_options();
    let path = TempPath::new("one-batch");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader.tail();

    append_blocks(path.path(), &schema, &[2], options);
    let batch = tail.poll_next().unwrap().expect("one committed block");
    assert_eq!(rows_of(&batch), [2]);
    assert_eq!(tail.poll_next().unwrap(), None);
}

#[test]
fn several_committed_frames_are_returned_once_and_in_file_order() {
    let schema = int64_schema(3);
    let options = one_block_options();
    let path = TempPath::new("several-frames");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader.tail();

    append_blocks(path.path(), &schema, &[2], options);
    append_blocks(path.path(), &schema, &[3], options);
    append_blocks(path.path(), &schema, &[4], options);

    let mut values = Vec::new();
    while let Some(batch) = tail.poll_next().unwrap() {
        values.extend(rows_of(&batch));
    }
    assert_eq!(values, [2, 3, 4]);
    // Everything was drained, so the next poll only refreshes and finds
    // nothing new: no block is duplicated by repeated pending polls.
    assert_eq!(tail.poll_next().unwrap(), None);
    assert_eq!(tail.poll_next().unwrap(), None);
}

#[test]
fn repeated_pending_polls_duplicate_nothing() {
    let schema = int64_schema(4);
    let options = one_block_options();
    let path = TempPath::new("pending-repeat");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader.tail();

    for _ in 0..3 {
        assert_eq!(tail.poll_next().unwrap(), None);
    }
    append_blocks(path.path(), &schema, &[2], options);
    let batch = tail.poll_next().unwrap().expect("one new block");
    assert_eq!(rows_of(&batch), [2]);
    for _ in 0..3 {
        assert_eq!(tail.poll_next().unwrap(), None);
    }
}

// ---------------------------------------------------------- buffered and cuts

#[test]
fn buffered_unpublished_writer_rows_remain_pending() {
    let schema = int64_schema(5);
    let path = TempPath::new("tail-buffered");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer.append(int64_batch(&schema, vec![1, 2, 3])).unwrap();

    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader.tail();
    assert_eq!(tail.poll_next().unwrap(), None);
    assert_eq!(tail.poll_next().unwrap(), None);

    drop(writer);
}

#[test]
fn a_partial_frame_remains_pending_and_is_never_exposed() {
    let schema = int64_schema(6);
    let options = one_block_options();
    let path = TempPath::new("partial");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader.tail();
    let base = fs::read(path.path()).unwrap();

    // A cut of one appended frame's prefix: the frame is incomplete and must
    // never surface as a batch, no matter how many times it is polled.
    let mut partial = base.clone();
    partial.extend_from_within(data_frame_offset(&base)..data_frame_offset(&base) + 7);
    fs::write(path.path(), &partial).unwrap();
    for _ in 0..3 {
        assert_eq!(tail.poll_next().unwrap(), None);
    }
    assert_eq!(reader.blocks().len(), 1);
}

#[test]
fn completing_the_frame_makes_it_available() {
    let schema = int64_schema(7);
    let options = one_block_options();
    let path = TempPath::new("completes");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader.tail();

    // Interrupt an append mid-prefix, then complete it with a real append.
    let base = fs::read(path.path()).unwrap();
    let mut partial = base.clone();
    partial.extend_from_within(data_frame_offset(&base)..data_frame_offset(&base) + 7);
    fs::write(path.path(), &partial).unwrap();
    assert_eq!(tail.poll_next().unwrap(), None);

    // Complete the interrupted frame by writing the full frame bytes; the
    // writer rightly refuses to append over an unrepaired tail.
    fs::write(
        path.path(),
        common::with_appended_frame(&base, data_frame_offset(&base), 2),
    )
    .unwrap();
    let batch = tail.poll_next().unwrap().expect("the completed frame");
    // The completed frame is a copy of the first block, so it carries [1].
    assert_eq!(rows_of(&batch), [1]);
    assert_eq!(tail.poll_next().unwrap(), None);
}

// --------------------------------------------------------------- cancellation

#[test]
fn dropping_a_tail_performs_no_blocking_work() {
    let schema = int64_schema(8);
    let path = TempPath::new("drop");
    write_blocks(path.path(), &schema, &[1], WriterOptions::default());
    let mut reader = Reader::open(path.path()).unwrap();

    // Tail has no thread, no timer, and no handle of its own to clean up;
    // dropping it just releases the borrow and returns immediately.
    let mut tail = reader.tail();
    let _ = tail.poll_next();
    drop(tail);

    // The reader is usable again after the tail is dropped.
    let mut tail = reader.tail();
    append_blocks(path.path(), &schema, &[2], one_block_options());
    assert_eq!(
        tail.poll_next().unwrap().map(|batch| rows_of(&batch)),
        Some(vec![2])
    );
}

// ------------------------------------------------------------ projection ranges

#[test]
fn projection_reorder_and_empty_projection_match_scan() {
    let schema = Schema::new(
        80,
        vec![
            Column::new(1, "a", LogicalType::Int64, false),
            Column::new(2, "b", LogicalType::Utf8, false),
            Column::new(3, "c", LogicalType::Int64, false),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Int64(PrimitiveArray::new(vec![1], None)),
            Array::Utf8(acta::Utf8Array::new(vec!["x".to_owned()], None)),
            Array::Int64(PrimitiveArray::new(vec![3], None)),
        ],
        1,
    )
    .unwrap();
    let path = TempPath::new("projection");
    let mut writer = Writer::create(path.path(), schema.clone(), WriterOptions::default()).unwrap();
    writer.append(batch).unwrap();
    let _ = writer.finish().unwrap();

    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader.tail().project(["c", "a"]).unwrap();
    append_blocks_wide(path.path(), &schema, one_block_options());

    let batch = tail.poll_next().unwrap().expect("one new block");
    assert_eq!(batch.schema().column_count(), 2);
    assert_eq!(batch.schema().columns()[0].name(), "c");
    assert_eq!(batch.schema().columns()[1].name(), "a");

    // A second append becomes one block for the empty-projection tail, which
    // starts after the reader's now-two-block snapshot.
    append_blocks_wide(path.path(), &schema, one_block_options());
    let mut tail = reader.tail().project(std::iter::empty::<&str>()).unwrap();
    let batch = tail.poll_next().unwrap().expect("one new block");
    assert_eq!(batch.schema().column_count(), 0);
    assert_eq!(batch.row_count(), 1);
}

/// Append one block of the wide schema from the projection test.
fn append_blocks_wide(path: &Path, schema: &Schema, options: WriterOptions) {
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Int64(PrimitiveArray::new(vec![9], None)),
            Array::Utf8(acta::Utf8Array::new(vec!["y".to_owned()], None)),
            Array::Int64(PrimitiveArray::new(vec![11], None)),
        ],
        1,
    )
    .unwrap();
    let mut writer = Writer::open(path, options).unwrap();
    writer.append(batch).unwrap();
    let _ = writer.finish().unwrap();
}

#[test]
fn timestamp_and_date32_ranges_filter_tail_blocks() {
    let schema = time_schema();
    let options = WriterOptions::default().with_row_block_target(1);
    let path = TempPath::new("timestamp-range");
    write_time_blocks(path.path(), &schema, &[1, 2, 3], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader
        .tail()
        .primary_range(PrimaryRange::timestamp(2, 4))
        .unwrap();

    append_time_blocks(path.path(), &schema, &[2, 5], options);
    append_time_blocks(path.path(), &schema, &[3], options);

    // Three new blocks (2, 5, and 3): the first and third match the range,
    // the second prunes. Each is polled exactly once, so the call sequence is
    // one match, one pruned None, one match.
    let mut values = Vec::new();
    for _ in 0..3 {
        let Some(batch) = tail.poll_next().unwrap() else {
            continue;
        };
        // The `value` column is timestamps * 10; this tail projects nothing,
        // so read the timestamp column instead.
        let timestamp = match &batch.columns()[0] {
            Array::Timestamp(array) => (0..batch.row_count())
                .map(|row| array.values()[row])
                .collect::<Vec<i64>>(),
            other => panic!("unexpected column {other:?}"),
        };
        values.extend(timestamp);
    }
    assert_eq!(values, [2, 3]);

    // date32 range over a fresh file.
    let date_schema = Schema::new(
        81,
        vec![Column::new(1, "day", LogicalType::Date32, false)],
        Some(1),
    );
    let date_path = TempPath::new("date-range");
    let mut writer = Writer::create(
        date_path.path(),
        date_schema.clone(),
        WriterOptions::default(),
    )
    .unwrap();
    writer
        .append(
            RecordBatch::try_new(
                Arc::new(date_schema.clone()),
                vec![Array::Date32(PrimitiveArray::new(vec![0, 1], None))],
                2,
            )
            .unwrap(),
        )
        .unwrap();
    let _ = writer.finish().unwrap();
    let mut reader = Reader::open(date_path.path()).unwrap();
    let mut tail = reader
        .tail()
        .primary_range(PrimaryRange::date32(0, 1))
        .unwrap();
    let mut writer = Writer::open(date_path.path(), WriterOptions::default()).unwrap();
    writer
        .append(
            RecordBatch::try_new(
                Arc::new(date_schema),
                vec![Array::Date32(PrimitiveArray::new(vec![-1, 0, 1, 2], None))],
                4,
            )
            .unwrap(),
        )
        .unwrap();
    let _ = writer.finish().unwrap();

    let batch = tail.poll_next().unwrap().expect("one matching block");
    match &batch.columns()[0] {
        Array::Date32(array) => assert_eq!(
            (0..batch.row_count())
                .map(|row| array.values()[row])
                .collect::<Vec<_>>(),
            [0]
        ),
        other => panic!("unexpected column {other:?}"),
    }
    assert_eq!(tail.poll_next().unwrap(), None);
}

#[test]
fn sorted_and_unsorted_primary_filtering_match_scan() {
    let schema = time_schema();
    let options = WriterOptions::default().with_row_block_target(1);
    let path = TempPath::new("sorted");
    write_time_blocks(path.path(), &schema, &[1, 2, 3], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader
        .tail()
        .primary_range(PrimaryRange::timestamp(1, 3))
        .unwrap();

    // A sorted block and an unsorted block both append; the range keeps the
    // sorted rows and the in-range unsorted rows in block order. The default
    // block target keeps each append to one block.
    append_time_blocks(path.path(), &schema, &[1, 2], WriterOptions::default());
    append_time_blocks(path.path(), &schema, &[4, 2, 5], WriterOptions::default());

    let mut values = Vec::new();
    for _ in 0..2 {
        let batch = tail.poll_next().unwrap().expect("one new block");
        let timestamp = match &batch.columns()[0] {
            Array::Timestamp(array) => (0..batch.row_count())
                .map(|row| array.values()[row])
                .collect::<Vec<i64>>(),
            other => panic!("unexpected column {other:?}"),
        };
        values.extend(timestamp);
    }
    assert_eq!(values, [1, 2, 2]);
}

#[test]
fn pruned_or_nonmatching_blocks_yield_no_empty_batch() {
    let schema = time_schema();
    let options = WriterOptions::default().with_row_block_target(1);
    let path = TempPath::new("pruned");
    write_time_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    // The range [100, 101) matches nothing that will ever be appended.
    let mut tail = reader
        .tail()
        .primary_range(PrimaryRange::timestamp(100, 101))
        .unwrap();

    append_time_blocks(path.path(), &schema, &[200, 300], options);
    // Every new block prunes on its bounds; poll through them without ever
    // returning an empty batch, and without exhausting the tail.
    assert_eq!(tail.poll_next().unwrap(), None);
    assert_eq!(tail.poll_next().unwrap(), None);
    assert_eq!(reader.blocks().len(), 3);
}

/// A poll that reaches a pruned block must keep going to the block behind it.
/// Returning `None` there would tell the caller to wait for data already on
/// disk, and would silently truncate any loop that treats `None` as the end.
#[test]
fn a_pruned_block_does_not_hide_the_matching_block_behind_it() {
    let schema = time_schema();
    let options = WriterOptions::default().with_row_block_target(1);
    let path = TempPath::new("prune-then-match");
    write_time_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let range = PrimaryRange::timestamp(150, 250);
    let mut tail = reader.tail().primary_range(range).unwrap();

    // 100 prunes on its bounds, 200 matches, 300 prunes: the matching block is
    // reachable only if the poll drains the pruned block ahead of it.
    append_time_blocks(path.path(), &schema, &[100, 200, 300], options);
    let batch = tail
        .poll_next()
        .unwrap()
        .expect("the pruned block must not hide the matching one");
    assert_eq!(timestamps_of(&batch), [200]);
    assert_eq!(tail.poll_next().unwrap(), None);

    // The same blocks through an ordinary snapshot scan, for comparison.
    let snapshot = Reader::open(path.path()).unwrap();
    let scanned: Vec<Vec<i64>> = snapshot
        .scan()
        .primary_range(range)
        .unwrap()
        .map(|batch| timestamps_of(&batch.unwrap()))
        .collect();
    assert_eq!(scanned, [vec![200]]);
}

/// The same shape, for a block that survives pruning but filters down to no
/// rows: the block is consumed by the poll that reaches it, not reported.
#[test]
fn a_block_filtered_to_no_rows_does_not_hide_the_block_behind_it() {
    let schema = time_schema();
    let path = TempPath::new("filter-then-match");
    write_time_blocks(
        path.path(),
        &schema,
        &[1],
        WriterOptions::default().with_row_block_target(1),
    );
    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader
        .tail()
        .primary_range(PrimaryRange::timestamp(150, 250))
        .unwrap();

    // One block spanning [100, 300] overlaps the range, so it is not pruned,
    // but holds no row inside it; the block after it does.
    let options = WriterOptions::default().with_row_block_target(2);
    append_time_blocks(path.path(), &schema, &[100, 300, 200, 210], options);
    let batch = tail
        .poll_next()
        .unwrap()
        .expect("the emptied block must not hide the matching one");
    assert_eq!(timestamps_of(&batch), [200, 210]);
    assert_eq!(tail.poll_next().unwrap(), None);
}

fn timestamps_of(batch: &RecordBatch) -> Vec<i64> {
    match &batch.columns()[0] {
        Array::Timestamp(array) => array.values().to_vec(),
        other => panic!("unexpected column {other:?}"),
    }
}

// ------------------------------------------------------------------- metrics

#[test]
fn tail_metrics_and_finite_cumulative_limits_apply_across_the_lifetime() {
    let schema = int64_schema(12);
    let options = one_block_options();
    let path = TempPath::new("metrics");
    write_blocks(path.path(), &schema, &[1, 2, 3], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader.tail();

    append_blocks(path.path(), &schema, &[4], options);
    append_blocks(path.path(), &schema, &[5], options);
    while tail.poll_next().unwrap().is_some() {}

    let metrics = tail.metrics();
    assert_eq!(metrics.rows_returned(), 2);
    assert_eq!(metrics.blocks_considered(), 2);

    // A tail's byte count covers discovery as well as decoding. Refresh
    // streams every newly committed frame once to check its commit trailer,
    // and the decode then reads that frame again, so a tail over two blocks
    // reads two blocks' worth of decoding plus both frames.
    let tailed = 2_u64;
    let discovered: u64 = reader.blocks()[3..]
        .iter()
        .map(|block| block.total_length())
        .sum();
    let snapshot = Reader::open(path.path()).unwrap();
    let block_count = snapshot.blocks().len() as u64;
    let mut scan = snapshot.scan();
    while scan.next().is_some() {}
    let scan_bytes = scan.metrics().bytes_read();
    // Every block here holds one row of one fixed-width column, so a scan's
    // cost divides evenly and gives the per-block decode cost exactly.
    assert_eq!(scan_bytes % block_count, 0);
    assert_eq!(
        metrics.bytes_read(),
        tailed * (scan_bytes / block_count) + discovered,
        "a tail's bytes are its decodes plus the frames its refreshes read"
    );

    // A finite cumulative row allowance is shared across every poll. The
    // first appended block fits; the second exhausts the one-row allowance.
    let limits = acta::Limits::default().with_max_rows_per_scan(1);
    let path = TempPath::new("finite-limit");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open_with_limits(path.path(), limits).unwrap();
    let mut tail = reader.tail();
    append_blocks(path.path(), &schema, &[3], options);
    let batch = tail.poll_next().unwrap().expect("the first block fits");
    assert_eq!(rows_of(&batch), [3]);
    append_blocks(path.path(), &schema, &[4, 5], options);
    let error = tail.poll_next().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ResourceLimit);
    // The allowance stays spent, so the next poll fails the next block too.
    assert_eq!(
        tail.poll_next().unwrap_err().kind(),
        ErrorKind::ResourceLimit
    );
}

// ------------------------------------------------------------ decode errors

#[test]
fn a_decode_error_matches_scan_and_consumes_the_failed_block() {
    let schema = int64_schema(13);
    let options = one_block_options();
    let path = TempPath::new("decode-error");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader.tail();

    // Two newly committed blocks follow the snapshot: [2] and [3].
    append_blocks(path.path(), &schema, &[2], options);
    append_blocks(path.path(), &schema, &[3], options);

    // Damage the stored bytes of the [2] block's value stream. Every frame
    // checksum is repaired, so only that stream's own CRC disagrees and the
    // corruption surfaces on decode, not on refresh.
    let mut bytes = fs::read(path.path()).unwrap();
    corrupt_stream_payload(&mut bytes, 1);
    fs::write(path.path(), &bytes).unwrap();

    // A snapshot scan of a fresh reader over the same bytes reports the error
    // shape the tail must match: the damaged block is one failed item, and
    // the block after it still decodes. The fresh reader is independent of
    // the tailed reader, which the tail borrows mutably.
    let fresh = Reader::open(path.path()).unwrap();
    let scan_error = fresh
        .scan()
        .nth(1)
        .unwrap()
        .expect_err("the stream CRC disagrees");
    assert_eq!(scan_error.kind(), ErrorKind::Corruption);
    let decoded_after = fresh
        .scan()
        .nth(2)
        .unwrap()
        .expect("the next block decodes");
    assert_eq!(rows_of(&decoded_after), [3]);

    // The tail discovers the same two blocks and yields the same error for
    // the damaged one, then the next block, one batch, on the following poll.
    let tail_error = tail.poll_next().unwrap_err();
    assert_eq!(tail_error.kind(), ErrorKind::Corruption);
    let batch = tail.poll_next().unwrap().expect("the next block decodes");
    assert_eq!(rows_of(&batch), [3]);
}

/// Flip a byte inside block `index`'s first stream payload, leaving every
/// frame checksum correct so only that stream's own CRC disagrees.
fn corrupt_stream_payload(bytes: &mut [u8], index: usize) {
    let frame = block_frame(bytes, index);
    let header = frame + PREFIX_SIZE;
    let table = read_u32(bytes, header + BLOCK_STREAM_TABLE_OFFSET) as usize;
    let descriptor = header + table;
    let offset = read_u64(bytes, descriptor + STREAM_PAYLOAD_OFFSET) as usize;
    let target = payload_offset(bytes, frame) + offset;
    bytes[target] ^= 0xff;
    repair_frame(bytes, frame);
}

fn block_frame(bytes: &[u8], index: usize) -> usize {
    let mut frame = data_frame_offset(bytes);
    for _ in 0..index {
        frame = frame_end(bytes, frame);
    }
    frame
}

// ------------------------------------------------- lifecycle across a repair

#[test]
fn tail_spans_interrupted_append_explicit_repair_writer_reopen_and_append() {
    let schema = int64_schema(14);
    let options = one_block_options();
    let path = TempPath::new("repair-lifecycle");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader.tail();
    let base = fs::read(path.path()).unwrap();

    // Interrupted append: a cut frame leaves the tail pending.
    let mut partial = base.clone();
    partial.extend_from_within(data_frame_offset(&base)..data_frame_offset(&base) + 7);
    fs::write(path.path(), &partial).unwrap();
    assert_eq!(tail.poll_next().unwrap(), None);

    // Explicit repair removes the tail; the tail accepts the result. The
    // reader snapshot is still its single committed block.
    acta::repair_incomplete_tail(path.path()).unwrap();
    assert_eq!(tail.poll_next().unwrap(), None);

    // A reopened writer continues with the correct sequence.
    let mut writer = Writer::open(path.path(), options).unwrap();
    writer.append(int64_batch(&schema, vec![2])).unwrap();
    let _ = writer.finish().unwrap();
    let batch = tail.poll_next().unwrap().expect("appended after repair");
    assert_eq!(rows_of(&batch), [2]);
    assert_eq!(tail.poll_next().unwrap(), None);
}

// -------------------------------------------------------------- multi-process

/// The appending helper is a bin target behind `test-fixtures`, so that
/// `cargo install` never puts a test fixture on a user's PATH. CI enables the
/// feature; a plain `cargo test` skips this one case rather than failing to
/// compile against a binary that was not built.
#[cfg(feature = "test-fixtures")]
#[test]
fn tail_follows_a_writer_running_in_another_process() {
    use std::process::Command;

    let schema = int64_schema(15);
    let options = one_block_options();
    let path = TempPath::new("multiprocess");
    write_blocks(path.path(), &schema, &[1], options);
    let mut reader = Reader::open(path.path()).unwrap();
    let mut tail = reader.tail();
    assert_eq!(tail.poll_next().unwrap(), None);

    let status = Command::new(env!("CARGO_BIN_EXE_acta_append"))
        .arg(path.path())
        .arg("3")
        .arg("100")
        .status()
        .expect("spawn the appending helper");
    assert!(status.success(), "the appending helper failed: {status:?}");

    let batch = tail
        .poll_next()
        .unwrap()
        .expect("the other process appended");
    assert_eq!(rows_of(&batch), [100, 101, 102]);
    assert_eq!(tail.poll_next().unwrap(), None);
}
