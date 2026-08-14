//! Metadata-first reader coverage.

mod common;

use std::fs::OpenOptions;
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};

use acta::{Array, ErrorKind, Limits, LogicalType, Reader, TimeUnit, TimeZone};
use common::{
    CHECKPOINT_FRAME_TYPE, FIXTURES, PREFIX_CRC, PREFIX_FRAME_TYPE, PREFIX_PAYLOAD_LENGTH,
    PREFIX_SIZE, PROLOGUE_FEATURE_FLAGS, PROLOGUE_FORMAT_MAJOR, PROLOGUE_SIZE, TRAILER_BODY_CRC,
    TRAILER_COMMIT_MAGIC, TRAILER_CRC, TRAILER_SEQUENCE, TRAILER_TOTAL_LENGTH, TemporaryFile,
    assert_reported, data_frame_offset, fixture, frame_length, payload_offset, put_u16, put_u32,
    put_u64, reference_fixture, repair_frame, repair_prefix, repair_prologue, simple_file,
    trailer_offset,
};

#[test]
fn every_compatibility_fixture_opens_and_exposes_schema_metadata() {
    for &(relative_path, _) in FIXTURES {
        let file = TemporaryFile::new("reader-fixture", &fixture(relative_path));
        let reader = Reader::open(file.path())
            .unwrap_or_else(|error| panic!("{relative_path} failed to open: {error}"));

        assert_eq!(reader.schema().schema_id(), 1, "{relative_path}");
        assert_eq!(
            reader.schema().column_count(),
            reader.schema().columns().len()
        );
        assert_eq!(reader.blocks().len(), 1, "{relative_path}");
        assert_eq!(reader.file_metadata().block_count(), 1, "{relative_path}");
        assert_eq!(reader.total_rows(), reader.file_metadata().total_rows());
        assert!(!reader.file_metadata().incomplete_tail(), "{relative_path}");
    }
}

#[test]
fn schema_only_file_is_a_complete_reader_snapshot() {
    let bytes = reference_fixture();
    let schema_end = data_frame_offset(&bytes);
    let file = TemporaryFile::new("reader-schema-only", &bytes[..schema_end]);

    let reader = Reader::open(file.path()).expect("schema-only file should open");

    assert_eq!(reader.schema().column_count(), 1);
    assert!(reader.blocks().is_empty());
    assert_eq!(reader.total_rows(), 0);
    assert!(!reader.file_metadata().incomplete_tail());
    assert_eq!(reader.file_metadata().last_good_offset(), schema_end as u64);
}

#[test]
fn scan_decodes_one_block() {
    let file = TemporaryFile::new("reader-scan-one-block", &reference_fixture());
    let reader = Reader::open(file.path()).expect("fixture should open");
    let mut scan = reader.scan();

    assert_eq!(scan.remaining_candidate_blocks(), 1);
    let batch = scan
        .next()
        .expect("the scan should yield one block")
        .unwrap();
    assert_eq!(batch.row_count(), 3);
    assert!(scan.next().is_none());
}

#[test]
fn scan_yields_multiple_blocks_in_file_order() {
    let file = TemporaryFile::new("reader-scan-order", &multi_block_int64_file());
    let reader = Reader::open(file.path()).expect("multi-block file should open");
    let batches = reader
        .scan()
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("every block should decode");

    let values: Vec<Vec<i64>> = batches.iter().map(int64_values).collect();
    assert_eq!(values, vec![vec![10, 20], vec![30, 40], vec![50, 60]]);
}

#[test]
fn scan_of_a_schema_only_file_is_empty() {
    let bytes = reference_fixture();
    let file = TemporaryFile::new(
        "reader-scan-schema-only",
        &bytes[..data_frame_offset(&bytes)],
    );
    let reader = Reader::open(file.path()).expect("schema-only file should open");

    assert_eq!(reader.scan().count(), 0);
}

#[test]
fn scan_surfaces_decode_errors_during_iteration() {
    let file = TemporaryFile::new("reader-scan-error", &undecodable_second_block());
    let reader = Reader::open(file.path()).expect("metadata should still be readable");
    let mut scan = reader.scan();

    scan.next()
        .expect("the first block should be yielded")
        .expect("the first block should decode");
    let error = scan
        .next()
        .expect("the second block should be attempted")
        .expect_err("the damaged block should fail during iteration");

    assert_eq!(error.kind(), ErrorKind::Corruption);
    assert!(scan.next().is_none());
}

#[test]
fn scan_is_isolated_from_later_appends() {
    let original = reference_fixture();
    let data_offset = data_frame_offset(&original);
    let file = TemporaryFile::new("reader-scan-snapshot", &original);
    let reader = Reader::open(file.path()).expect("initial snapshot should open");
    let appended = common::with_appended_frame(&original, data_offset, 2);

    let mut output = OpenOptions::new()
        .append(true)
        .open(file.path())
        .expect("open file for append");
    output
        .write_all(&appended[original.len()..])
        .expect("append complete frame");
    output.flush().expect("flush append");

    assert_eq!(reader.scan().count(), 1);
    assert_eq!(Reader::open(file.path()).unwrap().scan().count(), 2);
}

#[test]
fn one_block_metadata_has_the_expected_offset_length_and_sequence() {
    let bytes = reference_fixture();
    let data_offset = data_frame_offset(&bytes);
    let file = TemporaryFile::new("reader-one-block", &bytes);
    let reader = Reader::open(file.path()).expect("fixture should open");
    let block = &reader.blocks()[0];

    assert_eq!(block.sequence(), 1);
    assert_eq!(block.file_offset(), data_offset as u64);
    assert_eq!(
        block.total_length(),
        frame_length(&bytes, data_offset) as u64
    );
    assert_eq!(block.row_count(), 3);
}

#[test]
fn multiple_blocks_aggregate_rows_and_remain_in_sequence_order() {
    let first = reference_fixture();
    let data_offset = data_frame_offset(&first);
    let second = common::with_appended_frame(&first, data_offset, 2);
    let bytes = common::with_appended_frame(&second, data_offset, 3);
    let file = TemporaryFile::new("reader-multiple-blocks", &bytes);
    let reader = Reader::open(file.path()).expect("multi-block file should open");

    assert_eq!(reader.blocks().len(), 3);
    assert_eq!(reader.total_rows(), 9);
    assert_eq!(reader.file_metadata().total_rows(), 9);
    for (index, block) in reader.blocks().iter().enumerate() {
        assert_eq!(block.sequence(), index as u64 + 1);
        assert_eq!(block.row_count(), 3);
    }
    assert_eq!(reader.blocks()[1].file_offset(), first.len() as u64);
    assert_eq!(reader.blocks()[2].file_offset(), second.len() as u64);
}

#[test]
fn primary_bounds_and_ts_sorted_are_exposed() {
    let bytes = fixture("ts_sorted/ts_sorted.acta");
    let file = TemporaryFile::new("reader-ts-sorted", &bytes);
    let reader = Reader::open(file.path()).expect("ts_sorted should open");
    let block = &reader.blocks()[0];
    let bounds = block.primary_bounds().expect("primary bounds should exist");

    assert_eq!((bounds.min(), bounds.max()), (1_000_000, 3_000_000));
    assert!(block.ts_sorted());
}

#[test]
fn date_primary_bounds_are_signed_day_counts() {
    let bytes = fixture("date32/date32.acta");
    let file = TemporaryFile::new("reader-date32", &bytes);
    let reader = Reader::open(file.path()).expect("date32 should open");
    let block = &reader.blocks()[0];
    let bounds = block.primary_bounds().expect("date bounds should exist");

    assert_eq!((bounds.min(), bounds.max()), (20_455, 20_459));
    assert!(block.ts_sorted());
}

#[test]
fn no_primary_schema_has_no_bounds_or_sortedness() {
    let bytes = fixture("no_primary/no_primary.acta");
    let file = TemporaryFile::new("reader-no-primary", &bytes);
    let reader = Reader::open(file.path()).expect("no-primary fixture should open");

    assert!(reader.schema().primary_column().is_none());
    assert!(reader.blocks()[0].primary_bounds().is_none());
    assert!(!reader.blocks()[0].ts_sorted());
}

#[test]
fn schema_types_are_reconstructed_without_decoding_values() {
    let bytes = reference_fixture();
    let file = TemporaryFile::new("reader-schema-types", &bytes);
    let reader = Reader::open(file.path()).expect("fixture should open");
    let column = &reader.schema().columns()[0];

    assert_eq!(column.id(), 1);
    assert_eq!(column.name(), "time");
    assert!(!column.is_nullable());
    assert_eq!(
        column.logical_type(),
        &LogicalType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: TimeZone::Utc,
        }
    );
}

/// The checked-in fixture is the authority on how a timezone name is stored.
/// Its parameter record is padded, so the stored type-parameter length is
/// `8 + 13` rounded up to 24 rather than 21.
#[test]
fn the_timezone_fixture_reconstructs_its_iana_name() {
    let bytes = fixture("timezone/timezone.acta");
    let file = TemporaryFile::new("reader-timezone", &bytes);
    let reader = Reader::open(file.path()).expect("timezone fixture should open");
    let primary = reader.schema().primary_column().expect("primary column");

    assert_eq!(primary.name(), "time");
    assert_eq!(
        primary.logical_type(),
        &LogicalType::Timestamp {
            unit: TimeUnit::Millisecond,
            timezone: TimeZone::Iana("Europe/Berlin".to_owned()),
        }
    );
    assert!(reader.blocks()[0].ts_sorted());
    assert!(reader.blocks()[0].primary_bounds().is_some());
}

#[test]
fn an_incomplete_final_frame_is_ignored_but_reported() {
    let complete = reference_fixture();
    let data_offset = data_frame_offset(&complete);
    let appended = common::with_appended_frame(&complete, data_offset, 2);
    let truncated = &appended[..appended.len() - 1];
    let file = TemporaryFile::new("reader-incomplete-tail", truncated);
    let reader = Reader::open(file.path()).expect("incomplete append should be ignored");

    assert_eq!(reader.blocks().len(), 1);
    assert_eq!(reader.blocks()[0].sequence(), 1);
    assert!(reader.file_metadata().incomplete_tail());
    assert_eq!(
        reader.file_metadata().last_good_offset(),
        complete.len() as u64
    );
}

#[test]
fn reader_snapshot_does_not_observe_a_later_append() {
    let original = reference_fixture();
    let data_offset = data_frame_offset(&original);
    let file = TemporaryFile::new("reader-snapshot", &original);
    let reader = Reader::open(file.path()).expect("initial snapshot should open");
    let appended = common::with_appended_frame(&original, data_offset, 2);

    let mut output = OpenOptions::new()
        .append(true)
        .open(file.path())
        .expect("open file for append");
    output
        .write_all(&appended[original.len()..])
        .expect("append complete frame");
    output.flush().expect("flush append");

    assert_eq!(reader.blocks().len(), 1);
    assert_eq!(reader.file_metadata().file_size(), original.len() as u64);
}

#[test]
fn corruption_in_a_complete_frame_is_rejected() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    bytes[frame + common::PREFIX_SIZE + 64] ^= 1;
    let file = TemporaryFile::new("reader-corruption", &bytes);

    let error = Reader::open(file.path()).expect_err("corrupt complete frame should fail");

    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn corruption_in_complete_block_metadata_is_rejected() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    let row_count = frame + common::PREFIX_SIZE + 16;
    put_u32(&mut bytes, row_count, 0);
    repair_frame(&mut bytes, frame);
    let file = TemporaryFile::new("reader-block-metadata-corruption", &bytes);

    let error = Reader::open(file.path()).expect_err("zero-row block should fail");

    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn unknown_format_version_remains_unsupported() {
    let mut bytes = reference_fixture();
    common::put_u16(&mut bytes, PROLOGUE_FORMAT_MAJOR, 1);
    repair_prologue(&mut bytes);
    let file = TemporaryFile::new("reader-unsupported-version", &bytes);

    let error = Reader::open(file.path()).expect_err("unknown version should fail");

    assert_reported(&error, ErrorKind::UnsupportedVersion, PROLOGUE_FORMAT_MAJOR);
}

#[test]
fn malformed_lengths_are_rejected_without_panicking() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    put_u64(&mut bytes, frame + PREFIX_PAYLOAD_LENGTH, u64::MAX - 7);
    repair_prefix(&mut bytes, frame);
    let file = TemporaryFile::new("reader-huge-length", &bytes);

    let result = catch_unwind(AssertUnwindSafe(|| Reader::open(file.path())));
    assert!(result.is_ok(), "malformed length caused a reader panic");
    let error = result
        .expect("panic was checked above")
        .expect_err("huge length should fail safely");

    assert_eq!(error.kind(), ErrorKind::ResourceLimit);
}

#[test]
fn a_truncated_prologue_is_not_presented_as_a_schema() {
    let bytes = reference_fixture();
    let file = TemporaryFile::new("reader-short-prologue", &bytes[..PROLOGUE_SIZE - 1]);

    let error = Reader::open(file.path()).expect_err("short prologue should fail");

    assert_eq!(error.kind(), ErrorKind::Corruption);
}

// ------------------------------------------------------------ tail boundaries

/// Every cut at or after the schema frame must open, and the snapshot it
/// reports must be exactly the frames that fit entirely within that cut.
#[test]
fn every_truncation_after_the_schema_frame_yields_a_consistent_snapshot() {
    let one_block = reference_fixture();
    let data_offset = data_frame_offset(&one_block);
    let bytes = common::with_appended_frame(&one_block, data_offset, 2);
    let boundaries = [data_offset, one_block.len(), bytes.len()];

    for cut in data_offset..=bytes.len() {
        let file = TemporaryFile::new("reader-truncation-sweep", &bytes[..cut]);
        let reader = Reader::open(file.path())
            .unwrap_or_else(|error| panic!("cut at {cut} should open: {error}"));
        let metadata = reader.file_metadata();

        let complete = boundaries.iter().filter(|&&end| end <= cut).count() - 1;
        assert_eq!(reader.blocks().len(), complete, "cut at {cut}");
        assert_eq!(
            metadata.last_good_offset(),
            boundaries[complete] as u64,
            "cut at {cut}"
        );
        assert_eq!(
            metadata.incomplete_tail(),
            metadata.last_good_offset() != cut as u64,
            "cut at {cut}"
        );
    }
}

/// A frame that is present in full but damaged is not an interrupted append,
/// however close to the end of the file it sits.
#[test]
fn damage_anywhere_in_the_final_frame_is_corruption_not_a_tail() {
    let bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    let trailer = trailer_offset(&bytes, frame);
    let damaged = [
        ("frame magic", frame),
        ("prefix CRC", frame + PREFIX_CRC),
        ("header", frame + PREFIX_SIZE),
        ("payload", payload_offset(&bytes, frame)),
        ("trailer total length", trailer + TRAILER_TOTAL_LENGTH),
        ("trailer sequence", trailer + TRAILER_SEQUENCE),
        ("body CRC", trailer + TRAILER_BODY_CRC),
        ("trailer CRC", trailer + TRAILER_CRC),
        ("commit magic", trailer + TRAILER_COMMIT_MAGIC),
    ];

    for (field, index) in damaged {
        let mut candidate = bytes.clone();
        candidate[index] ^= 1;
        let file = TemporaryFile::new("reader-final-frame-damage", &candidate);

        let error = Reader::open(file.path())
            .err()
            .unwrap_or_else(|| panic!("damaged {field} was accepted as a complete snapshot"));
        assert_eq!(error.kind(), ErrorKind::Corruption, "{field}: {error}");
    }
}

/// Section 13.2 keeps mid-file corruption distinct from a recoverable tail.
#[test]
fn corruption_before_a_later_complete_block_is_still_reported() {
    let one_block = reference_fixture();
    let data_offset = data_frame_offset(&one_block);
    let mut bytes = common::with_appended_frame(&one_block, data_offset, 2);
    bytes[payload_offset(&one_block, data_offset)] ^= 1;
    let file = TemporaryFile::new("reader-mid-file-corruption", &bytes);

    let error = Reader::open(file.path()).expect_err("mid-file corruption should fail");

    assert_eq!(error.kind(), ErrorKind::Corruption);
}

// ------------------------------------------------------ unsupported and limits

#[test]
fn an_unknown_feature_bit_is_unsupported_rather_than_corrupt() {
    let mut bytes = reference_fixture();
    put_u64(&mut bytes, PROLOGUE_FEATURE_FLAGS, 1 << 3);
    repair_prologue(&mut bytes);
    let file = TemporaryFile::new("reader-unknown-feature", &bytes);

    let error = Reader::open(file.path()).expect_err("unknown feature should fail");

    assert_reported(
        &error,
        ErrorKind::UnsupportedFeature,
        PROLOGUE_FEATURE_FLAGS,
    );
}

#[test]
fn a_checkpoint_frame_is_unsupported_rather_than_corrupt() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    put_u16(&mut bytes, frame + PREFIX_FRAME_TYPE, CHECKPOINT_FRAME_TYPE);
    repair_prefix(&mut bytes, frame);
    let file = TemporaryFile::new("reader-checkpoint", &bytes);

    let error = Reader::open(file.path()).expect_err("checkpoint frame should fail");

    assert_reported(
        &error,
        ErrorKind::UnsupportedFrame,
        frame + PREFIX_FRAME_TYPE,
    );
}

#[test]
fn more_blocks_than_the_limit_allows_is_a_resource_limit_failure() {
    let bytes = simple_file(&[3, 3, 3]);
    let file = TemporaryFile::new("reader-block-limit", &bytes);

    Reader::open_with_limits(file.path(), Limits::default().with_max_blocks(3))
        .expect("three blocks are within a three-block limit");
    let error = Reader::open_with_limits(file.path(), Limits::default().with_max_blocks(2))
        .expect_err("three blocks exceed a two-block limit");

    assert_eq!(error.kind(), ErrorKind::ResourceLimit, "{error}");
}

// -------------------------------------------------------------- row identity

#[test]
fn the_row_ids_fixture_exposes_its_base_row_id() {
    let bytes = fixture("nyc_taxi_3_rows/nyc_taxi_3_rows.acta");
    let file = TemporaryFile::new("reader-row-ids", &bytes);
    let reader = Reader::open(file.path()).expect("row-ID fixture should open");

    assert_eq!(reader.file_metadata().feature_flags(), 1);
    assert_eq!(reader.blocks()[0].base_row_id(), Some(0));
    assert_eq!(reader.schema().column_count(), 16);
}

#[test]
fn every_fixture_is_classified_the_same_way_by_both_entry_points() {
    for &(relative_path, _) in FIXTURES {
        let file = TemporaryFile::new("reader-agreement", &fixture(relative_path));
        let report = acta::validate(file.path())
            .unwrap_or_else(|error| panic!("{relative_path} failed validation: {error}"));
        let reader = Reader::open(file.path())
            .unwrap_or_else(|error| panic!("{relative_path} failed to open: {error}"));

        // The schema frame accounts for the difference between the two counts.
        assert_eq!(
            report.frame_count(),
            reader.file_metadata().block_count() + 1,
            "{relative_path}"
        );
        assert_eq!(
            report.last_good_offset(),
            reader.file_metadata().last_good_offset(),
            "{relative_path}"
        );
        assert_eq!(
            report.incomplete_tail(),
            reader.file_metadata().incomplete_tail(),
            "{relative_path}"
        );
        assert_eq!(
            report.feature_flags(),
            reader.file_metadata().feature_flags(),
            "{relative_path}"
        );
    }
}

fn int64_values(batch: &acta::RecordBatch) -> Vec<i64> {
    match batch.column(0).expect("the block has one column") {
        Array::Int64(values) => values.values().to_vec(),
        other => panic!("expected an int64 array, found {other:?}"),
    }
}

fn multi_block_int64_file() -> Vec<u8> {
    let first = common::column_file(
        2,
        &common::TestColumn::new(
            common::int64_column(1, "value"),
            common::LAYOUT_PLAIN,
            vec![common::TestStream::new(
                common::STREAM_VALUES,
                common::TRANSFORM_RAW,
                2,
                int64_bytes(&[10, 20]),
            )],
        ),
    );
    let data_offset = data_frame_offset(&first);
    let mut bytes = common::with_appended_frame(&first, data_offset, 2);
    rewrite_int64_values(&mut bytes, first.len(), &[30, 40]);
    let second_offset = bytes.len();
    bytes = common::with_appended_frame(&bytes, data_offset, 3);
    rewrite_int64_values(&mut bytes, second_offset, &[50, 60]);
    bytes
}

fn int64_bytes(values: &[i64]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

fn rewrite_int64_values(bytes: &mut [u8], frame: usize, values: &[i64]) {
    let descriptor = common::stream_descriptor_offset(bytes, frame, 0);
    let payload = common::payload_offset(bytes, frame);
    let stream_offset =
        common::read_u64(bytes, descriptor + common::STREAM_PAYLOAD_OFFSET) as usize;
    let start = payload + stream_offset;
    let end = start + std::mem::size_of_val(values);
    bytes[start..end].copy_from_slice(&int64_bytes(values));
    common::put_u32(bytes, descriptor + 40, common::crc32c(&bytes[start..end]));
    common::repair_frame(bytes, frame);
}

fn undecodable_second_block() -> Vec<u8> {
    let first = reference_fixture();
    let data_offset = data_frame_offset(&first);
    let mut bytes = common::with_appended_frame(&first, data_offset, 2);
    let second = first.len();
    let payload = payload_offset(&bytes, second);
    bytes[payload] ^= 1;
    common::repair_frame(&mut bytes, second);
    bytes
}
