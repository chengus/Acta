//! What the validator rejects, and where it says the failure is.

mod common;

use acta::{ErrorKind, Limits};
use common::{
    CHECKPOINT_FRAME_TYPE, DATA_FRAME_TYPE, PREFIX_FRAME_FLAGS, PREFIX_FRAME_TYPE,
    PREFIX_FRAME_VERSION, PREFIX_HEADER_LENGTH, PREFIX_PAYLOAD_LENGTH, PREFIX_SEQUENCE,
    PREFIX_SIZE, PROLOGUE_CRC, PROLOGUE_FEATURE_FLAGS, PROLOGUE_FORMAT_MAJOR,
    PROLOGUE_FORMAT_MINOR, PROLOGUE_MAGIC, PROLOGUE_SCHEMA_FRAME_OFFSET, PROLOGUE_SIZE,
    PROLOGUE_SIZE_FIELD, SCHEMA_FRAME_TYPE, TRAILER_COMMIT_MAGIC, TRAILER_CRC, TRAILER_SEQUENCE,
    TRAILER_TOTAL_LENGTH, assert_reported, data_frame_offset, expect_error, put_u16, put_u32,
    put_u64, reference_fixture, repair_frame, repair_prefix, repair_prologue, repair_trailer_crc,
    schema_frame_offset, trailer_offset, validate_with_limits, with_appended_empty_frame,
    with_appended_frame,
};

// ------------------------------------------------------------------ prologue

#[test]
fn a_file_shorter_than_the_prologue_is_corruption() {
    let bytes = reference_fixture();

    for cut in [0, 1, PROLOGUE_SIZE - 1] {
        let error = expect_error("short-file", &bytes[..cut]);
        assert_reported(&error, ErrorKind::Corruption, cut);
    }
}

#[test]
fn bad_file_magic_is_corruption() {
    let mut bytes = reference_fixture();
    bytes[PROLOGUE_MAGIC] ^= 1;
    repair_prologue(&mut bytes);

    let error = expect_error("file-magic", &bytes);

    assert_reported(&error, ErrorKind::Corruption, PROLOGUE_MAGIC);
}

#[test]
fn a_bad_prologue_crc_is_corruption() {
    let mut bytes = reference_fixture();
    bytes[PROLOGUE_CRC] ^= 1;

    let error = expect_error("prologue-crc", &bytes);

    assert_reported(&error, ErrorKind::Corruption, 0);
}

#[test]
fn the_prologue_crc_is_checked_before_the_format_version() {
    let mut bytes = reference_fixture();
    put_u16(&mut bytes, PROLOGUE_FORMAT_MAJOR, 1);

    let error = expect_error("version-without-crc-repair", &bytes);

    assert_eq!(error.kind(), ErrorKind::Corruption, "{error}");
}

#[test]
fn an_unknown_major_version_is_unsupported() {
    let mut bytes = reference_fixture();
    put_u16(&mut bytes, PROLOGUE_FORMAT_MAJOR, 1);
    repair_prologue(&mut bytes);

    let error = expect_error("major-version", &bytes);

    assert_reported(&error, ErrorKind::UnsupportedVersion, PROLOGUE_FORMAT_MAJOR);
}

#[test]
fn an_unknown_minor_version_is_unsupported() {
    let mut bytes = reference_fixture();
    put_u16(&mut bytes, PROLOGUE_FORMAT_MINOR, 3);
    repair_prologue(&mut bytes);

    let error = expect_error("minor-version", &bytes);

    assert_reported(&error, ErrorKind::UnsupportedVersion, PROLOGUE_FORMAT_MAJOR);
}

#[test]
fn the_v0_1_format_version_is_unsupported() {
    let mut bytes = reference_fixture();
    put_u16(&mut bytes, PROLOGUE_FORMAT_MINOR, 1);
    repair_prologue(&mut bytes);

    let error = expect_error("v0-1", &bytes);

    assert_eq!(error.kind(), ErrorKind::UnsupportedVersion, "{error}");
}

#[test]
fn every_unknown_feature_bit_is_unsupported() {
    for bit in 1..64 {
        let mut bytes = reference_fixture();
        put_u64(&mut bytes, PROLOGUE_FEATURE_FLAGS, 1 << bit);
        repair_prologue(&mut bytes);

        let error = expect_error("feature-bit", &bytes);
        assert_reported(
            &error,
            ErrorKind::UnsupportedFeature,
            PROLOGUE_FEATURE_FLAGS,
        );
    }
}

#[test]
fn the_row_ids_feature_bit_is_accepted() {
    let bytes = common::fixture("nyc_taxi_3_rows/nyc_taxi_3_rows.acta");

    let report = common::expect_valid("row-ids-feature", &bytes);

    assert_eq!(report.feature_flags(), 1);
}

/// Section 8 requires every block to agree with the file about implicit row
/// IDs, so enabling the feature alone leaves the blocks contradicting it.
#[test]
fn enabling_row_ids_without_changing_the_blocks_is_corruption() {
    let mut bytes = reference_fixture();
    put_u64(&mut bytes, PROLOGUE_FEATURE_FLAGS, 1);
    repair_prologue(&mut bytes);

    let error = expect_error("row-ids-feature-only", &bytes);

    assert_eq!(error.kind(), ErrorKind::Corruption, "{error}");
}

#[test]
fn a_prologue_size_other_than_64_is_corruption() {
    let mut bytes = reference_fixture();
    put_u32(&mut bytes, PROLOGUE_SIZE_FIELD, 128);
    repair_prologue(&mut bytes);

    let error = expect_error("prologue-size", &bytes);

    assert_reported(&error, ErrorKind::Corruption, PROLOGUE_SIZE_FIELD);
}

#[test]
fn a_schema_frame_offset_other_than_64_is_corruption() {
    let mut bytes = reference_fixture();
    put_u64(&mut bytes, PROLOGUE_SCHEMA_FRAME_OFFSET, 128);
    repair_prologue(&mut bytes);

    let error = expect_error("schema-frame-offset", &bytes);

    assert_reported(&error, ErrorKind::Corruption, PROLOGUE_SCHEMA_FRAME_OFFSET);
}

// -------------------------------------------------------------- frame prefix

#[test]
fn bad_frame_magic_is_corruption() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    bytes[frame] ^= 1;
    repair_prefix(&mut bytes, frame);

    let error = expect_error("frame-magic", &bytes);

    assert_reported(&error, ErrorKind::Corruption, frame);
}

#[test]
fn a_bad_frame_prefix_crc_is_corruption() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    bytes[frame + common::PREFIX_CRC] ^= 1;

    let error = expect_error("prefix-crc", &bytes);

    assert_reported(&error, ErrorKind::Corruption, frame);
}

#[test]
fn the_prefix_crc_is_checked_before_the_declared_lengths() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    put_u64(&mut bytes, frame + PREFIX_PAYLOAD_LENGTH, 7);

    let error = expect_error("length-without-crc-repair", &bytes);

    assert_eq!(error.offset(), Some(frame as u64), "{error}");
}

#[test]
fn an_unknown_frame_envelope_version_is_unsupported() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    put_u16(&mut bytes, frame + PREFIX_FRAME_VERSION, 1);
    repair_prefix(&mut bytes, frame);

    let error = expect_error("frame-version", &bytes);

    assert_reported(
        &error,
        ErrorKind::UnsupportedFrame,
        frame + PREFIX_FRAME_VERSION,
    );
}

#[test]
fn every_unknown_frame_flag_is_unsupported() {
    for bit in 0..32 {
        let mut bytes = reference_fixture();
        let frame = data_frame_offset(&bytes);
        put_u32(&mut bytes, frame + PREFIX_FRAME_FLAGS, 1 << bit);
        repair_prefix(&mut bytes, frame);

        let error = expect_error("frame-flags", &bytes);
        assert_reported(
            &error,
            ErrorKind::UnsupportedFrame,
            frame + PREFIX_FRAME_FLAGS,
        );
    }
}

// ---------------------------------------------------------------- frame type

#[test]
fn a_checkpoint_frame_is_unsupported() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    put_u16(&mut bytes, frame + PREFIX_FRAME_TYPE, CHECKPOINT_FRAME_TYPE);
    repair_prefix(&mut bytes, frame);

    let error = expect_error("checkpoint-frame", &bytes);

    assert_reported(
        &error,
        ErrorKind::UnsupportedFrame,
        frame + PREFIX_FRAME_TYPE,
    );
}

#[test]
fn an_interrupted_checkpoint_frame_is_still_unsupported() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    put_u16(&mut bytes, frame + PREFIX_FRAME_TYPE, CHECKPOINT_FRAME_TYPE);
    repair_prefix(&mut bytes, frame);

    let error = expect_error("interrupted-checkpoint", &bytes[..frame + PREFIX_SIZE]);

    assert_reported(
        &error,
        ErrorKind::UnsupportedFrame,
        frame + PREFIX_FRAME_TYPE,
    );
}

#[test]
fn an_unknown_frame_type_is_corruption() {
    for frame_type in [0, 4, 65535] {
        let mut bytes = reference_fixture();
        let frame = data_frame_offset(&bytes);
        put_u16(&mut bytes, frame + PREFIX_FRAME_TYPE, frame_type);
        repair_prefix(&mut bytes, frame);

        let error = expect_error("unknown-frame-type", &bytes);
        assert_reported(&error, ErrorKind::Corruption, frame + PREFIX_FRAME_TYPE);
    }
}

#[test]
fn a_data_frame_in_the_schema_position_is_corruption() {
    let mut bytes = reference_fixture();
    let frame = schema_frame_offset();
    put_u16(&mut bytes, frame + PREFIX_FRAME_TYPE, DATA_FRAME_TYPE);
    repair_prefix(&mut bytes, frame);

    let error = expect_error("data-frame-first", &bytes);

    assert_reported(&error, ErrorKind::Corruption, frame + PREFIX_FRAME_TYPE);
}

#[test]
fn a_second_schema_frame_is_corruption() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    put_u16(&mut bytes, frame + PREFIX_FRAME_TYPE, SCHEMA_FRAME_TYPE);
    repair_prefix(&mut bytes, frame);

    let error = expect_error("second-schema-frame", &bytes);

    assert_reported(&error, ErrorKind::Corruption, frame + PREFIX_FRAME_TYPE);
}

// ------------------------------------------------------------ declared sizes

#[test]
fn an_unaligned_header_length_is_corruption() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    put_u32(&mut bytes, frame + PREFIX_HEADER_LENGTH, 161);
    repair_prefix(&mut bytes, frame);

    let error = expect_error("unaligned-header", &bytes);

    assert_reported(&error, ErrorKind::Corruption, frame + PREFIX_HEADER_LENGTH);
}

#[test]
fn an_unaligned_payload_length_is_corruption() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    put_u64(&mut bytes, frame + PREFIX_PAYLOAD_LENGTH, 25);
    repair_prefix(&mut bytes, frame);

    let error = expect_error("unaligned-payload", &bytes);

    assert_reported(&error, ErrorKind::Corruption, frame + PREFIX_PAYLOAD_LENGTH);
}

#[test]
fn a_schema_frame_header_other_than_24_bytes_is_corruption() {
    let mut bytes = reference_fixture();
    let frame = schema_frame_offset();
    put_u32(&mut bytes, frame + PREFIX_HEADER_LENGTH, 32);
    repair_prefix(&mut bytes, frame);

    let error = expect_error("schema-header-size", &bytes);

    assert_reported(&error, ErrorKind::Corruption, frame + PREFIX_HEADER_LENGTH);
}

#[test]
fn a_schema_frame_with_no_header_is_corruption() {
    let bytes = reference_fixture();
    let empty = with_appended_empty_frame(&bytes[..PROLOGUE_SIZE], SCHEMA_FRAME_TYPE, 0);

    let error = expect_error("empty-schema-frame", &empty);

    assert_reported(
        &error,
        ErrorKind::Corruption,
        PROLOGUE_SIZE + PREFIX_HEADER_LENGTH,
    );
}

#[test]
fn a_data_frame_shorter_than_its_block_header_is_corruption() {
    let bytes = reference_fixture();
    let appended = bytes.len();
    let empty = with_appended_empty_frame(&bytes, DATA_FRAME_TYPE, 2);

    let error = expect_error("empty-data-frame", &empty);

    assert_reported(
        &error,
        ErrorKind::Corruption,
        appended + PREFIX_HEADER_LENGTH,
    );
}

#[test]
fn a_frame_length_that_overflows_is_corruption() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    put_u64(&mut bytes, frame + PREFIX_PAYLOAD_LENGTH, u64::MAX - 7);
    repair_prefix(&mut bytes, frame);
    let limits = Limits::default().with_max_frame_payload_length(u64::MAX);

    let error = validate_with_limits("length-overflow", &bytes, limits).unwrap_err();

    assert_reported(&error, ErrorKind::Corruption, frame);
}

// ----------------------------------------------------------- sequence numbers

#[test]
fn a_frame_sequence_that_does_not_follow_is_corruption() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    put_u64(&mut bytes, frame + PREFIX_SEQUENCE, 7);
    repair_prefix(&mut bytes, frame);

    let error = expect_error("prefix-sequence", &bytes);

    assert_reported(&error, ErrorKind::Corruption, frame + PREFIX_SEQUENCE);
}

#[test]
fn a_repeated_sequence_number_is_corruption() {
    let bytes = three_frame_file();
    let third = second_data_frame_offset();
    let mut damaged = bytes;
    put_u64(&mut damaged, third + PREFIX_SEQUENCE, 1);
    repair_frame(&mut damaged, third);

    let error = expect_error("repeated-sequence", &damaged);

    assert_reported(&error, ErrorKind::Corruption, third + PREFIX_SEQUENCE);
}

#[test]
fn a_skipped_sequence_number_is_corruption() {
    let bytes = three_frame_file();
    let third = second_data_frame_offset();
    let mut damaged = bytes;
    put_u64(&mut damaged, third + PREFIX_SEQUENCE, 3);
    repair_frame(&mut damaged, third);

    let error = expect_error("skipped-sequence", &damaged);

    assert_reported(&error, ErrorKind::Corruption, third + PREFIX_SEQUENCE);
}

fn three_frame_file() -> Vec<u8> {
    let bytes = reference_fixture();
    let data_frame = data_frame_offset(&bytes);
    with_appended_frame(&bytes, data_frame, 2)
}

fn second_data_frame_offset() -> usize {
    reference_fixture().len()
}

// ----------------------------------------------------------------- trailer

#[test]
fn a_trailer_total_length_that_disagrees_with_the_prefix_is_corruption() {
    for length in [1_u64, u64::MAX] {
        let mut bytes = reference_fixture();
        let trailer = trailer_offset(&bytes, data_frame_offset(&bytes));
        put_u64(&mut bytes, trailer + TRAILER_TOTAL_LENGTH, length);
        repair_trailer_crc(&mut bytes, trailer);

        let error = expect_error("trailer-total-length", &bytes);
        assert_reported(&error, ErrorKind::Corruption, trailer);
    }
}

#[test]
fn a_trailer_sequence_that_disagrees_with_the_prefix_is_corruption() {
    let mut bytes = reference_fixture();
    let trailer = trailer_offset(&bytes, data_frame_offset(&bytes));
    put_u64(&mut bytes, trailer + TRAILER_SEQUENCE, 7);
    repair_trailer_crc(&mut bytes, trailer);

    let error = expect_error("trailer-sequence", &bytes);

    assert_reported(&error, ErrorKind::Corruption, trailer + TRAILER_SEQUENCE);
}

#[test]
fn bad_commit_magic_is_corruption() {
    let mut bytes = reference_fixture();
    let trailer = trailer_offset(&bytes, data_frame_offset(&bytes));
    bytes[trailer + TRAILER_COMMIT_MAGIC] ^= 1;
    repair_trailer_crc(&mut bytes, trailer);

    let error = expect_error("commit-magic", &bytes);

    assert_reported(
        &error,
        ErrorKind::Corruption,
        trailer + TRAILER_COMMIT_MAGIC,
    );
}

#[test]
fn a_bad_trailer_crc_is_corruption() {
    let mut bytes = reference_fixture();
    let trailer = trailer_offset(&bytes, data_frame_offset(&bytes));
    bytes[trailer + TRAILER_CRC] ^= 1;

    let error = expect_error("trailer-crc", &bytes);

    assert_reported(&error, ErrorKind::Corruption, trailer + TRAILER_CRC);
}

// -------------------------------------------------------------- body content

#[test]
fn a_corrupt_frame_header_fails_the_header_crc() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    let header = frame + PREFIX_SIZE;
    bytes[header] ^= 1;

    let error = expect_error("header-crc", &bytes);

    assert_reported(&error, ErrorKind::Corruption, header);
}

#[test]
fn a_corrupt_payload_fails_the_body_crc() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    let payload = common::payload_offset(&bytes, frame);
    bytes[payload] ^= 1;

    let error = expect_error("body-crc", &bytes);

    assert_reported(&error, ErrorKind::Corruption, frame);
}

#[test]
fn corruption_before_a_later_valid_frame_is_still_reported() {
    let bytes = three_frame_file();
    let first_data_frame = data_frame_offset(&bytes);
    let mut damaged = bytes;
    let payload = common::payload_offset(&damaged, first_data_frame);
    damaged[payload] ^= 1;

    let error = expect_error("mid-file-corruption", &damaged);

    assert_reported(&error, ErrorKind::Corruption, first_data_frame);
}

// ----------------------------------------------------------- resource limits

#[test]
fn a_payload_larger_than_the_limit_is_a_resource_limit_failure() {
    let bytes = reference_fixture();
    let frame = schema_frame_offset();
    let limits = Limits::default().with_max_frame_payload_length(8);

    let error = validate_with_limits("payload-limit", &bytes, limits).unwrap_err();

    assert_reported(
        &error,
        ErrorKind::ResourceLimit,
        frame + PREFIX_PAYLOAD_LENGTH,
    );
}

#[test]
fn a_limit_is_checked_before_the_frame_is_read() {
    let bytes = reference_fixture();
    let limits = Limits::default().with_max_frame_payload_length(8);

    let error = validate_with_limits("payload-limit", &bytes, limits).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::ResourceLimit, "{error}");
}

#[test]
fn a_header_larger_than_the_limit_is_a_resource_limit_failure() {
    let bytes = reference_fixture();
    let frame = schema_frame_offset();
    let limits = Limits::default().with_max_frame_header_length(8);

    let error = validate_with_limits("header-limit", &bytes, limits).unwrap_err();

    assert_reported(
        &error,
        ErrorKind::ResourceLimit,
        frame + PREFIX_HEADER_LENGTH,
    );
}

#[test]
fn a_declared_length_beyond_the_file_is_not_read() {
    let mut bytes = reference_fixture();
    let frame = data_frame_offset(&bytes);
    put_u32(&mut bytes, frame + PREFIX_HEADER_LENGTH, 4_294_967_288);
    repair_prefix(&mut bytes, frame);
    let limits = Limits::default().with_max_frame_header_length(u64::MAX);

    let report = validate_with_limits("huge-header", &bytes, limits).unwrap();

    assert_eq!(report.last_good_offset(), frame as u64);
}
