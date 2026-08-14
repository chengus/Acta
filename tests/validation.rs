//! What the validator accepts, and how it classifies an interrupted append.

mod common;

use acta::ErrorKind;
use common::{
    DATA_FRAME_TYPE, PREFIX_RESERVED, PROLOGUE_RESERVED, PROLOGUE_SIZE, assert_reported,
    data_frame_offset, expect_error, expect_valid, put_u32, reference_fixture, repair_frame,
    repair_prologue, with_appended_frame,
};

#[test]
fn a_complete_file_reports_every_frame() {
    let report = expect_valid("complete", &reference_fixture());

    assert_eq!(report.frame_count(), 2);
}

#[test]
fn a_complete_file_reports_no_incomplete_tail() {
    let report = expect_valid("complete", &reference_fixture());

    assert!(!report.incomplete_tail());
}

#[test]
fn a_complete_file_is_valid_through_its_final_byte() {
    let bytes = reference_fixture();

    let report = expect_valid("complete", &bytes);

    assert_eq!(report.last_good_offset(), bytes.len() as u64);
}

#[test]
fn a_schema_frame_alone_is_a_complete_file() {
    let bytes = reference_fixture();
    let data_frame = data_frame_offset(&bytes);

    let report = expect_valid("schema-only", &bytes[..data_frame]);

    assert_eq!(report.frame_count(), 1);
}

#[test]
fn a_schema_frame_alone_reports_no_incomplete_tail() {
    let bytes = reference_fixture();
    let data_frame = data_frame_offset(&bytes);

    let report = expect_valid("schema-only", &bytes[..data_frame]);

    assert!(!report.incomplete_tail());
}

// -------------------------------------------------- interrupted schema frame

#[test]
fn a_file_holding_only_a_prologue_is_an_incomplete_tail() {
    let bytes = reference_fixture();

    let error = expect_error("prologue-only", &bytes[..PROLOGUE_SIZE]);

    assert_reported(&error, ErrorKind::IncompleteTail, PROLOGUE_SIZE);
}

#[test]
fn every_truncation_inside_the_schema_frame_is_an_incomplete_tail() {
    let bytes = reference_fixture();
    let data_frame = data_frame_offset(&bytes);

    for cut in PROLOGUE_SIZE..data_frame {
        let error = expect_error("cut-in-schema-frame", &bytes[..cut]);
        assert_reported(&error, ErrorKind::IncompleteTail, PROLOGUE_SIZE);
    }
}

// ---------------------------------------------------- interrupted data frame

#[test]
fn every_truncation_inside_the_data_frame_is_reported_as_an_incomplete_tail() {
    let bytes = reference_fixture();
    let data_frame = data_frame_offset(&bytes);

    for cut in data_frame + 1..bytes.len() {
        let report = expect_valid("cut-in-data-frame", &bytes[..cut]);
        assert!(
            report.incomplete_tail(),
            "cut at {cut} was reported complete"
        );
    }
}

#[test]
fn an_interrupted_data_frame_leaves_the_schema_frame_readable() {
    let bytes = reference_fixture();
    let data_frame = data_frame_offset(&bytes);

    for cut in data_frame + 1..bytes.len() {
        let report = expect_valid("cut-in-data-frame", &bytes[..cut]);
        assert_eq!(report.frame_count(), 1, "cut at {cut}");
    }
}

#[test]
fn an_interrupted_data_frame_reports_the_retry_offset() {
    let bytes = reference_fixture();
    let data_frame = data_frame_offset(&bytes);

    for cut in data_frame + 1..bytes.len() {
        let report = expect_valid("cut-in-data-frame", &bytes[..cut]);
        assert_eq!(report.last_good_offset(), data_frame as u64, "cut at {cut}");
    }
}

#[test]
fn trailing_bytes_too_short_for_a_prefix_are_an_incomplete_tail() {
    let mut bytes = reference_fixture();
    let complete_length = bytes.len();
    bytes.extend_from_slice(&[0xff; 8]);

    let report = expect_valid("short-trailing-bytes", &bytes);

    assert_eq!(report.last_good_offset(), complete_length as u64);
}

// ------------------------------------------------------------ reserved bytes

#[test]
fn nonzero_prologue_reserved_bytes_are_ignored() {
    let mut bytes = reference_fixture();
    bytes[PROLOGUE_RESERVED] = 0xaa;
    repair_prologue(&mut bytes);

    let report = expect_valid("prologue-reserved", &bytes);

    assert_eq!(report.frame_count(), 2);
}

#[test]
fn a_nonzero_frame_prefix_reserved_field_is_ignored() {
    let mut bytes = reference_fixture();
    let data_frame = data_frame_offset(&bytes);
    put_u32(&mut bytes, data_frame + PREFIX_RESERVED, 0x5a5a_5a5a);
    repair_frame(&mut bytes, data_frame);

    let report = expect_valid("prefix-reserved", &bytes);

    assert_eq!(report.frame_count(), 2);
}

// -------------------------------------------------------- more than one block

#[test]
fn a_file_with_several_data_frames_validates() {
    let bytes = multi_frame_file();

    let report = expect_valid("three-frames", &bytes);

    assert_eq!(report.frame_count(), 4);
}

#[test]
fn a_file_with_several_data_frames_is_valid_through_its_final_byte() {
    let bytes = multi_frame_file();

    let report = expect_valid("three-frames", &bytes);

    assert_eq!(report.last_good_offset(), bytes.len() as u64);
}

#[test]
fn an_interrupted_frame_after_several_data_frames_keeps_the_earlier_blocks() {
    let complete = multi_frame_file();
    let last_frame = with_appended_frame(&complete, data_frame_offset(&complete), 4);

    for cut in complete.len() + 1..last_frame.len() {
        let report = expect_valid("interrupted-fourth-frame", &last_frame[..cut]);
        assert_eq!(
            report.last_good_offset(),
            complete.len() as u64,
            "cut at {cut}"
        );
    }
}

#[test]
fn an_interrupted_frame_after_several_data_frames_keeps_their_count() {
    let complete = multi_frame_file();
    let last_frame = with_appended_frame(&complete, data_frame_offset(&complete), 4);

    for cut in complete.len() + 1..last_frame.len() {
        let report = expect_valid("interrupted-fourth-frame", &last_frame[..cut]);
        assert_eq!(report.frame_count(), 4, "cut at {cut}");
    }
}

/// A schema frame followed by three data frames.
fn multi_frame_file() -> Vec<u8> {
    let bytes = reference_fixture();
    let data_frame = data_frame_offset(&bytes);
    let bytes = with_appended_frame(&bytes, data_frame, 2);
    with_appended_frame(&bytes, data_frame, 3)
}

#[test]
fn an_appended_frame_keeps_its_declared_type() {
    let bytes = reference_fixture();
    let data_frame = data_frame_offset(&bytes);
    let extended = with_appended_frame(&bytes, data_frame, 2);

    assert_eq!(
        common::read_u16(&extended, bytes.len() + common::PREFIX_FRAME_TYPE),
        DATA_FRAME_TYPE
    );
}
