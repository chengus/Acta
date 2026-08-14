//! Presentation and traversal tests for the native head command renderer.

#[path = "../rust/bin/acta/head.rs"]
mod head;

mod common;

use acta::{ErrorKind, Reader};
use common::{TemporaryFile, data_frame_offset, fixture, reference_fixture};

fn render(label: &str, bytes: &[u8], rows: usize) -> String {
    let file = TemporaryFile::new(label, bytes);
    head::head_path(file.path(), rows).unwrap_or_else(|error| panic!("{label}: {error}"))
}

#[test]
fn default_head_prints_at_most_ten_rows() {
    let output = render("head-default", &reference_fixture(), 10);

    assert_eq!(output.lines().count(), 4);
    assert_eq!(output.lines().next(), Some("time"));
}

#[test]
fn custom_head_count_stops_inside_a_block() {
    let output = render("head-custom", &reference_fixture(), 2);

    assert_eq!(output, "time\n1000000us\n2000000us\n");
}

#[test]
fn zero_rows_prints_only_schema_columns() {
    let output = render("head-zero", &reference_fixture(), 0);

    assert_eq!(output, "time\n");
}

#[test]
fn head_continues_across_block_boundaries() {
    let first = reference_fixture();
    let data_offset = data_frame_offset(&first);
    let bytes = common::with_appended_frame(&first, data_offset, 2);

    let output = render("head-cross-block", &bytes, 5);

    assert_eq!(output.lines().count(), 6);
    assert_eq!(output.lines().nth(4), Some("1000000us"));
    assert_eq!(output.lines().nth(5), Some("2000000us"));
}

#[test]
fn head_preserves_schema_order_and_formats_logical_types() {
    let bytes = fixture("nyc_taxi_3_rows/nyc_taxi_3_rows.acta");
    let output = render("head-types", &bytes, 1);

    let header = output.lines().next().expect("header row");
    assert!(header.starts_with("stored_and_forwarded  zone_delta"));
    assert!(output.contains("7.20"));
    assert!(output.contains("0xef000000ee00000001"));
    assert!(output.contains("credit_card"));
}

#[test]
fn schema_only_and_empty_files_are_supported() {
    let bytes = reference_fixture();
    let schema_end = data_frame_offset(&bytes);
    let output = render("head-schema-only", &bytes[..schema_end], 10);

    assert_eq!(output, "time\n");
}

#[test]
fn all_v0_2_fixtures_decode_through_the_reader() {
    for &(relative_path, _) in common::FIXTURES {
        let bytes = fixture(relative_path);
        let file = TemporaryFile::new("head-fixture", &bytes);
        let reader = Reader::open(file.path())
            .unwrap_or_else(|error| panic!("{relative_path} did not open: {error}"));
        let batch = reader
            .read_block(0)
            .unwrap_or_else(|error| panic!("{relative_path} did not decode: {error}"));

        assert_eq!(batch.row_count() as u64, reader.blocks()[0].row_count());
        assert_eq!(batch.columns().len(), reader.schema().column_count());
    }
}

/// A two-block file whose second block cannot be decoded, though its frame is
/// complete and every frame checksum agrees.
fn undecodable_second_block() -> Vec<u8> {
    let first = reference_fixture();
    let data_offset = data_frame_offset(&first);
    let mut bytes = common::with_appended_frame(&first, data_offset, 2);
    let second = first.len();
    let payload = common::payload_offset(&bytes, second);
    bytes[payload] ^= 1;
    common::repair_frame(&mut bytes, second);
    bytes
}

#[test]
fn head_stops_at_the_block_that_satisfies_the_row_count() {
    let output = render("head-lazy", &undecodable_second_block(), 3);

    assert_eq!(output.lines().count(), 4);
}

#[test]
fn head_reads_the_next_block_only_when_it_needs_more_rows() {
    let file = TemporaryFile::new("head-lazy-more", &undecodable_second_block());

    let error = head::head_path(file.path(), 4).expect_err("a fourth row needs the next block");

    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn a_long_value_is_elided_rather_than_setting_the_table_width() {
    let value = "x".repeat(200);
    let bytes = common::column_file(
        1,
        &common::TestColumn::new(
            common::descriptor(1, common::TYPE_UTF8, 0, "value", &[]),
            common::LAYOUT_PLAIN,
            vec![
                common::TestStream::new(
                    common::STREAM_VALUES,
                    common::TRANSFORM_RAW,
                    1,
                    value.clone().into_bytes(),
                ),
                common::TestStream::new(
                    common::STREAM_LENGTHS,
                    common::TRANSFORM_RAW,
                    1,
                    (value.len() as u32).to_le_bytes().to_vec(),
                ),
            ],
        ),
    );

    let output = render("head-long-value", &bytes, 1);

    assert_eq!(
        output.lines().nth(1).map(str::chars).map(Iterator::count),
        Some(65)
    );
}

#[test]
fn head_errors_remain_structured() {
    let corrupt = {
        let mut bytes = reference_fixture();
        bytes[0] ^= 1;
        bytes
    };
    let file = TemporaryFile::new("head-corrupt", &corrupt);
    let error = head::head_path(file.path(), 1).expect_err("corruption should fail");
    assert_eq!(error.kind(), ErrorKind::Corruption);
}
