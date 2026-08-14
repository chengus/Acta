//! What `acta inspect` prints, and when it refuses.
//!
//! The command's rendering lives in the binary rather than the library, so that
//! presentation never becomes part of the crate's public API. These tests reach
//! it directly instead of spawning a process, which keeps them fast and lets
//! them assert on exact text.

#[path = "../rust/bin/acta/inspect.rs"]
mod inspect;

mod common;

use acta::{ErrorKind, Reader};
use common::{
    PROLOGUE_FORMAT_MINOR, TS_SORTED_BLOCK_FLAG, TYPE_TIMESTAMP64, TemporaryFile, UINT64_MAX,
    block_header, build_file, data_frame_offset, descriptor, fixture, int64_column, put_u16,
    reference_fixture, repair_prologue, schema_header, timestamp_parameters, with_appended_frame,
    with_primary_bounds,
};
use inspect::{format_inspection, inspect_path};

// ------------------------------------------------------------------- harness

fn report(label: &str, bytes: &[u8]) -> String {
    let file = TemporaryFile::new(label, bytes);
    inspect_path(file.path()).unwrap_or_else(|error| panic!("{label} should inspect: {error}"))
}

fn rejection(label: &str, bytes: &[u8]) -> ErrorKind {
    let file = TemporaryFile::new(label, bytes);
    inspect_path(file.path())
        .err()
        .unwrap_or_else(|| panic!("{label} was unexpectedly inspected"))
        .kind()
}

/// The lines of one titled section, without its title or underline.
fn section<'a>(report: &'a str, title: &str) -> Vec<&'a str> {
    report
        .split("\n\n")
        .find_map(|section| {
            section.strip_prefix(&format!("{title}\n{}\n", "-".repeat(title.len())))
        })
        .unwrap_or_else(|| panic!("report has no {title} section:\n{report}"))
        .lines()
        .collect()
}

/// The cells of one table row.
///
/// Cells are padded and joined by at least two spaces, so a run of two or more
/// spaces separates them while a single space can appear inside one, as it does
/// in `timestamp64(us, UTC)`.
fn cells(row: &str) -> Vec<&str> {
    row.split("  ")
        .map(str::trim)
        .filter(|cell| !cell.is_empty())
        .collect()
}

fn field(report: &str, label: &str) -> String {
    section(report, "File")
        .iter()
        .find_map(|line| line.strip_prefix(&format!("{label}:")))
        .unwrap_or_else(|| panic!("File section has no `{label}` field:\n{report}"))
        .trim()
        .to_owned()
}

// -------------------------------------------------------------- known output

/// The whole report for a frozen fixture, so any change to the layout is a
/// deliberate edit here rather than a silent drift.
#[test]
fn the_minimal_fixture_renders_exactly() {
    let expected = "\
Acta v0.2

File
----
file id:   000102030405060708090a0b0c0d0e0f
features:  none
schema id: 1
size:      472 bytes
blocks:    1
rows:      3
primary:   time (column 1)
tail:      complete

Schema
------
ID  Name  Type                  Nullable  Primary
1   time  timestamp64(us, UTC)  no        yes

Blocks
------
Seq  Offset  Bytes  Rows  Base Row  Primary Min  Primary Max  Sorted
1    208     264    3     -         1000000      3000000      no
";

    assert_eq!(report("inspect-minimal", &reference_fixture()), expected);
}

#[test]
fn rendering_the_same_file_twice_gives_the_same_report() {
    let bytes = fixture("nyc_taxi_3_rows/nyc_taxi_3_rows.acta");

    assert_eq!(
        report("inspect-repeat-first", &bytes),
        report("inspect-repeat-second", &bytes)
    );
}

#[test]
fn inspect_path_renders_what_format_inspection_renders() {
    let file = TemporaryFile::new("inspect-entry-points", &reference_fixture());
    let reader = Reader::open(file.path()).expect("fixture should open");

    assert_eq!(
        inspect_path(file.path()).expect("fixture should inspect"),
        format_inspection(&reader)
    );
}

// -------------------------------------------------------------------- blocks

#[test]
fn a_schema_only_file_reports_no_blocks() {
    let bytes = reference_fixture();
    let schema_only = &bytes[..data_frame_offset(&bytes)];

    let report = report("inspect-schema-only", schema_only);

    assert_eq!(section(&report, "Blocks"), vec!["(no data blocks)"]);
}

#[test]
fn a_schema_only_file_reports_zero_rows() {
    let bytes = reference_fixture();
    let schema_only = &bytes[..data_frame_offset(&bytes)];

    assert_eq!(
        field(&report("inspect-schema-only-rows", schema_only), "rows"),
        "0"
    );
}

#[test]
fn a_one_block_file_lists_one_block() {
    let report = report("inspect-one-block", &reference_fixture());

    // One heading row and one block row.
    assert_eq!(section(&report, "Blocks").len(), 2);
}

#[test]
fn a_multi_block_file_lists_every_block_in_sequence_order() {
    let one = reference_fixture();
    let data_frame = data_frame_offset(&one);
    let two = with_appended_frame(&one, data_frame, 2);
    let three = with_appended_frame(&two, data_frame, 3);

    let report = report("inspect-multi-block", &three);

    let sequences: Vec<&str> = section(&report, "Blocks")[1..]
        .iter()
        .map(|row| cells(row)[0])
        .collect();
    assert_eq!(sequences, vec!["1", "2", "3"]);
}

#[test]
fn the_block_row_reports_the_offset_length_and_row_count() {
    let report = report("inspect-block-fields", &reference_fixture());

    assert_eq!(
        cells(section(&report, "Blocks")[1])[..4],
        ["1", "208", "264", "3"]
    );
}

// -------------------------------------------------------------------- schema

#[test]
fn a_nullable_column_is_distinguished_from_a_non_nullable_one() {
    let columns = [
        descriptor(
            1,
            TYPE_TIMESTAMP64,
            0,
            "time",
            &timestamp_parameters(2, 1, ""),
        ),
        descriptor(2, 11, 1, "price", &[]),
    ];
    let bytes = build_file(0, &schema_header(1, 2, 1, 0), &columns, &[]);

    let report = report("inspect-nullability", &bytes);

    let nullable: Vec<&str> = section(&report, "Schema")[1..]
        .iter()
        .map(|row| cells(row)[3])
        .collect();
    assert_eq!(nullable, vec!["no", "yes"]);
}

#[test]
fn the_primary_column_is_marked_in_the_schema_table() {
    let report = report("inspect-primary-mark", &reference_fixture());

    let row = section(&report, "Schema")[1];
    assert!(row.ends_with("yes"), "primary column not marked: {row}");
}

#[test]
fn the_primary_column_is_named_in_the_file_section() {
    let report = report("inspect-primary-name", &reference_fixture());

    assert_eq!(field(&report, "primary"), "time (column 1)");
}

#[test]
fn a_schema_without_a_primary_column_says_so() {
    let bytes = fixture("no_primary/no_primary.acta");

    assert_eq!(
        field(&report("inspect-no-primary", &bytes), "primary"),
        "none"
    );
}

#[test]
fn a_block_without_primary_bounds_shows_them_as_absent() {
    let bytes = fixture("no_primary/no_primary.acta");

    let report = report("inspect-no-bounds", &bytes);

    assert_eq!(
        cells(section(&report, "Blocks")[1])[4..],
        ["-", "-", "-", "no"]
    );
}

/// The logical types named in the schema table of a fixture using all of the
/// parameterised ones.
fn fixture_types() -> Vec<String> {
    let bytes = fixture("nyc_taxi_3_rows/nyc_taxi_3_rows.acta");
    let report = report("inspect-types", &bytes);
    section(&report, "Schema")[1..]
        .iter()
        .map(|row| cells(row)[2].to_owned())
        .collect()
}

#[test]
fn a_decimal_names_its_precision_and_scale() {
    assert!(
        fixture_types().contains(&"decimal64(precision=18, scale=2)".to_owned()),
        "decimal parameters missing from {:?}",
        fixture_types()
    );
}

#[test]
fn a_categorical_names_its_ordered_flag() {
    assert!(
        fixture_types().contains(&"categorical(ordered=false)".to_owned()),
        "categorical parameters missing from {:?}",
        fixture_types()
    );
}

#[test]
fn a_fixed_binary_names_its_width() {
    assert!(
        fixture_types().contains(&"fixed_binary(16)".to_owned()),
        "fixed_binary width missing from {:?}",
        fixture_types()
    );
}

#[test]
fn an_iana_timezone_is_named_in_the_type() {
    let bytes = fixture("timezone/timezone.acta");

    let report = report("inspect-timezone", &bytes);

    assert!(
        section(&report, "Schema")[1].contains("timestamp64(ms, Europe/Berlin)"),
        "timezone missing from:\n{report}"
    );
}

// ------------------------------------------------------------ implicit row IDs

#[test]
fn row_ids_enabled_reports_the_feature() {
    let bytes = fixture("nyc_taxi_3_rows/nyc_taxi_3_rows.acta");

    let report = report("inspect-row-ids", &bytes);

    assert_eq!(field(&report, "features"), "ROW_IDS");
}

#[test]
fn row_ids_enabled_shows_a_base_row_id_per_block() {
    let bytes = fixture("nyc_taxi_3_rows/nyc_taxi_3_rows.acta");

    let report = report("inspect-row-id-column", &bytes);

    assert_eq!(cells(section(&report, "Blocks")[1])[4], "0");
}

#[test]
fn row_ids_disabled_reports_no_features() {
    let report = report("inspect-no-row-ids", &reference_fixture());

    assert_eq!(field(&report, "features"), "none");
}

#[test]
fn row_ids_disabled_shows_an_absent_base_row_id() {
    let report = report("inspect-absent-row-id", &reference_fixture());

    assert_eq!(cells(section(&report, "Blocks")[1])[4], "-");
}

// ------------------------------------------------------------- sortedness

#[test]
fn a_sorted_block_is_reported_as_sorted() {
    let mut header = block_header(1, 3, UINT64_MAX, TS_SORTED_BLOCK_FLAG);
    with_primary_bounds(&mut header, 10, 30);
    let column = descriptor(
        1,
        TYPE_TIMESTAMP64,
        0,
        "time",
        &timestamp_parameters(2, 1, ""),
    );
    let bytes = build_file(0, &schema_header(1, 1, 1, 0), &[column], &[header]);

    let report = report("inspect-sorted", &bytes);

    assert!(
        section(&report, "Blocks")[1].ends_with("yes"),
        "block not reported as sorted:\n{report}"
    );
}

// --------------------------------------------------------------- an interrupted append

#[test]
fn an_incomplete_tail_reports_the_offset_to_resume_from() {
    let complete = reference_fixture();
    let data_frame = data_frame_offset(&complete);
    let appended = with_appended_frame(&complete, data_frame, 2);
    let interrupted = &appended[..appended.len() - 1];

    let report = report("inspect-tail", interrupted);

    assert_eq!(
        field(&report, "tail"),
        format!("incomplete, resume at offset {}", complete.len())
    );
}

// -------------------------------------------------------------------- refusals

#[test]
fn a_corrupt_file_is_refused_as_corruption() {
    let mut bytes = reference_fixture();
    let header = data_frame_offset(&bytes) + common::PREFIX_SIZE;
    bytes[header] ^= 1;

    assert_eq!(rejection("inspect-corrupt", &bytes), ErrorKind::Corruption);
}

/// A file this crate cannot read is not the same as a file that is damaged, and
/// the command must keep the two apart.
#[test]
fn an_unsupported_version_is_refused_as_unsupported() {
    let mut bytes = reference_fixture();
    put_u16(&mut bytes, PROLOGUE_FORMAT_MINOR, 3);
    repair_prologue(&mut bytes);

    assert_eq!(
        rejection("inspect-unsupported", &bytes),
        ErrorKind::UnsupportedVersion
    );
}

#[test]
fn a_missing_file_is_refused_as_an_io_failure() {
    let missing = std::env::temp_dir().join("acta-inspect-missing-file.acta");

    let error = inspect_path(&missing).expect_err("a missing file should be refused");

    assert_eq!(error.kind(), ErrorKind::Io);
}

#[test]
fn a_truncated_prologue_is_refused_rather_than_rendered() {
    let bytes = reference_fixture();

    assert_eq!(
        rejection("inspect-short-prologue", &bytes[..8]),
        ErrorKind::Corruption
    );
}

#[test]
fn an_empty_schema_is_refused_rather_than_rendered() {
    let bytes = build_file(0, &schema_header(1, 0, 0, 0), &[], &[]);

    assert_eq!(
        rejection("inspect-empty-schema", &bytes),
        ErrorKind::Corruption
    );
}

#[test]
fn a_block_that_contradicts_its_schema_is_refused() {
    let bytes = build_file(
        0,
        &schema_header(1, 1, 0, 0),
        &[int64_column(1, "value")],
        &[block_header(1, 0, UINT64_MAX, 0)],
    );

    assert_eq!(
        rejection("inspect-zero-rows", &bytes),
        ErrorKind::Corruption
    );
}
