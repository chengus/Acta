//! Schema descriptor and block metadata invariants from sections 7 and 8.
//!
//! Every case here is built from the specification field tables rather than
//! edited out of a fixture, so a test can state exactly which field is wrong
//! and nothing else. Each one is asserted against both public entry points:
//! `acta::validate` and `Reader::open` share one parser and must never disagree
//! about whether a file is well formed.

mod common;

use acta::{Error, ErrorKind, LogicalType, Reader, TimeUnit, TimeZone};
use common::{
    BLOCK_COLUMN_COUNT, BLOCK_COLUMN_TABLE_OFFSET, BLOCK_FLAGS, BLOCK_HEADER_SIZE, BLOCK_ROW_COUNT,
    BLOCK_SCHEMA_ID, BLOCK_STATISTICS_LENGTH, BLOCK_STATISTICS_OFFSET, BLOCK_STREAM_TABLE_OFFSET,
    DESCRIPTOR_LENGTH, DESCRIPTOR_NAME_LENGTH, DESCRIPTOR_PARAMETERS_LENGTH, ROW_IDS_BLOCK_FLAG,
    TS_SORTED_BLOCK_FLAG, TYPE_DATE32, TYPE_DECIMAL64, TYPE_FIXED_BINARY, TYPE_FLOAT64, TYPE_INT64,
    TYPE_TIMESTAMP64, TYPE_UTF8, TemporaryFile, UINT64_MAX, block_header, build_file, descriptor,
    int64_column, put_u32, put_u64, schema_header, timestamp_parameters, with_primary_bounds,
};

// ------------------------------------------------------------------- harness

/// Assert that both entry points reject `bytes`, and agree about why.
fn expect_rejected(label: &str, bytes: &[u8]) -> Error {
    let file = TemporaryFile::new(label, bytes);
    let reader = Reader::open(file.path())
        .err()
        .unwrap_or_else(|| panic!("{label}: Reader::open unexpectedly succeeded"));
    let validator = acta::validate(file.path())
        .err()
        .unwrap_or_else(|| panic!("{label}: acta::validate unexpectedly succeeded"));

    assert_eq!(
        reader.kind(),
        validator.kind(),
        "{label}: reader said {reader}, validator said {validator}"
    );
    assert_eq!(
        reader.offset(),
        validator.offset(),
        "{label}: reader and validator disagree about where"
    );
    reader
}

/// Assert that both entry points accept `bytes`, and return the reader.
fn expect_accepted(label: &str, bytes: &[u8]) -> Reader {
    let file = TemporaryFile::new(label, bytes);
    acta::validate(file.path())
        .unwrap_or_else(|error| panic!("{label}: acta::validate rejected it: {error}"));
    Reader::open(file.path())
        .unwrap_or_else(|error| panic!("{label}: Reader::open rejected it: {error}"))
}

fn expect_corruption(label: &str, bytes: &[u8]) {
    let error = expect_rejected(label, bytes);
    assert_eq!(error.kind(), ErrorKind::Corruption, "{label}: {error}");
}

/// A schema frame with the given descriptors and primary selection, no blocks.
fn schema_only(descriptors: &[Vec<u8>], primary_column_id: u32) -> Vec<u8> {
    build_file(
        0,
        &schema_header(1, descriptors.len() as u32, primary_column_id, 0),
        descriptors,
        &[],
    )
}

/// One `int64` column named `value`, with no primary column.
fn one_column() -> Vec<u8> {
    int64_column(1, "value")
}

/// A UTC microsecond `timestamp64` column, valid as a primary selection.
fn timestamp_column(column_id: u32, name: &str) -> Vec<u8> {
    descriptor(
        column_id,
        TYPE_TIMESTAMP64,
        0,
        name,
        &timestamp_parameters(2, 1, ""),
    )
}

/// A single-column timestamp file with one block carrying the given bounds.
fn timestamp_file(minimum: i64, maximum: i64, flags: u32) -> Vec<u8> {
    let mut header = block_header(1, 3, UINT64_MAX, flags);
    with_primary_bounds(&mut header, minimum, maximum);
    build_file(
        0,
        &schema_header(1, 1, 1, 0),
        &[timestamp_column(1, "time")],
        &[header],
    )
}

// ------------------------------------------------------- schema frame header

#[test]
fn a_zero_schema_id_is_corruption() {
    let bytes = build_file(0, &schema_header(0, 1, 0, 0), &[one_column()], &[]);

    expect_corruption("zero-schema-id", &bytes);
}

#[test]
fn a_zero_column_count_is_corruption() {
    let bytes = build_file(0, &schema_header(1, 0, 0, 0), &[], &[]);

    expect_corruption("zero-column-count", &bytes);
}

#[test]
fn nonzero_schema_flags_are_corruption() {
    let bytes = build_file(0, &schema_header(1, 1, 0, 1), &[one_column()], &[]);

    expect_corruption("schema-flags", &bytes);
}

/// The payload bounds the column count more tightly than any configured limit,
/// because every descriptor occupies at least its own 24-byte prefix.
#[test]
fn a_column_count_larger_than_the_payload_can_hold_is_corruption() {
    let bytes = build_file(0, &schema_header(1, 2, 0, 0), &[one_column()], &[]);

    expect_corruption("column-count-beyond-payload", &bytes);
}

#[test]
fn a_column_count_beyond_the_limit_is_a_resource_limit_failure() {
    let columns: Vec<Vec<u8>> = (1..=4)
        .map(|id| int64_column(id, &format!("column{id}")))
        .collect();
    let bytes = schema_only(&columns, 0);
    let limits = acta::Limits::default().with_max_schema_columns(3);
    let file = TemporaryFile::new("column-limit", &bytes);

    let error = Reader::open_with_limits(file.path(), limits).expect_err("limit should apply");

    assert_eq!(error.kind(), ErrorKind::ResourceLimit, "{error}");
}

#[test]
fn descriptors_must_consume_the_whole_payload() {
    let bytes = build_file(
        0,
        &schema_header(1, 1, 0, 0),
        &[one_column(), int64_column(2, "extra")],
        &[],
    );

    expect_corruption("payload-not-consumed", &bytes);
}

// ---------------------------------------------------------- column identity

#[test]
fn a_zero_column_id_is_corruption() {
    let bytes = schema_only(&[int64_column(0, "value")], 0);

    expect_corruption("zero-column-id", &bytes);
}

#[test]
fn duplicate_column_ids_are_corruption() {
    let bytes = schema_only(&[int64_column(1, "first"), int64_column(1, "second")], 0);

    expect_corruption("duplicate-column-id", &bytes);
}

#[test]
fn duplicate_column_names_are_corruption() {
    let bytes = schema_only(&[int64_column(1, "same"), int64_column(2, "same")], 0);

    expect_corruption("duplicate-column-name", &bytes);
}

/// Section 7 requires column IDs to be unique and nonzero. It imposes no order
/// on the schema descriptors; only the section 8.1 column table is sorted.
#[test]
fn unique_but_unsorted_column_ids_are_accepted() {
    let bytes = schema_only(&[int64_column(7, "seven"), int64_column(2, "two")], 0);

    let reader = expect_accepted("unsorted-column-ids", &bytes);

    let ids: Vec<u32> = reader.schema().columns().iter().map(|c| c.id()).collect();
    assert_eq!(ids, vec![7, 2]);
    assert_eq!(reader.schema().column_by_name("two").unwrap().id(), 2);
}

/// Uniqueness must not cost time quadratic in the column count.
#[test]
fn a_wide_schema_opens_promptly() {
    let columns: Vec<Vec<u8>> = (1..=20_000)
        .map(|id| int64_column(id, &format!("column{id}")))
        .collect();
    let bytes = schema_only(&columns, 0);
    let file = TemporaryFile::new("wide-schema", &bytes);

    let start = std::time::Instant::now();
    let reader = Reader::open(file.path()).expect("a wide schema should open");
    let elapsed = start.elapsed();

    assert_eq!(reader.schema().column_count(), 20_000);
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "opening a 20,000-column schema took {elapsed:?}, which suggests a scan per column"
    );
}

// -------------------------------------------------------- descriptor framing

#[test]
fn an_unaligned_descriptor_length_is_corruption() {
    let mut column = one_column();
    let length = column.len() as u32;
    put_u32(&mut column, DESCRIPTOR_LENGTH, length - 1);
    let bytes = schema_only(&[column], 0);

    expect_corruption("unaligned-descriptor", &bytes);
}

#[test]
fn a_descriptor_shorter_than_its_prefix_is_corruption() {
    let mut column = one_column();
    put_u32(&mut column, DESCRIPTOR_LENGTH, 16);
    let bytes = schema_only(&[column], 0);

    expect_corruption("short-descriptor", &bytes);
}

#[test]
fn a_descriptor_extending_beyond_its_payload_is_corruption() {
    let mut column = one_column();
    let length = column.len() as u32;
    put_u32(&mut column, DESCRIPTOR_LENGTH, length + 8);
    let bytes = schema_only(&[column], 0);

    expect_corruption("descriptor-past-payload", &bytes);
}

#[test]
fn a_name_longer_than_its_descriptor_is_corruption() {
    let mut column = one_column();
    put_u32(&mut column, DESCRIPTOR_NAME_LENGTH, 4096);
    let bytes = schema_only(&[column], 0);

    expect_corruption("name-past-descriptor", &bytes);
}

#[test]
fn unknown_column_flags_are_corruption() {
    for bit in 1..16 {
        let column = descriptor(1, TYPE_INT64, 1 << bit, "value", &[]);
        let bytes = schema_only(&[column], 0);

        expect_corruption("column-flags", &bytes);
    }
}

#[test]
fn a_nullable_column_flag_is_accepted() {
    let bytes = schema_only(&[descriptor(1, TYPE_INT64, 1, "value", &[])], 0);

    let reader = expect_accepted("nullable-column", &bytes);

    assert!(reader.schema().columns()[0].is_nullable());
}

#[test]
fn a_column_name_that_is_not_utf8_is_corruption() {
    let mut column = descriptor(1, TYPE_INT64, 0, "ab", &[]);
    column[24] = 0xff;
    let bytes = schema_only(&[column], 0);

    expect_corruption("non-utf8-name", &bytes);
}

// ------------------------------------------------------------- logical types

#[test]
fn every_v0_2_logical_type_is_reconstructed() {
    let expected: Vec<(u16, Vec<u8>, LogicalType)> = vec![
        (1, vec![], LogicalType::Bool),
        (2, vec![], LogicalType::Int8),
        (3, vec![], LogicalType::Int16),
        (4, vec![], LogicalType::Int32),
        (5, vec![], LogicalType::Int64),
        (6, vec![], LogicalType::UInt8),
        (7, vec![], LogicalType::UInt16),
        (8, vec![], LogicalType::UInt32),
        (9, vec![], LogicalType::UInt64),
        (10, vec![], LogicalType::Float32),
        (11, vec![], LogicalType::Float64),
        (
            12,
            decimal_parameters(9, -2),
            LogicalType::Decimal {
                precision: 9,
                scale: -2,
            },
        ),
        (
            13,
            timestamp_parameters(3, 0, ""),
            LogicalType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: TimeZone::Naive,
            },
        ),
        (14, vec![], LogicalType::Utf8),
        (
            15,
            categorical_parameters(true),
            LogicalType::Categorical { ordered: true },
        ),
        (16, vec![], LogicalType::Binary),
        (
            17,
            fixed_binary_parameters(12),
            LogicalType::FixedBinary { byte_width: 12 },
        ),
        (18, vec![], LogicalType::Date32),
    ];

    let columns: Vec<Vec<u8>> = expected
        .iter()
        .enumerate()
        .map(|(index, (type_id, parameters, _))| {
            descriptor(
                index as u32 + 1,
                *type_id,
                0,
                &format!("c{type_id}"),
                parameters,
            )
        })
        .collect();
    let bytes = schema_only(&columns, 0);

    let reader = expect_accepted("every-logical-type", &bytes);

    for (index, (type_id, _, logical_type)) in expected.iter().enumerate() {
        assert_eq!(
            reader.schema().columns()[index].logical_type(),
            logical_type,
            "logical type {type_id}"
        );
    }
}

fn decimal_parameters(precision: u16, scale: i16) -> Vec<u8> {
    let mut bytes = vec![0_u8; 8];
    bytes[0..2].copy_from_slice(&precision.to_le_bytes());
    bytes[2..4].copy_from_slice(&scale.to_le_bytes());
    bytes
}

fn categorical_parameters(ordered: bool) -> Vec<u8> {
    let mut bytes = vec![0_u8; 8];
    bytes[0] = u8::from(ordered);
    bytes
}

fn fixed_binary_parameters(byte_width: u32) -> Vec<u8> {
    let mut bytes = vec![0_u8; 8];
    bytes[0..4].copy_from_slice(&byte_width.to_le_bytes());
    bytes
}

#[test]
fn an_unknown_logical_type_is_unsupported() {
    for type_id in [0_u16, 19, 65535] {
        let bytes = schema_only(&[descriptor(1, type_id, 0, "value", &[])], 0);

        let error = expect_rejected("unknown-logical-type", &bytes);
        assert_eq!(error.kind(), ErrorKind::UnsupportedFrame, "{error}");
    }
}

#[test]
fn parameters_on_a_parameterless_type_are_corruption() {
    let bytes = schema_only(&[descriptor(1, TYPE_INT64, 0, "value", &[0; 8])], 0);

    expect_corruption("unexpected-parameters", &bytes);
}

#[test]
fn a_decimal_precision_outside_one_to_eighteen_is_corruption() {
    for precision in [0_u16, 19, 65535] {
        let column = descriptor(
            1,
            TYPE_DECIMAL64,
            0,
            "amount",
            &decimal_parameters(precision, 2),
        );

        expect_corruption("decimal-precision", &schema_only(&[column], 0));
    }
}

#[test]
fn an_unknown_timestamp_unit_is_corruption() {
    let column = descriptor(
        1,
        TYPE_TIMESTAMP64,
        0,
        "time",
        &timestamp_parameters(4, 0, ""),
    );

    expect_corruption("timestamp-unit", &schema_only(&[column], 0));
}

#[test]
fn a_timezone_mode_that_disagrees_with_its_name_is_corruption() {
    // Modes zero and one are name-free; mode two requires a name.
    for (mode, name) in [(0_u8, "UTC-ish"), (1, "UTC-ish"), (2, ""), (3, "")] {
        let column = descriptor(
            1,
            TYPE_TIMESTAMP64,
            0,
            "time",
            &timestamp_parameters(2, mode, name),
        );

        expect_corruption("timezone-mode", &schema_only(&[column], 0));
    }
}

#[test]
fn an_iana_timezone_name_is_reconstructed() {
    let column = descriptor(
        1,
        TYPE_TIMESTAMP64,
        0,
        "time",
        &timestamp_parameters(2, 2, "America/New_York"),
    );
    let bytes = schema_only(&[column], 1);

    let reader = expect_accepted("iana-timezone", &bytes);

    assert_eq!(
        reader.schema().columns()[0].logical_type(),
        &LogicalType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: TimeZone::Iana("America/New_York".to_owned()),
        }
    );
}

/// Section 7 pads the `timestamp64` parameter record itself, so a record that
/// stops at the end of its timezone name is not a v0.2 record.
#[test]
fn an_unpadded_timestamp_parameter_record_is_corruption() {
    let mut parameters = vec![0_u8; 11];
    parameters[0] = 2;
    parameters[1] = 2;
    put_u32(&mut parameters, 4, 3);
    parameters[8..11].copy_from_slice(b"UTC");
    let column = descriptor(1, TYPE_TIMESTAMP64, 0, "time", &parameters);

    expect_corruption("unpadded-timestamp-parameters", &schema_only(&[column], 0));
}

#[test]
fn a_timezone_name_that_is_not_utf8_is_corruption() {
    let mut parameters = timestamp_parameters(2, 2, "abcd");
    parameters[8] = 0xff;
    let column = descriptor(1, TYPE_TIMESTAMP64, 0, "time", &parameters);

    expect_corruption("non-utf8-timezone", &schema_only(&[column], 0));
}

#[test]
fn a_categorical_ordered_flag_above_one_is_corruption() {
    let mut parameters = categorical_parameters(false);
    parameters[0] = 2;
    let column = descriptor(1, 15, 0, "label", &parameters);

    expect_corruption("categorical-ordered", &schema_only(&[column], 0));
}

#[test]
fn a_zero_fixed_binary_width_is_corruption() {
    let column = descriptor(1, TYPE_FIXED_BINARY, 0, "key", &fixed_binary_parameters(0));

    expect_corruption("fixed-binary-width", &schema_only(&[column], 0));
}

#[test]
fn a_type_parameter_length_beyond_its_descriptor_is_corruption() {
    let mut column = descriptor(1, TYPE_DECIMAL64, 0, "amount", &decimal_parameters(9, 2));
    put_u32(&mut column, DESCRIPTOR_PARAMETERS_LENGTH, 4096);

    expect_corruption("parameters-past-descriptor", &schema_only(&[column], 0));
}

// --------------------------------------------------------- primary selection

#[test]
fn a_primary_column_id_that_is_not_declared_is_corruption() {
    let bytes = schema_only(&[timestamp_column(1, "time")], 9);

    expect_corruption("undeclared-primary", &bytes);
}

#[test]
fn a_nullable_primary_column_is_corruption() {
    let column = descriptor(
        1,
        TYPE_TIMESTAMP64,
        1,
        "time",
        &timestamp_parameters(2, 1, ""),
    );

    expect_corruption("nullable-primary", &schema_only(&[column], 1));
}

#[test]
fn a_primary_column_that_is_neither_timestamp_nor_date_is_corruption() {
    for type_id in [TYPE_INT64, TYPE_UTF8, TYPE_FLOAT64] {
        let column = descriptor(1, type_id, 0, "not_time", &[]);

        expect_corruption("non-temporal-primary", &schema_only(&[column], 1));
    }
}

#[test]
fn a_date32_column_may_be_the_primary_selection() {
    let bytes = schema_only(&[descriptor(1, TYPE_DATE32, 0, "day", &[])], 1);

    let reader = expect_accepted("date32-primary", &bytes);

    assert_eq!(reader.schema().primary_column_id(), Some(1));
    assert_eq!(
        reader.schema().primary_column().unwrap().logical_type(),
        &LogicalType::Date32
    );
}

#[test]
fn a_schema_without_a_primary_column_reports_none() {
    let reader = expect_accepted("no-primary", &schema_only(&[one_column()], 0));

    assert_eq!(reader.schema().primary_column_id(), None);
    assert!(reader.schema().primary_column().is_none());
}

// ---------------------------------------------------------- block geometry

/// Build a valid one-column, one-block file and let the caller damage the
/// block header before it is framed.
fn block_file(damage: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut header = block_header(1, 3, UINT64_MAX, 0);
    damage(&mut header);
    build_file(0, &schema_header(1, 1, 0, 0), &[one_column()], &[header])
}

#[test]
fn a_valid_single_block_file_is_accepted() {
    let reader = expect_accepted("valid-block", &block_file(|_| {}));

    assert_eq!(reader.blocks().len(), 1);
    assert_eq!(reader.blocks()[0].row_count(), 3);
    assert_eq!(reader.total_rows(), 3);
}

#[test]
fn a_zero_row_count_is_corruption() {
    let bytes = block_file(|header| put_u32(header, BLOCK_ROW_COUNT, 0));

    expect_corruption("zero-row-count", &bytes);
}

#[test]
fn a_block_schema_id_that_does_not_match_the_schema_is_corruption() {
    let bytes = block_file(|header| put_u64(header, BLOCK_SCHEMA_ID, 2));

    expect_corruption("block-schema-id", &bytes);
}

#[test]
fn a_block_column_count_that_does_not_match_the_schema_is_corruption() {
    for count in [0_u32, 2] {
        let bytes = block_file(|header| put_u32(header, BLOCK_COLUMN_COUNT, count));

        expect_corruption("block-column-count", &bytes);
    }
}

#[test]
fn a_column_table_offset_other_than_64_is_corruption() {
    let bytes = block_file(|header| put_u32(header, BLOCK_COLUMN_TABLE_OFFSET, 72));

    expect_corruption("column-table-offset", &bytes);
}

/// Section 8 places the stream table immediately after the column table, so a
/// gap or an overlap is a layout the format cannot express.
#[test]
fn a_stream_table_offset_that_does_not_follow_the_column_table_is_corruption() {
    for offset in [64_u32, 88, 104] {
        let bytes = block_file(|header| put_u32(header, BLOCK_STREAM_TABLE_OFFSET, offset));

        expect_corruption("stream-table-offset", &bytes);
    }
}

#[test]
fn a_statistics_area_before_the_stream_table_is_corruption() {
    let bytes = block_file(|header| put_u32(header, BLOCK_STATISTICS_OFFSET, 64));

    expect_corruption("statistics-before-streams", &bytes);
}

#[test]
fn a_stream_table_length_that_is_not_a_multiple_of_48_is_corruption() {
    let stream_table = (BLOCK_HEADER_SIZE + 32) as u32;
    let bytes = block_file(|header| {
        header.resize(header.len() + 48, 0);
        put_u32(header, BLOCK_STATISTICS_OFFSET, stream_table + 8);
    });

    expect_corruption("stream-table-length", &bytes);
}

#[test]
fn a_statistics_area_beyond_the_padded_header_is_corruption() {
    let bytes = block_file(|header| put_u32(header, BLOCK_STATISTICS_LENGTH, 4096));

    expect_corruption("statistics-past-header", &bytes);
}

#[test]
fn unknown_block_flags_are_corruption() {
    for bit in 2..32 {
        let bytes = block_file(|header| put_u32(header, BLOCK_FLAGS, 1 << bit));

        expect_corruption("block-flags", &bytes);
    }
}

// ------------------------------------------------------------ implicit row IDs

/// A file with `ROW_IDS` enabled and a correctly chained base row ID per block.
fn row_id_file(bases: &[u64], rows: u32) -> Vec<u8> {
    let blocks: Vec<Vec<u8>> = bases
        .iter()
        .map(|&base| block_header(1, rows, base, ROW_IDS_BLOCK_FLAG))
        .collect();
    build_file(1, &schema_header(1, 1, 0, 0), &[one_column()], &blocks)
}

#[test]
fn the_base_row_id_chain_accumulates_row_counts() {
    let reader = expect_accepted("row-id-chain", &row_id_file(&[0, 4, 8], 4));

    let bases: Vec<Option<u64>> = reader.blocks().iter().map(|b| b.base_row_id()).collect();
    assert_eq!(bases, vec![Some(0), Some(4), Some(8)]);
    assert_eq!(reader.total_rows(), 12);
    assert_eq!(reader.file_metadata().feature_flags(), 1);
}

#[test]
fn a_first_base_row_id_other_than_zero_is_corruption() {
    expect_corruption("row-id-first-base", &row_id_file(&[1], 4));
}

#[test]
fn a_base_row_id_that_breaks_the_chain_is_corruption() {
    expect_corruption("row-id-broken-chain", &row_id_file(&[0, 5], 4));
}

#[test]
fn a_block_row_ids_flag_that_disagrees_with_the_file_is_corruption() {
    // The feature is enabled, but the block does not declare it.
    let enabled = build_file(
        1,
        &schema_header(1, 1, 0, 0),
        &[one_column()],
        &[block_header(1, 3, 0, 0)],
    );
    expect_corruption("row-ids-block-flag-clear", &enabled);

    // The feature is disabled, but the block declares it.
    let disabled = build_file(
        0,
        &schema_header(1, 1, 0, 0),
        &[one_column()],
        &[block_header(1, 3, 0, ROW_IDS_BLOCK_FLAG)],
    );
    expect_corruption("row-ids-block-flag-set", &disabled);
}

#[test]
fn a_base_row_id_other_than_uint64_max_without_the_feature_is_corruption() {
    let bytes = build_file(
        0,
        &schema_header(1, 1, 0, 0),
        &[one_column()],
        &[block_header(1, 3, 0, 0)],
    );

    expect_corruption("base-row-id-without-feature", &bytes);
}

#[test]
fn row_ids_disabled_reports_no_base_row_id() {
    let reader = expect_accepted("no-row-ids", &block_file(|_| {}));

    assert_eq!(reader.blocks()[0].base_row_id(), None);
}

// ------------------------------------------------------------ primary bounds

#[test]
fn primary_bounds_and_ts_sorted_are_exposed_together() {
    let bytes = timestamp_file(1_000, 3_000, TS_SORTED_BLOCK_FLAG);

    let reader = expect_accepted("primary-bounds", &bytes);

    let bounds = reader.blocks()[0].primary_bounds().expect("bounds");
    assert_eq!((bounds.min(), bounds.max()), (1_000, 3_000));
    assert!(reader.blocks()[0].ts_sorted());
}

#[test]
fn negative_primary_bounds_are_signed() {
    let bytes = timestamp_file(-3_000, -1_000, 0);

    let reader = expect_accepted("negative-bounds", &bytes);

    let bounds = reader.blocks()[0].primary_bounds().expect("bounds");
    assert_eq!((bounds.min(), bounds.max()), (-3_000, -1_000));
    assert!(!reader.blocks()[0].ts_sorted());
}

#[test]
fn a_primary_minimum_above_its_maximum_is_corruption() {
    expect_corruption("inverted-bounds", &timestamp_file(3_000, 1_000, 0));
}

#[test]
fn nonzero_bounds_without_a_primary_column_are_corruption() {
    for (minimum, maximum) in [(1_i64, 0_i64), (0, 1), (1, 1)] {
        let bytes = block_file(|header| with_primary_bounds(header, minimum, maximum));

        expect_corruption("bounds-without-primary", &bytes);
    }
}

#[test]
fn ts_sorted_without_a_primary_column_is_corruption() {
    let bytes = block_file(|header| put_u32(header, BLOCK_FLAGS, TS_SORTED_BLOCK_FLAG));

    expect_corruption("ts-sorted-without-primary", &bytes);
}
