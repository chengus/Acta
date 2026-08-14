//! Stage 3 validation-level and optional-statistics coverage.
//!
//! Section 11 statistics are optional and no writer in this crate emits them,
//! so every file here is built by splicing a statistics area into a file that
//! is otherwise ordinary. The splice is the only way to reach the verification
//! code at all, and doing it to real Stage 7b writer output rather than to a
//! hand-assembled block keeps the surrounding frame honest.
//!
//! The parsing helpers themselves are unit-tested next to the code, in
//! `rust/validate/statistics.rs`. What is tested here is the part that only a
//! whole file can show: that a claim spliced into a real block is read from the
//! right bytes, checked against the right decoded values, and reported with the
//! right column.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use acta::{
    Array, BinaryArray, BooleanArray, Column, DecimalArray, Error, ErrorKind, LogicalType,
    PrimitiveArray, RecordBatch, Schema, TimeUnit, TimeZone, TimestampArray, Utf8Array,
    Utf8Array as CategoricalArray, ValidationLevel, ValidationOptions, Writer, WriterOptions,
};
use common::{
    BLOCK_FLAGS, BLOCK_HEADER_SIZE, BLOCK_PRIMARY_MAX, BLOCK_PRIMARY_MIN, BLOCK_STATISTICS_LENGTH,
    BLOCK_STATISTICS_OFFSET, BLOCK_STREAM_TABLE_OFFSET, COLUMN_IMPLICIT_VALIDITY,
    PREFIX_HEADER_LENGTH, PREFIX_SIZE, STREAM_CODEC, STREAM_TRANSFORM, STREAM_VALUES,
    TRANSFORM_RAW, TS_SORTED_BLOCK_FLAG, TYPE_FIXED_BINARY, TYPE_FLOAT64, TYPE_TIMESTAMP64,
    TemporaryFile, TestColumn, TestStream, column_file, data_frame_offset, descriptor, fixture,
    int64_column, payload_offset, put_u16, put_u32, put_u64, repair_frame, schema_frame_offset,
    timestamp_parameters,
};

const COLUMN_HAS_STATS: u16 = 2;
const STATS_MIN_MAX: u16 = 1;
const COLUMN_DESCRIPTOR_SIZE: usize = 32;

fn full(label: &str, bytes: &[u8]) -> Result<acta::ValidationReport, Error> {
    let file = TemporaryFile::new(&label.replace('/', "-"), bytes);
    acta::validate_with_options(
        file.path(),
        ValidationOptions::default().with_level(ValidationLevel::Full),
    )
}

/// Splice a statistics area holding `statistics` into the first data block and
/// point column zero's descriptor at it.
fn with_min_max(bytes: Vec<u8>, statistics: &[u8]) -> Vec<u8> {
    with_min_max_for(bytes, 0, statistics)
}

/// The same splice, but claiming the statistics for column `column_index`.
///
/// Section 8.1 numbers descriptors by position in the column table, which is
/// schema column-ID order, so the index is the schema position.
fn with_min_max_for(mut bytes: Vec<u8>, column_index: usize, statistics: &[u8]) -> Vec<u8> {
    let frame_offset = data_frame_offset(&bytes);
    let header_length_offset = frame_offset + PREFIX_HEADER_LENGTH;
    let old_header_length = common::read_u32(&bytes, header_length_offset) as usize;
    let old_payload_offset = payload_offset(&bytes, frame_offset);
    let padded_length = statistics.len().next_multiple_of(8);
    let mut stored_statistics = statistics.to_vec();
    stored_statistics.resize(padded_length, 0);
    bytes.splice(old_payload_offset..old_payload_offset, stored_statistics);

    let header_offset = frame_offset + PREFIX_SIZE;
    put_u32(
        &mut bytes,
        header_length_offset,
        (old_header_length + padded_length) as u32,
    );
    put_u32(
        &mut bytes,
        header_offset + BLOCK_STATISTICS_OFFSET,
        old_header_length as u32,
    );
    put_u32(
        &mut bytes,
        header_offset + BLOCK_STATISTICS_LENGTH,
        padded_length as u32,
    );
    let descriptor = header_offset + BLOCK_HEADER_SIZE + column_index * COLUMN_DESCRIPTOR_SIZE;
    let flags = common::read_u16(&bytes, descriptor + 6) | COLUMN_HAS_STATS;
    put_u16(&mut bytes, descriptor + 6, flags);
    put_u16(&mut bytes, descriptor + 22, STATS_MIN_MAX);
    put_u32(&mut bytes, descriptor + 24, old_header_length as u32);
    put_u32(&mut bytes, descriptor + 28, statistics.len() as u32);
    repair_frame(&mut bytes, frame_offset);
    bytes
}

fn int64_file(values: &[i64]) -> Vec<u8> {
    let payload = values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect();
    column_file(
        values.len() as u32,
        &TestColumn::new(
            int64_column(1, "value"),
            0,
            vec![TestStream::new(
                STREAM_VALUES,
                TRANSFORM_RAW,
                values.len() as u64,
                payload,
            )],
        ),
    )
}

// ------------------------------------------------- writer-produced base files

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

/// Serialize `arrays` through the Stage 7b writer and return the file's bytes.
///
/// The statistics tests need a real block to splice into: one whose streams,
/// transforms, validity representation, and padding are whatever the writer
/// actually chose, so a verification that only works against hand-built blocks
/// would not pass here.
fn written(columns: Vec<Column>, arrays: Vec<Array>, rows: usize) -> Vec<u8> {
    let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "acta-full-validation-{}-{id}.acta",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let schema = Schema::new(3, columns, None);
    let mut writer =
        Writer::create(&path, schema.clone(), WriterOptions::default()).expect("create");
    writer
        .append(RecordBatch::try_new(Arc::new(schema), arrays, rows).expect("batch"))
        .expect("append");
    let _ = writer.finish().expect("finish");
    let bytes = std::fs::read(&path).expect("read");
    let _ = std::fs::remove_file(&path);
    bytes
}

fn one_column(name: &str, logical_type: LogicalType, array: Array, rows: usize) -> Vec<u8> {
    written(
        vec![Column::new(1, name, logical_type, true)],
        vec![array],
        rows,
    )
}

// --------------------------------------------------------------- entry points

#[test]
fn default_and_explicit_validation_levels_have_the_documented_defaults() {
    let options = ValidationOptions::default();
    assert_eq!(options.level(), ValidationLevel::Structural);
    assert_eq!(options.limits(), acta::Limits::default());
    assert_eq!(
        options.with_level(ValidationLevel::Full).level(),
        ValidationLevel::Full
    );
}

#[test]
fn full_validation_decodes_every_checked_in_fixture() {
    for (relative_path, _) in common::FIXTURES {
        let report = full(relative_path, &fixture(relative_path))
            .unwrap_or_else(|error| panic!("{relative_path}: full validation failed: {error}"));
        assert!(!report.incomplete_tail(), "{relative_path}");
    }
}

#[test]
fn structural_validation_can_pass_when_full_validation_reaches_a_bad_stream() {
    let mut bytes = int64_file(&[1, 2, 3]);
    let frame_offset = data_frame_offset(&bytes);
    let payload = payload_offset(&bytes, frame_offset);
    bytes[payload] ^= 0x80;
    repair_frame(&mut bytes, frame_offset);

    let file = TemporaryFile::new("hidden-stream-corruption", &bytes);
    assert!(acta::validate(file.path()).is_ok());
    assert_eq!(
        acta::validate_with_options(
            file.path(),
            ValidationOptions::default().with_level(ValidationLevel::Full),
        )
        .unwrap_err()
        .kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn full_and_structural_validation_report_the_same_incomplete_tail() {
    let bytes = fixture("minimal/minimal.acta");
    let cut = data_frame_offset(&bytes) + 1;
    let structural_file = TemporaryFile::new("structural-tail", &bytes[..cut]);
    let full_file = TemporaryFile::new("full-tail", &bytes[..cut]);
    let structural = acta::validate(structural_file.path()).unwrap();
    let full = acta::validate_with_options(
        full_file.path(),
        ValidationOptions::default().with_level(ValidationLevel::Full),
    )
    .unwrap();
    assert_eq!(structural, full);
}

// ------------------------------------------------- statistics, type by type

/// One case: a column, its values, and the canonical bytes of its true
/// extrema.
struct TypeCase {
    name: &'static str,
    logical_type: LogicalType,
    array: Array,
    rows: usize,
    minimum: Vec<u8>,
    maximum: Vec<u8>,
}

fn type_cases() -> Vec<TypeCase> {
    fn case(
        name: &'static str,
        logical_type: LogicalType,
        array: Array,
        rows: usize,
        minimum: Vec<u8>,
        maximum: Vec<u8>,
    ) -> TypeCase {
        TypeCase {
            name,
            logical_type,
            array,
            rows,
            minimum,
            maximum,
        }
    }

    vec![
        case(
            "bool",
            LogicalType::Bool,
            Array::Bool(BooleanArray::new(vec![false, true, true], None)),
            3,
            vec![0],
            vec![1],
        ),
        case(
            "int8",
            LogicalType::Int8,
            Array::Int8(PrimitiveArray::new(vec![-5, 0, 7], None)),
            3,
            (-5_i8).to_le_bytes().to_vec(),
            7_i8.to_le_bytes().to_vec(),
        ),
        case(
            "int16",
            LogicalType::Int16,
            Array::Int16(PrimitiveArray::new(vec![i16::MIN, 0, i16::MAX], None)),
            3,
            i16::MIN.to_le_bytes().to_vec(),
            i16::MAX.to_le_bytes().to_vec(),
        ),
        case(
            "int32",
            LogicalType::Int32,
            Array::Int32(PrimitiveArray::new(vec![i32::MIN, 0, i32::MAX], None)),
            3,
            i32::MIN.to_le_bytes().to_vec(),
            i32::MAX.to_le_bytes().to_vec(),
        ),
        case(
            "int64",
            LogicalType::Int64,
            Array::Int64(PrimitiveArray::new(vec![i64::MIN, 0, i64::MAX], None)),
            3,
            i64::MIN.to_le_bytes().to_vec(),
            i64::MAX.to_le_bytes().to_vec(),
        ),
        case(
            "uint8",
            LogicalType::UInt8,
            Array::UInt8(PrimitiveArray::new(vec![0, 128, u8::MAX], None)),
            3,
            0_u8.to_le_bytes().to_vec(),
            u8::MAX.to_le_bytes().to_vec(),
        ),
        case(
            "uint16",
            LogicalType::UInt16,
            Array::UInt16(PrimitiveArray::new(vec![0, 40_000, u16::MAX], None)),
            3,
            0_u16.to_le_bytes().to_vec(),
            u16::MAX.to_le_bytes().to_vec(),
        ),
        case(
            "uint32",
            LogicalType::UInt32,
            Array::UInt32(PrimitiveArray::new(vec![0, 3_000_000_000, u32::MAX], None)),
            3,
            0_u32.to_le_bytes().to_vec(),
            u32::MAX.to_le_bytes().to_vec(),
        ),
        // Above `i64::MAX`, so a signed reading of these bytes would order
        // them the other way round.
        case(
            "uint64",
            LogicalType::UInt64,
            Array::UInt64(PrimitiveArray::new(
                vec![1, u64::MAX, u64::MAX / 2 + 1],
                None,
            )),
            3,
            1_u64.to_le_bytes().to_vec(),
            u64::MAX.to_le_bytes().to_vec(),
        ),
        case(
            "float32",
            LogicalType::Float32,
            Array::Float32(PrimitiveArray::new(vec![-1.5, 0.0, 2.5], None)),
            3,
            (-1.5_f32).to_le_bytes().to_vec(),
            2.5_f32.to_le_bytes().to_vec(),
        ),
        case(
            "float64",
            LogicalType::Float64,
            Array::Float64(PrimitiveArray::new(vec![-1.5, 0.0, 2.5], None)),
            3,
            (-1.5_f64).to_le_bytes().to_vec(),
            2.5_f64.to_le_bytes().to_vec(),
        ),
        case(
            "decimal64",
            LogicalType::Decimal {
                precision: 18,
                scale: 4,
            },
            Array::Decimal(DecimalArray::new(vec![-1_000, 0, 5_000], None, 18, 4)),
            3,
            (-1_000_i64).to_le_bytes().to_vec(),
            5_000_i64.to_le_bytes().to_vec(),
        ),
        case(
            "timestamp64",
            LogicalType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: TimeZone::Naive,
            },
            Array::Timestamp(TimestampArray::new(
                vec![-7, 0, 99],
                None,
                TimeUnit::Nanosecond,
                TimeZone::Naive,
            )),
            3,
            (-7_i64).to_le_bytes().to_vec(),
            99_i64.to_le_bytes().to_vec(),
        ),
        case(
            "date32",
            LogicalType::Date32,
            Array::Date32(PrimitiveArray::new(vec![-100, 0, 20_000], None)),
            3,
            (-100_i32).to_le_bytes().to_vec(),
            20_000_i32.to_le_bytes().to_vec(),
        ),
        // Ordered lexicographically, so the extrema differ from the values a
        // leading-byte comparison would pick.
        case(
            "fixed_binary",
            LogicalType::FixedBinary { byte_width: 3 },
            Array::FixedBinary(BinaryArray::new(
                vec![
                    vec![0x01, 0xff, 0xff],
                    vec![0x02, 0x00, 0x00],
                    vec![0x01, 0xff, 0xfe],
                ],
                None,
            )),
            3,
            vec![0x01, 0xff, 0xfe],
            vec![0x02, 0x00, 0x00],
        ),
    ]
}

#[test]
fn every_supported_logical_type_accepts_its_true_min_and_max() {
    for case in type_cases() {
        let base = one_column(case.name, case.logical_type, case.array, case.rows);
        let mut statistics = case.minimum.clone();
        statistics.extend_from_slice(&case.maximum);
        full(case.name, &with_min_max(base, &statistics))
            .unwrap_or_else(|error| panic!("{}: a true claim was rejected: {error}", case.name));
    }
}

#[test]
fn every_supported_logical_type_rejects_a_false_min_or_max() {
    for case in type_cases() {
        let base = one_column(case.name, case.logical_type, case.array, case.rows);

        // A minimum that is really the maximum.
        let mut raised = case.maximum.clone();
        raised.extend_from_slice(&case.maximum);
        let error = full(
            &format!("{}-false-min", case.name),
            &with_min_max(base.clone(), &raised),
        )
        .expect_err(case.name);
        assert_eq!(error.kind(), ErrorKind::Corruption, "{}", case.name);
        assert!(
            error.to_string().contains(case.name),
            "{}: the error does not name the column: {error}",
            case.name
        );

        // A maximum that is really the minimum.
        let mut lowered = case.minimum.clone();
        lowered.extend_from_slice(&case.minimum);
        assert_eq!(
            full(
                &format!("{}-false-max", case.name),
                &with_min_max(base.clone(), &lowered)
            )
            .expect_err(case.name)
            .kind(),
            ErrorKind::Corruption,
            "{}",
            case.name
        );

        // A minimum above its own maximum, which is unsatisfiable whatever
        // the column holds.
        let mut reversed = case.maximum.clone();
        reversed.extend_from_slice(&case.minimum);
        assert_eq!(
            full(
                &format!("{}-reversed", case.name),
                &with_min_max(base.clone(), &reversed)
            )
            .expect_err(case.name)
            .kind(),
            ErrorKind::Corruption,
            "{}",
            case.name
        );

        // One byte short of the canonical pair.
        let mut short = case.minimum.clone();
        short.extend_from_slice(&case.maximum);
        short.pop();
        assert_eq!(
            full(&format!("{}-short", case.name), &with_min_max(base, &short))
                .expect_err(case.name)
                .kind(),
            ErrorKind::Corruption,
            "{}",
            case.name
        );
    }
}

#[test]
fn unsupported_logical_types_cannot_carry_min_max() {
    let cases: Vec<(&str, LogicalType, Array)> = vec![
        (
            "text",
            LogicalType::Utf8,
            Array::Utf8(Utf8Array::new(vec!["a".into(), "b".into()], None)),
        ),
        (
            "cat",
            LogicalType::Categorical { ordered: false },
            Array::Categorical(CategoricalArray::new(vec!["a".into(), "b".into()], None)),
        ),
        (
            "bin",
            LogicalType::Binary,
            Array::Binary(BinaryArray::new(vec![vec![1], vec![2]], None)),
        ),
    ];
    for (name, logical_type, array) in cases {
        let base = one_column(name, logical_type, array, 2);
        let error = full(name, &with_min_max(base, &[0; 16])).expect_err(name);
        assert_eq!(error.kind(), ErrorKind::Corruption, "{name}");
        assert!(error.to_string().contains(name), "{name}: {error}");
    }
}

// -------------------------------------------------------- null and NaN rules

#[test]
fn nulls_are_excluded_from_the_verified_extrema() {
    let base = one_column(
        "value",
        LogicalType::Int64,
        Array::Int64(PrimitiveArray::new(
            vec![100, 5, 0, 20],
            Some(vec![false, true, false, true]),
        )),
        4,
    );
    let mut statistics = 5_i64.to_le_bytes().to_vec();
    statistics.extend_from_slice(&20_i64.to_le_bytes());
    full("nulls-excluded", &with_min_max(base.clone(), &statistics))
        .expect("the null rows are not candidates");

    // The extrema a reader would get if it counted the null slots.
    let mut counting_nulls = 0_i64.to_le_bytes().to_vec();
    counting_nulls.extend_from_slice(&100_i64.to_le_bytes());
    assert_eq!(
        full("nulls-counted", &with_min_max(base, &counting_nulls))
            .expect_err("null slots are not values")
            .kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn nans_are_excluded_and_infinities_are_not() {
    let base = one_column(
        "value",
        LogicalType::Float64,
        Array::Float64(PrimitiveArray::new(
            vec![f64::NAN, -2.5, f64::INFINITY, f64::NEG_INFINITY],
            None,
        )),
        4,
    );
    let mut statistics = f64::NEG_INFINITY.to_le_bytes().to_vec();
    statistics.extend_from_slice(&f64::INFINITY.to_le_bytes());
    full("nan-excluded", &with_min_max(base.clone(), &statistics))
        .expect("infinities bound the column and the NaN is ignored");

    // A NaN bound is not a bound at all.
    let mut nan_bound = f64::NAN.to_le_bytes().to_vec();
    nan_bound.extend_from_slice(&f64::INFINITY.to_le_bytes());
    assert_eq!(
        full("nan-bound", &with_min_max(base, &nan_bound))
            .expect_err("NaN cannot be a minimum")
            .kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn an_all_nan_or_all_null_column_cannot_claim_a_min_or_max() {
    let all_nan = one_column(
        "value",
        LogicalType::Float64,
        Array::Float64(PrimitiveArray::new(vec![f64::NAN, f64::NAN], None)),
        2,
    );
    let mut statistics = 1_f64.to_le_bytes().to_vec();
    statistics.extend_from_slice(&1_f64.to_le_bytes());
    assert_eq!(
        full("all-nan", &with_min_max(all_nan, &statistics))
            .expect_err("an all-NaN column has no extrema")
            .kind(),
        ErrorKind::Corruption
    );

    let all_null = one_column(
        "value",
        LogicalType::Int64,
        Array::Int64(PrimitiveArray::new(vec![0, 0], Some(vec![false, false]))),
        2,
    );
    let mut statistics = 0_i64.to_le_bytes().to_vec();
    statistics.extend_from_slice(&1_i64.to_le_bytes());
    assert_eq!(
        full("all-null-written", &with_min_max(all_null, &statistics))
            .expect_err("an all-null column has no extrema")
            .kind(),
        ErrorKind::Corruption
    );
}

/// The same rule reached through the hand-built implicit all-null encoding,
/// where the block stores no values stream at all.
#[test]
fn a_present_statistic_on_an_implicit_all_null_column_is_corruption() {
    let column = TestColumn::new(int64_column(1, "value"), 0, Vec::new())
        .with_nulls(2, COLUMN_IMPLICIT_VALIDITY);
    let bytes = column_file(2, &column);
    let statistics = [0_i64.to_le_bytes(), 1_i64.to_le_bytes()].concat();
    assert_eq!(
        full("all-null-statistics", &with_min_max(bytes, &statistics))
            .unwrap_err()
            .kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn either_signed_zero_encoding_satisfies_a_zero_bound() {
    let base = one_column(
        "value",
        LogicalType::Float64,
        Array::Float64(PrimitiveArray::new(vec![-0.0, 0.0], None)),
        2,
    );
    // Section 3 preserves the stored sign of zero, but a section 11 bound is a
    // numeric claim and both encodings are the same number, so all four
    // spellings describe this column correctly.
    for (label, minimum, maximum) in [
        ("pos-pos", 0.0_f64, 0.0_f64),
        ("neg-neg", -0.0, -0.0),
        ("neg-pos", -0.0, 0.0),
        ("pos-neg", 0.0, -0.0),
    ] {
        let mut statistics = minimum.to_le_bytes().to_vec();
        statistics.extend_from_slice(&maximum.to_le_bytes());
        full(
            &format!("signed-zero-{label}"),
            &with_min_max(base.clone(), &statistics),
        )
        .unwrap_or_else(|error| panic!("{label}: {error}"));
    }
}

#[test]
fn fixed_binary_statistics_use_canonical_lexicographic_order() {
    let mut parameters = vec![0_u8; 8];
    put_u32(&mut parameters, 0, 2);
    let bytes = column_file(
        2,
        &TestColumn::new(
            descriptor(1, TYPE_FIXED_BINARY, 0, "value", &parameters),
            0,
            vec![TestStream::new(
                STREAM_VALUES,
                TRANSFORM_RAW,
                2,
                vec![2, 0, 1, 255],
            )],
        ),
    );
    full(
        "fixed-binary-statistics",
        &with_min_max(bytes.clone(), &[1, 255, 2, 0]),
    )
    .unwrap_or_else(|error| panic!("fixed binary statistics failed: {error}"));

    // `[1, 255]` and `[2, 0]` order one way byte by byte and the other way if
    // only the trailing byte is compared, so this claim distinguishes the two.
    assert_eq!(
        full(
            "fixed-binary-wrong-order",
            &with_min_max(bytes, &[2, 0, 1, 255])
        )
        .expect_err("the minimum is above the maximum lexicographically")
        .kind(),
        ErrorKind::Corruption
    );
}

// --------------------------------------------------- per-column attribution

/// Verification is attached to the column it describes, not to the block.
///
/// The error names the offending column, which is what lets Stage 5 verify
/// only the columns a projection decoded: the check has no dependency on any
/// other column having been reconstructed.
#[test]
fn a_false_statistic_is_reported_against_the_column_that_carries_it() {
    let columns = vec![
        Column::new(1, "first", LogicalType::Int64, false),
        Column::new(2, "second", LogicalType::Int64, false),
        Column::new(3, "third", LogicalType::Int64, false),
    ];
    let arrays = vec![
        Array::Int64(PrimitiveArray::new(vec![1, 2, 3], None)),
        Array::Int64(PrimitiveArray::new(vec![10, 20, 30], None)),
        Array::Int64(PrimitiveArray::new(vec![100, 200, 300], None)),
    ];
    let base = written(columns, arrays, 3);

    // True for "second", and false for either neighbour.
    let mut statistics = 10_i64.to_le_bytes().to_vec();
    statistics.extend_from_slice(&30_i64.to_le_bytes());

    full(
        "attribution-true",
        &with_min_max_for(base.clone(), 1, &statistics),
    )
    .expect("the claim describes the second column exactly");

    for (index, name) in [(0, "first"), (2, "third")] {
        let error = full(
            &format!("attribution-{name}"),
            &with_min_max_for(base.clone(), index, &statistics),
        )
        .expect_err("the claim is false for this column");
        assert_eq!(error.kind(), ErrorKind::Corruption);
        assert!(
            error.to_string().contains(name),
            "the error should name {name}: {error}"
        );
    }
}

/// A stream-descriptor rejection names the column that owns the stream.
///
/// Section 8.1 gives each column a contiguous stream range, so a bare stream
/// index leaves an operator to work the ownership out by hand from a table of
/// near-identical 48-byte records.
#[test]
fn stream_descriptor_errors_name_the_owning_column() {
    let base = one_column(
        "measurement",
        LogicalType::Int64,
        Array::Int64(PrimitiveArray::new(vec![1, 2, 3], None)),
        3,
    );
    let data = data_frame_offset(&base);
    let header = data + PREFIX_SIZE;
    let stream_table = common::read_u32(&base, header + BLOCK_STREAM_TABLE_OFFSET) as usize;

    let mut bytes = base;
    put_u16(&mut bytes, header + stream_table + STREAM_TRANSFORM, 42);
    repair_frame(&mut bytes, data);
    let error = full("unknown-transform", &bytes).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::UnsupportedFrame);
    assert!(
        error.to_string().contains("measurement"),
        "the error should name the column: {error}"
    );
}

/// A codec that cannot decode the bytes it is handed is reported against the
/// column, even though the codec itself never learns which column that is.
#[test]
fn a_failed_decompression_names_its_column() {
    let base = one_column(
        "measurement",
        LogicalType::Int64,
        Array::Int64(PrimitiveArray::new(vec![1, 2, 3], None)),
        3,
    );
    let data = data_frame_offset(&base);
    let header = data + PREFIX_SIZE;
    let stream_table = common::read_u32(&base, header + BLOCK_STREAM_TABLE_OFFSET) as usize;

    // Claim Zstandard over bytes the writer stored uncompressed. Without the
    // feature this is an unsupported build; with it, the stream is malformed.
    let mut bytes = base;
    put_u16(&mut bytes, header + stream_table + STREAM_CODEC, 1);
    repair_frame(&mut bytes, data);
    let error = full("undecodable-codec", &bytes).unwrap_err();
    assert!(
        matches!(
            error.kind(),
            ErrorKind::Corruption | ErrorKind::UnsupportedFrame
        ),
        "{error}"
    );
    assert!(
        error.to_string().contains("measurement"),
        "the error should name the column: {error}"
    );
}

#[test]
fn primary_header_bounds_and_optional_statistics_are_checked_independently() {
    let payload = [10_i64, 20, 30]
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect();
    let mut bytes = column_file(
        3,
        &TestColumn::new(
            descriptor(
                1,
                TYPE_TIMESTAMP64,
                0,
                "timestamp",
                &timestamp_parameters(0, 0, ""),
            ),
            0,
            vec![TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 3, payload)],
        ),
    );
    let schema = schema_frame_offset() + PREFIX_SIZE;
    put_u32(&mut bytes, schema + 12, 1);
    repair_frame(&mut bytes, schema_frame_offset());
    let data = data_frame_offset(&bytes);
    let header = data + PREFIX_SIZE;
    put_u64(&mut bytes, header + BLOCK_PRIMARY_MIN, 10);
    put_u64(&mut bytes, header + BLOCK_PRIMARY_MAX, 30);
    put_u32(&mut bytes, header + BLOCK_FLAGS, TS_SORTED_BLOCK_FLAG);
    repair_frame(&mut bytes, data);

    let statistics = [10_i64.to_le_bytes(), 30_i64.to_le_bytes()].concat();
    assert!(full("primary-statistics", &with_min_max(bytes, &statistics)).is_ok());
}

#[test]
fn float_nan_is_ignored_and_all_nan_has_no_valid_statistic() {
    let values = [f64::NAN, 1.0, f64::INFINITY];
    let payload = values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect();
    let bytes = column_file(
        3,
        &TestColumn::new(
            descriptor(1, TYPE_FLOAT64, 0, "value", &[]),
            0,
            vec![TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 3, payload)],
        ),
    );
    let mut statistics = Vec::new();
    statistics.extend_from_slice(&1_f64.to_le_bytes());
    statistics.extend_from_slice(&f64::INFINITY.to_le_bytes());
    assert!(full("float-statistics", &with_min_max(bytes, &statistics)).is_ok());
}

#[test]
fn valid_integer_statistics_pass_and_false_bounds_fail() {
    let valid = with_min_max(int64_file(&[1, 2, 3]), &[0; 16]);
    let mut valid = valid;
    let data = data_frame_offset(&valid);
    let stats = payload_offset(&valid, data) - 16;
    valid[stats..stats + 8].copy_from_slice(&1_i64.to_le_bytes());
    valid[stats + 8..stats + 16].copy_from_slice(&3_i64.to_le_bytes());
    repair_frame(&mut valid, data);
    assert!(full("valid-int64-statistics", &valid).is_ok());

    let mut false_bounds = valid;
    false_bounds[stats..stats + 8].copy_from_slice(&2_i64.to_le_bytes());
    repair_frame(&mut false_bounds, data);
    assert_eq!(
        full("false-int64-statistics", &false_bounds)
            .unwrap_err()
            .kind(),
        ErrorKind::Corruption
    );
}

// ------------------------------------------- descriptor rules the parser owns

/// An unsupported statistics kind is rejected while the frame header is being
/// parsed, before any column is decoded, and is reported as an unsupported
/// frame rather than as corruption.
///
/// The statistics module makes the same distinction for its own callers, but
/// cannot be reached with a bad kind through a file; that branch is covered by
/// the unit tests in `rust/validate/statistics.rs`.
#[test]
fn an_unsupported_statistics_kind_is_rejected_by_the_frame_parser() {
    let mut kind = with_min_max(int64_file(&[1, 2, 3]), &[0; 16]);
    let data = data_frame_offset(&kind);
    let descriptor = data + PREFIX_SIZE + BLOCK_HEADER_SIZE;
    put_u16(&mut kind, descriptor + 22, 2);
    repair_frame(&mut kind, data);
    let error = full("unknown-statistics-kind", &kind).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::UnsupportedFrame);
    assert!(error.to_string().contains("value"), "{error}");
}

/// A declared length that is not the canonical pair width is the statistics
/// module's own check: the descriptor is internally consistent and the range
/// lies in the statistics area, so nothing before it objects.
#[test]
fn a_statistics_length_that_is_not_the_canonical_pair_is_rejected() {
    let mut length = with_min_max(int64_file(&[1, 2, 3]), &[0; 16]);
    let data = data_frame_offset(&length);
    let descriptor = data + PREFIX_SIZE + BLOCK_HEADER_SIZE;
    put_u32(&mut length, descriptor + 28, 8);
    repair_frame(&mut length, data);
    let error = full("short-statistics", &length).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Corruption);
    assert!(error.to_string().contains("16 bytes, found 8"), "{error}");
}

/// A statistics area that runs past the padded frame header is rejected while
/// the header is parsed, so no allocation is ever sized from it.
///
/// The statistics module's own range and allocation guards are unit-tested in
/// `rust/validate/statistics.rs`; they cannot be reached through a file because
/// this check fires first.
#[test]
fn a_statistics_area_beyond_the_frame_header_is_rejected_before_allocation() {
    let mut bytes = with_min_max(int64_file(&[1, 2, 3]), &[0; 16]);
    let data = data_frame_offset(&bytes);
    let header = data + PREFIX_SIZE;
    put_u32(&mut bytes, header + BLOCK_STATISTICS_LENGTH, u32::MAX);
    repair_frame(&mut bytes, data);
    let file = TemporaryFile::new("statistics-limit", &bytes);
    let error = acta::validate_with_options(
        file.path(),
        ValidationOptions::default()
            .with_level(ValidationLevel::Full)
            .with_limits(acta::Limits::default().with_max_frame_header_length(16 * 1024)),
    )
    .unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Corruption | ErrorKind::ResourceLimit
    ));
}

/// A column that claims no statistics must not leave a stale offset or length
/// behind. This is stricter than section 8.1 spells out; see `CHANGELOG.md`.
#[test]
fn a_column_without_statistics_must_zero_its_statistics_fields() {
    for field in [24_usize, 28] {
        let mut bytes = int64_file(&[1, 2, 3]);
        let data = data_frame_offset(&bytes);
        let descriptor = data + PREFIX_SIZE + BLOCK_HEADER_SIZE;
        put_u32(&mut bytes, descriptor + field, 64);
        repair_frame(&mut bytes, data);
        let error = full(&format!("stale-statistics-{field}"), &bytes).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Corruption);
        assert!(error.to_string().contains("value"), "{error}");
    }
}
