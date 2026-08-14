//! Stream, transform, and layout decoding through the public reader.
//!
//! The byte sequences below are written from the field tables and transform
//! definitions in `spec/v0.2/format_v0.2.md`, not produced by an encoder, so a
//! decoder that misreads the format cannot agree with a test that misreads it
//! the same way. Where a transform needs more bytes than are readable inline,
//! the test builds them with a few lines transcribed from the specification.

mod common;

use acta::{Array, Error, ErrorKind, Limits, Reader, RecordBatch, ScalarValue};
use common::{
    CODEC_ZSTD, COLUMN_IMPLICIT_VALIDITY, FIXTURES, LAYOUT_CONSTANT, LAYOUT_DICTIONARY,
    LAYOUT_PLAIN, LAYOUT_RUN_LENGTH, STREAM_DICTIONARY_LENGTHS, STREAM_DICTIONARY_VALUES,
    STREAM_INDICES, STREAM_LENGTHS, STREAM_RUN_LENGTHS, STREAM_RUN_VALUES, STREAM_VALIDITY,
    STREAM_VALUES, TRANSFORM_BIT_PACKED, TRANSFORM_BOOLEAN_RLE, TRANSFORM_BYTE_STREAM_SPLIT,
    TRANSFORM_DELTA, TRANSFORM_DELTA_OF_DELTA, TRANSFORM_FRAME_OF_REFERENCE, TRANSFORM_RAW,
    TYPE_BOOL, TYPE_FIXED_BINARY, TYPE_FLOAT32, TYPE_FLOAT64, TYPE_INT64, TYPE_UINT64, TYPE_UTF8,
    TemporaryFile, TestColumn, TestStream, column_file, descriptor, fixture, int64_column,
    reference_fixture,
};

// --------------------------------------------------------------- test harness

fn decode(label: &str, bytes: &[u8]) -> Result<RecordBatch, Error> {
    decode_with_limits(label, bytes, Limits::default())
}

fn decode_with_limits(label: &str, bytes: &[u8], limits: Limits) -> Result<RecordBatch, Error> {
    let file = TemporaryFile::new(label, bytes);
    Reader::open_with_limits(file.path(), limits).and_then(|reader| reader.read_block(0))
}

fn decoded(label: &str, bytes: &[u8]) -> RecordBatch {
    decode(label, bytes).unwrap_or_else(|error| panic!("{label} should decode: {error}"))
}

fn rejected(label: &str, bytes: &[u8]) -> Error {
    match decode(label, bytes) {
        Err(error) => error,
        Ok(batch) => panic!(
            "{label} decoded {} rows instead of failing",
            batch.row_count()
        ),
    }
}

fn column(batch: &RecordBatch) -> &Array {
    batch.column(0).expect("the block has one column")
}

fn int64_values(batch: &RecordBatch) -> Vec<Option<i64>> {
    scalars(batch, |value| match value {
        ScalarValue::Int64(value) => value,
        other => panic!("expected an int64, found {other:?}"),
    })
}

fn text_values(batch: &RecordBatch) -> Vec<Option<String>> {
    scalars(batch, |value| match value {
        ScalarValue::Utf8(value) => value.to_owned(),
        other => panic!("expected a utf8 value, found {other:?}"),
    })
}

fn scalars<T>(batch: &RecordBatch, convert: impl Fn(ScalarValue<'_>) -> T) -> Vec<Option<T>> {
    (0..batch.row_count())
        .map(|row| column(batch).value_at(row).map(&convert))
        .collect()
}

fn int64_file(layout: u16, row_count: u32, streams: Vec<TestStream>) -> Vec<u8> {
    column_file(
        row_count,
        &TestColumn::new(int64_column(1, "value"), layout, streams),
    )
}

fn int64_bytes(values: &[i64]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

fn utf8_column(nullable: bool) -> Vec<u8> {
    descriptor(1, TYPE_UTF8, u16::from(nullable), "value", &[])
}

fn lengths_stream(kind: u16, lengths: &[u32]) -> TestStream {
    let payload = lengths
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect();
    TestStream::new(kind, TRANSFORM_RAW, lengths.len() as u64, payload)
}

/// Section 9.6: all byte-zero values, then all byte-one values, and so on.
fn byte_stream_split(width: usize, values: &[u8]) -> Vec<u8> {
    let count = values.len() / width;
    let mut split = vec![0_u8; values.len()];
    for value in 0..count {
        for byte in 0..width {
            split[byte * count + value] = values[value * width + byte];
        }
    }
    split
}

// ------------------------------------------------------------ transform paths

#[test]
fn every_integer_transform_restores_the_same_values() {
    // Each payload holds the canonical first or base value, then a bit width
    // byte, then LSB-first packed data. The derivations are in the comments.
    let mut delta = int64_bytes(&[10]);
    delta.extend_from_slice(&[5, 0x74, 0x02]); // zigzag(+10)=20, zigzag(-10)=19
    let mut frame_of_reference = int64_bytes(&[10]);
    frame_of_reference.extend_from_slice(&[4, 0xa0, 0x00]); // offsets 0, 10, 0
    let mut delta_of_delta = int64_bytes(&[10, 10]);
    delta_of_delta.extend_from_slice(&[6, 0x27]); // zigzag(-20)=39

    let cases = [
        (TRANSFORM_RAW, int64_bytes(&[10, 20, 10])),
        (TRANSFORM_BIT_PACKED, vec![5, 0x8a, 0x2a]),
        (TRANSFORM_FRAME_OF_REFERENCE, frame_of_reference),
        (TRANSFORM_DELTA, delta),
        (TRANSFORM_DELTA_OF_DELTA, delta_of_delta),
    ];

    for (transform, payload) in cases {
        let bytes = int64_file(
            LAYOUT_PLAIN,
            3,
            vec![TestStream::new(STREAM_VALUES, transform, 3, payload)],
        );
        assert_eq!(
            int64_values(&decoded(&format!("transform {transform}"), &bytes)),
            vec![Some(10), Some(20), Some(10)],
            "transform {transform}"
        );
    }
}

#[test]
fn a_zero_bit_width_decodes_as_zero_values() {
    let bytes = int64_file(
        LAYOUT_PLAIN,
        3,
        vec![TestStream::new(
            STREAM_VALUES,
            TRANSFORM_BIT_PACKED,
            3,
            vec![0],
        )],
    );

    assert_eq!(
        int64_values(&decoded("zero width", &bytes)),
        vec![Some(0), Some(0), Some(0)]
    );
}

#[test]
fn the_maximum_bit_width_decodes_whole_words() {
    let mut payload = vec![64];
    payload.extend_from_slice(&u64::MAX.to_le_bytes());
    payload.extend_from_slice(&1_u64.to_le_bytes());
    let bytes = column_file(
        2,
        &TestColumn::new(
            descriptor(1, TYPE_UINT64, 0, "value", &[]),
            LAYOUT_PLAIN,
            vec![TestStream::new(
                STREAM_VALUES,
                TRANSFORM_BIT_PACKED,
                2,
                payload,
            )],
        ),
    );

    let batch = decoded("width 64", &bytes);
    assert_eq!(
        column(&batch).value_at(0),
        Some(ScalarValue::UInt64(u64::MAX))
    );
}

#[test]
fn a_packed_stream_shorter_than_its_element_count_is_corruption() {
    // Three five-bit values need ceil(15 / 8) = 2 bytes after the width byte.
    let bytes = int64_file(
        LAYOUT_PLAIN,
        3,
        vec![TestStream::new(
            STREAM_VALUES,
            TRANSFORM_BIT_PACKED,
            3,
            vec![5, 0x8a],
        )],
    );

    assert_eq!(
        rejected("short packed", &bytes).kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn packed_high_bits_outside_the_element_count_must_be_zero() {
    // Section 9.2: the unused high bits of the final byte are zero.
    let bytes = int64_file(
        LAYOUT_PLAIN,
        3,
        vec![TestStream::new(
            STREAM_VALUES,
            TRANSFORM_BIT_PACKED,
            3,
            vec![5, 0x8a, 0xaa],
        )],
    );

    assert_eq!(
        rejected("dirty padding", &bytes).kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn a_delta_chain_that_leaves_the_column_width_is_corruption() {
    let mut payload = int64_bytes(&[i64::MAX]);
    payload.extend_from_slice(&[2, 0x02]); // zigzag(+1) = 2
    let bytes = int64_file(
        LAYOUT_PLAIN,
        2,
        vec![TestStream::new(STREAM_VALUES, TRANSFORM_DELTA, 2, payload)],
    );

    assert_eq!(
        rejected("delta overflow", &bytes).kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn a_delta_of_delta_difference_beyond_int64_is_corruption() {
    // Section 9.5 requires every intermediate difference to fit in int64.
    let mut payload = int64_bytes(&[0, i64::MAX]);
    payload.push(64);
    payload.extend_from_slice(&((i64::MAX as u64) << 1).to_le_bytes());
    let bytes = int64_file(
        LAYOUT_PLAIN,
        3,
        vec![TestStream::new(
            STREAM_VALUES,
            TRANSFORM_DELTA_OF_DELTA,
            3,
            payload,
        )],
    );

    assert_eq!(
        rejected("delta-of-delta overflow", &bytes).kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn byte_stream_split_restores_exact_float64_bits() {
    let expected = [1.5_f64, -0.0_f64, f64::from_bits(0x7ff8_0000_dead_beef)];
    let canonical: Vec<u8> = expected
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect();
    let bytes = column_file(
        3,
        &TestColumn::new(
            descriptor(1, TYPE_FLOAT64, 0, "value", &[]),
            LAYOUT_PLAIN,
            vec![TestStream::new(
                STREAM_VALUES,
                TRANSFORM_BYTE_STREAM_SPLIT,
                3,
                byte_stream_split(8, &canonical),
            )],
        ),
    );

    let batch = decoded("split float64", &bytes);
    let decoded_bits: Vec<u64> = (0..3)
        .map(|row| match column(&batch).value_at(row) {
            Some(ScalarValue::Float64(value)) => value.to_bits(),
            other => panic!("expected a float64, found {other:?}"),
        })
        .collect();
    assert_eq!(
        decoded_bits,
        expected
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>()
    );
}

#[test]
fn raw_float32_preserves_signed_zero_and_nan_payloads() {
    let expected = [f32::INFINITY, -0.0_f32, f32::from_bits(0x7fc0_beef)];
    let payload = expected
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect();
    let bytes = column_file(
        3,
        &TestColumn::new(
            descriptor(1, TYPE_FLOAT32, 0, "value", &[]),
            LAYOUT_PLAIN,
            vec![TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 3, payload)],
        ),
    );

    let batch = decoded("float32", &bytes);
    let decoded_bits: Vec<u32> = (0..3)
        .map(|row| match column(&batch).value_at(row) {
            Some(ScalarValue::Float32(value)) => value.to_bits(),
            other => panic!("expected a float32, found {other:?}"),
        })
        .collect();
    assert_eq!(
        decoded_bits,
        expected
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>()
    );
}

#[test]
fn byte_stream_split_restores_fixed_binary_values() {
    let width = 3_u32;
    let parameters = [width.to_le_bytes(), 0_u32.to_le_bytes()].concat();
    let canonical = vec![1, 2, 3, 4, 5, 6];
    let bytes = column_file(
        2,
        &TestColumn::new(
            descriptor(1, TYPE_FIXED_BINARY, 0, "value", &parameters),
            LAYOUT_PLAIN,
            vec![TestStream::new(
                STREAM_VALUES,
                TRANSFORM_BYTE_STREAM_SPLIT,
                2,
                byte_stream_split(3, &canonical),
            )],
        ),
    );

    let batch = decoded("split fixed_binary", &bytes);
    assert_eq!(
        column(&batch).value_at(1),
        Some(ScalarValue::FixedBinary(&[4, 5, 6]))
    );
}

#[test]
fn boolean_run_length_values_expand_to_their_runs() {
    // Section 9.7: run count, LSB-first run value bits, then packed lengths.
    // Two runs, values true then false, lengths 2 and 3 at two bits each.
    let payload = vec![2, 0, 0, 0, 0b0000_0001, 2, 0b0000_1110];
    let bytes = column_file(
        5,
        &TestColumn::new(
            descriptor(1, TYPE_BOOL, 0, "flag", &[]),
            LAYOUT_PLAIN,
            vec![TestStream::new(
                STREAM_VALUES,
                TRANSFORM_BOOLEAN_RLE,
                5,
                payload,
            )],
        ),
    );

    let batch = decoded("boolean RLE", &bytes);
    assert_eq!(
        scalars(&batch, |value| match value {
            ScalarValue::Bool(value) => value,
            other => panic!("expected a bool, found {other:?}"),
        }),
        vec![
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false)
        ]
    );
}

#[test]
fn boolean_run_length_validity_selects_the_dense_values() {
    // Three runs: present, absent, present, with lengths 1, 2 and 1 packed at
    // two bits each into bits 0-1, 2-3 and 4-5.
    let validity = vec![3, 0, 0, 0, 0b0000_0101, 2, 0b0001_1001];
    let bytes = column_file(
        4,
        &TestColumn::new(
            descriptor(1, TYPE_INT64, 1, "value", &[]),
            LAYOUT_PLAIN,
            vec![
                TestStream::new(STREAM_VALIDITY, TRANSFORM_BOOLEAN_RLE, 4, validity),
                TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 2, int64_bytes(&[10, 20])),
            ],
        )
        .with_nulls(2, 0),
    );

    assert_eq!(
        int64_values(&decoded("RLE validity", &bytes)),
        vec![Some(10), None, None, Some(20)]
    );
}

// -------------------------------------------------------------------- validity

#[test]
fn an_explicit_validity_bitmap_restores_one_slot_per_row() {
    let bytes = column_file(
        3,
        &TestColumn::new(
            descriptor(1, TYPE_INT64, 1, "value", &[]),
            LAYOUT_PLAIN,
            vec![
                TestStream::new(STREAM_VALIDITY, TRANSFORM_RAW, 3, vec![0b0000_0101]),
                TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 2, int64_bytes(&[10, 20])),
            ],
        )
        .with_nulls(1, 0),
    );

    assert_eq!(
        int64_values(&decoded("mixed validity", &bytes)),
        vec![Some(10), None, Some(20)]
    );
}

#[test]
fn an_implicit_all_null_column_needs_no_streams_at_all() {
    let bytes = column_file(
        3,
        &TestColumn::new(
            descriptor(1, TYPE_INT64, 1, "value", &[]),
            LAYOUT_PLAIN,
            Vec::new(),
        )
        .with_nulls(3, COLUMN_IMPLICIT_VALIDITY),
    );

    assert_eq!(
        int64_values(&decoded("all null", &bytes)),
        vec![None, None, None]
    );
}

#[test]
fn an_implicit_all_valid_column_needs_no_validity_stream() {
    let bytes = column_file(
        3,
        &TestColumn::new(
            descriptor(1, TYPE_INT64, 1, "value", &[]),
            LAYOUT_PLAIN,
            vec![TestStream::new(
                STREAM_VALUES,
                TRANSFORM_RAW,
                3,
                int64_bytes(&[10, 20, 30]),
            )],
        )
        .with_nulls(0, COLUMN_IMPLICIT_VALIDITY),
    );

    assert_eq!(
        int64_values(&decoded("all valid", &bytes)),
        vec![Some(10), Some(20), Some(30)]
    );
}

// ---------------------------------------------------------------- byte values

#[test]
fn a_utf8_dictionary_expands_its_block_local_values() {
    let bytes = column_file(
        3,
        &TestColumn::new(
            utf8_column(false),
            LAYOUT_DICTIONARY,
            vec![
                TestStream::new(
                    STREAM_DICTIONARY_VALUES,
                    TRANSFORM_RAW,
                    2,
                    b"alphabeta".to_vec(),
                ),
                lengths_stream(STREAM_DICTIONARY_LENGTHS, &[5, 4]),
                // Indices 0, 1, 0 packed at one bit each.
                TestStream::new(STREAM_INDICES, TRANSFORM_BIT_PACKED, 3, vec![1, 0b010]),
            ],
        ),
    );

    assert_eq!(
        text_values(&decoded("utf8 dictionary", &bytes)),
        vec![
            Some("alpha".to_owned()),
            Some("beta".to_owned()),
            Some("alpha".to_owned())
        ]
    );
}

#[test]
fn utf8_run_length_values_expand_to_their_runs() {
    let bytes = column_file(
        3,
        &TestColumn::new(
            utf8_column(false),
            LAYOUT_RUN_LENGTH,
            vec![
                TestStream::new(STREAM_RUN_VALUES, TRANSFORM_RAW, 2, b"xyy".to_vec()),
                lengths_stream(STREAM_LENGTHS, &[1, 2]),
                // Run lengths 1 and 2 packed at two bits each.
                TestStream::new(
                    STREAM_RUN_LENGTHS,
                    TRANSFORM_BIT_PACKED,
                    2,
                    vec![2, 0b0000_1001],
                ),
            ],
        ),
    );

    assert_eq!(
        text_values(&decoded("utf8 runs", &bytes)),
        vec![
            Some("x".to_owned()),
            Some("yy".to_owned()),
            Some("yy".to_owned())
        ]
    );
}

#[test]
fn a_constant_utf8_column_stores_one_value_and_one_length() {
    let bytes = column_file(
        3,
        &TestColumn::new(
            utf8_column(false),
            LAYOUT_CONSTANT,
            vec![
                TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 1, b"hi".to_vec()),
                lengths_stream(STREAM_LENGTHS, &[2]),
            ],
        ),
    );

    assert_eq!(
        text_values(&decoded("constant utf8", &bytes)),
        vec![
            Some("hi".to_owned()),
            Some("hi".to_owned()),
            Some("hi".to_owned())
        ]
    );
}

#[test]
fn variable_width_values_may_be_empty() {
    let bytes = column_file(
        2,
        &TestColumn::new(
            utf8_column(false),
            LAYOUT_PLAIN,
            vec![
                TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 2, b"abcdef".to_vec()),
                lengths_stream(STREAM_LENGTHS, &[0, 6]),
            ],
        ),
    );

    assert_eq!(
        text_values(&decoded("empty value", &bytes)),
        vec![Some(String::new()), Some("abcdef".to_owned())]
    );
}

#[test]
fn a_utf8_value_that_is_not_utf8_is_corruption() {
    let bytes = column_file(
        1,
        &TestColumn::new(
            utf8_column(false),
            LAYOUT_PLAIN,
            vec![
                TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 1, vec![0xff]),
                lengths_stream(STREAM_LENGTHS, &[1]),
            ],
        ),
    );

    assert_eq!(rejected("bad utf8", &bytes).kind(), ErrorKind::Corruption);
}

#[test]
fn value_lengths_that_overrun_the_values_stream_are_corruption() {
    let bytes = column_file(
        2,
        &TestColumn::new(
            utf8_column(false),
            LAYOUT_PLAIN,
            vec![
                TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 2, b"abc".to_vec()),
                lengths_stream(STREAM_LENGTHS, &[2, 4]),
            ],
        ),
    );

    assert_eq!(
        rejected("long lengths", &bytes).kind(),
        ErrorKind::Corruption
    );
}

// -------------------------------------------------------------- malformed use

#[test]
fn a_dictionary_index_outside_the_dictionary_is_corruption() {
    let bytes = int64_file(
        LAYOUT_DICTIONARY,
        3,
        vec![
            TestStream::new(
                STREAM_DICTIONARY_VALUES,
                TRANSFORM_RAW,
                2,
                int64_bytes(&[10, 20]),
            ),
            // Indices 0, 1, 2 packed at two bits each; the dictionary has two.
            TestStream::new(
                STREAM_INDICES,
                TRANSFORM_BIT_PACKED,
                3,
                vec![2, 0b0010_0100],
            ),
        ],
    );

    assert_eq!(rejected("bad index", &bytes).kind(), ErrorKind::Corruption);
}

#[test]
fn dictionary_indices_that_are_not_bit_packed_are_unsupported() {
    let bytes = int64_file(
        LAYOUT_DICTIONARY,
        1,
        vec![
            TestStream::new(
                STREAM_DICTIONARY_VALUES,
                TRANSFORM_RAW,
                1,
                int64_bytes(&[10]),
            ),
            TestStream::new(STREAM_INDICES, TRANSFORM_RAW, 1, vec![0, 0, 0, 0]),
        ],
    );

    assert_eq!(
        rejected("raw indices", &bytes).kind(),
        ErrorKind::UnsupportedFrame
    );
}

#[test]
fn run_lengths_that_do_not_sum_to_the_dense_count_are_corruption() {
    let bytes = int64_file(
        LAYOUT_RUN_LENGTH,
        3,
        vec![
            TestStream::new(STREAM_RUN_VALUES, TRANSFORM_RAW, 2, int64_bytes(&[10, 20])),
            // Run lengths 1 and 1 sum to two, but the block declares three rows.
            TestStream::new(
                STREAM_RUN_LENGTHS,
                TRANSFORM_BIT_PACKED,
                2,
                vec![2, 0b0000_0101],
            ),
        ],
    );

    assert_eq!(rejected("short runs", &bytes).kind(), ErrorKind::Corruption);
}

#[test]
fn a_zero_length_run_is_corruption() {
    let bytes = int64_file(
        LAYOUT_RUN_LENGTH,
        3,
        vec![
            TestStream::new(STREAM_RUN_VALUES, TRANSFORM_RAW, 2, int64_bytes(&[10, 20])),
            // Run lengths 0 and 3 at two bits each.
            TestStream::new(
                STREAM_RUN_LENGTHS,
                TRANSFORM_BIT_PACKED,
                2,
                vec![2, 0b0000_1100],
            ),
        ],
    );

    assert_eq!(rejected("zero run", &bytes).kind(), ErrorKind::Corruption);
}

#[test]
fn a_validity_stream_that_contradicts_the_null_count_is_corruption() {
    let bytes = column_file(
        3,
        &TestColumn::new(
            descriptor(1, TYPE_INT64, 1, "value", &[]),
            LAYOUT_PLAIN,
            vec![
                // One null, but the descriptor below declares two.
                TestStream::new(STREAM_VALIDITY, TRANSFORM_RAW, 3, vec![0b0000_0101]),
                TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 1, int64_bytes(&[10])),
            ],
        )
        .with_nulls(2, 0),
    );

    assert_eq!(
        rejected("null count mismatch", &bytes).kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn an_all_null_column_stores_no_value_streams() {
    let bytes = column_file(
        3,
        &TestColumn::new(
            descriptor(1, TYPE_INT64, 1, "value", &[]),
            LAYOUT_PLAIN,
            vec![TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 0, Vec::new())],
        )
        .with_nulls(3, COLUMN_IMPLICIT_VALIDITY),
    );

    assert_eq!(
        rejected("all-null values", &bytes).kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn a_stream_kind_the_layout_does_not_use_is_corruption() {
    let bytes = int64_file(
        LAYOUT_PLAIN,
        1,
        vec![
            TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 1, int64_bytes(&[10])),
            TestStream::new(STREAM_INDICES, TRANSFORM_BIT_PACKED, 1, vec![0]),
        ],
    );

    assert_eq!(
        rejected("stray stream", &bytes).kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn implicit_validity_does_not_carry_a_validity_stream() {
    let bytes = column_file(
        3,
        &TestColumn::new(
            descriptor(1, TYPE_INT64, 1, "value", &[]),
            LAYOUT_PLAIN,
            vec![TestStream::new(
                STREAM_VALIDITY,
                TRANSFORM_RAW,
                3,
                vec![0b0000_0000],
            )],
        )
        .with_nulls(3, COLUMN_IMPLICIT_VALIDITY),
    );

    assert_eq!(
        rejected("implicit plus stream", &bytes).kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn a_stream_element_count_that_disagrees_with_the_block_is_corruption() {
    let bytes = int64_file(
        LAYOUT_PLAIN,
        3,
        vec![
            TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 3, int64_bytes(&[10, 20, 10]))
                .with_element_count(2),
        ],
    );

    assert_eq!(
        rejected("element count", &bytes).kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn an_element_count_beyond_the_row_limit_fails_before_allocation() {
    let bytes = int64_file(
        LAYOUT_PLAIN,
        3,
        vec![
            TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 3, int64_bytes(&[10, 20, 10]))
                .with_element_count(u64::MAX),
        ],
    );

    let result = std::panic::catch_unwind(|| decode("huge element count", &bytes).is_err());

    assert_eq!(result.ok(), Some(true), "a huge element count panicked");
}

#[test]
fn a_stream_whose_crc_does_not_cover_its_bytes_is_corruption() {
    let bytes = int64_file(
        LAYOUT_PLAIN,
        3,
        vec![
            TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 3, int64_bytes(&[10, 20, 10]))
                .with_crc(0),
        ],
    );

    assert_eq!(rejected("stream CRC", &bytes).kind(), ErrorKind::Corruption);
}

// ------------------------------------------------------------ resource limits

#[test]
fn one_block_decode_stays_within_its_byte_allowance() {
    let bytes = int64_file(
        LAYOUT_PLAIN,
        3,
        vec![TestStream::new(
            STREAM_VALUES,
            TRANSFORM_RAW,
            3,
            int64_bytes(&[10, 20, 10]),
        )],
    );
    let limits = Limits::default().with_max_decoded_block_bytes(16);

    let error = decode_with_limits("decode budget", &bytes, limits)
        .expect_err("three values exceed a sixteen-byte allowance");

    assert_eq!(error.kind(), ErrorKind::ResourceLimit);
}

#[test]
fn a_compressed_stream_is_charged_before_it_is_decompressed() {
    // A tiny stored stream declaring an enormous transformed length must be
    // refused rather than reserving room for what it claims to expand into.
    let bytes = int64_file(
        LAYOUT_PLAIN,
        3,
        vec![TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 3, vec![0; 8]).compressed(1 << 40)],
    );

    assert_eq!(
        rejected("declared expansion", &bytes).kind(),
        ErrorKind::ResourceLimit
    );
}

#[test]
fn a_block_wider_than_the_allowance_fails_before_it_is_all_decoded() {
    // Each column is small; together they are not. The allowance is what makes
    // the difference between the two visible.
    let column = TestColumn::new(
        int64_column(1, "value"),
        LAYOUT_PLAIN,
        vec![TestStream::new(
            STREAM_VALUES,
            TRANSFORM_RAW,
            3,
            int64_bytes(&[10, 20, 10]),
        )],
    );
    let bytes = column_file(3, &column);

    decode_with_limits(
        "wide block",
        &bytes,
        Limits::default().with_max_decoded_block_bytes(4096),
    )
    .expect("one small column fits in a four-kilobyte allowance");
    assert_eq!(
        decode_with_limits(
            "wide block",
            &bytes,
            Limits::default().with_max_decoded_block_bytes(32)
        )
        .expect_err("the same column does not fit in thirty-two bytes")
        .kind(),
        ErrorKind::ResourceLimit
    );
}

/// Three `int64` values cost their stored bytes and the vector they decode
/// into, and nothing else: a column with no nulls becomes its array without a
/// second copy. Twenty-four bytes each way is the whole price.
#[test]
fn a_column_without_nulls_is_not_copied_into_its_array() {
    let bytes = int64_file(
        LAYOUT_PLAIN,
        3,
        vec![TestStream::new(
            STREAM_VALUES,
            TRANSFORM_RAW,
            3,
            int64_bytes(&[10, 20, 10]),
        )],
    );

    decode_with_limits(
        "no copy",
        &bytes,
        Limits::default().with_max_decoded_block_bytes(48),
    )
    .expect("stored bytes plus one decoded vector is the whole cost");
}

#[cfg(feature = "zstd")]
#[test]
fn a_zstandard_stream_decodes_after_its_crc_is_verified() {
    let source = int64_bytes(&[10, 20, 10]);
    let compressed = zstd::bulk::compress(&source, 1).expect("test compression should succeed");
    let bytes = int64_file(
        LAYOUT_PLAIN,
        3,
        vec![
            TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 3, compressed)
                .compressed(source.len() as u64),
        ],
    );

    assert_eq!(
        int64_values(&decoded("zstandard", &bytes)),
        vec![Some(10), Some(20), Some(10)]
    );
}

#[cfg(feature = "zstd")]
#[test]
fn a_malformed_zstandard_stream_is_corruption() {
    let bytes = int64_file(
        LAYOUT_PLAIN,
        3,
        vec![TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 3, vec![0xff; 16]).compressed(24)],
    );

    assert_eq!(
        rejected("bad zstandard", &bytes).kind(),
        ErrorKind::Corruption
    );
}

#[cfg(feature = "zstd")]
#[test]
fn a_zstandard_stream_shorter_than_its_declaration_is_corruption() {
    let source = int64_bytes(&[10, 20, 10]);
    let compressed = zstd::bulk::compress(&source, 1).expect("test compression should succeed");
    let bytes = int64_file(
        LAYOUT_PLAIN,
        4,
        vec![
            TestStream::new(STREAM_VALUES, TRANSFORM_RAW, 4, compressed)
                .compressed(source.len() as u64 + 8),
        ],
    );

    assert_eq!(
        rejected("short expansion", &bytes).kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn an_unsupported_codec_is_reported_as_an_unsupported_frame() {
    let mut bytes = int64_file(
        LAYOUT_PLAIN,
        3,
        vec![TestStream::new(
            STREAM_VALUES,
            TRANSFORM_RAW,
            3,
            int64_bytes(&[10, 20, 10]),
        )],
    );
    let frame = common::data_frame_offset(&bytes);
    let stream_descriptor = common::stream_descriptor_offset(&bytes, frame, 0);
    common::put_u16(&mut bytes, stream_descriptor + 4, CODEC_ZSTD + 1);
    common::repair_frame(&mut bytes, frame);

    assert_eq!(
        rejected("unknown codec", &bytes).kind(),
        ErrorKind::UnsupportedFrame
    );
}

// -------------------------------------------------------------- reader shape

#[test]
fn every_fixture_decodes_one_array_per_schema_column() {
    for &(relative_path, _) in FIXTURES {
        let bytes = fixture(relative_path);
        let batch = decoded("fixture", &bytes);

        assert_eq!(
            batch.columns().len(),
            batch.schema().column_count(),
            "{relative_path}"
        );
        for (column, array) in batch.schema().columns().iter().zip(batch.columns()) {
            assert_eq!(
                array.len(),
                batch.row_count(),
                "{relative_path}: {}",
                column.name()
            );
        }
    }
}

#[test]
fn a_block_index_past_the_snapshot_is_a_caller_error() {
    let bytes = reference_fixture();
    let file = TemporaryFile::new("block-index", &bytes);
    let reader = Reader::open(file.path()).expect("the fixture should open");

    let error = reader.read_block(7).expect_err("there is no eighth block");

    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn malformed_transform_metadata_cannot_panic() {
    let mut bytes = reference_fixture();
    let frame = common::data_frame_offset(&bytes);
    let stream_descriptor = common::stream_descriptor_offset(&bytes, frame, 0);
    common::put_u16(&mut bytes, stream_descriptor + 2, TRANSFORM_BIT_PACKED);
    common::repair_frame(&mut bytes, frame);

    let result = std::panic::catch_unwind(|| decode("transform", &bytes).is_err());

    assert_eq!(result.ok(), Some(true), "a malformed transform panicked");
}
