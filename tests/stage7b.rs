//! Stage 7b writer policy, transform, layout, and round-trip coverage.
//!
//! Every file these tests write goes through the same three checks: the
//! structural validator accepts it, the reader decodes every block, and every
//! decoded slot matches the value that was written. Floating-point values are
//! compared as bit patterns, so a transform that loses a NaN payload or the
//! sign of a zero fails rather than passes.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use acta::{
    Array, BinaryArray, BooleanArray, Column, DecimalArray, LogicalType, PrimitiveArray, Reader,
    RecordBatch, ScalarValue, Schema, TimeUnit, TimeZone, TimestampArray, Utf8Array, Writer,
    WriterCodec, WriterEncoding, WriterOptions, WriterTransform,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

/// A temporary file that removes itself, so a failing assertion does not leave
/// a path behind that the next exclusive create would collide with.
struct TempPath(std::path::PathBuf);

impl TempPath {
    fn new(label: &str) -> Self {
        let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "acta-stage7b-{label}-{}-{id}.acta",
            std::process::id()
        )))
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// The layouts and streams one file declares, read out of its serialized
/// descriptor tables rather than inferred from the writer's intent.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Descriptors {
    layouts: Vec<u16>,
    /// `(kind, transform, codec, element count, stored length, transformed
    /// length, CRC32C)` for every stream in file order.
    streams: Vec<(u16, u16, u16, u64, u64, u64, u32)>,
}

impl Descriptors {
    fn transforms(&self) -> Vec<u16> {
        self.streams.iter().map(|stream| stream.1).collect()
    }

    fn of_kind(&self, kind: u16) -> Vec<&(u16, u16, u16, u64, u64, u64, u32)> {
        self.streams.iter().filter(|s| s.0 == kind).collect()
    }
}

fn read_descriptors(bytes: &[u8]) -> Descriptors {
    let u16at = |o: usize| u16::from_le_bytes(bytes[o..o + 2].try_into().unwrap());
    let u32at = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
    let u64at = |o: usize| u64::from_le_bytes(bytes[o..o + 8].try_into().unwrap());

    let schema_frame = 64_usize;
    let mut offset = schema_frame
        + 48
        + u32at(schema_frame + 16) as usize
        + u64at(schema_frame + 24) as usize
        + 32;
    let mut layouts = Vec::new();
    let mut streams = Vec::new();
    while offset < bytes.len() {
        let total = 48 + u32at(offset + 16) as usize + u64at(offset + 24) as usize + 32;
        let header = offset + 48;
        let columns = u32at(header + 20) as usize;
        let stream_table = u32at(header + 44) as usize;
        let statistics = u32at(header + 48) as usize;
        for index in 0..columns {
            layouts.push(u16at(header + 64 + index * 32 + 4));
        }
        let mut stream = header + stream_table;
        while stream < header + statistics {
            streams.push((
                u16at(stream),
                u16at(stream + 2),
                u16at(stream + 4),
                u64at(stream + 32),
                u64at(stream + 16),
                u64at(stream + 24),
                u32at(stream + 40),
            ));
            stream += 48;
        }
        offset += total;
    }
    Descriptors { layouts, streams }
}

/// Write, validate, decode, and compare. Returns the file's own descriptors and
/// its exact bytes.
fn round_trip(
    label: &str,
    schema: &Schema,
    batches: &[RecordBatch],
    codec: WriterCodec,
    encoding: WriterEncoding,
    block_rows: u64,
) -> (Descriptors, Vec<u8>) {
    let file = TempPath::new(label);
    let mut writer = Writer::create(
        file.path(),
        schema.clone(),
        WriterOptions::default()
            .with_row_block_target(block_rows)
            .with_codec(codec)
            .with_encoding(encoding),
    )
    .unwrap_or_else(|error| panic!("{label}: create: {error}"));
    for batch in batches {
        writer
            .append(batch.clone())
            .unwrap_or_else(|error| panic!("{label}: append: {error}"));
    }
    let _summary = writer
        .finish()
        .unwrap_or_else(|error| panic!("{label}: finish: {error}"));

    let report = acta::validate(file.path())
        .unwrap_or_else(|error| panic!("{label}: the validator rejects the file: {error}"));
    assert!(!report.incomplete_tail(), "{label}: incomplete tail");

    let reader = Reader::open(file.path()).unwrap_or_else(|error| panic!("{label}: open: {error}"));
    let decoded: Vec<RecordBatch> = reader
        .scan()
        .map(|block| block.unwrap_or_else(|error| panic!("{label}: decode: {error}")))
        .collect();
    compare(label, schema, batches, &decoded);

    let bytes = std::fs::read(file.path()).expect("read the written file");
    (read_descriptors(&bytes), bytes)
}

fn compare(label: &str, schema: &Schema, written: &[RecordBatch], decoded: &[RecordBatch]) {
    for column in 0..schema.column_count() {
        assert_eq!(
            slots(written, column),
            slots(decoded, column),
            "{label}: column {} did not survive the round trip",
            schema.columns()[column].name()
        );
    }
}

/// Every slot of one column, rendered so that equality is bit equality.
fn slots(batches: &[RecordBatch], column: usize) -> Vec<Option<String>> {
    let mut values = Vec::new();
    for batch in batches {
        let array = batch.column(column).expect("the batch has this column");
        for row in 0..array.len() {
            values.push(array.value_at(row).map(|value| match value {
                ScalarValue::Float32(value) => format!("f32:{:#010x}", value.to_bits()),
                ScalarValue::Float64(value) => format!("f64:{:#018x}", value.to_bits()),
                other => format!("{other:?}"),
            }));
        }
    }
    values
}

fn codecs() -> Vec<WriterCodec> {
    #[cfg(feature = "zstd")]
    {
        vec![WriterCodec::None, WriterCodec::Zstandard]
    }
    #[cfg(not(feature = "zstd"))]
    {
        vec![WriterCodec::None]
    }
}

const STREAM_KIND_VALIDITY: u16 = 1;
const STREAM_KIND_VALUES: u16 = 2;
const STREAM_KIND_INDICES: u16 = 6;
const STREAM_KIND_RUN_LENGTHS: u16 = 8;
const COLUMN_LAYOUT_PLAIN: u16 = 0;
const COLUMN_LAYOUT_CONSTANT: u16 = 1;
const COLUMN_LAYOUT_DICTIONARY: u16 = 2;
const COLUMN_LAYOUT_RUN_LENGTH: u16 = 3;
const TRANSFORM_RAW: u16 = 0;
const TRANSFORM_BIT_PACKED: u16 = 1;
const TRANSFORM_FRAME_OF_REFERENCE: u16 = 2;
const TRANSFORM_DELTA: u16 = 3;
const TRANSFORM_DELTA_OF_DELTA: u16 = 4;
const TRANSFORM_BYTE_STREAM_SPLIT: u16 = 5;
const TRANSFORM_BOOLEAN_RLE: u16 = 6;

// ------------------------------------------------------- coverage of the shapes

/// Every logical type, several value shapes, both codecs, and two block
/// geometries, all through validator and reader. Together these reach every
/// v0.2 column layout and every v0.2 stream transform.
#[test]
fn every_layout_and_transform_round_trips_through_the_validator_and_reader() {
    let mut layouts = BTreeSet::new();
    let mut transforms = BTreeSet::new();

    for rows in [1_usize, 2, 3, 8, 9, 64, 513] {
        for shape in 0..6_usize {
            let schema = wide_schema();
            let batch = wide_batch(&schema, shape, 0, rows);
            for codec in codecs() {
                for block in [65_536, 64] {
                    let (descriptors, _) = round_trip(
                        "wide",
                        &schema,
                        std::slice::from_ref(&batch),
                        codec,
                        WriterEncoding::Adaptive,
                        block,
                    );
                    layouts.extend(descriptors.layouts.iter().copied());
                    transforms.extend(descriptors.transforms());
                }
            }
        }
    }

    assert_eq!(
        layouts,
        BTreeSet::from([
            COLUMN_LAYOUT_PLAIN,
            COLUMN_LAYOUT_CONSTANT,
            COLUMN_LAYOUT_DICTIONARY,
            COLUMN_LAYOUT_RUN_LENGTH,
        ]),
        "the shapes above no longer reach every v0.2 column layout"
    );
    let mut expected = BTreeSet::from([
        TRANSFORM_RAW,
        TRANSFORM_BIT_PACKED,
        TRANSFORM_FRAME_OF_REFERENCE,
        TRANSFORM_DELTA,
        TRANSFORM_DELTA_OF_DELTA,
        TRANSFORM_BOOLEAN_RLE,
    ]);
    // Byte-stream split only reorders bytes, so it is exactly as long as raw
    // and can never clear the savings margin on its own. It becomes selectable
    // only once a codec can compress the transposed bytes, which is what
    // section 12 means by "before Zstandard".
    if cfg!(feature = "zstd") {
        expected.insert(TRANSFORM_BYTE_STREAM_SPLIT);
    }
    assert_eq!(
        transforms, expected,
        "the shapes above no longer reach every reachable v0.2 stream transform"
    );
}

/// Byte-stream split is a permutation of the same bytes, so it is profitable
/// only in front of a codec. Selecting it without one would mean the writer
/// took on decode complexity for nothing.
#[test]
fn byte_stream_split_is_selected_only_when_a_codec_can_exploit_it() {
    let rows = 4096;
    let schema = Schema::new(
        41,
        vec![Column::new(1, "signal", LogicalType::Float64, false)],
        None,
    );
    // A smooth signal: the high-order bytes barely change, which is the shape
    // transposition exists for.
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Float64(PrimitiveArray::new(
            (0..rows)
                .map(|row| 1_000.0 + row as f64 * 0.000_001)
                .collect(),
            None,
        ))],
        rows,
    )
    .expect("a well-formed batch");

    let (plain, _) = round_trip(
        "split-none",
        &schema,
        std::slice::from_ref(&batch),
        WriterCodec::None,
        WriterEncoding::Adaptive,
        65_536,
    );
    assert!(
        !plain.transforms().contains(&TRANSFORM_BYTE_STREAM_SPLIT),
        "byte-stream split was selected with no codec to exploit it: {plain:?}"
    );

    #[cfg(feature = "zstd")]
    {
        let (compressed, _) = round_trip(
            "split-zstd",
            &schema,
            std::slice::from_ref(&batch),
            WriterCodec::Zstandard,
            WriterEncoding::Adaptive,
            65_536,
        );
        assert!(
            compressed
                .transforms()
                .contains(&TRANSFORM_BYTE_STREAM_SPLIT),
            "byte-stream split lost to raw on a smooth signal: {compressed:?}"
        );
    }
}

fn wide_schema() -> Schema {
    Schema::new(
        21,
        vec![
            Column::new(1, "bool", LogicalType::Bool, true),
            Column::new(2, "i8", LogicalType::Int8, true),
            Column::new(3, "i16", LogicalType::Int16, false),
            Column::new(4, "i32", LogicalType::Int32, true),
            Column::new(5, "i64", LogicalType::Int64, false),
            Column::new(6, "u8", LogicalType::UInt8, true),
            Column::new(7, "u16", LogicalType::UInt16, false),
            Column::new(8, "u32", LogicalType::UInt32, true),
            Column::new(9, "u64", LogicalType::UInt64, false),
            Column::new(10, "f32", LogicalType::Float32, true),
            Column::new(11, "f64", LogicalType::Float64, false),
            Column::new(
                12,
                "dec",
                LogicalType::Decimal {
                    precision: 18,
                    scale: 4,
                },
                true,
            ),
            Column::new(
                13,
                "ts",
                LogicalType::Timestamp {
                    unit: TimeUnit::Nanosecond,
                    timezone: TimeZone::Naive,
                },
                false,
            ),
            Column::new(14, "date", LogicalType::Date32, true),
            Column::new(15, "text", LogicalType::Utf8, true),
            Column::new(
                16,
                "cat",
                LogicalType::Categorical { ordered: false },
                false,
            ),
            Column::new(17, "bin", LogicalType::Binary, true),
            Column::new(18, "fix", LogicalType::FixedBinary { byte_width: 5 }, true),
        ],
        None,
    )
}

/// Shapes: 0 constant, 1 monotonic, 2 low cardinality, 3 extremes with an
/// all-null validity, 4 pseudorandom, 5 long runs. Rows are addressed by
/// absolute index, so a range of one shape is a slice of the same data.
fn wide_batch(schema: &Schema, shape: usize, start: usize, end: usize) -> RecordBatch {
    let rows = end - start;
    let span = || start..end;
    let key = |row: usize| -> i64 {
        match shape {
            0 => 7,
            1 => row as i64 * 3,
            2 => (row % 4) as i64,
            3 => {
                if row % 2 == 0 {
                    i64::MIN / 2
                } else {
                    i64::MAX / 2
                }
            }
            4 => (row as i64).wrapping_mul(0x9e37_79b9_7f4a_7c15_u64 as i64),
            _ => (row / 64) as i64,
        }
    };
    let validity = |offset: usize| -> Option<Vec<bool>> {
        match shape {
            0 => None,
            1 => Some(span().map(|row| (row + offset) % 5 != 0).collect()),
            2 => Some(vec![true; rows]),
            3 => Some(vec![false; rows]),
            4 => Some(span().map(|row| (row + offset) % 2 == 0).collect()),
            _ => Some(span().map(|row| row / 64 % 2 == 0).collect()),
        }
    };
    // Signed zero, both NaN signs with payloads, and an infinity, so a
    // byte-stream split that lost a bit could not pass.
    let doubles = [
        0x0000_0000_0000_0000_u64,
        0x8000_0000_0000_0000,
        0x7ff8_0000_0000_0042,
        0xfff8_0000_0000_0001,
        0x7ff0_0000_0000_0000,
        0x3ff0_0000_0000_0000,
    ];
    let singles = [
        0x0000_0000_u32,
        0x8000_0000,
        0x7fc0_0042,
        0xffc0_0001,
        0x3f80_0000,
    ];

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Bool(BooleanArray::new(
                span().map(|row| key(row) % 2 == 0).collect(),
                validity(0),
            )),
            Array::Int8(PrimitiveArray::new(
                span().map(|row| key(row) as i8).collect(),
                validity(1),
            )),
            Array::Int16(PrimitiveArray::new(
                span().map(|row| key(row) as i16).collect(),
                None,
            )),
            Array::Int32(PrimitiveArray::new(
                span().map(|row| key(row) as i32).collect(),
                validity(2),
            )),
            Array::Int64(PrimitiveArray::new(span().map(key).collect(), None)),
            Array::UInt8(PrimitiveArray::new(
                span().map(|row| key(row) as u8).collect(),
                validity(3),
            )),
            Array::UInt16(PrimitiveArray::new(
                span().map(|row| key(row) as u16).collect(),
                None,
            )),
            Array::UInt32(PrimitiveArray::new(
                span().map(|row| key(row) as u32).collect(),
                validity(4),
            )),
            Array::UInt64(PrimitiveArray::new(
                span().map(|row| key(row) as u64).collect(),
                None,
            )),
            Array::Float32(PrimitiveArray::new(
                span()
                    .map(|row| f32::from_bits(singles[row % singles.len()]))
                    .collect(),
                validity(0),
            )),
            Array::Float64(PrimitiveArray::new(
                span()
                    .map(|row| f64::from_bits(doubles[row % doubles.len()]))
                    .collect(),
                None,
            )),
            Array::Decimal(DecimalArray::new(
                span().map(key).collect(),
                validity(1),
                18,
                4,
            )),
            Array::Timestamp(TimestampArray::new(
                span().map(|row| key(row) / 4).collect(),
                None,
                TimeUnit::Nanosecond,
                TimeZone::Naive,
            )),
            Array::Date32(PrimitiveArray::new(
                span().map(|row| key(row) as i32 / 8).collect(),
                validity(2),
            )),
            Array::Utf8(Utf8Array::new(
                span()
                    .map(|row| match shape {
                        // An entire column of empty strings gives the values
                        // stream no bytes at all.
                        0 => String::new(),
                        4 => format!("value-{}", key(row)),
                        _ => format!("v{}", key(row).rem_euclid(6)),
                    })
                    .collect(),
                validity(3),
            )),
            Array::Categorical(Utf8Array::new(
                span()
                    .map(|row| format!("c{}", key(row).rem_euclid(3)))
                    .collect(),
                None,
            )),
            Array::Binary(BinaryArray::new(
                span()
                    .map(|row| vec![key(row) as u8; (row % 4) * usize::from(shape != 0)])
                    .collect(),
                validity(4),
            )),
            Array::FixedBinary(BinaryArray::new(
                span().map(|row| vec![key(row) as u8; 5]).collect(),
                validity(0),
            )),
        ],
        rows,
    )
    .expect("a well-formed batch")
}

// --------------------------------------------------------------- determinism

/// Selection must depend only on a block's values, never on how those rows were
/// divided among appends, and never on which run produced the file.
#[test]
fn adaptive_output_is_deterministic_and_append_boundary_independent() {
    for rows in [64_usize, 513] {
        for shape in 0..6_usize {
            let schema = wide_schema();
            let whole = wide_batch(&schema, shape, 0, rows);
            let split: Vec<RecordBatch> = (0..rows)
                .step_by(37)
                .map(|start| wide_batch(&schema, shape, start, (start + 37).min(rows)))
                .collect();

            for codec in codecs() {
                let (first, first_bytes) = round_trip(
                    "whole",
                    &schema,
                    std::slice::from_ref(&whole),
                    codec,
                    WriterEncoding::Adaptive,
                    65_536,
                );
                let (again, again_bytes) = round_trip(
                    "whole-again",
                    &schema,
                    std::slice::from_ref(&whole),
                    codec,
                    WriterEncoding::Adaptive,
                    65_536,
                );
                let (split, split_bytes) = round_trip(
                    "split",
                    &schema,
                    &split,
                    codec,
                    WriterEncoding::Adaptive,
                    65_536,
                );

                assert_eq!(first_bytes, again_bytes, "rows {rows} shape {shape}");
                assert_eq!(first, again, "rows {rows} shape {shape}");
                assert_eq!(
                    first_bytes, split_bytes,
                    "rows {rows} shape {shape}: appends changed the file"
                );
                assert_eq!(first, split, "rows {rows} shape {shape}");
            }
        }
    }
}

/// The default policy must keep producing exactly what it produced before
/// adaptive encoding existed: plain layout and the raw transform, everywhere.
#[test]
fn the_raw_policy_still_transforms_nothing() {
    for rows in [1_usize, 9, 513] {
        for shape in 0..6_usize {
            let schema = wide_schema();
            let batch = wide_batch(&schema, shape, 0, rows);
            for codec in codecs() {
                let (descriptors, _) = round_trip(
                    "raw-policy",
                    &schema,
                    std::slice::from_ref(&batch),
                    codec,
                    WriterEncoding::Raw,
                    65_536,
                );

                assert!(
                    descriptors
                        .layouts
                        .iter()
                        .all(|layout| *layout == COLUMN_LAYOUT_PLAIN),
                    "rows {rows} shape {shape}: {:?}",
                    descriptors.layouts
                );
                assert!(
                    descriptors
                        .transforms()
                        .iter()
                        .all(|transform| *transform == TRANSFORM_RAW),
                    "rows {rows} shape {shape}: {:?}",
                    descriptors.transforms()
                );
            }
        }
    }
}

/// The fixed policy prices nothing, so every block of a column takes the
/// requested transform whether or not it pays, and the validity stream beside
/// it stays raw.
#[test]
fn fixed_policy_uses_the_requested_transform_for_every_block() {
    let schema = Schema::new(
        42,
        vec![Column::new(1, "value", LogicalType::Int64, true)],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Int64(PrimitiveArray::new(
            (0..128).map(|row| row as i64 * 3).collect(),
            Some((0..128).map(|row| row % 5 != 0).collect()),
        ))],
        128,
    )
    .expect("a well-formed batch");

    let (descriptors, _) = round_trip(
        "fixed-delta",
        &schema,
        &[batch],
        WriterCodec::None,
        WriterEncoding::Fixed(WriterTransform::Delta),
        32,
    );

    assert!(
        descriptors
            .layouts
            .iter()
            .all(|layout| *layout == COLUMN_LAYOUT_PLAIN)
    );
    assert!(
        descriptors
            .of_kind(STREAM_KIND_VALUES)
            .iter()
            .all(|stream| stream.1 == TRANSFORM_DELTA)
    );
    assert!(
        descriptors
            .of_kind(STREAM_KIND_VALIDITY)
            .iter()
            .all(|stream| stream.1 == TRANSFORM_RAW)
    );
}

/// A transform no column of the schema could ever use is refused before the
/// file exists, so a caller learns about it without a partial file to clean up.
#[test]
fn fixed_policy_rejects_a_transform_incompatible_with_the_schema() {
    let file = TempPath::new("fixed-schema-error");
    let schema = Schema::new(
        43,
        vec![Column::new(1, "flag", LogicalType::Bool, false)],
        None,
    );

    let error = match Writer::create(
        file.path(),
        schema,
        WriterOptions::default().with_encoding(WriterEncoding::Fixed(WriterTransform::Delta)),
    ) {
        Ok(_) => panic!("a delta transform must be rejected for boolean columns"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), acta::ErrorKind::InvalidArgument);
    assert!(
        error
            .to_string()
            .contains("fixed transform Delta is not offered for column flag"),
        "{error}"
    );
    assert!(
        !file.path().exists(),
        "a refused policy must not leave a file behind"
    );
}

/// The fixed policy offers only what the adaptive policy would have priced,
/// which is narrower than the format permits. A dictionary `timestamp64`
/// column is legal v0.2 that section 12 does not list among the timestamp
/// candidates, so neither policy writes one and the fixed policy says so
/// before the file exists rather than at the first block.
#[test]
fn fixed_policy_offers_only_the_adaptive_candidate_set() {
    let file = TempPath::new("fixed-not-offered");
    let schema = Schema::new(
        46,
        vec![Column::new(
            1,
            "ts",
            LogicalType::Timestamp {
                unit: TimeUnit::Millisecond,
                timezone: TimeZone::Utc,
            },
            false,
        )],
        None,
    );

    let error = match Writer::create(
        file.path(),
        schema,
        WriterOptions::default().with_encoding(WriterEncoding::Fixed(WriterTransform::Dictionary)),
    ) {
        Ok(_) => panic!("a dictionary timestamp column is not offered"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), acta::ErrorKind::InvalidArgument);
    assert!(
        error
            .to_string()
            .contains("fixed transform Dictionary is not offered for column ts"),
        "{error}"
    );
}

/// A block whose values the requested transform cannot describe is an error,
/// never a quiet fallback to raw.
#[test]
fn fixed_policy_rejects_values_the_transform_cannot_describe() {
    let file = TempPath::new("fixed-value-error");
    let schema = Schema::new(
        44,
        vec![Column::new(1, "value", LogicalType::Int64, false)],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Int64(PrimitiveArray::new(vec![7], None))],
        1,
    )
    .expect("a well-formed batch");
    let mut writer = Writer::create(
        file.path(),
        schema,
        WriterOptions::default()
            .with_row_block_target(1)
            .with_encoding(WriterEncoding::Fixed(WriterTransform::DeltaOfDelta)),
    )
    .expect("the transform is schema-compatible");

    let error = writer
        .append(batch)
        .expect_err("a one-value block cannot use delta-of-delta");
    assert_eq!(error.kind(), acta::ErrorKind::InvalidArgument);
    assert!(
        error
            .to_string()
            .contains("fixed transform DeltaOfDelta on value")
    );
}

/// `Fixed(Raw)` is the default policy under another name, so it must write the
/// same file: the fixed path delegates to the raw encoder rather than
/// reimplementing it, and this is what keeps that true.
#[test]
fn the_fixed_raw_transform_writes_the_raw_policy_bytes() {
    for shape in 0..6_usize {
        let schema = wide_schema();
        let batch = wide_batch(&schema, shape, 0, 129);
        for codec in codecs() {
            let (_, raw) = round_trip(
                "raw-policy",
                &schema,
                std::slice::from_ref(&batch),
                codec,
                WriterEncoding::Raw,
                32,
            );
            let (_, fixed) = round_trip(
                "fixed-raw",
                &schema,
                std::slice::from_ref(&batch),
                codec,
                WriterEncoding::Fixed(WriterTransform::Raw),
                32,
            );
            assert_eq!(
                raw, fixed,
                "shape {shape}: the fixed raw transform diverged from the raw policy"
            );
        }
    }
}

/// Every transform the fixed policy offers, on a schema and block shape that
/// can use it, through the validator, the reader, and a value comparison.
///
/// [`fixed_case`] is an exhaustive match over [`WriterTransform`], so a new
/// transform cannot be added to the public enum without a case here.
#[test]
fn the_fixed_policy_applies_every_transform_it_offers() {
    for transform in ALL_FIXED_TRANSFORMS {
        let case = fixed_case(transform);
        for codec in codecs() {
            // One block holding every row, then four blocks holding five each,
            // so a transform that only works over a whole batch fails here.
            for block in [65_536, 5] {
                let (descriptors, _) = round_trip(
                    case.label,
                    &case.schema,
                    std::slice::from_ref(&case.batch),
                    codec,
                    WriterEncoding::Fixed(transform),
                    block,
                );

                assert!(
                    descriptors
                        .layouts
                        .iter()
                        .all(|layout| *layout == case.layout),
                    "{transform:?} block {block}: layouts {:?}",
                    descriptors.layouts
                );
                let (kind, expected) = case.marker;
                let marked = descriptors.of_kind(kind);
                assert!(
                    !marked.is_empty(),
                    "{transform:?} block {block}: no stream of kind {kind}"
                );
                assert!(
                    marked.iter().all(|stream| stream.1 == expected),
                    "{transform:?} block {block}: {marked:?}"
                );
                let validity = descriptors.of_kind(STREAM_KIND_VALIDITY);
                assert!(
                    !validity.is_empty(),
                    "{transform:?} block {block}: this case no longer has nulls to cover"
                );
                assert!(
                    validity.iter().all(|stream| stream.1 == TRANSFORM_RAW),
                    "{transform:?} block {block}: validity did not stay raw"
                );
            }
        }
    }
}

const ALL_FIXED_TRANSFORMS: [WriterTransform; 10] = [
    WriterTransform::Raw,
    WriterTransform::BitPacked,
    WriterTransform::BooleanRle,
    WriterTransform::FrameOfReference,
    WriterTransform::Delta,
    WriterTransform::DeltaOfDelta,
    WriterTransform::ByteStreamSplit,
    WriterTransform::Constant,
    WriterTransform::Dictionary,
    WriterTransform::RunLength,
];

/// Rows in every fixed-policy case, and the nulls interleaved through them so
/// that each case also covers the raw validity stream beside its values. Five
/// rows per null keeps at least one null and four dense values in every block
/// of the five-row geometry above, which is what delta-of-delta needs.
const FIXED_CASE_ROWS: usize = 20;

/// One fixed-policy case: values the transform accepts, the column layout it
/// must produce, and one `(stream kind, transform)` pair that proves it was
/// applied rather than something cheaper.
struct FixedCase {
    label: &'static str,
    schema: Schema,
    batch: RecordBatch,
    layout: u16,
    marker: (u16, u16),
}

impl FixedCase {
    fn new(
        label: &'static str,
        logical_type: LogicalType,
        values: Array,
        layout: u16,
        marker: (u16, u16),
    ) -> Self {
        let schema = Schema::new(45, vec![Column::new(1, "value", logical_type, true)], None);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![values], FIXED_CASE_ROWS)
            .expect("a well-formed batch");
        Self {
            label,
            schema,
            batch,
            layout,
            marker,
        }
    }
}

fn fixed_case_validity() -> Vec<bool> {
    (0..FIXED_CASE_ROWS).map(|row| row % 5 != 0).collect()
}

fn fixed_case(transform: WriterTransform) -> FixedCase {
    let rows = FIXED_CASE_ROWS;
    let valid = || Some(fixed_case_validity());
    match transform {
        WriterTransform::Raw => FixedCase::new(
            "fixed-raw-values",
            LogicalType::Int64,
            Array::Int64(PrimitiveArray::new(
                (0..rows).map(|row| row as i64).collect(),
                valid(),
            )),
            COLUMN_LAYOUT_PLAIN,
            (STREAM_KIND_VALUES, TRANSFORM_RAW),
        ),
        WriterTransform::BitPacked => FixedCase::new(
            "fixed-bit-packed",
            LogicalType::UInt32,
            Array::UInt32(PrimitiveArray::new(
                (0..rows).map(|row| (row % 7) as u32).collect(),
                valid(),
            )),
            COLUMN_LAYOUT_PLAIN,
            (STREAM_KIND_VALUES, TRANSFORM_BIT_PACKED),
        ),
        WriterTransform::BooleanRle => FixedCase::new(
            "fixed-boolean-rle",
            LogicalType::Bool,
            Array::Bool(BooleanArray::new(
                (0..rows).map(|row| row / 4 % 2 == 0).collect(),
                valid(),
            )),
            COLUMN_LAYOUT_PLAIN,
            (STREAM_KIND_VALUES, TRANSFORM_BOOLEAN_RLE),
        ),
        WriterTransform::FrameOfReference => FixedCase::new(
            "fixed-frame-of-reference",
            LogicalType::Int64,
            Array::Int64(PrimitiveArray::new(
                (0..rows).map(|row| 1_000_000 + (row % 9) as i64).collect(),
                valid(),
            )),
            COLUMN_LAYOUT_PLAIN,
            (STREAM_KIND_VALUES, TRANSFORM_FRAME_OF_REFERENCE),
        ),
        WriterTransform::Delta => FixedCase::new(
            "fixed-delta-timestamp",
            LogicalType::Timestamp {
                unit: TimeUnit::Millisecond,
                timezone: TimeZone::Utc,
            },
            Array::Timestamp(TimestampArray::new(
                (0..rows)
                    .map(|row| 1_700_000_000_000 + row as i64 * 1_000)
                    .collect(),
                valid(),
                TimeUnit::Millisecond,
                TimeZone::Utc,
            )),
            COLUMN_LAYOUT_PLAIN,
            (STREAM_KIND_VALUES, TRANSFORM_DELTA),
        ),
        WriterTransform::DeltaOfDelta => FixedCase::new(
            "fixed-delta-of-delta",
            LogicalType::Int64,
            Array::Int64(PrimitiveArray::new(
                (0..rows).map(|row| row as i64 * 7).collect(),
                valid(),
            )),
            COLUMN_LAYOUT_PLAIN,
            (STREAM_KIND_VALUES, TRANSFORM_DELTA_OF_DELTA),
        ),
        WriterTransform::ByteStreamSplit => FixedCase::new(
            "fixed-byte-stream-split",
            LogicalType::Float64,
            Array::Float64(PrimitiveArray::new(
                (0..rows).map(|row| row as f64 * 0.5).collect(),
                valid(),
            )),
            COLUMN_LAYOUT_PLAIN,
            (STREAM_KIND_VALUES, TRANSFORM_BYTE_STREAM_SPLIT),
        ),
        // The three layout policies below transform the values stream itself
        // no further, so the layout is what proves the policy was applied,
        // together with the packed indices or run lengths beside it.
        WriterTransform::Constant => FixedCase::new(
            "fixed-constant",
            LogicalType::Utf8,
            Array::Utf8(Utf8Array::new(
                (0..rows).map(|_| "same".to_string()).collect(),
                valid(),
            )),
            COLUMN_LAYOUT_CONSTANT,
            (STREAM_KIND_VALUES, TRANSFORM_RAW),
        ),
        WriterTransform::Dictionary => FixedCase::new(
            "fixed-dictionary",
            LogicalType::Utf8,
            Array::Utf8(Utf8Array::new(
                (0..rows)
                    .map(|row| ["north", "south", "east"][row % 3].to_string())
                    .collect(),
                valid(),
            )),
            COLUMN_LAYOUT_DICTIONARY,
            (STREAM_KIND_INDICES, TRANSFORM_BIT_PACKED),
        ),
        WriterTransform::RunLength => FixedCase::new(
            "fixed-run-length",
            LogicalType::Int64,
            Array::Int64(PrimitiveArray::new(
                (0..rows).map(|row| (row / 4) as i64).collect(),
                valid(),
            )),
            COLUMN_LAYOUT_RUN_LENGTH,
            (STREAM_KIND_RUN_LENGTHS, TRANSFORM_BIT_PACKED),
        ),
    }
}

// ------------------------------------------------------------- the threshold

/// A specialized encoding is taken only when it beats raw by at least the
/// larger of 64 bytes or one percent.
///
/// A constant `uint8` column makes the boundary exact and computable: raw costs
/// one 48-byte descriptor plus its values padded to eight bytes, and the
/// cheapest specialized candidate costs one descriptor plus one padded unit, or
/// 56 bytes. Below 65 rows the 64-byte floor is not cleared and the writer must
/// keep raw; from 65 rows it is cleared exactly.
#[test]
fn a_specialized_encoding_needs_the_documented_margin_over_raw() {
    let schema = Schema::new(
        30,
        vec![Column::new(1, "v", LogicalType::UInt8, false)],
        None,
    );
    for (rows, expected, why) in [
        (63_usize, TRANSFORM_RAW, "56 + 64 > 48 + 64"),
        (64, TRANSFORM_RAW, "56 + 64 > 48 + 64"),
        (65, TRANSFORM_BIT_PACKED, "56 + 64 == 48 + 72"),
        (128, TRANSFORM_BIT_PACKED, "56 + 64 < 48 + 128"),
    ] {
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Array::UInt8(PrimitiveArray::new(vec![0; rows], None))],
            rows,
        )
        .expect("a well-formed batch");

        let (descriptors, _) = round_trip(
            "threshold",
            &schema,
            &[batch],
            WriterCodec::None,
            WriterEncoding::Adaptive,
            65_536,
        );

        assert_eq!(
            descriptors.transforms(),
            vec![expected],
            "{rows} rows: raw costs {}, and {why}",
            48 + rows.div_ceil(8) * 8
        );
    }
}

/// A column whose values compress into nothing must still be stored raw when
/// the saving would not cover the decode complexity.
#[test]
fn an_unprofitable_column_falls_back_to_plain_raw() {
    let rows = 128;
    let schema = Schema::new(
        12,
        vec![Column::new(1, "random", LogicalType::UInt64, false)],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::UInt64(PrimitiveArray::new(
            (0..rows)
                .map(|row| (row as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
                .collect(),
            None,
        ))],
        rows,
    )
    .expect("a well-formed batch");

    let (descriptors, _) = round_trip(
        "unprofitable",
        &schema,
        &[batch],
        WriterCodec::None,
        WriterEncoding::Adaptive,
        65_536,
    );

    assert_eq!(descriptors.layouts, vec![COLUMN_LAYOUT_PLAIN]);
    assert_eq!(descriptors.transforms(), vec![TRANSFORM_RAW]);
}

// ------------------------------------------------------------ resource limits

/// More distinct values than the profiling cap allows must abandon the
/// dictionary and still produce a readable file.
#[test]
fn a_column_past_the_distinct_value_cap_still_writes_a_readable_file() {
    let schema = Schema::new(
        32,
        vec![Column::new(1, "s", LogicalType::Utf8, false)],
        None,
    );
    for rows in [4_095_usize, 4_096, 4_097, 9_000] {
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Array::Utf8(Utf8Array::new(
                (0..rows).map(|row| format!("{row:08}")).collect(),
                None,
            ))],
            rows,
        )
        .expect("a well-formed batch");

        let (descriptors, _) = round_trip(
            "distinct-cap",
            &schema,
            &[batch],
            WriterCodec::None,
            WriterEncoding::Adaptive,
            65_536,
        );

        // Every value is distinct, so a dictionary could never pay for itself
        // whether or not the cap was reached.
        assert_eq!(
            descriptors.layouts,
            vec![COLUMN_LAYOUT_PLAIN],
            "{rows} rows"
        );
    }
}

/// Few but very large distinct values cross the byte cap rather than the
/// cardinality cap.
#[test]
fn a_column_past_the_distinct_byte_cap_still_writes_a_readable_file() {
    let rows = 2_048;
    let schema = Schema::new(
        33,
        vec![Column::new(1, "b", LogicalType::Binary, false)],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Binary(BinaryArray::new(
            (0..rows)
                .map(|row| {
                    let mut value = vec![0_u8; 1024];
                    value[0] = row as u8;
                    value[1] = (row >> 8) as u8;
                    value
                })
                .collect(),
            None,
        ))],
        rows,
    )
    .expect("a well-formed batch");

    let (descriptors, _) = round_trip(
        "byte-cap",
        &schema,
        &[batch],
        WriterCodec::None,
        WriterEncoding::Adaptive,
        65_536,
    );

    assert_eq!(descriptors.layouts, vec![COLUMN_LAYOUT_PLAIN]);
}

/// Values whose adjacent differences leave `int64` must disqualify delta and
/// delta-of-delta without disqualifying frame of reference, whose span still
/// fits `uint64`.
#[test]
fn arithmetic_overflow_falls_back_without_producing_a_malformed_stream() {
    let rows = 1024;
    let schema = Schema::new(
        31,
        vec![
            Column::new(1, "i", LogicalType::Int64, false),
            Column::new(2, "u", LogicalType::UInt64, false),
            Column::new(
                3,
                "t",
                LogicalType::Timestamp {
                    unit: TimeUnit::Second,
                    timezone: TimeZone::Utc,
                },
                false,
            ),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Int64(PrimitiveArray::new(
                (0..rows)
                    .map(|row| if row % 2 == 0 { i64::MIN } else { i64::MAX })
                    .collect(),
                None,
            )),
            Array::UInt64(PrimitiveArray::new(
                (0..rows)
                    .map(|row| if row % 2 == 0 { 0 } else { u64::MAX })
                    .collect(),
                None,
            )),
            Array::Timestamp(TimestampArray::new(
                (0..rows)
                    .map(|row| if row % 2 == 0 { i64::MIN } else { i64::MAX })
                    .collect(),
                None,
                TimeUnit::Second,
                TimeZone::Utc,
            )),
        ],
        rows,
    )
    .expect("a well-formed batch");

    let (descriptors, _) = round_trip(
        "overflow",
        &schema,
        &[batch],
        WriterCodec::None,
        WriterEncoding::Adaptive,
        65_536,
    );

    assert!(
        !descriptors.transforms().contains(&TRANSFORM_DELTA),
        "a delta stream survived an int64 overflow: {descriptors:?}"
    );
    assert!(
        !descriptors.transforms().contains(&TRANSFORM_DELTA_OF_DELTA),
        "a delta-of-delta stream survived an int64 overflow: {descriptors:?}"
    );
    // The timestamp row of section 12 offers no dictionary, and a 64-bit frame
    // of reference cannot beat raw, so that column is the raw fallback.
    assert_eq!(
        descriptors.layouts[2], COLUMN_LAYOUT_PLAIN,
        "{descriptors:?}"
    );
}

// ------------------------------------------------------------------- validity

/// All-valid, all-null, and mixed columns, and the two validity stream
/// representations a writer can actually choose between.
#[test]
fn validity_representations_cover_implicit_raw_and_run_length_forms() {
    let schema = Schema::new(
        34,
        vec![Column::new(1, "v", LogicalType::Int32, true)],
        None,
    );
    let mut transforms = BTreeSet::new();

    for (label, rows, run, expect_stream) in [
        ("all-valid", 256_usize, 0_usize, false),
        ("all-null", 256, 0, false),
        ("short-mixed", 256, 1, true),
        ("alternating", 20_000, 1, true),
        ("long-runs", 20_000, 5_000, true),
    ] {
        let validity: Option<Vec<bool>> = match label {
            "all-valid" => None,
            "all-null" => Some(vec![false; rows]),
            _ => Some((0..rows).map(|row| (row / run) % 2 == 0).collect()),
        };
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Array::Int32(PrimitiveArray::new(
                (0..rows).map(|row| row as i32).collect(),
                validity,
            ))],
            rows,
        )
        .expect("a well-formed batch");

        let (descriptors, _) = round_trip(
            label,
            &schema,
            &[batch],
            WriterCodec::None,
            WriterEncoding::Adaptive,
            65_536,
        );

        let validity_streams = descriptors.of_kind(STREAM_KIND_VALIDITY);
        assert_eq!(
            !validity_streams.is_empty(),
            expect_stream,
            "{label}: {descriptors:?}"
        );
        for stream in validity_streams {
            // Section 8.1 counts validity in rows, not in dense values.
            assert_eq!(stream.3, rows as u64, "{label}");
            transforms.insert(stream.1);
        }
    }

    assert_eq!(
        transforms,
        BTreeSet::from([TRANSFORM_RAW, TRANSFORM_BOOLEAN_RLE]),
        "a validity stream used a representation the writer should not choose"
    );
}

/// A packed bitmap carries a width byte a raw bitmap does not, so it is always
/// the larger of the two and must never be written for validity.
#[test]
fn validity_is_never_bit_packed() {
    let schema = Schema::new(
        38,
        vec![Column::new(1, "v", LogicalType::Int64, true)],
        None,
    );
    for run in [1_usize, 3, 64, 977, 5_000] {
        let rows = 20_000;
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Array::Int64(PrimitiveArray::new(
                (0..rows).map(|row| row as i64).collect(),
                Some((0..rows).map(|row| (row / run) % 2 == 0).collect()),
            ))],
            rows,
        )
        .expect("a well-formed batch");

        let (descriptors, _) = round_trip(
            "validity-packing",
            &schema,
            &[batch],
            WriterCodec::None,
            WriterEncoding::Adaptive,
            65_536,
        );

        for stream in descriptors.of_kind(STREAM_KIND_VALIDITY) {
            assert_ne!(stream.1, TRANSFORM_BIT_PACKED, "run {run}");
        }
    }
}

// -------------------------------------------------------------------- booleans

/// Boolean columns reach constant, bit packing, and boolean RLE, and an
/// all-false column is the section 9.2 zero-width case.
#[test]
fn boolean_columns_round_trip_every_shape() {
    let schema = Schema::new(
        35,
        vec![Column::new(1, "f", LogicalType::Bool, false)],
        None,
    );
    for (label, values) in [
        ("all-false", vec![false; 4096]),
        ("all-true", vec![true; 4096]),
        (
            "long-runs",
            (0..4096).map(|row: usize| row / 512 % 2 == 0).collect(),
        ),
        (
            "alternating",
            (0..4096).map(|row: usize| row % 2 == 0).collect(),
        ),
        ("single", vec![true]),
    ] {
        let rows = values.len();
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Array::Bool(BooleanArray::new(values, None))],
            rows,
        )
        .expect("a well-formed batch");

        let (descriptors, _) = round_trip(
            label,
            &schema,
            &[batch],
            WriterCodec::None,
            WriterEncoding::Adaptive,
            65_536,
        );

        // Whatever was chosen, the values stream still describes every row.
        for stream in descriptors.of_kind(STREAM_KIND_VALUES) {
            assert!(
                stream.3 == rows as u64 || descriptors.layouts[0] == COLUMN_LAYOUT_CONSTANT,
                "{label}: {descriptors:?}"
            );
        }
    }
}

// ----------------------------------------------------------------- edge values

/// Values that are entirely empty give the values stream no bytes at all, under
/// both codecs.
#[test]
fn all_empty_variable_width_values_round_trip() {
    let rows = 500;
    let schema = Schema::new(
        37,
        vec![
            Column::new(1, "s", LogicalType::Utf8, false),
            Column::new(2, "b", LogicalType::Binary, true),
        ],
        None,
    );
    for codec in codecs() {
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Array::Utf8(Utf8Array::new(vec![String::new(); rows], None)),
                Array::Binary(BinaryArray::new(
                    vec![Vec::new(); rows],
                    Some((0..rows).map(|row| row % 3 != 0).collect()),
                )),
            ],
            rows,
        )
        .expect("a well-formed batch");

        round_trip(
            "empty-values",
            &schema,
            &[batch],
            codec,
            WriterEncoding::Adaptive,
            65_536,
        );
    }
}

/// One row is the smallest legal block, and every candidate has to survive it.
#[test]
fn one_row_blocks_round_trip() {
    let schema = Schema::new(
        36,
        vec![
            Column::new(1, "i", LogicalType::Int64, false),
            Column::new(2, "missing", LogicalType::Utf8, true),
            Column::new(3, "empty", LogicalType::Utf8, false),
            Column::new(4, "f", LogicalType::Float64, false),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Int64(PrimitiveArray::new(vec![i64::MIN], None)),
            Array::Utf8(Utf8Array::new(vec![String::new()], Some(vec![false]))),
            Array::Utf8(Utf8Array::new(vec![String::new()], None)),
            Array::Float64(PrimitiveArray::new(vec![-0.0], None)),
        ],
        1,
    )
    .expect("a well-formed batch");

    round_trip(
        "one-row",
        &schema,
        &[batch],
        WriterCodec::None,
        WriterEncoding::Adaptive,
        65_536,
    );
}

/// The widths at the edge of each integer type, plus the remaining logical
/// types adaptive encoding has candidates for.
#[test]
fn integer_boundaries_and_the_remaining_logical_types_round_trip() {
    let rows = 512;
    let schema = Schema::new(
        11,
        vec![
            Column::new(1, "int8", LogicalType::Int8, false),
            Column::new(2, "uint64", LogicalType::UInt64, false),
            Column::new(
                3,
                "decimal",
                LogicalType::Decimal {
                    precision: 18,
                    scale: 2,
                },
                false,
            ),
            Column::new(4, "date", LogicalType::Date32, false),
            Column::new(5, "float32", LogicalType::Float32, false),
            Column::new(
                6,
                "category",
                LogicalType::Categorical { ordered: true },
                false,
            ),
            Column::new(
                7,
                "fixed",
                LogicalType::FixedBinary { byte_width: 4 },
                false,
            ),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Int8(PrimitiveArray::new(
                (0..rows)
                    .map(|row| if row % 2 == 0 { i8::MIN } else { i8::MAX })
                    .collect(),
                None,
            )),
            Array::UInt64(PrimitiveArray::new(
                (0..rows).map(|row| u64::MAX - (row % 16) as u64).collect(),
                None,
            )),
            Array::Decimal(DecimalArray::new(vec![12_345; rows], None, 18, 2)),
            Array::Date32(PrimitiveArray::new(
                (0..rows).map(|row| -20_000 + row as i32).collect(),
                None,
            )),
            Array::Float32(PrimitiveArray::new(
                (0..rows).map(|row| (row as f32) * 0.25).collect(),
                None,
            )),
            Array::Categorical(Utf8Array::new(
                (0..rows).map(|row| format!("group-{}", row % 5)).collect(),
                None,
            )),
            Array::FixedBinary(BinaryArray::new(
                (0..rows).map(|row| [row as u8, 1, 2, 3].to_vec()).collect(),
                None,
            )),
        ],
        rows,
    )
    .expect("a well-formed batch");

    round_trip(
        "boundaries",
        &schema,
        &[batch],
        WriterCodec::None,
        WriterEncoding::Adaptive,
        65_536,
    );
}

// ---------------------------------------------------------------- descriptors

/// Read the serialized stream table back and check the bookkeeping a reader
/// depends on: element counts, lengths, alignment, codec, and CRC32C.
#[test]
fn serialized_stream_descriptors_agree_with_the_payload_they_describe() {
    let rows = 4096;
    let schema = Schema::new(
        9,
        vec![
            Column::new(1, "packed", LogicalType::Int64, false),
            Column::new(2, "offset", LogicalType::Int64, false),
            Column::new(
                3,
                "time",
                LogicalType::Timestamp {
                    unit: TimeUnit::Millisecond,
                    timezone: TimeZone::Utc,
                },
                false,
            ),
            Column::new(4, "flag", LogicalType::Bool, false),
            Column::new(5, "label", LogicalType::Utf8, false),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Int64(PrimitiveArray::new(
                (0..rows).map(|row| (row % 4096) as i64).collect(),
                None,
            )),
            Array::Int64(PrimitiveArray::new(
                (0..rows)
                    .map(|row| -1_000_000 + (row % 1024) as i64)
                    .collect(),
                None,
            )),
            Array::Timestamp(TimestampArray::new(
                (0..rows)
                    .map(|row| 1_700_000_000_000 + row as i64 * 1_000)
                    .collect(),
                None,
                TimeUnit::Millisecond,
                TimeZone::Utc,
            )),
            Array::Bool(BooleanArray::new(
                (0..rows).map(|row| row / 128 % 2 == 0).collect(),
                None,
            )),
            Array::Utf8(Utf8Array::new(
                (0..rows)
                    .map(|row| format!("label-{}", row / 128 % 8))
                    .collect(),
                None,
            )),
        ],
        rows,
    )
    .expect("a well-formed batch");

    for codec in codecs() {
        let (descriptors, bytes) = round_trip(
            "descriptors",
            &schema,
            std::slice::from_ref(&batch),
            codec,
            WriterEncoding::Adaptive,
            65_536,
        );

        let expected_codec = u16::from(matches!(codec, WriterCodec::Zstandard));
        for (kind, transform, stream_codec, elements, stored, transformed, crc) in
            &descriptors.streams
        {
            assert_eq!(*stream_codec, expected_codec, "{kind}/{transform}");
            assert!(*elements > 0, "{kind}/{transform} describes no elements");
            if expected_codec == 0 {
                // Section 10: with no codec the stored bytes are the
                // transformed bytes, and the reader rejects any disagreement.
                assert_eq!(stored, transformed, "{kind}/{transform}");
            }
            assert_ne!(*crc, 0, "{kind}/{transform} has a suspicious CRC");
        }

        // The whole file must survive the reader's own CRC and range checks,
        // which `round_trip` has already exercised; here the payload offsets
        // are checked for the alignment section 8.2 requires.
        let payload_offsets = stream_payload_offsets(&bytes);
        assert!(
            payload_offsets.iter().all(|offset| offset % 8 == 0),
            "a stream is not eight-byte aligned: {payload_offsets:?}"
        );
        let mut sorted = payload_offsets.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            payload_offsets.len(),
            "two streams share a payload offset"
        );
    }
}

fn stream_payload_offsets(bytes: &[u8]) -> Vec<u64> {
    let u32at = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap()) as usize;
    let u64at = |o: usize| u64::from_le_bytes(bytes[o..o + 8].try_into().unwrap());
    let schema_frame = 64_usize;
    let mut offset =
        schema_frame + 48 + u32at(schema_frame + 16) + u64at(schema_frame + 24) as usize + 32;
    let mut offsets = Vec::new();
    while offset < bytes.len() {
        let total = 48 + u32at(offset + 16) + u64at(offset + 24) as usize + 32;
        let header = offset + 48;
        let mut stream = header + u32at(header + 44);
        let end = header + u32at(header + 48);
        while stream < end {
            offsets.push(u64at(stream + 8));
            stream += 48;
        }
        offset += total;
    }
    offsets
}

// ------------------------------------------------------------------- codecs

/// Adaptive encoding and Zstandard are independent choices, and the pair has to
/// work without either changing what the other means.
#[cfg(feature = "zstd")]
#[test]
fn adaptive_and_zstandard_compose() {
    let rows = 1024;
    let schema = Schema::new(
        15,
        vec![Column::new(1, "value", LogicalType::UInt32, false)],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::UInt32(PrimitiveArray::new(
            (0..rows).map(|row| (row % 16) as u32).collect(),
            None,
        ))],
        rows,
    )
    .expect("a well-formed batch");

    let (_, plain) = round_trip(
        "compose-none",
        &schema,
        std::slice::from_ref(&batch),
        WriterCodec::None,
        WriterEncoding::Adaptive,
        65_536,
    );
    let (_, compressed) = round_trip(
        "compose-zstd",
        &schema,
        std::slice::from_ref(&batch),
        WriterCodec::Zstandard,
        WriterEncoding::Adaptive,
        65_536,
    );

    assert!(compressed.len() <= plain.len());
}

/// Without the optional dependency the writer refuses Zstandard before it
/// creates anything, whichever encoding policy was asked for.
#[cfg(not(feature = "zstd"))]
#[test]
fn zstandard_is_refused_without_the_feature_under_every_encoding() {
    let schema = Schema::new(
        39,
        vec![Column::new(1, "v", LogicalType::Int64, false)],
        None,
    );
    for encoding in [WriterEncoding::Raw, WriterEncoding::Adaptive] {
        let file = TempPath::new("no-zstd");
        let error = Writer::create(
            file.path(),
            schema.clone(),
            WriterOptions::default()
                .with_codec(WriterCodec::Zstandard)
                .with_encoding(encoding),
        )
        .expect_err("Zstandard needs the zstd feature");

        assert_eq!(error.kind(), acta::ErrorKind::InvalidArgument);
        assert!(!file.path().exists(), "a refused writer left a file behind");
    }
}

/// Adaptive encoding is available in both feature configurations; only the
/// codec is optional.
#[test]
fn adaptive_encoding_is_available_without_the_zstd_feature() {
    let rows = 512;
    let schema = Schema::new(
        40,
        vec![Column::new(1, "v", LogicalType::UInt32, false)],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::UInt32(PrimitiveArray::new(
            (0..rows).map(|row| (row % 8) as u32).collect(),
            None,
        ))],
        rows,
    )
    .expect("a well-formed batch");

    let (descriptors, _) = round_trip(
        "no-feature-adaptive",
        &schema,
        &[batch],
        WriterCodec::None,
        WriterEncoding::Adaptive,
        65_536,
    );

    assert!(
        descriptors
            .transforms()
            .iter()
            .any(|transform| *transform != TRANSFORM_RAW),
        "adaptive encoding transformed nothing: {descriptors:?}"
    );
}
