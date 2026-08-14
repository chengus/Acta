//! Stage 7b writer output against Stage 3 validation.
//!
//! Stages 7b and 3 are two halves of one contract: whatever the writer chooses
//! to emit, the validator has to accept and decode. Neither side's own tests
//! can establish that. The writer's tests prove it produced the representation
//! it intended; the validator's tests prove it rejects what it should. Only a
//! corpus that crosses them proves a file the writer really produces is a file
//! the validator really accepts, at both validation levels, with the same
//! answer.
//!
//! The corpus is deliberately built from axes rather than from a list of
//! interesting files, and the axes are the ones that change the bytes: the
//! encoding policy, the stream codec, the value shape a column takes, whether
//! it is nullable and how its nulls fall, how many rows go in a block, whether
//! row IDs are on, whether the primary column is sorted, and where the caller
//! happened to split its appends. Combinations are then thinned, because the
//! product is large and most of it is redundant: every value of every axis
//! appears, paired against every encoding and codec, without taking the whole
//! cross product.
//!
//! Thinning is only safe if something checks that the corpus still reaches
//! everything, so [`every_representation_survives_both_validation_levels`]
//! reads the layouts, transforms, and codecs back out of the serialized
//! descriptor tables and asserts the exact set. A corpus that asks for
//! adaptive encoding and silently gets plain/raw fails there rather than
//! passing quietly.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use acta::{
    Array, BinaryArray, BooleanArray, Column, DecimalArray, LogicalType, PrimaryRange,
    PrimitiveArray, Reader, RecordBatch, Schema, TimeUnit, TimeZone, TimestampArray, Utf8Array,
    ValidationLevel, ValidationOptions, ValidationReport, Writer, WriterCodec, WriterEncoding,
    WriterOptions,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

/// A temporary file that removes itself, so a failing assertion cannot leave a
/// path behind that the next exclusive create would collide with.
struct TempPath(std::path::PathBuf);

impl TempPath {
    fn new(label: &str) -> Self {
        let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "acta-cross-stage-{label}-{}-{id}.acta",
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

// ------------------------------------------------------------ wire inspection

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

const CODEC_NONE: u16 = 0;
const CODEC_ZSTD: u16 = 1;

const STATS_NONE: u16 = 0;

const DATA_FRAME_TYPE: u16 = 2;

/// What a file's serialized descriptor tables declare, read out of the bytes
/// rather than taken from the writer's intent.
#[derive(Debug, Default, PartialEq, Eq)]
struct Wire {
    layouts: BTreeSet<u16>,
    transforms: BTreeSet<u16>,
    codecs: BTreeSet<u16>,
    statistics_kinds: BTreeSet<u16>,
    data_frames: usize,
}

impl Wire {
    fn absorb(&mut self, other: Wire) {
        self.layouts.extend(other.layouts);
        self.transforms.extend(other.transforms);
        self.codecs.extend(other.codecs);
        self.statistics_kinds.extend(other.statistics_kinds);
        self.data_frames += other.data_frames;
    }
}

fn u16at(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(bytes[at..at + 2].try_into().expect("two bytes"))
}

fn u32at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("four bytes"))
}

fn u64at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("eight bytes"))
}

/// Walk every frame and read the column and stream descriptor tables.
///
/// Section 6.1 puts the frame type at prefix offset 8, the padded header
/// length at 16, and the padded payload length at 24; section 6.2 adds a
/// 32-byte commit trailer. Section 8 puts the column count at header offset 20
/// and the three table offsets at 40, 44, and 48.
fn read_wire(bytes: &[u8]) -> Wire {
    let mut wire = Wire::default();
    let mut offset = 64;
    while offset + 48 <= bytes.len() {
        let header = offset + 48;
        let header_length = u32at(bytes, offset + 16) as usize;
        let payload_length = u64at(bytes, offset + 24) as usize;
        assert!(
            header + header_length <= bytes.len(),
            "frame header at {offset} runs past the file"
        );

        if u16at(bytes, offset + 8) == DATA_FRAME_TYPE {
            wire.data_frames += 1;
            let columns = u32at(bytes, header + 20) as usize;
            let column_table = u32at(bytes, header + 40) as usize;
            let stream_table = u32at(bytes, header + 44) as usize;
            let statistics = u32at(bytes, header + 48) as usize;
            for index in 0..columns {
                let descriptor = header + column_table + index * 32;
                wire.layouts.insert(u16at(bytes, descriptor + 4));
                wire.statistics_kinds.insert(u16at(bytes, descriptor + 22));
            }
            for index in 0..(statistics - stream_table) / 48 {
                let descriptor = header + stream_table + index * 48;
                wire.transforms.insert(u16at(bytes, descriptor + 2));
                wire.codecs.insert(u16at(bytes, descriptor + 4));
            }
        }
        offset += 48 + header_length + payload_length + 32;
    }
    wire
}

// ------------------------------------------------------------------- the data

/// Every logical type, in both a nullable and a non-nullable form where the
/// distinction changes the encoding, plus the three null patterns a column can
/// take and the two value patterns that only a dedicated column produces.
fn corpus_schema(primary: bool) -> Schema {
    let columns = vec![
        Column::new(1, "bool_null", LogicalType::Bool, true),
        Column::new(2, "bool", LogicalType::Bool, false),
        Column::new(3, "i8", LogicalType::Int8, true),
        Column::new(4, "i16", LogicalType::Int16, false),
        Column::new(5, "i32", LogicalType::Int32, true),
        Column::new(6, "i64", LogicalType::Int64, false),
        Column::new(7, "u8", LogicalType::UInt8, true),
        Column::new(8, "u16", LogicalType::UInt16, false),
        Column::new(9, "u32", LogicalType::UInt32, true),
        Column::new(10, "u64", LogicalType::UInt64, false),
        Column::new(11, "f32", LogicalType::Float32, true),
        Column::new(12, "f64", LogicalType::Float64, false),
        Column::new(
            13,
            "decimal",
            LogicalType::Decimal {
                precision: 18,
                scale: 4,
            },
            true,
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
        Column::new(
            18,
            "fixed",
            LogicalType::FixedBinary { byte_width: 5 },
            true,
        ),
        // The three null patterns, held fixed so every case carries all of
        // them regardless of which value shape it selected.
        Column::new(19, "all_null", LogicalType::Int64, true),
        Column::new(20, "all_valid", LogicalType::Int32, true),
        Column::new(21, "constant", LogicalType::Int64, false),
        Column::new(22, "long_runs", LogicalType::Int64, false),
        Column::new(
            23,
            "ts",
            LogicalType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: TimeZone::Naive,
            },
            false,
        ),
    ];
    Schema::new(21, columns, primary.then_some(23))
}

/// The five value shapes, chosen for the encodings they make profitable:
/// constant, monotonic, low cardinality, long runs, and pseudorandom.
///
/// Rows are addressed absolutely, so a slice of one shape is the same data
/// whichever append delivered it. That is what makes the append-boundary
/// axis testable.
fn corpus_batch(
    schema: &Schema,
    shape: usize,
    start: usize,
    end: usize,
    sorted: bool,
) -> RecordBatch {
    let rows = end - start;
    let span = || start..end;
    let key = |row: usize| -> i64 {
        match shape {
            0 => 7,
            1 => row as i64 * 3,
            2 => (row % 4) as i64,
            3 => (row / 32) as i64,
            _ => (row as i64).wrapping_mul(0x9e37_79b9_7f4a_7c15_u64 as i64),
        }
    };
    let nulls = |offset: usize| -> Option<Vec<bool>> {
        match shape {
            0 => None,
            1 => Some(span().map(|row| (row + offset) % 5 != 0).collect()),
            2 => Some(vec![true; rows]),
            3 => Some(span().map(|row| row / 32 % 2 == 0).collect()),
            _ => Some(span().map(|row| (row + offset) % 2 == 0).collect()),
        }
    };
    // Signed zero, both NaN signs with payloads, and an infinity, so a
    // transform that dropped a bit could not pass the value comparison.
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

    let columns = vec![
        Array::Bool(BooleanArray::new(
            span().map(|row| row % 3 == 0).collect(),
            nulls(0),
        )),
        // Runs long enough that Boolean RLE can beat bit packing once the
        // block is large; see the 4096-row case below.
        Array::Bool(BooleanArray::new(
            span().map(|row| row / 256 % 2 == 0).collect(),
            None,
        )),
        Array::Int8(PrimitiveArray::new(
            span().map(|row| key(row) as i8).collect(),
            nulls(1),
        )),
        Array::Int16(PrimitiveArray::new(
            span().map(|row| key(row) as i16).collect(),
            None,
        )),
        Array::Int32(PrimitiveArray::new(
            span().map(|row| key(row) as i32).collect(),
            nulls(2),
        )),
        Array::Int64(PrimitiveArray::new(span().map(key).collect(), None)),
        Array::UInt8(PrimitiveArray::new(
            span().map(|row| key(row) as u8).collect(),
            nulls(3),
        )),
        Array::UInt16(PrimitiveArray::new(
            span().map(|row| key(row) as u16).collect(),
            None,
        )),
        Array::UInt32(PrimitiveArray::new(
            span().map(|row| key(row) as u32).collect(),
            nulls(0),
        )),
        Array::UInt64(PrimitiveArray::new(
            span().map(|row| key(row) as u64).collect(),
            None,
        )),
        Array::Float32(PrimitiveArray::new(
            span()
                .map(|row| f32::from_bits(singles[row % singles.len()]))
                .collect(),
            nulls(1),
        )),
        Array::Float64(PrimitiveArray::new(
            span()
                .map(|row| f64::from_bits(doubles[row % doubles.len()]))
                .collect(),
            None,
        )),
        Array::Decimal(DecimalArray::new(
            span().map(key).collect(),
            nulls(2),
            18,
            4,
        )),
        Array::Date32(PrimitiveArray::new(
            span().map(|row| key(row) as i32).collect(),
            nulls(3),
        )),
        Array::Utf8(Utf8Array::new(
            span().map(|row| format!("v{}", key(row) % 8)).collect(),
            nulls(0),
        )),
        Array::Categorical(Utf8Array::new(
            span().map(|row| format!("c{}", row % 3)).collect(),
            None,
        )),
        Array::Binary(BinaryArray::new(
            span().map(|row| vec![(row % 7) as u8; 3]).collect(),
            nulls(1),
        )),
        Array::FixedBinary(BinaryArray::new(
            span().map(|row| vec![(row % 5) as u8; 5]).collect(),
            nulls(2),
        )),
        Array::Int64(PrimitiveArray::new(vec![0; rows], Some(vec![false; rows]))),
        Array::Int32(PrimitiveArray::new(
            span().map(|row| row as i32).collect(),
            Some(vec![true; rows]),
        )),
        Array::Int64(PrimitiveArray::new(vec![42; rows], None)),
        Array::Int64(PrimitiveArray::new(
            span().map(|row| (row / 16) as i64).collect(),
            None,
        )),
        Array::Timestamp(TimestampArray::new(
            span()
                .map(|row| {
                    if sorted {
                        row as i64 * 1_000
                    } else {
                        ((row % 8) as i64) * 1_000 - row as i64
                    }
                })
                .collect(),
            None,
            TimeUnit::Nanosecond,
            TimeZone::Naive,
        )),
    ];
    RecordBatch::try_new(Arc::new(schema.clone()), columns, rows).expect("a well-formed batch")
}

// ------------------------------------------------------------------ the cases

#[derive(Debug, Clone)]
struct Case {
    label: String,
    encoding: WriterEncoding,
    codec: WriterCodec,
    row_ids: bool,
    primary: bool,
    sorted: bool,
    shape: usize,
    rows: usize,
    block_rows: u64,
    splits: &'static [usize],
}

/// Block geometries: one row, one block, several blocks, and a ragged tail.
const GEOMETRIES: [(usize, u64); 4] = [(1, 65_536), (300, 65_536), (300, 64), (513, 128)];

/// Append-batch boundaries, including one that never aligns with a block.
const SPLITS: [&[usize]; 4] = [&[10_000], &[1], &[7, 13, 64, 128], &[150, 150]];

/// Primary column: present and sorted, present and unsorted, absent.
const PRIMARY: [(bool, bool); 3] = [(true, true), (true, false), (false, false)];

fn codecs() -> Vec<WriterCodec> {
    if cfg!(feature = "zstd") {
        vec![WriterCodec::None, WriterCodec::Zstandard]
    } else {
        vec![WriterCodec::None]
    }
}

/// The thinned matrix.
///
/// Encoding and codec are crossed in full, because they are the axis the
/// compatibility gate is actually about. Everything else is rotated against
/// them by index, so each value of each remaining axis is exercised under
/// every encoding and codec without the full product.
fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    let mut pair = 0;
    for encoding in [WriterEncoding::Raw, WriterEncoding::Adaptive] {
        for codec in codecs() {
            for shape in 0..5 {
                for step in 0..2 {
                    let index = shape * 2 + step;
                    let (rows, block_rows) = GEOMETRIES[index % GEOMETRIES.len()];
                    let splits = SPLITS[(index + pair) % SPLITS.len()];
                    let (primary, sorted) = PRIMARY[(index + pair) % PRIMARY.len()];
                    let row_ids = (index + pair) % 2 == 0;
                    cases.push(Case {
                        label: format!("{encoding:?}-{codec:?}-shape{shape}-{index}"),
                        encoding,
                        codec,
                        row_ids,
                        primary,
                        sorted,
                        shape,
                        rows,
                        block_rows,
                        splits,
                    });
                }
            }
            pair += 1;
        }
    }
    cases
}

/// Write one case and return the input batches beside the file's bytes.
fn write_case(case: &Case, path: &std::path::Path) -> Vec<RecordBatch> {
    let schema = corpus_schema(case.primary);
    let mut writer = Writer::create(
        path,
        schema.clone(),
        WriterOptions::default()
            .with_row_ids(case.row_ids)
            .with_row_block_target(case.block_rows)
            .with_codec(case.codec)
            .with_encoding(case.encoding),
    )
    .unwrap_or_else(|error| panic!("{}: create: {error}", case.label));

    let mut appended = Vec::new();
    let mut start = 0;
    for split in case
        .splits
        .iter()
        .copied()
        .chain(std::iter::once(usize::MAX))
    {
        if start >= case.rows {
            break;
        }
        let end = start.saturating_add(split).min(case.rows);
        let batch = corpus_batch(&schema, case.shape, start, end, case.sorted);
        writer
            .append(batch.clone())
            .unwrap_or_else(|error| panic!("{}: append: {error}", case.label));
        appended.push(batch);
        start = end;
    }
    let _ = writer
        .finish()
        .unwrap_or_else(|error| panic!("{}: finish: {error}", case.label));
    appended
}

fn full(path: &std::path::Path) -> acta::Result<ValidationReport> {
    acta::validate_with_options(
        path,
        ValidationOptions::default().with_level(ValidationLevel::Full),
    )
}

/// One column's values as comparable per-row text.
///
/// Floats print their bits rather than their value, so a transform that lost a
/// NaN payload or a zero's sign fails here instead of comparing equal.
fn slots(batches: &[RecordBatch], column: usize) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let array = batch.column(column).expect("a decoded column");
        for row in 0..batch.row_count() {
            out.push(match array.value_at(row) {
                Some(acta::ScalarValue::Float32(value)) => format!("f32:{:08x}", value.to_bits()),
                Some(acta::ScalarValue::Float64(value)) => format!("f64:{:016x}", value.to_bits()),
                other => format!("{other:?}"),
            });
        }
    }
    out
}

/// The rows a mask keeps, in the order they were already in.
fn kept(values: Vec<String>, mask: &[bool]) -> Vec<String> {
    values
        .into_iter()
        .zip(mask)
        .filter_map(|(value, keep)| keep.then_some(value))
        .collect()
}

/// The primary column's values, in file order.
fn primary_values(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    let mut values = Vec::new();
    for batch in batches {
        let array = batch.column(column).expect("a decoded primary column");
        for row in 0..batch.row_count() {
            match array.value_at(row) {
                Some(acta::ScalarValue::Timestamp { value, .. }) => values.push(value),
                other => panic!("the primary column decoded as {other:?}"),
            }
        }
    }
    values
}

/// Every Stage 5 read of a file the Stage 7b writer produced.
///
/// The full scan above establishes what the file means. Everything here has to
/// agree with it: a projection is the same values in the requested order, and
/// a range is the same values with the rows outside it removed. Running this
/// for every corpus case is what makes the gate cross-stage — each layout,
/// transform, and codec the writer chooses is read back through projection and
/// filtering, not only through a full scan.
fn assert_projections_and_ranges(case: &Case, reader: &Reader, full_scan: &[RecordBatch]) {
    let label = &case.label;
    let schema = corpus_schema(case.primary);
    let names: Vec<&str> = schema.columns().iter().map(Column::name).collect();
    let rows: usize = full_scan.iter().map(RecordBatch::row_count).sum();

    let collect = |scan: acta::Scan<'_>| -> Vec<RecordBatch> {
        scan.map(|batch| batch.unwrap_or_else(|error| panic!("{label}: {error}")))
            .collect()
    };

    // A sparse projection, and the same columns in the opposite order.
    let sparse: Vec<&str> = names.iter().copied().step_by(4).collect();
    let reversed: Vec<&str> = sparse.iter().copied().rev().collect();
    for projection in [sparse.clone(), reversed] {
        let batches = collect(
            reader
                .scan()
                .project(&projection)
                .unwrap_or_else(|error| panic!("{label}: projection: {error}")),
        );
        for (position, name) in projection.iter().enumerate() {
            let source = names.iter().position(|column| column == name).unwrap();
            assert_eq!(
                batches[0].schema().columns()[position].id(),
                schema.columns()[source].id(),
                "{label}: projected column {name} lost its ID"
            );
            assert_eq!(
                slots(&batches, position),
                slots(full_scan, source),
                "{label}: projected column {name} does not match the full scan"
            );
        }
    }

    // An empty projection still reports the shape of the file.
    let empty = collect(reader.scan().project([] as [&str; 0]).unwrap());
    assert!(
        empty.iter().all(|batch| batch.schema().column_count() == 0),
        "{label}: an empty projection produced columns"
    );
    assert_eq!(
        empty.iter().map(RecordBatch::row_count).sum::<usize>(),
        rows,
        "{label}: an empty projection lost rows"
    );

    let Some(primary_id) = schema.primary_column_id() else {
        assert_eq!(
            reader
                .scan()
                .primary_range(PrimaryRange::timestamp(0, 1))
                .expect_err("a file with no primary column cannot be range scanned")
                .kind(),
            acta::ErrorKind::InvalidArgument,
            "{label}"
        );
        return;
    };
    let primary = schema
        .columns()
        .iter()
        .position(|column| column.id() == primary_id)
        .expect("the primary column is in the schema");

    // A range that keeps a middle slice of the values actually present, so it
    // has both a lower and an upper edge inside the data.
    let values = primary_values(full_scan, primary);
    let mut sorted = values.clone();
    sorted.sort_unstable();
    let start = sorted[sorted.len() / 4];
    let end = sorted[sorted.len() * 3 / 4].saturating_add(1);
    let mask: Vec<bool> = values
        .iter()
        .map(|value| *value >= start && *value < end)
        .collect();
    let expected_rows = mask.iter().filter(|keep| **keep).count();
    // A range that happened to keep everything, or nothing, would let a broken
    // filter pass. Every case large enough to have a middle must lose rows to
    // it and keep rows through it.
    if rows >= 8 {
        assert!(
            expected_rows > 0 && expected_rows < rows,
            "{label}: range [{start}, {end}) selected {expected_rows} of {rows} rows, \
             which does not exercise filtering"
        );
    }

    // With the primary projected, and without it.
    let with_primary: Vec<&str> = std::iter::once(names[primary])
        .chain(
            sparse
                .iter()
                .copied()
                .filter(|name| *name != names[primary]),
        )
        .collect();
    let without_primary: Vec<&str> = sparse
        .iter()
        .copied()
        .filter(|name| *name != names[primary])
        .collect();
    for projection in [with_primary, without_primary] {
        let projects_primary = projection.contains(&names[primary]);
        let mut scan = reader
            .scan()
            .project(&projection)
            .unwrap()
            .primary_range(PrimaryRange::timestamp(start, end))
            .unwrap_or_else(|error| panic!("{label}: range: {error}"))
            .file_order();
        let batches: Vec<RecordBatch> = scan
            .by_ref()
            .map(|batch| batch.unwrap_or_else(|error| panic!("{label}: range scan: {error}")))
            .collect();

        assert_eq!(
            batches.iter().map(RecordBatch::row_count).sum::<usize>(),
            expected_rows,
            "{label}: range [{start}, {end}) returned the wrong number of rows"
        );
        assert_eq!(
            scan.metrics().rows_returned(),
            expected_rows as u64,
            "{label}: metrics disagree with the rows returned"
        );
        for (position, name) in projection.iter().enumerate() {
            let source = names.iter().position(|column| column == name).unwrap();
            assert_eq!(
                slots(&batches, position),
                kept(slots(full_scan, source), &mask),
                "{label}: range scan of {name} does not match the filtered full scan"
            );
        }
        if let Some(batch) = batches.first() {
            assert_eq!(
                batch.schema().primary_column_id(),
                projects_primary.then_some(primary_id),
                "{label}: the primary designation should survive exactly when its column does"
            );
        }
    }

    // An empty range decodes nothing at all, whatever the block layouts are.
    let mut empty_range = reader
        .scan()
        .primary_range(PrimaryRange::timestamp(start, start))
        .unwrap();
    assert!(
        empty_range.by_ref().next().is_none(),
        "{label}: an empty range returned rows"
    );
    let metrics = empty_range.metrics();
    assert_eq!(
        metrics.blocks_pruned(),
        metrics.blocks_considered(),
        "{label}"
    );
    assert_eq!(
        metrics.bytes_read(),
        0,
        "{label}: an empty range read bytes"
    );
}

/// Everything the acceptance gate asserts about one generated file.
fn assert_case(case: &Case) -> Wire {
    let file = TempPath::new(&case.label);
    let written = write_case(case, file.path());
    let bytes = std::fs::read(file.path()).expect("the written file");
    let label = &case.label;

    let structural = acta::validate(file.path())
        .unwrap_or_else(|error| panic!("{label}: structural validation failed: {error}"));
    let full = full(file.path())
        .unwrap_or_else(|error| panic!("{label}: full validation failed: {error}"));
    assert_eq!(
        structural, full,
        "{label}: the two validation levels describe the file differently"
    );
    assert!(!full.incomplete_tail(), "{label}");
    assert_eq!(full.file_size(), bytes.len() as u64, "{label}");
    assert_eq!(full.last_good_offset(), bytes.len() as u64, "{label}");

    let reader = Reader::open(file.path()).unwrap_or_else(|error| panic!("{label}: open: {error}"));
    let decoded: Vec<RecordBatch> = reader
        .scan()
        .map(|block| block.unwrap_or_else(|error| panic!("{label}: scan: {error}")))
        .collect();
    let expected_rows: usize = written.iter().map(RecordBatch::row_count).sum();
    let decoded_rows: usize = decoded.iter().map(RecordBatch::row_count).sum();
    assert_eq!(decoded_rows, expected_rows, "{label}: row count");
    for column in 0..written[0].schema().column_count() {
        assert_eq!(
            slots(&decoded, column),
            slots(&written, column),
            "{label}: column {column} did not survive the round trip"
        );
    }

    assert_projections_and_ranges(case, &reader, &decoded);

    assert_eq!(
        std::fs::read(file.path()).expect("the file after validation"),
        bytes,
        "{label}: validating or reading the file modified it"
    );

    let again = TempPath::new(&format!("{label}-again"));
    write_case(case, again.path());
    assert_eq!(
        std::fs::read(again.path()).expect("the rewritten file"),
        bytes,
        "{label}: the writer is not deterministic across runs"
    );

    let wire = read_wire(&bytes);
    assert_eq!(
        wire.statistics_kinds,
        BTreeSet::from([STATS_NONE]),
        "{label}: Stage 7b writes no statistics, so full validation is \
         accepting their absence rather than verifying them"
    );
    wire
}

// -------------------------------------------------------------------- the gate

/// The acceptance gate: every file the writer produces passes both validation
/// levels with the same answer, decodes to what went in, and is unchanged by
/// being read.
#[test]
fn every_representation_survives_both_validation_levels() {
    let mut wire = Wire::default();
    for case in cases() {
        wire.absorb(assert_case(&case));
    }
    // Boolean RLE only beats bit packing once a block is long enough for its
    // runs to pay for their own descriptors, which no case above reaches.
    wire.absorb(assert_case(&Case {
        label: "boolean-rle".to_owned(),
        encoding: WriterEncoding::Adaptive,
        codec: WriterCodec::None,
        row_ids: true,
        primary: true,
        sorted: true,
        shape: 3,
        rows: 4096,
        block_rows: 65_536,
        splits: &[10_000],
    }));

    assert!(wire.data_frames > 0, "the corpus produced no data frames");
    assert_eq!(
        wire.layouts,
        BTreeSet::from([
            COLUMN_LAYOUT_PLAIN,
            COLUMN_LAYOUT_CONSTANT,
            COLUMN_LAYOUT_DICTIONARY,
            COLUMN_LAYOUT_RUN_LENGTH,
        ]),
        "the corpus no longer reaches every v0.2 column layout"
    );

    let mut transforms = BTreeSet::from([
        TRANSFORM_RAW,
        TRANSFORM_BIT_PACKED,
        TRANSFORM_FRAME_OF_REFERENCE,
        TRANSFORM_DELTA,
        TRANSFORM_DELTA_OF_DELTA,
        TRANSFORM_BOOLEAN_RLE,
    ]);
    let mut codecs = BTreeSet::from([CODEC_NONE]);
    if cfg!(feature = "zstd") {
        // Byte-stream split is a permutation of the same bytes, so it can
        // never pay for itself without a codec behind it.
        transforms.insert(TRANSFORM_BYTE_STREAM_SPLIT);
        codecs.insert(CODEC_ZSTD);
    }
    assert_eq!(
        wire.transforms, transforms,
        "the corpus no longer reaches every reachable v0.2 stream transform"
    );
    assert_eq!(
        wire.codecs, codecs,
        "the corpus no longer reaches every available stream codec"
    );
}

/// Where a caller split its appends is not part of the file's meaning.
#[test]
fn append_boundaries_do_not_change_the_logical_file() {
    let base = Case {
        label: "boundary".to_owned(),
        encoding: WriterEncoding::Adaptive,
        codec: *codecs().last().expect("at least one codec"),
        row_ids: true,
        primary: true,
        sorted: true,
        shape: 4,
        rows: 300,
        block_rows: 65_536,
        splits: &[10_000],
    };

    let mut expected: Option<Vec<Vec<String>>> = None;
    let mut expected_bytes: Option<Vec<u8>> = None;
    for splits in SPLITS {
        let case = Case {
            label: format!("boundary-{}", splits.len()),
            splits,
            ..base.clone()
        };
        let file = TempPath::new(&case.label);
        write_case(&case, file.path());
        full(file.path()).unwrap_or_else(|error| panic!("{}: full: {error}", case.label));

        let reader = Reader::open(file.path()).expect("open");
        let batches: Vec<RecordBatch> = reader.scan().map(|block| block.expect("scan")).collect();
        let columns: Vec<Vec<String>> = (0..batches[0].schema().column_count())
            .map(|column| slots(&batches, column))
            .collect();
        let bytes = std::fs::read(file.path()).expect("the written file");

        match (&expected, &expected_bytes) {
            (Some(expected), Some(expected_bytes)) => {
                assert_eq!(&columns, expected, "{}: logical rows differ", case.label);
                // Section 12 selects per block from the block's values, which
                // do not depend on how the rows arrived, so the bytes match
                // too. This is a stronger claim than the logical one and is
                // asserted separately so a future relaxation is visible.
                assert_eq!(
                    &bytes, expected_bytes,
                    "{}: serialized bytes differ",
                    case.label
                );
            }
            _ => {
                expected = Some(columns);
                expected_bytes = Some(bytes);
            }
        }
    }
}

// -------------------------------------------------- Stage 7b byte compatibility

/// FNV-1a over the whole file. Any change to the writer's output moves it.
fn digest(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Fixed Stage 7b inputs and the exact output they produced at the Stage 7b
/// baseline, commit `34e99c9`.
///
/// Stage 3 is a reader-side change and must not move a single writer byte.
/// The round-trip assertions above would not notice if it did, because a
/// changed file that still decodes correctly still passes them. These digests
/// are the tripwire. A failure here means writer output changed: either
/// deliberately, in which case update the constants in the same commit that
/// changes the writer, or accidentally, in which case do not.
/// One frozen writer output: the inputs that produce it, then its exact byte
/// length and digest.
type Baseline = (
    &'static str,
    WriterEncoding,
    WriterCodec,
    bool,
    usize,
    u64,
    u64,
    u64,
);

#[rustfmt::skip]
const BASELINE: [Baseline; 6] = [
    ("raw-none",             WriterEncoding::Raw,      WriterCodec::None,      false, 300, 65_536, 33_240, 18_145_985_840_752_257_073),
    ("raw-none-rowids",      WriterEncoding::Raw,      WriterCodec::None,      true,  300,     64, 43_872, 13_749_189_719_651_632_285),
    ("adaptive-none",        WriterEncoding::Adaptive, WriterCodec::None,      false, 300, 65_536,  6_224,  9_492_155_810_594_973_009),
    ("adaptive-none-blocks", WriterEncoding::Adaptive, WriterCodec::None,      true,  513,    128, 19_800, 10_569_379_940_790_712_607),
    ("raw-zstd",             WriterEncoding::Raw,      WriterCodec::Zstandard, false, 300, 65_536, 10_296, 12_030_481_320_663_855_013),
    ("adaptive-zstd",        WriterEncoding::Adaptive, WriterCodec::Zstandard, true,  300,     64, 19_464, 13_334_588_205_221_543_959),
];

#[test]
fn stage_7b_writer_output_is_byte_identical_to_its_baseline() {
    for (label, encoding, codec, row_ids, rows, block_rows, length, expected) in BASELINE {
        if codec == WriterCodec::Zstandard && !cfg!(feature = "zstd") {
            continue;
        }
        let case = Case {
            label: format!("baseline-{label}"),
            encoding,
            codec,
            row_ids,
            primary: true,
            sorted: true,
            shape: 1,
            rows,
            block_rows,
            splits: &[64],
        };
        let file = TempPath::new(&case.label);
        write_case(&case, file.path());
        let bytes = std::fs::read(file.path()).expect("the written file");
        assert_eq!(
            (bytes.len() as u64, digest(&bytes)),
            (length, expected),
            "{label}: Stage 7b writer output changed"
        );
    }
}
