//! Stage 7c writer statistics generation, selection, and cross-stage coverage.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use acta::{
    Array, BinaryArray, BooleanArray, Column, DecimalArray, LogicalType, PrimitiveArray, Reader,
    RecordBatch, Schema, TimeUnit, TimeZone, TimestampArray, Utf8Array, ValidationLevel,
    ValidationOptions, Writer, WriterCodec, WriterEncoding, WriterOptions, WriterStatistics,
    WriterTransform,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TempPath(std::path::PathBuf);

impl TempPath {
    fn new(label: &str) -> Self {
        let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "acta-stage7c-{label}-{}-{id}.acta",
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

/// What one file's data blocks declare, read back out of the bytes on disk.
///
/// The statistics assertions below compare against expected bytes written by
/// hand from section 11 rather than against anything the writer produced, so a
/// shared mistake in the generator cannot make them pass.
#[derive(Debug, Default)]
struct Wire {
    kinds: Vec<u16>,
    statistics: Vec<Vec<u8>>,
    column_ids: Vec<u32>,
    /// Per data block: the stream descriptor table, which carries the layout,
    /// transform, and codec Stage 7b selected.
    stream_tables: Vec<Vec<u8>>,
    /// Per data block: the frame payload, which is every encoded value.
    payloads: Vec<Vec<u8>>,
    /// Per data block: every column descriptor with its statistics flag bit and
    /// statistics triple cleared, so two policies over the same rows can be
    /// compared on everything else.
    descriptors_without_statistics: Vec<Vec<u8>>,
}

fn u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("u16"))
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("u32"))
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("u64"))
}

/// Walk the file's frames and check every section 8 and section 11 rule the
/// bytes alone can settle, then return what the blocks declare.
fn wire(bytes: &[u8]) -> Wire {
    let mut result = Wire::default();
    let mut offset = 64;
    while offset < bytes.len() {
        let header_length = u32_at(bytes, offset + 16) as usize;
        let payload_length = u64_at(bytes, offset + 24) as usize;
        let header = offset + 48;
        if u16_at(bytes, offset + 8) == 2 {
            let columns = u32_at(bytes, header + 20) as usize;
            let column_table = u32_at(bytes, header + 40) as usize;
            let stream_table = u32_at(bytes, header + 44) as usize;
            let statistics_area = u32_at(bytes, header + 48) as usize;
            let statistics_length = u32_at(bytes, header + 52) as usize;

            // Section 8 leaves no untyped gap between the header tables, and
            // the statistics area is the last of them.
            assert_eq!(column_table, 64, "the column table must start at 64");
            assert_eq!(
                stream_table,
                64 + 32 * columns,
                "the stream table must follow the column table"
            );
            assert!(statistics_area >= stream_table);
            assert_eq!(
                (statistics_area - stream_table) % 48,
                0,
                "the statistics area must start on a stream descriptor boundary"
            );
            assert!(
                statistics_area + statistics_length <= header_length,
                "the statistics area must lie inside the padded frame header"
            );
            assert_eq!(u32_at(bytes, header + 60), 0, "reserved field must be zero");

            result
                .stream_tables
                .push(bytes[header + stream_table..header + statistics_area].to_vec());
            result.payloads.push(
                bytes[header + header_length..header + header_length + payload_length].to_vec(),
            );

            let mut declared = 0;
            let mut previous_id = 0;
            let mut without_statistics = Vec::new();
            for index in 0..columns {
                let descriptor = header + 64 + index * 32;
                let column_id = u32_at(bytes, descriptor);
                assert!(
                    column_id > previous_id,
                    "section 8.1: the column table is sorted by column ID"
                );
                previous_id = column_id;
                result.column_ids.push(column_id);

                let flags = u16_at(bytes, descriptor + 6);
                let kind = u16_at(bytes, descriptor + 22);
                let relative = u32_at(bytes, descriptor + 24) as usize;
                let length = u32_at(bytes, descriptor + 28) as usize;
                assert_eq!(
                    flags & 0b10 != 0,
                    kind != 0,
                    "the statistics flag and kind must agree"
                );
                result.kinds.push(kind);

                let mut cleared = bytes[descriptor..descriptor + 32].to_vec();
                cleared[6] &= !0b10;
                cleared[22..32].fill(0);
                without_statistics.extend_from_slice(&cleared);

                if kind == 0 {
                    assert_eq!(
                        (relative, length),
                        (0, 0),
                        "a column without statistics must declare a zero offset and length"
                    );
                    continue;
                }
                assert_eq!(kind, 1, "v0.2 defines only statistics kind one");
                assert!(
                    relative >= statistics_area
                        && relative + length <= statistics_area + statistics_length,
                    "statistics must lie inside the declared statistics area"
                );
                declared += length;
                result
                    .statistics
                    .push(bytes[header + relative..header + relative + length].to_vec());
            }
            result
                .descriptors_without_statistics
                .push(without_statistics);
            // The area holds the pairs plus at most one alignment unit.
            assert!(
                statistics_length - declared < 8,
                "the statistics area must not be padded beyond its alignment"
            );
            if declared == 0 {
                assert_eq!(
                    statistics_length, 0,
                    "an area with no statistics must have zero length"
                );
            }
        }
        offset += 48 + header_length + payload_length + 32;
    }
    result
}

fn write(
    schema: Schema,
    batch: RecordBatch,
    options: WriterOptions,
    label: &str,
) -> (TempPath, Vec<u8>) {
    write_all(schema, vec![batch], options, label)
}

fn write_all(
    schema: Schema,
    batches: Vec<RecordBatch>,
    options: WriterOptions,
    label: &str,
) -> (TempPath, Vec<u8>) {
    let path = TempPath::new(label);
    let mut writer = Writer::create(path.path(), schema, options).expect("create");
    for batch in batches {
        writer.append(batch).expect("append");
    }
    let _ = writer.finish().expect("finish");
    let bytes = std::fs::read(path.path()).expect("read");
    (path, bytes)
}

/// Write, then require both validation levels and a full scan to accept it.
fn write_and_validate(
    schema: Schema,
    batches: Vec<RecordBatch>,
    options: WriterOptions,
    label: &str,
) -> (TempPath, Vec<u8>) {
    let (path, bytes) = write_all(schema, batches, options, label);
    acta::validate(path.path()).unwrap_or_else(|error| panic!("{label} structural: {error}"));
    acta::validate_with_options(
        path.path(),
        ValidationOptions::default().with_level(ValidationLevel::Full),
    )
    .unwrap_or_else(|error| panic!("{label} full: {error}"));
    let reader = Reader::open(path.path()).expect("reader");
    for batch in reader.scan() {
        batch.unwrap_or_else(|error| panic!("{label} scan: {error}"));
    }
    (path, bytes)
}

fn scalar_schema() -> Schema {
    Schema::new(
        701,
        vec![
            Column::new(
                1,
                "timestamp",
                LogicalType::Timestamp {
                    unit: TimeUnit::Millisecond,
                    timezone: TimeZone::Utc,
                },
                false,
            ),
            Column::new(2, "value", LogicalType::Int64, false),
            Column::new(3, "flag", LogicalType::Bool, false),
            Column::new(
                4,
                "fixed",
                LogicalType::FixedBinary { byte_width: 3 },
                false,
            ),
            Column::new(5, "text", LogicalType::Utf8, false),
        ],
        Some(1),
    )
}

fn scalar_batch(schema: &Schema, rows: usize) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Timestamp(TimestampArray::new(
                (0..rows).map(|row| row as i64 * 10).collect(),
                None,
                TimeUnit::Millisecond,
                TimeZone::Utc,
            )),
            Array::Int64(PrimitiveArray::new(
                (0..rows).map(|row| row as i64 - 100).collect(),
                None,
            )),
            Array::Bool(BooleanArray::new(
                (0..rows).map(|row| row % 2 == 0).collect(),
                None,
            )),
            Array::FixedBinary(BinaryArray::new(
                (0..rows)
                    .map(|row| vec![row as u8, 0xff, (rows - row) as u8])
                    .collect(),
                None,
            )),
            Array::Utf8(Utf8Array::new(
                (0..rows).map(|row| format!("v{}", row % 4)).collect(),
                None,
            )),
        ],
        rows,
    )
    .expect("batch")
}

#[test]
fn none_is_the_default_and_preserves_byte_identity() {
    let schema = scalar_schema();
    let batch = scalar_batch(&schema, 128);
    let (_, default_bytes) = write(
        schema.clone(),
        batch.clone(),
        WriterOptions::default(),
        "default",
    );
    let (_, explicit_bytes) = write(
        schema,
        batch,
        WriterOptions::default().with_statistics(WriterStatistics::None),
        "explicit-none",
    );
    assert_eq!(default_bytes, explicit_bytes);
    assert!(wire(&default_bytes).kinds.iter().all(|kind| *kind == 0));
}

#[test]
fn minmax_generates_canonical_statistics_and_round_trips() {
    let schema = scalar_schema();
    let batch = scalar_batch(&schema, 128);
    let (path, bytes) = write(
        schema,
        batch,
        WriterOptions::default()
            .with_encoding(WriterEncoding::Adaptive)
            .with_statistics(WriterStatistics::MinMax),
        "minmax",
    );
    let wire = wire(&bytes);
    assert_eq!(wire.kinds, vec![1, 1, 1, 1, 0]);
    assert_eq!(
        wire.statistics[0],
        0_i64
            .to_le_bytes()
            .into_iter()
            .chain(1_270_i64.to_le_bytes())
            .collect::<Vec<_>>()
    );
    assert_eq!(wire.statistics[0].len(), 16);
    assert_eq!(
        wire.statistics[1],
        (-100_i64)
            .to_le_bytes()
            .into_iter()
            .chain(27_i64.to_le_bytes())
            .collect::<Vec<_>>()
    );
    assert_eq!(wire.statistics[2], vec![0, 1]);
    assert_eq!(wire.statistics[3], vec![0, 0xff, 128, 127, 0xff, 1]);

    acta::validate_with_options(
        path.path(),
        ValidationOptions::default().with_level(ValidationLevel::Full),
    )
    .expect("full validation");
    let reader = Reader::open(path.path()).expect("reader");
    let rows = reader
        .scan()
        .map(|batch| batch.expect("scan"))
        .map(|batch| batch.row_count())
        .sum::<usize>();
    assert_eq!(rows, 128);
}

#[test]
fn automatic_statistics_are_conservative_and_skip_primary_bounds() {
    let schema = scalar_schema();
    let batch = scalar_batch(&schema, 128);
    let (path, bytes) = write(
        schema.clone(),
        batch,
        WriterOptions::default().with_statistics(WriterStatistics::Automatic),
        "automatic",
    );
    assert_eq!(wire(&bytes).kinds, vec![0, 1, 1, 1, 0]);
    acta::validate_with_options(
        path.path(),
        ValidationOptions::default().with_level(ValidationLevel::Full),
    )
    .expect("full validation");

    let reader = Reader::open(path.path()).expect("reader");
    let mut scan = reader
        .scan()
        .primary_range(acta::PrimaryRange::timestamp(640, 960))
        .expect("range");
    let rows = scan
        .by_ref()
        .map(|batch| batch.expect("scan"))
        .map(|batch| batch.row_count())
        .sum::<usize>();
    assert_eq!(rows, 32);
    assert_eq!(scan.metrics().blocks_pruned(), 0);
}

#[test]
fn automatic_statistics_omit_small_blocks_and_all_nan_columns() {
    let schema = Schema::new(
        702,
        vec![
            Column::new(1, "small", LogicalType::Int64, false),
            Column::new(2, "nan", LogicalType::Float64, true),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Int64(PrimitiveArray::new(
                (0..32).map(|value| value as i64).collect(),
                None,
            )),
            Array::Float64(PrimitiveArray::new(
                vec![f64::NAN; 32],
                Some(vec![true; 32]),
            )),
        ],
        32,
    );
    // Keep the construction above explicit and typed: the small block is
    // below the automatic threshold and the second column has no extrema.
    let batch = batch.expect("batch");
    let (_, bytes) = write(
        schema,
        batch,
        WriterOptions::default().with_statistics(WriterStatistics::Automatic),
        "automatic-small",
    );
    assert_eq!(wire(&bytes).kinds, vec![0, 0]);
}

#[test]
fn minmax_covers_all_fixed_width_logical_types() {
    let schema = Schema::new(
        703,
        vec![
            Column::new(1, "bool", LogicalType::Bool, false),
            Column::new(2, "i8", LogicalType::Int8, false),
            Column::new(3, "i16", LogicalType::Int16, false),
            Column::new(4, "i32", LogicalType::Int32, false),
            Column::new(5, "i64", LogicalType::Int64, false),
            Column::new(6, "u8", LogicalType::UInt8, false),
            Column::new(7, "u16", LogicalType::UInt16, false),
            Column::new(8, "u32", LogicalType::UInt32, false),
            Column::new(9, "u64", LogicalType::UInt64, false),
            Column::new(10, "f32", LogicalType::Float32, false),
            Column::new(11, "f64", LogicalType::Float64, false),
            Column::new(
                12,
                "decimal",
                LogicalType::Decimal {
                    precision: 18,
                    scale: 2,
                },
                false,
            ),
            Column::new(
                13,
                "timestamp",
                LogicalType::Timestamp {
                    unit: TimeUnit::Nanosecond,
                    timezone: TimeZone::Naive,
                },
                false,
            ),
            Column::new(
                14,
                "fixed",
                LogicalType::FixedBinary { byte_width: 2 },
                false,
            ),
            Column::new(15, "date", LogicalType::Date32, false),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Bool(BooleanArray::new(vec![true, false, true], None)),
            Array::Int8(PrimitiveArray::new(vec![-2, 4, 1], None)),
            Array::Int16(PrimitiveArray::new(vec![-3, 8, 1], None)),
            Array::Int32(PrimitiveArray::new(vec![-4, 16, 1], None)),
            Array::Int64(PrimitiveArray::new(vec![-5, 32, 1], None)),
            Array::UInt8(PrimitiveArray::new(vec![2, 4, 1], None)),
            Array::UInt16(PrimitiveArray::new(vec![3, 8, 1], None)),
            Array::UInt32(PrimitiveArray::new(vec![4, 16, 1], None)),
            Array::UInt64(PrimitiveArray::new(vec![5, 32, 1], None)),
            Array::Float32(PrimitiveArray::new(vec![-1.5, 2.5, 0.0], None)),
            Array::Float64(PrimitiveArray::new(vec![-2.5, 3.5, 0.0], None)),
            Array::Decimal(DecimalArray::new(vec![-6, 64, 1], None, 18, 2)),
            Array::Timestamp(TimestampArray::new(
                vec![-7, 128, 1],
                None,
                TimeUnit::Nanosecond,
                TimeZone::Naive,
            )),
            Array::FixedBinary(BinaryArray::new(
                vec![vec![2, 0], vec![1, 255], vec![1, 0]],
                None,
            )),
            Array::Date32(PrimitiveArray::new(vec![-9, 256, 1], None)),
        ],
        3,
    )
    .expect("batch");
    let (path, bytes) = write(
        schema,
        batch,
        WriterOptions::default().with_statistics(WriterStatistics::MinMax),
        "all-types",
    );
    let wire = wire(&bytes);
    assert_eq!(wire.kinds, vec![1; 15]);

    // Section 11 byte for byte, in column order, written out here rather than
    // taken from the writer: each pair is the canonical minimum followed by the
    // canonical maximum, little endian, at the logical type's own width.
    let expected: Vec<Vec<u8>> = vec![
        vec![0, 1],
        vec![(-2_i8) as u8, 4],
        [(-3_i16).to_le_bytes(), 8_i16.to_le_bytes()].concat(),
        [(-4_i32).to_le_bytes(), 16_i32.to_le_bytes()].concat(),
        [(-5_i64).to_le_bytes(), 32_i64.to_le_bytes()].concat(),
        vec![1, 4],
        [1_u16.to_le_bytes(), 8_u16.to_le_bytes()].concat(),
        [1_u32.to_le_bytes(), 16_u32.to_le_bytes()].concat(),
        [1_u64.to_le_bytes(), 32_u64.to_le_bytes()].concat(),
        [
            (-1.5_f32).to_bits().to_le_bytes(),
            2.5_f32.to_bits().to_le_bytes(),
        ]
        .concat(),
        [
            (-2.5_f64).to_bits().to_le_bytes(),
            3.5_f64.to_bits().to_le_bytes(),
        ]
        .concat(),
        // A decimal bound is its unscaled integer, and a timestamp bound is its
        // epoch count; neither carries its parameters.
        [(-6_i64).to_le_bytes(), 64_i64.to_le_bytes()].concat(),
        [(-7_i64).to_le_bytes(), 128_i64.to_le_bytes()].concat(),
        // Lexicographic: [1, 0] < [1, 255] < [2, 0].
        vec![1, 0, 2, 0],
        [(-9_i32).to_le_bytes(), 256_i32.to_le_bytes()].concat(),
    ];
    assert_eq!(wire.statistics, expected);

    acta::validate_with_options(
        path.path(),
        ValidationOptions::default().with_level(ValidationLevel::Full),
    )
    .expect("all generated statistics validate");
}

// ------------------------------------------------- value edge cases on the wire

/// Nulls, NaNs, infinities, signed zero, and an all-null column, checked
/// against bytes written by hand from section 11 rather than by the writer.
#[test]
fn extrema_ignore_nulls_and_nans_and_keep_infinities() {
    let schema = Schema::new(
        710,
        vec![
            Column::new(1, "gaps", LogicalType::Int64, true),
            Column::new(2, "edges", LogicalType::Float64, true),
            Column::new(3, "zeroes", LogicalType::Float32, false),
            Column::new(4, "absent", LogicalType::Int32, true),
            Column::new(5, "unbounded", LogicalType::Float64, false),
            Column::new(6, "fixed", LogicalType::FixedBinary { byte_width: 4 }, true),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            // The null positions hold the type's extremes, so a statistic that
            // failed to skip them could not produce the expected bytes.
            Array::Int64(PrimitiveArray::new(
                vec![i64::MIN, 7, -3, 42, i64::MAX],
                Some(vec![false, true, true, true, false]),
            )),
            Array::Float64(PrimitiveArray::new(
                vec![f64::NAN, f64::NEG_INFINITY, f64::INFINITY, 1.5, -999.0],
                Some(vec![true, true, true, true, false]),
            )),
            // Every value is numerically zero, so the first bit pattern wins.
            Array::Float32(PrimitiveArray::new(vec![-0.0, 0.0, -0.0, 0.0, 0.0], None)),
            Array::Int32(PrimitiveArray::new(
                vec![1, 2, 3, 4, 5],
                Some(vec![false; 5]),
            )),
            Array::Float64(PrimitiveArray::new(vec![f64::NAN; 5], None)),
            // 0x00ffffff sorts below 0x01000000 only under byte order.
            Array::FixedBinary(BinaryArray::new(
                vec![
                    vec![0x01, 0x00, 0x00, 0x00],
                    vec![0x00, 0xff, 0xff, 0xff],
                    vec![0xff, 0xff, 0xff, 0xff],
                    vec![0x01, 0x00, 0x00, 0x01],
                    vec![0x7f, 0x00, 0x00, 0x00],
                ],
                Some(vec![true, true, false, true, true]),
            )),
        ],
        5,
    )
    .expect("batch");

    let (_path, bytes) = write_and_validate(
        schema,
        vec![batch],
        WriterOptions::default().with_statistics(WriterStatistics::MinMax),
        "edges",
    );
    let wire = wire(&bytes);
    // The all-null and all-NaN columns have no extrema and claim nothing.
    assert_eq!(wire.kinds, vec![1, 1, 1, 0, 0, 1]);

    let mut present = wire.statistics.into_iter();
    assert_eq!(
        present.next().expect("gaps"),
        [(-3_i64).to_le_bytes(), 42_i64.to_le_bytes()].concat()
    );
    assert_eq!(
        present.next().expect("edges"),
        [
            f64::NEG_INFINITY.to_bits().to_le_bytes(),
            f64::INFINITY.to_bits().to_le_bytes()
        ]
        .concat()
    );
    assert_eq!(
        present.next().expect("zeroes"),
        [(-0.0_f32).to_bits().to_le_bytes(); 2].concat(),
        "the first bit pattern of an all-zero column is kept for both bounds"
    );
    assert_eq!(
        present.next().expect("fixed"),
        vec![0x00, 0xff, 0xff, 0xff, 0x7f, 0x00, 0x00, 0x00]
    );
    assert!(present.next().is_none());
}

/// Section 8.1 sorts the column table by ID even when the schema does not, so
/// each statistic must follow its own column rather than its schema position.
#[test]
fn statistics_follow_column_ids_through_an_unsorted_schema() {
    let schema = Schema::new(
        711,
        vec![
            Column::new(30, "third", LogicalType::Int64, false),
            Column::new(10, "second", LogicalType::Int8, false),
            Column::new(
                20,
                "bytes",
                LogicalType::FixedBinary { byte_width: 2 },
                false,
            ),
            Column::new(5, "first", LogicalType::UInt16, false),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Int64(PrimitiveArray::new(vec![1000, 2000, 3000], None)),
            Array::Int8(PrimitiveArray::new(vec![-8, -7, -6], None)),
            Array::FixedBinary(BinaryArray::new(
                vec![vec![0x20, 0x01], vec![0x20, 0x00], vec![0x20, 0x02]],
                None,
            )),
            Array::UInt16(PrimitiveArray::new(vec![500, 400, 600], None)),
        ],
        3,
    )
    .expect("batch");

    let (_path, bytes) = write_and_validate(
        schema,
        vec![batch],
        WriterOptions::default().with_statistics(WriterStatistics::MinMax),
        "unsorted",
    );
    let wire = wire(&bytes);
    assert_eq!(wire.column_ids, vec![5, 10, 20, 30]);
    assert_eq!(
        wire.statistics,
        vec![
            [400_u16.to_le_bytes(), 600_u16.to_le_bytes()].concat(),
            vec![(-8_i8) as u8, (-6_i8) as u8],
            vec![0x20, 0x00, 0x20, 0x02],
            [1000_i64.to_le_bytes(), 3000_i64.to_le_bytes()].concat(),
        ]
    );
}

// ------------------------------------------------------- determinism and blocks

fn series_schema() -> Schema {
    Schema::new(
        712,
        vec![
            Column::new(
                1,
                "t",
                LogicalType::Timestamp {
                    unit: TimeUnit::Millisecond,
                    timezone: TimeZone::Utc,
                },
                false,
            ),
            Column::new(2, "v", LogicalType::Int64, false),
        ],
        Some(1),
    )
}

fn series_batch(schema: &Schema, start: usize, end: usize) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Timestamp(TimestampArray::new(
                (start..end).map(|row| row as i64 * 10).collect(),
                None,
                TimeUnit::Millisecond,
                TimeZone::Utc,
            )),
            Array::Int64(PrimitiveArray::new(
                (start..end).map(|row| (row as i64 * 7) % 101).collect(),
                None,
            )),
        ],
        end - start,
    )
    .expect("batch")
}

/// The same rows in the same order must produce the same file however they
/// were divided across appends, including where a batch straddles a block.
#[test]
fn append_partitioning_does_not_change_the_bytes() {
    let schema = series_schema();
    let options = WriterOptions::default()
        .with_statistics(WriterStatistics::MinMax)
        .with_row_block_target(100);
    let partitionings = [
        vec![0_usize, 300],
        vec![0, 1, 99, 100, 101, 250, 300],
        vec![0, 50, 150, 151, 299, 300],
    ];

    let mut expected: Option<Vec<u8>> = None;
    for (index, cuts) in partitionings.iter().enumerate() {
        let batches = cuts
            .windows(2)
            .map(|pair| series_batch(&schema, pair[0], pair[1]))
            .collect::<Vec<_>>();
        let (_path, bytes) =
            write_and_validate(schema.clone(), batches, options, &format!("part{index}"));
        match &expected {
            None => {
                // Three blocks of a hundred rows, each bounded by its own rows.
                let wire = wire(&bytes);
                assert_eq!(wire.kinds, vec![1; 6]);
                assert_eq!(
                    wire.statistics[0],
                    [0_i64.to_le_bytes(), 990_i64.to_le_bytes()].concat()
                );
                assert_eq!(
                    wire.statistics[2],
                    [1_000_i64.to_le_bytes(), 1_990_i64.to_le_bytes()].concat()
                );
                assert_eq!(
                    wire.statistics[4],
                    [2_000_i64.to_le_bytes(), 2_990_i64.to_le_bytes()].concat()
                );
                expected = Some(bytes);
            }
            Some(first) => assert_eq!(
                &bytes, first,
                "partitioning {index} produced different bytes"
            ),
        }
    }
}

// --------------------------------------------------------- policy independence

/// Statistics must not disturb anything Stage 7b decided. Only the statistics
/// flag bit, the statistics triple, and the area itself may differ.
#[test]
fn statistics_leave_every_encoding_and_codec_choice_alone() {
    let schema = series_schema();
    let mut policies = vec![
        ("raw", WriterOptions::default()),
        (
            "adaptive",
            WriterOptions::default().with_encoding(WriterEncoding::Adaptive),
        ),
        (
            "fixed-delta",
            WriterOptions::default().with_encoding(WriterEncoding::Fixed(WriterTransform::Delta)),
        ),
    ];
    if cfg!(feature = "zstd") {
        policies.push((
            "zstd",
            WriterOptions::default().with_codec(WriterCodec::Zstandard),
        ));
    }

    for (label, base) in policies {
        let base = base.with_row_block_target(100);
        let reference = write_and_validate(
            schema.clone(),
            vec![series_batch(&schema, 0, 300)],
            base.with_statistics(WriterStatistics::None),
            &format!("{label}-none"),
        )
        .1;
        let reference_wire = wire(&reference);
        assert!(reference_wire.kinds.iter().all(|kind| *kind == 0));

        for statistics in [WriterStatistics::MinMax, WriterStatistics::Automatic] {
            let (_path, bytes) = write_and_validate(
                schema.clone(),
                vec![series_batch(&schema, 0, 300)],
                base.with_statistics(statistics),
                &format!("{label}-{statistics:?}"),
            );
            let with = wire(&bytes);
            assert!(
                with.kinds.contains(&1),
                "{label}/{statistics:?} wrote no statistics at all"
            );
            assert_eq!(
                with.payloads, reference_wire.payloads,
                "{label}/{statistics:?} changed an encoded payload"
            );
            assert_eq!(
                with.stream_tables, reference_wire.stream_tables,
                "{label}/{statistics:?} changed a stream descriptor"
            );
            assert_eq!(
                with.descriptors_without_statistics, reference_wire.descriptors_without_statistics,
                "{label}/{statistics:?} changed a column descriptor beyond its statistics"
            );
        }
    }
}

/// `None` must reproduce the pre-statistics writer under every other option,
/// which is what lets this change be additive.
#[test]
fn the_default_policy_is_none_under_every_other_option() {
    let schema = series_schema();
    let mut options = vec![
        WriterOptions::default(),
        WriterOptions::default().with_row_ids(true),
        WriterOptions::default().with_row_block_target(37),
        WriterOptions::default().with_encoding(WriterEncoding::Adaptive),
        WriterOptions::default().with_encoding(WriterEncoding::Fixed(WriterTransform::Delta)),
    ];
    if cfg!(feature = "zstd") {
        options.push(
            WriterOptions::default()
                .with_codec(WriterCodec::Zstandard)
                .with_encoding(WriterEncoding::Adaptive),
        );
    }

    for (index, base) in options.into_iter().enumerate() {
        let implicit = write_all(
            schema.clone(),
            vec![series_batch(&schema, 0, 300)],
            base,
            &format!("implicit{index}"),
        )
        .1;
        let explicit = write_all(
            schema.clone(),
            vec![series_batch(&schema, 0, 300)],
            base.with_statistics(WriterStatistics::None),
            &format!("explicit{index}"),
        )
        .1;
        assert_eq!(implicit, explicit, "option set {index}");
        assert!(wire(&implicit).kinds.iter().all(|kind| *kind == 0));
    }
}

// --------------------------------------------------------- automatic thresholds

/// The value floor is 64 for every type whose values cost a byte or more, and
/// 121 for `bool`, where a two-byte pair needs sixteen bitmap bytes.
#[test]
fn the_automatic_value_floor_is_higher_for_booleans() {
    for (rows, expected) in [(63_usize, vec![0, 0]), (64, vec![0, 1]), (121, vec![1, 1])] {
        let schema = Schema::new(
            713,
            vec![
                Column::new(1, "flag", LogicalType::Bool, false),
                Column::new(2, "count", LogicalType::Int64, false),
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Array::Bool(BooleanArray::new(
                    (0..rows).map(|row| row % 3 == 0).collect(),
                    None,
                )),
                Array::Int64(PrimitiveArray::new(
                    (0..rows).map(|row| row as i64).collect(),
                    None,
                )),
            ],
            rows,
        )
        .expect("batch");
        let (_path, bytes) = write_and_validate(
            schema,
            vec![batch],
            WriterOptions::default().with_statistics(WriterStatistics::Automatic),
            &format!("floor{rows}"),
        );
        assert_eq!(wire(&bytes).kinds, expected, "{rows} rows");
    }
}

/// `MinMax` must be able to override every omission `Automatic` makes,
/// including on the primary column.
#[test]
fn forced_minmax_overrides_every_automatic_omission() {
    let schema = series_schema();
    let batches = || vec![series_batch(&schema, 0, 300)];
    let options = WriterOptions::default().with_row_block_target(100);

    let automatic = write_and_validate(
        schema.clone(),
        batches(),
        options.with_statistics(WriterStatistics::Automatic),
        "override-auto",
    )
    .1;
    // The primary column carries the mandatory bounds already, so automatic
    // declines to repeat them; the value column is above every threshold.
    assert_eq!(wire(&automatic).kinds, vec![0, 1, 0, 1, 0, 1]);

    let forced = write_and_validate(
        schema.clone(),
        batches(),
        options.with_statistics(WriterStatistics::MinMax),
        "override-forced",
    )
    .1;
    assert_eq!(wire(&forced).kinds, vec![1; 6]);
}

// ------------------------------------------------------------- resource limits

/// A pair is charged against the frame header budget, so a `fixed_binary`
/// column wide enough to exhaust it must be refused by name rather than
/// reported as an oversize row or written as an unreadable file.
#[test]
fn statistics_too_large_for_the_frame_header_are_refused_by_name() {
    let width = 40_000_000_u32;
    let schema = Schema::new(
        714,
        vec![Column::new(
            1,
            "wide",
            LogicalType::FixedBinary { byte_width: width },
            false,
        )],
        None,
    );
    let batch = || {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Array::FixedBinary(BinaryArray::new(
                (0..2).map(|row| vec![row as u8; width as usize]).collect(),
                None,
            ))],
            2,
        )
        .expect("batch")
    };
    let options = WriterOptions::default().with_byte_block_target(1 << 31);

    let path = TempPath::new("wide-refused");
    let mut writer = Writer::create(
        path.path(),
        schema.clone(),
        options.with_statistics(WriterStatistics::MinMax),
    )
    .expect("create");
    let error = writer
        .append(batch())
        .expect_err("an eighty-megabyte pair cannot fit a sixty-four-megabyte header");
    assert_eq!(error.kind(), acta::ErrorKind::InvalidArgument);
    assert!(
        error.to_string().contains("WriterStatistics::None"),
        "the error must name the option that caused it: {error}"
    );

    // The same rows remain writable without statistics, which is what makes the
    // suggestion in that error true.
    let path = TempPath::new("wide-accepted");
    let mut writer = Writer::create(path.path(), schema.clone(), options).expect("create");
    writer.append(batch()).expect("append");
    let summary = writer.finish().expect("finish");
    assert_eq!(summary.rows_written(), 2);
    assert!(
        wire(&std::fs::read(path.path()).expect("read"))
            .kinds
            .iter()
            .all(|kind| *kind == 0)
    );
}
