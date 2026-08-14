//! Helpers shared by the integration tests.
//!
//! Every constant here is transcribed from the field tables in
//! `spec/v0.2/format_v0.2.md`, and the checksum below is written directly from
//! the polynomial definition in section 2. Nothing in this module is derived
//! from the crate under test, so a mistake in the implementation cannot hide
//! itself by also appearing in the tests.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use acta::{Error, ErrorKind, Limits, ValidationReport};

// Section 5, 6.1 and 6.2 structure sizes.
pub const PROLOGUE_SIZE: usize = 64;
pub const PREFIX_SIZE: usize = 48;
pub const TRAILER_SIZE: usize = 32;

// Section 5, prologue field offsets.
pub const PROLOGUE_MAGIC: usize = 0;
pub const PROLOGUE_FORMAT_MAJOR: usize = 8;
pub const PROLOGUE_FORMAT_MINOR: usize = 10;
pub const PROLOGUE_SIZE_FIELD: usize = 12;
pub const PROLOGUE_FEATURE_FLAGS: usize = 16;
pub const PROLOGUE_FILE_ID: usize = 24;
pub const PROLOGUE_SCHEMA_FRAME_OFFSET: usize = 40;
pub const PROLOGUE_RESERVED: usize = 48;
pub const PROLOGUE_CRC: usize = 60;

// Section 6.1, prefix field offsets relative to the first prefix byte.
pub const PREFIX_MAGIC: usize = 0;
pub const PREFIX_FRAME_TYPE: usize = 8;
pub const PREFIX_FRAME_VERSION: usize = 10;
pub const PREFIX_FRAME_FLAGS: usize = 12;
pub const PREFIX_HEADER_LENGTH: usize = 16;
pub const PREFIX_RESERVED: usize = 20;
pub const PREFIX_PAYLOAD_LENGTH: usize = 24;
pub const PREFIX_SEQUENCE: usize = 32;
pub const PREFIX_HEADER_CRC: usize = 40;
pub const PREFIX_CRC: usize = 44;

// Section 6.2, trailer field offsets relative to the first trailer byte.
pub const TRAILER_TOTAL_LENGTH: usize = 0;
pub const TRAILER_SEQUENCE: usize = 8;
pub const TRAILER_BODY_CRC: usize = 16;
pub const TRAILER_CRC: usize = 20;
pub const TRAILER_COMMIT_MAGIC: usize = 24;

// Section 7, schema-frame header and column descriptor field offsets.
pub const SCHEMA_HEADER_SIZE: usize = 24;
pub const SCHEMA_ID: usize = 0;
pub const SCHEMA_COLUMN_COUNT: usize = 8;
pub const SCHEMA_PRIMARY_COLUMN_ID: usize = 12;
pub const SCHEMA_FLAGS: usize = 16;
pub const DESCRIPTOR_PREFIX_SIZE: usize = 24;
pub const DESCRIPTOR_LENGTH: usize = 0;
pub const DESCRIPTOR_COLUMN_ID: usize = 4;
pub const DESCRIPTOR_TYPE: usize = 8;
pub const DESCRIPTOR_FLAGS: usize = 10;
pub const DESCRIPTOR_NAME_LENGTH: usize = 12;
pub const DESCRIPTOR_PARAMETERS_LENGTH: usize = 16;

// Section 8, data-frame block header field offsets.
pub const BLOCK_HEADER_SIZE: usize = 64;
pub const BLOCK_SCHEMA_ID: usize = 0;
pub const BLOCK_BASE_ROW_ID: usize = 8;
pub const BLOCK_ROW_COUNT: usize = 16;
pub const BLOCK_COLUMN_COUNT: usize = 20;
pub const BLOCK_PRIMARY_MIN: usize = 24;
pub const BLOCK_PRIMARY_MAX: usize = 32;
pub const BLOCK_COLUMN_TABLE_OFFSET: usize = 40;
pub const BLOCK_STREAM_TABLE_OFFSET: usize = 44;
pub const BLOCK_STATISTICS_OFFSET: usize = 48;
pub const BLOCK_STATISTICS_LENGTH: usize = 52;
pub const BLOCK_FLAGS: usize = 56;
pub const BLOCK_COLUMN_DESCRIPTOR_SIZE: usize = 32;
pub const STREAM_DESCRIPTOR_SIZE: usize = 48;
pub const STREAM_TRANSFORM: usize = 2;
pub const STREAM_CODEC: usize = 4;
pub const STREAM_PAYLOAD_OFFSET: usize = 8;
pub const STREAM_STORED_LENGTH: usize = 16;
pub const STREAM_TRANSFORMED_LENGTH: usize = 24;
pub const STREAM_ELEMENT_COUNT: usize = 32;
pub const STREAM_CRC: usize = 40;

// Section 4, frame types.
pub const SCHEMA_FRAME_TYPE: u16 = 1;
pub const DATA_FRAME_TYPE: u16 = 2;
pub const CHECKPOINT_FRAME_TYPE: u16 = 3;

// Section 3, the logical type IDs used by the constructed files below.
pub const TYPE_BOOL: u16 = 1;
pub const TYPE_INT64: u16 = 5;
pub const TYPE_UINT64: u16 = 9;
pub const TYPE_FLOAT32: u16 = 10;
pub const TYPE_FLOAT64: u16 = 11;
pub const TYPE_DECIMAL64: u16 = 12;
pub const TYPE_BINARY: u16 = 16;

// Section 8.1, column layouts and the flag bits of a column descriptor.
pub const LAYOUT_PLAIN: u16 = 0;
pub const LAYOUT_CONSTANT: u16 = 1;
pub const LAYOUT_DICTIONARY: u16 = 2;
pub const LAYOUT_RUN_LENGTH: u16 = 3;
pub const COLUMN_IMPLICIT_VALIDITY: u16 = 1;

// Section 8.2, stream kinds.
pub const STREAM_VALIDITY: u16 = 1;
pub const STREAM_VALUES: u16 = 2;
pub const STREAM_LENGTHS: u16 = 3;
pub const STREAM_DICTIONARY_VALUES: u16 = 4;
pub const STREAM_DICTIONARY_LENGTHS: u16 = 5;
pub const STREAM_INDICES: u16 = 6;
pub const STREAM_RUN_VALUES: u16 = 7;
pub const STREAM_RUN_LENGTHS: u16 = 8;

// Section 9 transforms and section 10 codecs.
pub const TRANSFORM_RAW: u16 = 0;
pub const TRANSFORM_BIT_PACKED: u16 = 1;
pub const TRANSFORM_FRAME_OF_REFERENCE: u16 = 2;
pub const TRANSFORM_DELTA: u16 = 3;
pub const TRANSFORM_DELTA_OF_DELTA: u16 = 4;
pub const TRANSFORM_BYTE_STREAM_SPLIT: u16 = 5;
pub const TRANSFORM_BOOLEAN_RLE: u16 = 6;
pub const CODEC_NONE: u16 = 0;
pub const CODEC_ZSTD: u16 = 1;
pub const TYPE_TIMESTAMP64: u16 = 13;
pub const TYPE_UTF8: u16 = 14;
pub const TYPE_CATEGORICAL: u16 = 15;
pub const TYPE_FIXED_BINARY: u16 = 17;
pub const TYPE_DATE32: u16 = 18;

// Section 8, block flags.
pub const ROW_IDS_BLOCK_FLAG: u32 = 1;
pub const TS_SORTED_BLOCK_FLAG: u32 = 2;

pub const UINT64_MAX: u64 = u64::MAX;

pub const FILE_MAGIC: [u8; 8] = *b"ACTA\r\n\x1a\n";
pub const FRAME_MAGIC: [u8; 8] = *b"ACTAFRM\n";
pub const COMMIT_MAGIC: [u8; 8] = *b"ACTAEND\n";

/// The v0.2 fixture files, with the feature flags each one declares.
pub const FIXTURES: &[(&str, u64)] = &[
    ("minimal/minimal.acta", 0),
    ("nyc_taxi_3_rows/nyc_taxi_3_rows.acta", 1),
    ("ts_sorted/ts_sorted.acta", 0),
    ("date32/date32.acta", 0),
    ("no_primary/no_primary.acta", 0),
    ("timezone/timezone.acta", 0),
];

/// The fixture used wherever a test only needs some structurally valid file.
pub const REFERENCE_FIXTURE: &str = "minimal/minimal.acta";

// ---------------------------------------------------------------- checksums

/// CRC32C written directly from the section 2 definition: the reflected
/// Castagnoli polynomial applied one bit at a time.
pub fn crc32c(bytes: &[u8]) -> u32 {
    const REFLECTED_CASTAGNOLI: u32 = 0x82f6_3b78;

    let mut state = u32::MAX;
    for &byte in bytes {
        state ^= u32::from(byte);
        for _ in 0..8 {
            state = if state & 1 == 0 {
                state >> 1
            } else {
                (state >> 1) ^ REFLECTED_CASTAGNOLI
            };
        }
    }
    !state
}

// ------------------------------------------------------------ little-endian

pub fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

pub fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

pub fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

pub fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

pub fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

pub fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

// ------------------------------------------------------------------ fixtures

pub fn fixture(relative_path: &str) -> Vec<u8> {
    std::fs::read(fixture_path(relative_path)).unwrap()
}

pub fn reference_fixture() -> Vec<u8> {
    fixture(REFERENCE_FIXTURE)
}

pub fn fixture_path(relative_path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("spec/v0.2/fixtures")
        .join(relative_path)
}

// -------------------------------------------------------------------- frames

/// The offset of the schema frame, which always follows the prologue.
pub fn schema_frame_offset() -> usize {
    PROLOGUE_SIZE
}

/// The offset of the first data frame, which follows the schema frame.
pub fn data_frame_offset(bytes: &[u8]) -> usize {
    frame_end(bytes, schema_frame_offset())
}

/// The total length of the frame beginning at `frame_offset`.
pub fn frame_length(bytes: &[u8], frame_offset: usize) -> usize {
    PREFIX_SIZE
        + read_u32(bytes, frame_offset + PREFIX_HEADER_LENGTH) as usize
        + read_u64(bytes, frame_offset + PREFIX_PAYLOAD_LENGTH) as usize
        + TRAILER_SIZE
}

/// The offset one byte past the frame beginning at `frame_offset`.
pub fn frame_end(bytes: &[u8], frame_offset: usize) -> usize {
    frame_offset + frame_length(bytes, frame_offset)
}

pub fn trailer_offset(bytes: &[u8], frame_offset: usize) -> usize {
    frame_end(bytes, frame_offset) - TRAILER_SIZE
}

pub fn payload_offset(bytes: &[u8], frame_offset: usize) -> usize {
    frame_offset + PREFIX_SIZE + read_u32(bytes, frame_offset + PREFIX_HEADER_LENGTH) as usize
}

// ------------------------------------------------------------------- repairs

/// Recompute the prologue CRC so a mutated field is not masked by it.
pub fn repair_prologue(bytes: &mut [u8]) {
    let crc = crc32c(&bytes[..PROLOGUE_CRC]);
    put_u32(bytes, PROLOGUE_CRC, crc);
}

/// Recompute only the prefix CRC of the frame at `frame_offset`.
pub fn repair_prefix(bytes: &mut [u8], frame_offset: usize) {
    let crc = crc32c(&bytes[frame_offset..frame_offset + PREFIX_CRC]);
    put_u32(bytes, frame_offset + PREFIX_CRC, crc);
}

/// Recompute every checksum and trailer field of the frame at `frame_offset`.
pub fn repair_frame(bytes: &mut [u8], frame_offset: usize) {
    let header_length = read_u32(bytes, frame_offset + PREFIX_HEADER_LENGTH) as usize;
    let payload_length = read_u64(bytes, frame_offset + PREFIX_PAYLOAD_LENGTH) as usize;
    let body_length = PREFIX_SIZE + header_length + payload_length;
    let total_length = body_length + TRAILER_SIZE;
    let header_offset = frame_offset + PREFIX_SIZE;
    let trailer = frame_offset + body_length;

    let header_crc = crc32c(&bytes[header_offset..header_offset + header_length]);
    put_u32(bytes, frame_offset + PREFIX_HEADER_CRC, header_crc);
    repair_prefix(bytes, frame_offset);

    put_u64(bytes, trailer + TRAILER_TOTAL_LENGTH, total_length as u64);
    let sequence = read_u64(bytes, frame_offset + PREFIX_SEQUENCE);
    put_u64(bytes, trailer + TRAILER_SEQUENCE, sequence);
    let body_crc = crc32c(&bytes[frame_offset..frame_offset + body_length]);
    put_u32(bytes, trailer + TRAILER_BODY_CRC, body_crc);
    bytes[trailer + TRAILER_COMMIT_MAGIC..trailer + TRAILER_SIZE].copy_from_slice(&COMMIT_MAGIC);
    repair_trailer_crc(bytes, trailer);
}

/// Section 6.2: the trailer CRC covers bytes `[0, 20)` then `[24, 32)`.
pub fn repair_trailer_crc(bytes: &mut [u8], trailer: usize) {
    let mut covered = Vec::with_capacity(TRAILER_SIZE - 4);
    covered.extend_from_slice(&bytes[trailer..trailer + TRAILER_CRC]);
    covered.extend_from_slice(&bytes[trailer + TRAILER_COMMIT_MAGIC..trailer + TRAILER_SIZE]);
    let crc = crc32c(&covered);
    put_u32(bytes, trailer + TRAILER_CRC, crc);
}

/// Append a copy of the frame at `source_frame_offset` carrying `sequence`.
///
/// Stage 1 validates framing only, so a repeated data frame is a structurally
/// valid way to build a file with more than two frames.
pub fn with_appended_frame(bytes: &[u8], source_frame_offset: usize, sequence: u64) -> Vec<u8> {
    let length = frame_length(bytes, source_frame_offset);
    let mut extended = bytes.to_vec();
    let appended = extended.len();
    extended.extend_from_within(source_frame_offset..source_frame_offset + length);
    put_u64(&mut extended, appended + PREFIX_SEQUENCE, sequence);
    repair_frame(&mut extended, appended);
    extended
}

/// Build a frame with no header and no payload at the end of `bytes`.
pub fn with_appended_empty_frame(bytes: &[u8], frame_type: u16, sequence: u64) -> Vec<u8> {
    let mut extended = bytes.to_vec();
    let appended = extended.len();
    extended.extend_from_slice(&[0_u8; PREFIX_SIZE + TRAILER_SIZE]);
    extended[appended..appended + FRAME_MAGIC.len()].copy_from_slice(&FRAME_MAGIC);
    put_u16(&mut extended, appended + PREFIX_FRAME_TYPE, frame_type);
    put_u64(&mut extended, appended + PREFIX_SEQUENCE, sequence);
    repair_frame(&mut extended, appended);
    extended
}

// ------------------------------------------------------------------ builders
//
// The fixtures anchor compatibility, but they cannot express a file that is
// wrong in one specific way. These builders assemble a file from the section 5,
// 6, 7 and 8 field tables so a test can state exactly which field it damaged.
// They deliberately apply no validation of their own.

fn pad8(bytes: &[u8]) -> Vec<u8> {
    let mut padded = bytes.to_vec();
    padded.resize(bytes.len().next_multiple_of(8), 0);
    padded
}

/// A well-formed 64-byte prologue declaring `feature_flags`.
pub fn prologue(feature_flags: u64) -> Vec<u8> {
    let mut bytes = vec![0_u8; PROLOGUE_SIZE];
    bytes[PROLOGUE_MAGIC..PROLOGUE_MAGIC + 8].copy_from_slice(&FILE_MAGIC);
    put_u16(&mut bytes, PROLOGUE_FORMAT_MAJOR, 0);
    put_u16(&mut bytes, PROLOGUE_FORMAT_MINOR, 2);
    put_u32(&mut bytes, PROLOGUE_SIZE_FIELD, PROLOGUE_SIZE as u32);
    put_u64(&mut bytes, PROLOGUE_FEATURE_FLAGS, feature_flags);
    for index in 0..16 {
        bytes[PROLOGUE_FILE_ID + index] = index as u8;
    }
    put_u64(
        &mut bytes,
        PROLOGUE_SCHEMA_FRAME_OFFSET,
        PROLOGUE_SIZE as u64,
    );
    repair_prologue(&mut bytes);
    bytes
}

/// A complete frame, padded and checksummed, around the given body.
pub fn frame(frame_type: u16, sequence: u64, header: &[u8], payload: &[u8]) -> Vec<u8> {
    let header = pad8(header);
    let payload = pad8(payload);
    let mut bytes = vec![0_u8; PREFIX_SIZE + header.len() + payload.len() + TRAILER_SIZE];

    bytes[PREFIX_MAGIC..PREFIX_MAGIC + 8].copy_from_slice(&FRAME_MAGIC);
    put_u16(&mut bytes, PREFIX_FRAME_TYPE, frame_type);
    put_u32(&mut bytes, PREFIX_HEADER_LENGTH, header.len() as u32);
    put_u64(&mut bytes, PREFIX_PAYLOAD_LENGTH, payload.len() as u64);
    put_u64(&mut bytes, PREFIX_SEQUENCE, sequence);
    bytes[PREFIX_SIZE..PREFIX_SIZE + header.len()].copy_from_slice(&header);
    let payload_start = PREFIX_SIZE + header.len();
    bytes[payload_start..payload_start + payload.len()].copy_from_slice(&payload);

    repair_frame(&mut bytes, 0);
    bytes
}

/// A 24-byte schema-frame header.
pub fn schema_header(
    schema_id: u64,
    column_count: u32,
    primary_column_id: u32,
    flags: u32,
) -> Vec<u8> {
    let mut bytes = vec![0_u8; SCHEMA_HEADER_SIZE];
    put_u64(&mut bytes, SCHEMA_ID, schema_id);
    put_u32(&mut bytes, SCHEMA_COLUMN_COUNT, column_count);
    put_u32(&mut bytes, SCHEMA_PRIMARY_COLUMN_ID, primary_column_id);
    put_u32(&mut bytes, SCHEMA_FLAGS, flags);
    bytes
}

/// One padded schema column descriptor.
pub fn descriptor(
    column_id: u32,
    type_id: u16,
    flags: u16,
    name: &str,
    parameters: &[u8],
) -> Vec<u8> {
    let name = name.as_bytes();
    let content = DESCRIPTOR_PREFIX_SIZE + name.len() + parameters.len();
    let length = content.next_multiple_of(8);
    let mut bytes = vec![0_u8; length];

    put_u32(&mut bytes, DESCRIPTOR_LENGTH, length as u32);
    put_u32(&mut bytes, DESCRIPTOR_COLUMN_ID, column_id);
    put_u16(&mut bytes, DESCRIPTOR_TYPE, type_id);
    put_u16(&mut bytes, DESCRIPTOR_FLAGS, flags);
    put_u32(&mut bytes, DESCRIPTOR_NAME_LENGTH, name.len() as u32);
    put_u32(
        &mut bytes,
        DESCRIPTOR_PARAMETERS_LENGTH,
        parameters.len() as u32,
    );
    let name_start = DESCRIPTOR_PREFIX_SIZE;
    bytes[name_start..name_start + name.len()].copy_from_slice(name);
    bytes[name_start + name.len()..content].copy_from_slice(parameters);
    bytes
}

/// A non-nullable `int64` column, the simplest descriptor a schema can hold.
pub fn int64_column(column_id: u32, name: &str) -> Vec<u8> {
    descriptor(column_id, TYPE_INT64, 0, name, &[])
}

/// The section 7 `timestamp64` parameter record, padded to eight bytes.
pub fn timestamp_parameters(unit: u8, timezone_mode: u8, timezone_name: &str) -> Vec<u8> {
    let name = timezone_name.as_bytes();
    let mut bytes = vec![0_u8; (8 + name.len()).next_multiple_of(8)];
    bytes[0] = unit;
    bytes[1] = timezone_mode;
    put_u32(&mut bytes, 4, name.len() as u32);
    bytes[8..8 + name.len()].copy_from_slice(name);
    bytes
}

/// A data-frame header whose tables are laid out exactly as section 8 requires.
///
/// The column and stream descriptors are left zeroed. R1 validates the table
/// geometry without interpreting the tables themselves.
pub fn block_header(column_count: u32, row_count: u32, base_row_id: u64, flags: u32) -> Vec<u8> {
    let stream_table = BLOCK_HEADER_SIZE + BLOCK_COLUMN_DESCRIPTOR_SIZE * column_count as usize;
    let mut bytes = vec![0_u8; stream_table];

    put_u64(&mut bytes, BLOCK_SCHEMA_ID, 1);
    put_u64(&mut bytes, BLOCK_BASE_ROW_ID, base_row_id);
    put_u32(&mut bytes, BLOCK_ROW_COUNT, row_count);
    put_u32(&mut bytes, BLOCK_COLUMN_COUNT, column_count);
    put_u32(
        &mut bytes,
        BLOCK_COLUMN_TABLE_OFFSET,
        BLOCK_HEADER_SIZE as u32,
    );
    put_u32(&mut bytes, BLOCK_STREAM_TABLE_OFFSET, stream_table as u32);
    put_u32(&mut bytes, BLOCK_STATISTICS_OFFSET, stream_table as u32);
    put_u32(&mut bytes, BLOCK_STATISTICS_LENGTH, 0);
    put_u32(&mut bytes, BLOCK_FLAGS, flags);
    bytes
}

/// Set the primary bounds of a header built by [`block_header`].
pub fn with_primary_bounds(header: &mut [u8], minimum: i64, maximum: i64) {
    put_u64(header, BLOCK_PRIMARY_MIN, minimum as u64);
    put_u64(header, BLOCK_PRIMARY_MAX, maximum as u64);
}

/// Assemble a prologue, a schema frame, and zero or more data frames.
pub fn build_file(
    feature_flags: u64,
    schema_header: &[u8],
    descriptors: &[Vec<u8>],
    blocks: &[Vec<u8>],
) -> Vec<u8> {
    let mut bytes = prologue(feature_flags);
    bytes.extend_from_slice(&frame(
        SCHEMA_FRAME_TYPE,
        0,
        schema_header,
        &descriptors.concat(),
    ));
    for (index, header) in blocks.iter().enumerate() {
        bytes.extend_from_slice(&frame(DATA_FRAME_TYPE, index as u64 + 1, header, &[]));
    }
    bytes
}

/// The absolute offset of one stream descriptor in a data frame, located
/// through the stream table offset the block header itself declares.
pub fn stream_descriptor_offset(bytes: &[u8], frame_offset: usize, index: usize) -> usize {
    let header = frame_offset + PREFIX_SIZE;
    let table = read_u32(bytes, header + BLOCK_STREAM_TABLE_OFFSET) as usize;
    header + table + index * STREAM_DESCRIPTOR_SIZE
}

/// One physical stream of a constructed block.
///
/// Every field is written to the descriptor as given, so a test can declare a
/// length, an element count, or a checksum that the payload contradicts.
pub struct TestStream {
    pub kind: u16,
    pub transform: u16,
    pub codec: u16,
    pub element_count: u64,
    pub transformed_length: u64,
    pub crc: Option<u32>,
    pub payload: Vec<u8>,
}

impl TestStream {
    /// An uncompressed stream whose declarations match its payload.
    pub fn new(kind: u16, transform: u16, element_count: u64, payload: Vec<u8>) -> Self {
        Self {
            kind,
            transform,
            codec: CODEC_NONE,
            element_count,
            transformed_length: payload.len() as u64,
            crc: None,
            payload,
        }
    }

    pub fn compressed(mut self, transformed_length: u64) -> Self {
        self.codec = CODEC_ZSTD;
        self.transformed_length = transformed_length;
        self
    }

    pub fn with_element_count(mut self, element_count: u64) -> Self {
        self.element_count = element_count;
        self
    }

    pub fn with_crc(mut self, crc: u32) -> Self {
        self.crc = Some(crc);
        self
    }
}

/// One column of a constructed block, with the schema descriptor that declares
/// it and the streams that carry it.
pub struct TestColumn {
    pub descriptor: Vec<u8>,
    pub layout: u16,
    pub flags: u16,
    pub null_count: u32,
    pub streams: Vec<TestStream>,
}

impl TestColumn {
    pub fn new(descriptor: Vec<u8>, layout: u16, streams: Vec<TestStream>) -> Self {
        Self {
            descriptor,
            layout,
            flags: 0,
            null_count: 0,
            streams,
        }
    }

    pub fn with_nulls(mut self, null_count: u32, flags: u16) -> Self {
        self.null_count = null_count;
        self.flags = flags;
        self
    }
}

/// A complete one-column file with one data block, no primary timestamp, and
/// no row IDs: the smallest file that exercises a decoding path end to end.
pub fn column_file(row_count: u32, column: &TestColumn) -> Vec<u8> {
    let stream_table = BLOCK_HEADER_SIZE + BLOCK_COLUMN_DESCRIPTOR_SIZE;
    let statistics = stream_table + column.streams.len() * STREAM_DESCRIPTOR_SIZE;
    let mut header = block_header(1, row_count, UINT64_MAX, 0);
    header.resize(statistics, 0);
    put_u32(&mut header, BLOCK_STATISTICS_OFFSET, statistics as u32);

    put_u32(&mut header, BLOCK_HEADER_SIZE, 1);
    put_u16(&mut header, BLOCK_HEADER_SIZE + 4, column.layout);
    put_u16(&mut header, BLOCK_HEADER_SIZE + 6, column.flags);
    put_u32(&mut header, BLOCK_HEADER_SIZE + 8, column.null_count);
    put_u32(
        &mut header,
        BLOCK_HEADER_SIZE + 12,
        row_count - column.null_count,
    );
    put_u32(&mut header, BLOCK_HEADER_SIZE + 16, 0);
    put_u16(
        &mut header,
        BLOCK_HEADER_SIZE + 20,
        column.streams.len() as u16,
    );

    let mut payload: Vec<u8> = Vec::new();
    for (index, stream) in column.streams.iter().enumerate() {
        let offset = payload.len().next_multiple_of(8);
        payload.resize(offset + stream.payload.len(), 0);
        payload[offset..offset + stream.payload.len()].copy_from_slice(&stream.payload);

        let descriptor = stream_table + index * STREAM_DESCRIPTOR_SIZE;
        put_u16(&mut header, descriptor, stream.kind);
        put_u16(&mut header, descriptor + 2, stream.transform);
        put_u16(&mut header, descriptor + 4, stream.codec);
        put_u64(&mut header, descriptor + 8, offset as u64);
        put_u64(&mut header, descriptor + 16, stream.payload.len() as u64);
        put_u64(&mut header, descriptor + 24, stream.transformed_length);
        put_u64(&mut header, descriptor + 32, stream.element_count);
        put_u32(
            &mut header,
            descriptor + 40,
            stream.crc.unwrap_or_else(|| crc32c(&stream.payload)),
        );
    }

    let mut bytes = prologue(0);
    bytes.extend_from_slice(&frame(
        SCHEMA_FRAME_TYPE,
        0,
        &schema_header(1, 1, 0, 0),
        &column.descriptor,
    ));
    bytes.extend_from_slice(&frame(DATA_FRAME_TYPE, 1, &header, &payload));
    bytes
}

/// The smallest file this crate accepts: one `int64` column, no primary, no
/// row IDs, and one block per entry in `rows`.
pub fn simple_file(rows: &[u32]) -> Vec<u8> {
    let blocks: Vec<Vec<u8>> = rows
        .iter()
        .map(|&count| block_header(1, count, UINT64_MAX, 0))
        .collect();
    build_file(
        0,
        &schema_header(1, 1, 0, 0),
        &[int64_column(1, "value")],
        &blocks,
    )
}

// ----------------------------------------------------------------- assertion

/// Validate `bytes` as a file and return whatever the validator decided.
pub fn validate(label: &str, bytes: &[u8]) -> Result<ValidationReport, Error> {
    validate_with_limits(label, bytes, Limits::default())
}

pub fn validate_with_limits(
    label: &str,
    bytes: &[u8],
    limits: Limits,
) -> Result<ValidationReport, Error> {
    let file = TemporaryFile::new(label, bytes);
    acta::validate_with_limits(file.path(), limits)
}

pub fn expect_valid(label: &str, bytes: &[u8]) -> ValidationReport {
    validate(label, bytes).unwrap_or_else(|error| panic!("{label} failed validation: {error}"))
}

pub fn expect_error(label: &str, bytes: &[u8]) -> Error {
    validate(label, bytes)
        .err()
        .unwrap_or_else(|| panic!("{label} unexpectedly validated"))
}

/// Assert that an error names the expected failure at the expected place.
pub fn assert_reported(error: &Error, kind: ErrorKind, offset: usize) {
    assert_eq!(error.kind(), kind, "unexpected kind: {error}");
    assert_eq!(
        error.offset(),
        Some(offset as u64),
        "unexpected offset: {error}"
    );
    assert!(!error.context().is_empty(), "missing context: {error}");
}

// ------------------------------------------------------------ temporary file

pub struct TemporaryFile {
    path: PathBuf,
}

impl TemporaryFile {
    pub fn new(label: &str, bytes: &[u8]) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        let path = std::env::temp_dir().join(format!(
            "acta-{label}-{}-{}.acta",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, bytes).unwrap();
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
