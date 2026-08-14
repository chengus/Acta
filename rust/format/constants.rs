//! Fixed values of the Acta v0.2 wire format.
//!
//! Every constant here transcribes a field table in `spec/v0.2/format_v0.2.md`.
//! Names ending in `_FIELD` are byte offsets of a field within its own
//! structure; names ending in `_SIZE` are structure or field lengths.

pub(crate) const FILE_MAGIC: [u8; 8] = *b"ACTA\r\n\x1a\n";
pub(crate) const FRAME_MAGIC: [u8; 8] = *b"ACTAFRM\n";
pub(crate) const COMMIT_MAGIC: [u8; 8] = *b"ACTAEND\n";
pub(crate) const MAGIC_SIZE: usize = 8;

pub(crate) const FORMAT_MAJOR: u16 = 0;
pub(crate) const FORMAT_MINOR: u16 = 2;

/// Prologue feature bit zero, which enables implicit row IDs (section 5).
pub(crate) const ROW_IDS_FEATURE: u64 = 1;

/// Bit zero is `ROW_IDS`. Section 5 assigns no other v0.2 feature bit.
pub(crate) const SUPPORTED_FEATURES: u64 = ROW_IDS_FEATURE;

/// Header and payload lengths are multiples of eight (section 6).
pub(crate) const FRAME_ALIGNMENT: u64 = 8;

pub(crate) const PROLOGUE_SIZE: usize = 64;
pub(crate) const PREFIX_SIZE: usize = 48;
pub(crate) const TRAILER_SIZE: usize = 32;

pub(crate) const FILE_ID_SIZE: usize = 16;
pub(crate) const PROLOGUE_RESERVED_SIZE: usize = 12;
pub(crate) const PREFIX_RESERVED_SIZE: usize = 4;

// Section 5, file prologue.
pub(crate) const PROLOGUE_FORMAT_VERSION_FIELD: usize = 8;
pub(crate) const PROLOGUE_SIZE_FIELD: usize = 12;
pub(crate) const PROLOGUE_FEATURE_FLAGS_FIELD: usize = 16;
pub(crate) const PROLOGUE_SCHEMA_FRAME_OFFSET_FIELD: usize = 40;
pub(crate) const PROLOGUE_CRC_FIELD: usize = 60;

/// The schema frame always begins immediately after the prologue.
pub(crate) const SCHEMA_FRAME_OFFSET: u64 = PROLOGUE_SIZE as u64;

// Section 6.1, generic frame prefix.
pub(crate) const PREFIX_FRAME_TYPE_FIELD: usize = 8;
pub(crate) const PREFIX_FRAME_VERSION_FIELD: usize = 10;
pub(crate) const PREFIX_FRAME_FLAGS_FIELD: usize = 12;
pub(crate) const PREFIX_HEADER_LENGTH_FIELD: usize = 16;
pub(crate) const PREFIX_PAYLOAD_LENGTH_FIELD: usize = 24;
pub(crate) const PREFIX_SEQUENCE_FIELD: usize = 32;
pub(crate) const PREFIX_CRC_FIELD: usize = 44;

// Section 6.2, commit trailer.
pub(crate) const TRAILER_SEQUENCE_FIELD: usize = 8;
pub(crate) const TRAILER_CRC_FIELD: usize = 20;
pub(crate) const TRAILER_COMMIT_MAGIC_FIELD: usize = 24;

// Section 4, frame types.
pub(crate) const SCHEMA_FRAME_TYPE: u16 = 1;
pub(crate) const DATA_FRAME_TYPE: u16 = 2;
pub(crate) const CHECKPOINT_FRAME_TYPE: u16 = 3;

/// Sequence zero belongs to the schema frame; data frames follow from one.
pub(crate) const SCHEMA_FRAME_SEQUENCE: u64 = 0;

/// The sequence number of the first data frame (section 4).
pub(crate) const FIRST_DATA_FRAME_SEQUENCE: u64 = 1;

pub(crate) const FRAME_VERSION: u16 = 0;
pub(crate) const FRAME_FLAGS: u32 = 0;

/// Section 7 gives the schema frame a 24-byte frame-specific header.
pub(crate) const SCHEMA_FRAME_HEADER_SIZE: u64 = 24;

// Section 7, schema-frame header and column descriptors.
pub(crate) const SCHEMA_ID_FIELD: usize = 0;
pub(crate) const SCHEMA_COLUMN_COUNT_FIELD: usize = 8;
pub(crate) const SCHEMA_PRIMARY_COLUMN_ID_FIELD: usize = 12;
pub(crate) const SCHEMA_FLAGS_FIELD: usize = 16;
pub(crate) const SCHEMA_RESERVED_SIZE: usize = 4;

/// Section 7 assigns column flag bit zero to nullability and no other bit.
pub(crate) const NULLABLE_COLUMN_FLAG: u16 = 1;

pub(crate) const SCHEMA_DESCRIPTOR_SIZE: u64 = 24;
pub(crate) const SCHEMA_DESCRIPTOR_RESERVED_SIZE: usize = 4;

/// Section 7 reserves column ID zero as the "no primary column" sentinel.
pub(crate) const NO_PRIMARY_COLUMN_ID: u32 = 0;
pub(crate) const SCHEMA_DESCRIPTOR_LENGTH_FIELD: usize = 0;
pub(crate) const SCHEMA_DESCRIPTOR_COLUMN_ID_FIELD: usize = 4;
pub(crate) const SCHEMA_DESCRIPTOR_TYPE_FIELD: usize = 8;
pub(crate) const SCHEMA_DESCRIPTOR_FLAGS_FIELD: usize = 10;
pub(crate) const SCHEMA_DESCRIPTOR_NAME_LENGTH_FIELD: usize = 12;
pub(crate) const SCHEMA_DESCRIPTOR_PARAMETERS_LENGTH_FIELD: usize = 16;

// Section 3, v0.2 logical type IDs.
pub(crate) const TYPE_BOOL: u16 = 1;
pub(crate) const TYPE_INT8: u16 = 2;
pub(crate) const TYPE_INT16: u16 = 3;
pub(crate) const TYPE_INT32: u16 = 4;
pub(crate) const TYPE_INT64: u16 = 5;
pub(crate) const TYPE_UINT8: u16 = 6;
pub(crate) const TYPE_UINT16: u16 = 7;
pub(crate) const TYPE_UINT32: u16 = 8;
pub(crate) const TYPE_UINT64: u16 = 9;
pub(crate) const TYPE_FLOAT32: u16 = 10;
pub(crate) const TYPE_FLOAT64: u16 = 11;
pub(crate) const TYPE_DECIMAL64: u16 = 12;
pub(crate) const TYPE_TIMESTAMP64: u16 = 13;
pub(crate) const TYPE_UTF8: u16 = 14;
pub(crate) const TYPE_CATEGORICAL: u16 = 15;
pub(crate) const TYPE_BINARY: u16 = 16;
pub(crate) const TYPE_FIXED_BINARY: u16 = 17;
pub(crate) const TYPE_DATE32: u16 = 18;

/// Section 8 places a 64-byte block header at the start of a data-frame header.
pub(crate) const DATA_BLOCK_HEADER_SIZE: u64 = 64;

// Section 8, data-frame block header.
pub(crate) const BLOCK_SCHEMA_ID_FIELD: usize = 0;
pub(crate) const BLOCK_BASE_ROW_ID_FIELD: usize = 8;
pub(crate) const BLOCK_ROW_COUNT_FIELD: usize = 16;
pub(crate) const BLOCK_COLUMN_COUNT_FIELD: usize = 20;
pub(crate) const BLOCK_PRIMARY_MIN_FIELD: usize = 24;
pub(crate) const BLOCK_COLUMN_TABLE_OFFSET_FIELD: usize = 40;
pub(crate) const BLOCK_STREAM_TABLE_OFFSET_FIELD: usize = 44;
pub(crate) const BLOCK_STATISTICS_OFFSET_FIELD: usize = 48;
pub(crate) const BLOCK_STATISTICS_LENGTH_FIELD: usize = 52;
pub(crate) const BLOCK_FLAGS_FIELD: usize = 56;
pub(crate) const BLOCK_RESERVED_SIZE: usize = 4;
pub(crate) const BLOCK_COLUMN_DESCRIPTOR_SIZE: u64 = 32;
pub(crate) const BLOCK_STREAM_DESCRIPTOR_SIZE: u64 = 48;
pub(crate) const ROW_IDS_BLOCK_FLAG: u32 = 1;
pub(crate) const TS_SORTED_BLOCK_FLAG: u32 = 2;

// Section 8.1, column descriptors.
pub(crate) const COLUMN_LAYOUT_PLAIN: u16 = 0;
pub(crate) const COLUMN_LAYOUT_CONSTANT: u16 = 1;
pub(crate) const COLUMN_LAYOUT_DICTIONARY: u16 = 2;
pub(crate) const COLUMN_LAYOUT_RUN_LENGTH: u16 = 3;
pub(crate) const COLUMN_IMPLICIT_VALIDITY_FLAG: u16 = 1;
pub(crate) const COLUMN_HAS_STATS_FLAG: u16 = 2;

// Section 8.2, stream descriptors and stream kinds.
pub(crate) const STREAM_KIND_VALIDITY: u16 = 1;
pub(crate) const STREAM_KIND_VALUES: u16 = 2;
pub(crate) const STREAM_KIND_LENGTHS: u16 = 3;
pub(crate) const STREAM_KIND_DICTIONARY_VALUES: u16 = 4;
pub(crate) const STREAM_KIND_DICTIONARY_LENGTHS: u16 = 5;
pub(crate) const STREAM_KIND_INDICES: u16 = 6;
pub(crate) const STREAM_KIND_RUN_VALUES: u16 = 7;
pub(crate) const STREAM_KIND_RUN_LENGTHS: u16 = 8;

// Section 9, stream transforms.
pub(crate) const TRANSFORM_RAW: u16 = 0;
pub(crate) const TRANSFORM_BIT_PACKED: u16 = 1;
pub(crate) const TRANSFORM_FRAME_OF_REFERENCE: u16 = 2;
pub(crate) const TRANSFORM_DELTA: u16 = 3;
pub(crate) const TRANSFORM_DELTA_OF_DELTA: u16 = 4;
pub(crate) const TRANSFORM_BYTE_STREAM_SPLIT: u16 = 5;
pub(crate) const TRANSFORM_BOOLEAN_RLE: u16 = 6;

// Section 10, stream compression codecs.
pub(crate) const CODEC_NONE: u16 = 0;
pub(crate) const CODEC_ZSTD: u16 = 1;

// Section 11, statistics.
pub(crate) const STATS_NONE: u16 = 0;
pub(crate) const STATS_MIN_MAX: u16 = 1;

/// Section 8 gives the first block base row ID zero when `ROW_IDS` is enabled,
/// and `UINT64_MAX` to every block when it is disabled.
pub(crate) const FIRST_BASE_ROW_ID: u64 = 0;
pub(crate) const UNAVAILABLE_BASE_ROW_ID: u64 = u64::MAX;
