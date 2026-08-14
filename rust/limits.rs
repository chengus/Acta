//! Bounds applied to sizes a file declares about itself.

/// The default maximum frame header length.
///
/// A v0.2 frame header holds a 24- or 64-byte fixed part, 32 bytes per column
/// descriptor, 48 bytes per stream descriptor, and a statistics area, so this
/// bound accommodates far more columns than any practical schema.
const DEFAULT_MAX_FRAME_HEADER_LENGTH: u64 = 64 * 1024 * 1024;

/// The default maximum frame payload length.
///
/// The reference block target in specification section 2 is 65,536 rows, so
/// this bound leaves roughly 64 KiB per row for variable-width values.
const DEFAULT_MAX_FRAME_PAYLOAD_LENGTH: u64 = 4 * 1024 * 1024 * 1024;

/// The largest number of schema columns accepted by the metadata reader.
///
/// Column names and IDs are checked for uniqueness in time linear in the column
/// count, so this bound exists to cap memory rather than to cap work. It still
/// admits far more columns than any schema the reference block target suits.
const DEFAULT_MAX_SCHEMA_COLUMNS: u64 = 65_536;

/// The largest UTF-8 name or encoded type-parameter record accepted before
/// the reader allocates storage for it.
const DEFAULT_MAX_SCHEMA_FIELD_LENGTH: u64 = 16 * 1024 * 1024;

/// The largest number of data blocks retained in a reader snapshot.
const DEFAULT_MAX_BLOCKS: u64 = 10_000_000;

/// The largest logical row count a block decoder will materialize.
const DEFAULT_MAX_ROWS_PER_BLOCK: u64 = 16 * 1024 * 1024;

/// The largest number of bytes one block decode may materialize in total.
///
/// A block decode holds every decoded column at once, and each declaration a
/// block makes about itself is individually small: a row count, an element
/// count, a stream length. Decoding multiplies them, so bounding each
/// declaration on its own still leaves the total unbounded.
///
/// A fixed-width value costs its stored bytes and the vector it decodes into,
/// so roughly twice its logical width for a column with no nulls. This default
/// therefore admits the reference 65,536-row block of specification section 2
/// with several hundred columns, and it is the bound to raise for wider or
/// longer blocks.
const DEFAULT_MAX_DECODED_BLOCK_BYTES: u64 = 1024 * 1024 * 1024;

/// Bounds checked against declared sizes before any file region is read.
///
/// Specification section 2 requires readers to validate length arithmetic for
/// overflow and recommends configurable resource limits. Each bound is an
/// inclusive maximum; a larger declared size fails with
/// [`ErrorKind::ResourceLimit`](crate::ErrorKind::ResourceLimit).
///
/// Most bounds describe one structure. Two describe a whole
/// [`Scan`](crate::Scan) instead, and are spent cumulatively across its blocks:
/// [`Self::max_rows_per_scan`] and [`Self::max_decoded_scan_bytes`]. Both
/// default to no limit, so an existing scan cannot acquire a surprising cap,
/// and both charge only for work the scan really does: a pruned block and an
/// unprojected column cost nothing. Per-block bounds still apply on top of
/// them, and [`Reader::read_block`](crate::Reader::read_block) is a single
/// block decode that never sees a scan's cumulative state.
///
/// ```
/// let limits = acta::Limits::default().with_max_frame_payload_length(1 << 20);
/// assert_eq!(limits.max_frame_payload_length(), 1 << 20);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_frame_header_length: u64,
    max_frame_payload_length: u64,
    max_schema_columns: u64,
    max_schema_field_length: u64,
    max_blocks: u64,
    max_rows_per_block: u64,
    max_decoded_block_bytes: u64,
    max_rows_per_scan: u64,
    max_decoded_scan_bytes: u64,
}

impl Limits {
    /// The largest frame header length this configuration accepts.
    pub fn max_frame_header_length(&self) -> u64 {
        self.max_frame_header_length
    }

    /// The largest frame payload length this configuration accepts.
    pub fn max_frame_payload_length(&self) -> u64 {
        self.max_frame_payload_length
    }

    /// The largest schema column count accepted by metadata parsing.
    pub fn max_schema_columns(&self) -> u64 {
        self.max_schema_columns
    }

    /// The largest column name or encoded type-parameter record accepted by
    /// metadata parsing.
    pub fn max_schema_field_length(&self) -> u64 {
        self.max_schema_field_length
    }

    /// The largest number of data blocks retained in one reader snapshot.
    pub fn max_blocks(&self) -> u64 {
        self.max_blocks
    }

    /// The largest block row count a logical decoder will materialize.
    pub fn max_rows_per_block(&self) -> u64 {
        self.max_rows_per_block
    }

    /// The largest total number of bytes one block decode may materialize.
    ///
    /// The allowance covers every buffer a decode creates, including the
    /// stored and decompressed stream bytes and the intermediate value
    /// representations, and it is not refunded when a buffer is released.
    pub fn max_decoded_block_bytes(&self) -> u64 {
        self.max_decoded_block_bytes
    }

    /// The largest number of block rows one scan may decode cumulatively.
    ///
    /// A block is charged for the rows it decodes, which is its whole row
    /// count: a range filter narrows what the scan returns, not what it had to
    /// materialize to decide. Pruned blocks decode nothing and are not
    /// charged.
    pub fn max_rows_per_scan(&self) -> u64 {
        self.max_rows_per_scan
    }

    /// The largest number of decoded and intermediate bytes one scan may
    /// charge cumulatively across all its blocks and selected columns.
    pub fn max_decoded_scan_bytes(&self) -> u64 {
        self.max_decoded_scan_bytes
    }

    /// Return these limits with a different maximum frame header length.
    pub fn with_max_frame_header_length(mut self, bytes: u64) -> Self {
        self.max_frame_header_length = bytes;
        self
    }

    /// Return these limits with a different maximum frame payload length.
    pub fn with_max_frame_payload_length(mut self, bytes: u64) -> Self {
        self.max_frame_payload_length = bytes;
        self
    }

    /// Return these limits with a different maximum schema column count.
    pub fn with_max_schema_columns(mut self, columns: u64) -> Self {
        self.max_schema_columns = columns;
        self
    }

    /// Return these limits with a different maximum schema field length.
    pub fn with_max_schema_field_length(mut self, bytes: u64) -> Self {
        self.max_schema_field_length = bytes;
        self
    }

    /// Return these limits with a different maximum block count.
    pub fn with_max_blocks(mut self, blocks: u64) -> Self {
        self.max_blocks = blocks;
        self
    }

    /// Return these limits with a different maximum decoded block row count.
    pub fn with_max_rows_per_block(mut self, rows: u64) -> Self {
        self.max_rows_per_block = rows;
        self
    }

    /// Return these limits with a different decoded block memory allowance.
    pub fn with_max_decoded_block_bytes(mut self, bytes: u64) -> Self {
        self.max_decoded_block_bytes = bytes;
        self
    }

    /// Return these limits with a different cumulative scan row allowance.
    pub fn with_max_rows_per_scan(mut self, rows: u64) -> Self {
        self.max_rows_per_scan = rows;
        self
    }

    /// Return these limits with a different cumulative scan decoded-byte
    /// allowance.
    pub fn with_max_decoded_scan_bytes(mut self, bytes: u64) -> Self {
        self.max_decoded_scan_bytes = bytes;
        self
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_header_length: DEFAULT_MAX_FRAME_HEADER_LENGTH,
            max_frame_payload_length: DEFAULT_MAX_FRAME_PAYLOAD_LENGTH,
            max_schema_columns: DEFAULT_MAX_SCHEMA_COLUMNS,
            max_schema_field_length: DEFAULT_MAX_SCHEMA_FIELD_LENGTH,
            max_blocks: DEFAULT_MAX_BLOCKS,
            max_rows_per_block: DEFAULT_MAX_ROWS_PER_BLOCK,
            max_decoded_block_bytes: DEFAULT_MAX_DECODED_BLOCK_BYTES,
            // A scan is a sequence of independently bounded block decodes.
            // Keep the cumulative defaults effectively unbounded so existing
            // readers do not acquire a surprising file-size cap.
            max_rows_per_scan: u64::MAX,
            max_decoded_scan_bytes: u64::MAX,
        }
    }
}
