//! Public metadata for one committed data frame.

/// The inclusive primary timestamp/date bounds declared by one block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimaryBounds {
    minimum: i64,
    maximum: i64,
}

impl PrimaryBounds {
    pub(crate) fn new(minimum: i64, maximum: i64) -> Self {
        Self { minimum, maximum }
    }

    /// The declared minimum, represented as timestamp units or signed days.
    pub fn min(&self) -> i64 {
        self.minimum
    }

    /// The declared maximum, represented as timestamp units or signed days.
    pub fn max(&self) -> i64 {
        self.maximum
    }
}

/// Read-only metadata for one complete, committed data block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockMetadata {
    sequence: u64,
    file_offset: u64,
    total_length: u64,
    row_count: u64,
    base_row_id: Option<u64>,
    primary_bounds: Option<PrimaryBounds>,
    ts_sorted: bool,
}

impl BlockMetadata {
    pub(crate) fn new(
        sequence: u64,
        file_offset: u64,
        total_length: u64,
        row_count: u64,
        base_row_id: Option<u64>,
        primary_bounds: Option<PrimaryBounds>,
        ts_sorted: bool,
    ) -> Self {
        Self {
            sequence,
            file_offset,
            total_length,
            row_count,
            base_row_id,
            primary_bounds,
            ts_sorted,
        }
    }

    /// The contiguous data-frame sequence number, beginning at one.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// The absolute byte offset of this frame's prefix.
    pub fn file_offset(&self) -> u64 {
        self.file_offset
    }

    /// The complete committed frame length, including prefix and trailer.
    pub fn total_length(&self) -> u64 {
        self.total_length
    }

    /// The logical row count declared by the block.
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// The base row ID when the file's `ROW_IDS` feature is enabled.
    pub fn base_row_id(&self) -> Option<u64> {
        self.base_row_id
    }

    /// The block's primary timestamp/date bounds, if the schema has a primary.
    pub fn primary_bounds(&self) -> Option<PrimaryBounds> {
        self.primary_bounds
    }

    /// Whether this block declares nondecreasing primary values.
    ///
    /// The claim covers this block's primary timestamp column only. It says
    /// nothing about ordering between blocks or about any other column, and a
    /// metadata-only open does not verify it against the stored values.
    pub fn ts_sorted(&self) -> bool {
        self.ts_sorted
    }
}
