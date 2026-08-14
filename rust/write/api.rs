//! Public writer configuration and accounting types.

/// The default maximum number of logical rows buffered into one data block.
pub const DEFAULT_ROW_BLOCK_TARGET: u64 = 65_536;

/// The default maximum estimated raw frame size buffered into one data block.
pub const DEFAULT_BYTE_BLOCK_TARGET: u64 = 64 * 1024 * 1024;

/// The default Zstandard compression level used by [`WriterOptions`].
pub const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// The stream codec selected by a [`crate::Writer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriterCodec {
    /// Store the existing raw transformed streams unchanged.
    #[default]
    None,
    /// Compress each existing raw transformed stream as an independent
    /// Zstandard frame.
    Zstandard,
}

/// The block-local min/max statistics policy selected by a [`crate::Writer`].
///
/// Statistics are written only for v0.2 logical types with a fixed canonical
/// representation: booleans, numeric types, decimals, timestamps, dates, and
/// fixed-width binary values. Variable-width strings and binary values have no
/// v0.2 min/max representation and are left without statistics under either
/// enabled policy. `None` is the default and preserves the writer's historical
/// byte output.
///
/// Under both enabled policies nulls and floating-point NaNs are ignored, a
/// column left with no value has no statistic at all, and infinities are
/// ordinary bounds. Floating-point bounds are numeric, so `-0.0` and `0.0` are
/// interchangeable as a bound; the writer keeps whichever bit pattern it saw
/// first, which makes the choice deterministic without claiming that one
/// encoding of zero is smaller than the other.
///
/// A pair costs twice its logical type's canonical width and lives in the data
/// frame header, which readers bound at 64 MiB. A `fixed_binary` column
/// therefore charges twice its byte width against that budget, and a schema
/// wide enough to exhaust it is refused by [`crate::Writer::append`] rather
/// than written; such a schema can still be written with [`Self::None`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriterStatistics {
    /// Do not write optional column statistics.
    #[default]
    None,
    /// Write min/max statistics whenever a supported column has at least one
    /// non-null, non-NaN value.
    ///
    /// This includes the primary timestamp column. Section 11 makes that
    /// repetition unnecessary, because the block header already carries the
    /// same bounds as the mandatory pruning statistic, but writing it anyway
    /// keeps this policy's output a function of the schema alone and is what
    /// lets it override an [`Self::Automatic`] omission on any column.
    MinMax,
    /// Write statistics only where a deterministic rule predicts that the
    /// header bytes are justified.
    ///
    /// The rule is exactly this. A column gets a pair when it is not the
    /// primary timestamp column, whose block-header bounds are already the
    /// complete mandatory pruning statistic; and it has at least 64 non-null,
    /// non-NaN values; and its raw dense value bytes are at least eight times
    /// the pair. The last test binds only on `bool`, whose values cost one bit
    /// each, where it raises the effective floor to 121 values; for every other
    /// supported type the 64-value floor is the stricter of the two.
    ///
    /// The decision reads only the block's values and its schema. It does not
    /// depend on elapsed time, iteration order, randomness, earlier blocks, or
    /// how the rows were divided across [`crate::Writer::append`] calls, so
    /// equal block contents produce equal bytes.
    Automatic,
}

/// A value transform/layout that a writer can apply without profiling.
///
/// Some variants describe a complete column layout rather than one physical
/// stream transform. For example, `Dictionary` uses raw dictionary values and
/// bit-packed indices. Those supporting streams retain the transforms required
/// by the v0.2 layout; the selected variant is the fixed policy for the dense
/// column values.
///
/// Not every variant applies to every logical type, and
/// [`WriterEncoding::Fixed`] documents which set a column offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterTransform {
    /// Store canonical values without transforming them.
    Raw,
    /// Store unsigned values in a bit-packed stream.
    BitPacked,
    /// Store boolean values as value/run pairs.
    BooleanRle,
    /// Store a minimum followed by packed differences.
    FrameOfReference,
    /// Store the first value followed by packed deltas.
    Delta,
    /// Store the first value, first delta, and packed delta differences.
    DeltaOfDelta,
    /// Transpose fixed-width values by byte position.
    ByteStreamSplit,
    /// Store one value and refer to it for every row.
    Constant,
    /// Store distinct values once and pack row indices.
    Dictionary,
    /// Store one value and run length for each contiguous run.
    RunLength,
}

/// The writer-side encoding policy.
///
/// `Raw` is the default and preserves the plain-layout, raw-transform stream
/// choices, so a file written without asking for anything else is byte-for-byte
/// what earlier versions of this writer produced. `Adaptive` profiles each dense
/// stream once and selects a specialized v0.2 layout only after the complete
/// stored representation — every stream, its descriptor, its padding, and its
/// compressed size — beats the raw baseline by at least the larger of 64 bytes
/// or one percent. Selection is deterministic and depends only on the block's
/// values, never on how those rows were divided among [`crate::Writer::append`]
/// calls.
///
/// `Fixed` applies one requested transform or layout to every dense column
/// value stream. It prices nothing and never falls back:
/// [`crate::Writer::create`] returns an error for a transform this writer does
/// not offer for one of the schema's logical types, and a block whose values
/// the transform cannot describe fails when it is published.
///
/// The offered set is exactly the candidate set `Adaptive` would have priced,
/// so a fixed file is always a shape adaptive could also have written. That set
/// is narrower than what the format permits: a `timestamp64` column offers raw,
/// frame of reference, delta, and delta-of-delta and nothing else, so
/// `Fixed(WriterTransform::Dictionary)` is refused for one even though a
/// dictionary `timestamp64` column is a legal v0.2 shape.
///
/// Validity streams stay raw under `Fixed`, because they are independent
/// boolean streams that the requested value transform does not describe.
///
/// `Adaptive` profiling holds the block's dense values a second time while it
/// prices candidates, so it raises peak writer memory by a small multiple of
/// [`WriterOptions::byte_block_target`]. Lower that target to lower the peak.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriterEncoding {
    /// Plain layout with the raw transform for every stream.
    #[default]
    Raw,
    /// Deterministically choose profitable v0.2 layouts and transforms.
    Adaptive,
    /// Apply one transform/layout to every compatible column without profiling.
    Fixed(WriterTransform),
}

/// A point-in-time view of writer data accounting.
///
/// Row counts are logical rows. Byte counts are the estimated *raw* serialized
/// data-frame lengths used for bounded buffering, so they are independent of
/// both the selected compression codec and the selected encoding policy: a
/// column that Zstandard or an adaptive transform shrinks is still counted at
/// its raw size. They describe how much the writer is holding and has accepted,
/// not how much it wrote. [`WriteSummary::bytes_written`] is the actual final
/// file length, including the prologue and schema frame.
///
/// Every count here belongs to one writer session. A writer from
/// [`crate::Writer::open`] starts them all at zero, whatever the file already
/// contains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WriteAccounting {
    pub(super) buffered_rows: u64,
    pub(super) buffered_bytes: u64,
    pub(super) published_rows: u64,
    pub(super) published_bytes: u64,
    pub(super) durable_rows: u64,
    pub(super) durable_bytes: u64,
    pub(super) total_rows: u64,
    pub(super) total_bytes: u64,
}

impl WriteAccounting {
    /// Rows currently held in the in-memory block buffer.
    pub fn buffered_rows(&self) -> u64 {
        self.buffered_rows
    }

    /// Estimated raw bytes currently held in the in-memory block buffer.
    pub fn buffered_bytes(&self) -> u64 {
        self.buffered_bytes
    }

    /// Rows whose complete frames have been written to the sink.
    pub fn published_rows(&self) -> u64 {
        self.published_rows
    }

    /// Estimated raw bytes represented by published data blocks.
    pub fn published_bytes(&self) -> u64 {
        self.published_bytes
    }

    /// Rows covered by a completed sink synchronization.
    pub fn durable_rows(&self) -> u64 {
        self.durable_rows
    }

    /// Estimated raw bytes covered by a completed sink synchronization.
    pub fn durable_bytes(&self) -> u64 {
        self.durable_bytes
    }

    /// All rows accepted by the writer, whether buffered or published.
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// All estimated raw bytes accepted by the writer, whether buffered or
    /// published.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

/// Options for buffered Stage 7 writing.
///
/// Blocks are published when either target is reached, so the buffer holds at
/// most one target's worth of rows and the two targets together bound the
/// writer's memory. A single row whose encoded representation exceeds the byte
/// target is accepted as an unavoidable oversize block; this keeps the buffer
/// bounded for all splittable input while preserving row order. The default
/// codec is raw, so default output remains deterministic and uses the same raw
/// wire representation as the Stage 6 writer.
///
/// This type is `#[non_exhaustive]`, so build it from [`WriterOptions::new`] or
/// [`Default`] and the `with_` methods rather than a struct literal. Later
/// format work can then add an option without breaking callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriterOptions {
    /// Whether blocks carry the implicit contiguous row-ID sequence.
    pub row_ids: bool,
    /// Maximum logical rows in one buffered block.
    pub row_block_target: u64,
    /// Maximum estimated raw serialized bytes in one buffered block.
    pub byte_block_target: u64,
    /// Codec applied independently after each selected transform.
    pub codec: WriterCodec,
    /// Zstandard compression level used when `codec` is [`WriterCodec::Zstandard`].
    ///
    /// The value is ignored for [`WriterCodec::None`]. Supported levels are
    /// checked when the writer is created or opened.
    pub zstd_level: i32,
    /// Layout and transform selection policy.
    pub encoding: WriterEncoding,
    /// Optional block-local min/max statistics policy.
    pub statistics: WriterStatistics,
}

impl WriterOptions {
    /// Construct the default buffered raw-writing options.
    pub const fn new() -> Self {
        Self {
            row_ids: false,
            row_block_target: DEFAULT_ROW_BLOCK_TARGET,
            byte_block_target: DEFAULT_BYTE_BLOCK_TARGET,
            codec: WriterCodec::None,
            zstd_level: DEFAULT_ZSTD_LEVEL,
            encoding: WriterEncoding::Raw,
            statistics: WriterStatistics::None,
        }
    }

    /// Enable or disable implicit row IDs.
    pub const fn with_row_ids(mut self, enabled: bool) -> Self {
        self.row_ids = enabled;
        self
    }

    /// Set the maximum logical rows in one block.
    pub const fn with_row_block_target(mut self, rows: u64) -> Self {
        self.row_block_target = rows;
        self
    }

    /// Set the maximum estimated raw serialized bytes in one block.
    pub const fn with_byte_block_target(mut self, bytes: u64) -> Self {
        self.byte_block_target = bytes;
        self
    }

    /// Select raw or Zstandard stream storage.
    pub const fn with_codec(mut self, codec: WriterCodec) -> Self {
        self.codec = codec;
        self
    }

    /// Set the Zstandard compression level.
    ///
    /// The setting is used only when [`Self::with_codec`] selects
    /// [`WriterCodec::Zstandard`]. The default is [`DEFAULT_ZSTD_LEVEL`].
    pub const fn with_zstd_level(mut self, level: i32) -> Self {
        self.zstd_level = level;
        self
    }

    /// Select the raw, adaptive, or fixed-transform writer policy.
    pub const fn with_encoding(mut self, encoding: WriterEncoding) -> Self {
        self.encoding = encoding;
        self
    }

    /// Select the optional min/max statistics policy.
    pub const fn with_statistics(mut self, statistics: WriterStatistics) -> Self {
        self.statistics = statistics;
        self
    }
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// The result of a successfully finished writer.
///
/// Row, block, and sequence counts describe *this writer session*. A session
/// from [`crate::Writer::open`] reports only what it appended, so a reopened
/// file's earlier rows and blocks are not counted again.
/// [`Self::bytes_written`] is the exception: it is the length of the whole
/// file, including anything earlier sessions wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct WriteSummary {
    pub(super) rows_written: u64,
    pub(super) blocks_written: u64,
    pub(super) bytes_written: u64,
    pub(super) last_sequence: Option<u64>,
    pub(super) accounting: WriteAccounting,
}

impl WriteSummary {
    /// The number of logical rows this session committed to data frames.
    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    /// The number of data frames this session committed.
    pub fn blocks_written(&self) -> u64 {
        self.blocks_written
    }

    /// The final length of the whole file, including the prologue, the schema
    /// frame, and any frames written before this session.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// The last data-frame sequence *this session* wrote, or `None` when it
    /// appended no batch.
    ///
    /// A reopened session that publishes nothing reports `None` even though the
    /// file already holds data frames.
    pub fn last_sequence(&self) -> Option<u64> {
        self.last_sequence
    }

    /// The final row and byte accounting snapshot. `finish` has published and
    /// synchronized the final partial block, so buffered counts are zero.
    pub fn accounting(&self) -> WriteAccounting {
        self.accounting
    }
}
