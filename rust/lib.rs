//! Acta v0.2 file validation and metadata reading.
//!
//! [`validate`] checks a file's framing, schema descriptors, and block
//! metadata. [`validate_with_options`] can additionally decode every complete
//! block and verify logical values and optional statistics. [`Reader`] exposes
//! that same information as an open-time snapshot of the schema and the
//! committed blocks. Both walk the file through one shared parser, so they
//! agree about which files are well formed.
//!
//! Logical block decoding is available through [`Reader::read_block`], which
//! decodes one whole block, and the lazy sequential [`Reader::scan`] API, which
//! can restrict a read to a projection of the columns and to a half-open
//! [`PrimaryRange`] over the primary column, pruning blocks by their stored
//! bounds before reading them. A snapshot [`Scan`] is immutable: it decodes
//! only the blocks the reader held when it was created, so later appends
//! cannot enter it. To follow an append-only file as it grows,
//! [`Reader::refresh`] extends the snapshot in place with frames committed
//! since it was opened, and [`Reader::tail`] returns a [`Tail`] that polls the
//! path for newly committed frames synchronously, one block per poll, without
//! sleeping or blocking. A tail never exposes a partial frame, never infers a
//! global primary ordering, and stops cleanly by being dropped. [`Writer`]
//! creates deterministic plain/raw v0.2 files by default, buffers appended
//! batches into blocks bounded by a row and a byte target, and can explicitly
//! select Zstandard when the default `zstd` feature is enabled. Writer-side
//! transforms, fixed policies, deterministic adaptive encoding, and optional
//! block-local min/max statistics are available through additive writer
//! options; their defaults leave the plain/raw output unchanged. Statistics
//! are written under [`WriterStatistics`], which is generation only: nothing
//! reads them for pruning, since [`PrimaryRange`] prunes on the mandatory
//! primary bounds in the block header. [`Writer::open`] reconstructs a
//! complete existing file and resumes the same append engine that
//! [`Writer::create`] enters, exposing the reconstructed schema through
//! [`Writer::schema`]; [`Writer::open_with_schema`] adds an exact schema guard
//! and [`Writer::open_with_limits`] reads the existing file under explicit
//! [`Limits`]. Both entry points hold a cooperative exclusive writer lock that
//! never blocks readers. Incomplete tails require explicit recovery and are
//! not silently truncated. [`inspect_recovery`] is a read-only snapshot, while
//! [`repair_incomplete_tail`] is a destructive operation narrowly limited to a
//! structurally verified incomplete final data frame.
//!
//! ```
//! let report = acta::validate("spec/v0.2/fixtures/minimal/minimal.acta")?;
//!
//! assert_eq!(report.format_version(), (0, 2));
//! assert_eq!(report.frame_count(), 2);
//! assert!(!report.incomplete_tail());
//! # Ok::<(), acta::Error>(())
//! ```
//!
//! Explicit recovery is a read-only inspection followed by an intentional,
//! narrowly bounded repair:
//!
//! ```no_run
//! let path = std::path::Path::new("ticks.acta");
//! let plan = acta::inspect_recovery(path)?;
//! if plan.requires_repair() {
//!     let summary = acta::repair_incomplete_tail(path)?;
//!     println!("removed {} bytes", summary.bytes_removed());
//! }
//! # Ok::<(), acta::Error>(())
//! ```
//!
//! Full validation is opt-in because it decodes every complete block and
//! verifies optional statistics, while structural validation only walks the
//! metadata needed to establish a safe snapshot.

mod array;
mod batch;
mod codec;
mod crc32c;
mod error;
mod format;
mod limits;
mod lock;
mod read;
mod recovery;
mod schema;
mod validate;
mod write;

pub use array::{
    Array, BinaryArray, BooleanArray, DecimalArray, PrimitiveArray, ScalarValue, TimestampArray,
    Utf8Array,
};
pub use batch::RecordBatch;
pub use error::{Error, ErrorContext, ErrorKind, Result};
pub use limits::Limits;
pub use read::{
    BlockMetadata, FILE_ID_SIZE, FileMetadata, PrimaryBounds, PrimaryRange, Reader, RefreshReport,
    Scan, ScanMetrics, Tail,
};
pub use recovery::{
    RecoveryAction, RecoveryPlan, RecoverySummary, inspect_recovery, inspect_recovery_with_limits,
    repair_incomplete_tail, repair_incomplete_tail_with_limits,
};
pub use schema::{Column, LogicalType, Schema, TimeUnit, TimeZone};
pub use validate::{ValidationLevel, ValidationOptions, ValidationReport};
pub use write::{
    DEFAULT_BYTE_BLOCK_TARGET, DEFAULT_ROW_BLOCK_TARGET, DEFAULT_ZSTD_LEVEL, WriteAccounting,
    WriteSummary, Writer, WriterCodec, WriterEncoding, WriterOptions, WriterStatistics,
    WriterTransform,
};

use std::path::Path;

/// Validate the Acta v0.2 structure of `path` under the default [`Limits`].
///
/// This is the inexpensive metadata-only level. It covers the prologue, frame
/// envelopes and CRCs, the schema frame's column descriptors, and each data
/// frame's block header. It does not decode logical streams or inspect claims
/// that require values, such as `TS_SORTED` ordering and optional statistics.
/// Use [`validate_with_options`] with [`ValidationLevel::Full`] for that more
/// expensive pass.
///
/// A complete file returns a report whose [`incomplete_tail`] method is false.
/// If the file ends inside a frame after its schema frame, validation succeeds
/// with that method returning true and [`last_good_offset`] identifying the
/// byte after the last complete frame. A complete but corrupt frame returns an
/// error, as does a file that ends before its schema frame is complete.
///
/// [`incomplete_tail`]: ValidationReport::incomplete_tail
/// [`last_good_offset`]: ValidationReport::last_good_offset
pub fn validate<P: AsRef<Path>>(path: P) -> Result<ValidationReport> {
    validate_with_options(path, ValidationOptions::default())
}

/// Validate the Acta v0.2 framing of `path` under caller-supplied `limits`.
///
/// ```
/// let limits = acta::Limits::default().with_max_frame_payload_length(8);
/// let error = acta::validate_with_limits(
///     "spec/v0.2/fixtures/minimal/minimal.acta",
///     limits,
/// )
/// .unwrap_err();
///
/// assert_eq!(error.kind(), acta::ErrorKind::ResourceLimit);
/// ```
pub fn validate_with_limits<P: AsRef<Path>>(path: P, limits: Limits) -> Result<ValidationReport> {
    validate_with_options(path, ValidationOptions::default().with_limits(limits))
}

/// Validate an Acta file with an explicit level and resource limits.
///
/// Structural validation reads metadata and checks frame integrity without
/// materializing logical columns. Full validation has the cost of decoding
/// every complete block, including all stream transforms and codecs, and also
/// verifies optional min/max statistics against the decoded values.
///
/// The two levels report identical [`ValidationReport`] values for any file
/// both accept. They do not accept exactly the same files: full validation
/// builds a [`Reader`] snapshot and so additionally enforces the limits that
/// only apply to a snapshot, notably [`Limits::max_blocks`] and the per-block
/// decode limits. A file with more blocks than that limit passes structural
/// validation and fails full validation with
/// [`ErrorKind::ResourceLimit`]; raise the
/// limit to validate it fully.
pub fn validate_with_options<P: AsRef<Path>>(
    path: P,
    options: ValidationOptions,
) -> Result<ValidationReport> {
    validate::validate_path(path.as_ref(), options)
}
