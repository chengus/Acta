//! Full validation over one captured reader snapshot.

use std::path::Path;

use crate::error::Result;
use crate::read::Reader;

use super::report::ValidationReport;

/// Decode every complete block in the snapshot discovered by `Reader::open`.
///
/// # Snapshot
///
/// `Reader::open` captures the file extent before it walks the file and
/// retains the complete block list, and every read this pass makes is bounded
/// by that extent. A concurrent append therefore cannot enter this pass: bytes
/// written after the extent was measured are outside every read.
///
/// Replacement is a weaker guarantee than that, and deliberately so. The scan
/// opens the path a second time, so a file that is atomically replaced between
/// the open and the scan is read from the new inode under the old extent. That
/// is caught rather than believed: the new bytes fail the frame magic, the
/// frame CRCs, or the read itself, and the pass returns a structured error.
/// What it does not do is report on a file that no longer exists at that path.
/// A caller that needs replacement-proof validation should validate a file
/// handle it holds open, which this crate does not yet expose.
///
/// # Frame count
///
/// The report's frame count comes from the shared structural walk by way of
/// the reader snapshot, not from the block count. Deriving it here would
/// silently disagree with structural validation the moment a frame type that
/// is not a data frame becomes legal.
pub(crate) fn validate_path(
    path: &Path,
    limits: crate::limits::Limits,
) -> Result<ValidationReport> {
    let reader = Reader::open_with_limits(path, limits)?;
    for block in reader.scan() {
        block?;
    }

    let metadata = reader.file_metadata();
    Ok(ValidationReport::from_snapshot(
        metadata.format_version(),
        metadata.feature_flags(),
        metadata.frame_count(),
        metadata.file_size(),
        metadata.last_good_offset(),
        metadata.incomplete_tail(),
    ))
}
