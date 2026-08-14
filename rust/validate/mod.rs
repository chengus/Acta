mod full;
mod options;
mod report;
pub(crate) mod statistics;

pub use options::{ValidationLevel, ValidationOptions};
pub use report::ValidationReport;

use std::path::Path;

use crate::error::Result;
use crate::format::scan::FileScan;
use crate::limits::Limits;

/// Walk every frame in `path` from the prologue to the end of the file.
///
/// The walk itself lives in [`FileScan`], which the reader shares, so the two
/// entry points cannot disagree about which files are well formed. Validation
/// keeps only the counts; it retains no per-block metadata.
pub(crate) fn validate_path(path: &Path, options: ValidationOptions) -> Result<ValidationReport> {
    if options.level() == ValidationLevel::Full {
        return full::validate_path(path, options.limits());
    }

    validate_structural_path(path, options.limits())
}

fn validate_structural_path(path: &Path, limits: Limits) -> Result<ValidationReport> {
    let mut scan = FileScan::open(path, limits)?;
    let walk = scan.walk_data_frames(|_frame, _block| Ok(()))?;

    let prologue = scan.prologue();
    let file_size = scan.file_size();
    if walk.incomplete_tail {
        return Ok(ValidationReport::with_incomplete_tail(
            prologue,
            walk.frame_count,
            file_size,
            walk.last_good_offset,
        ));
    }
    Ok(ValidationReport::complete(
        prologue,
        walk.frame_count,
        file_size,
    ))
}
