//! Fully decode and validate an Acta file.
//!
//! Run with:
//!
//!     cargo run --release --example validate_acta -- path/to/file.acta

use acta::{Reader, ValidationLevel, ValidationOptions, validate_with_options};

fn main() -> acta::Result<()> {
    let path = std::env::args().nth(1).expect("expected an Acta path");

    let reader = Reader::open(&path)?;
    let report = validate_with_options(
        &path,
        ValidationOptions::default().with_level(ValidationLevel::Full),
    )?;

    println!(
        "full validation: format={:?}, frames={}, rows={}, incomplete_tail={}",
        report.format_version(),
        report.frame_count(),
        reader.total_rows(),
        report.incomplete_tail(),
    );
    Ok(())
}
