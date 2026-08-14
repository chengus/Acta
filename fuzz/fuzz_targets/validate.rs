//! Fuzz the top-level validator with arbitrary bytes.
//!
//! ```text
//! cargo +nightly fuzz run validate -- -max_len=8192
//! ```
//!
//! Seeding the corpus from `spec/v0.2/fixtures` gives the fuzzer valid framing
//! to mutate rather than making it rediscover the prologue magic and CRCs.

#![no_main]

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use tempfile::NamedTempFile;

/// One scratch file for the whole run, so each case costs a single rewrite.
static SCRATCH: OnceLock<NamedTempFile> = OnceLock::new();

fuzz_target!(|data: &[u8]| {
    let scratch = SCRATCH.get_or_init(|| NamedTempFile::new().expect("scratch file"));
    std::fs::write(scratch.path(), data).expect("write scratch file");

    // Any verdict is acceptable. The properties under test are that no input
    // panics or hangs, and that an accepted file is described truthfully.
    if let Ok(report) = acta::validate(scratch.path()) {
        assert_eq!(report.file_size(), data.len() as u64);
        assert!(report.last_good_offset() <= report.file_size());
        assert!(report.frame_count() >= 1);
    }
});
