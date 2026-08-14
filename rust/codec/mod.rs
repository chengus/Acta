//! Stream transform and compression algorithms.
//!
//! These are pure functions over borrowed bytes. They know nothing about
//! files, schemas, or block layouts, so a writer can reuse them unchanged.

#[cfg(feature = "zstd")]
pub(crate) mod compression;
pub(crate) mod transform;
