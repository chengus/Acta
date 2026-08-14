//! Metadata-first snapshot reader.

mod block;
mod budget;
mod decode;
mod identity;
mod reader;
mod scan;
mod tail;
mod values;

pub use block::{BlockMetadata, PrimaryBounds};
pub use budget::ScanMetrics;
pub use reader::{FILE_ID_SIZE, FileMetadata, Reader, RefreshReport};
pub use scan::{PrimaryRange, Scan};
pub use tail::Tail;
