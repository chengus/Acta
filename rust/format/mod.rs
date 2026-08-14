//! Checked decoding of the Acta v0.2 wire format.

pub(crate) mod constants;
pub(crate) mod cursor;
pub(crate) mod data_frame;
pub(crate) mod frame;
pub(crate) mod prologue;
pub(crate) mod scan;
pub(crate) mod schema_frame;

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use crate::error::{Error, Result};

/// The absolute file offset of a field located `field` bytes into a structure
/// that begins at `base`.
pub(crate) fn field_offset(base: u64, field: usize) -> Option<u64> {
    u64::try_from(field)
        .ok()
        .and_then(|field| base.checked_add(field))
}

/// Fill `buffer` from `offset`.
///
/// Callers check every read against the file extent first, so a short read here
/// means the file shrank underneath us and is reported as an I/O failure.
pub(crate) fn read_exact_at(file: &mut File, offset: u64, buffer: &mut [u8]) -> Result<()> {
    seek(file, offset)?;
    file.read_exact(buffer)
        .map_err(|error| Error::io(error, Some(offset)))
}

pub(crate) fn seek(file: &mut File, offset: u64) -> Result<()> {
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| Error::io(error, Some(offset)))?;
    Ok(())
}
