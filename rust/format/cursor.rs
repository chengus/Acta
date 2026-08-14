use crate::error::{Error, Result};

/// A bounds-checked forward reader over one wire structure.
///
/// Parsers decode fields in declaration order instead of slicing at literal
/// offsets, and every read reports the file offset of the field that failed.
pub(crate) struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
    base_offset: u64,
}

impl<'a> Cursor<'a> {
    /// Read `bytes`, which begins at `base_offset` in the file.
    pub(crate) fn new(bytes: &'a [u8], base_offset: u64) -> Self {
        Self {
            bytes,
            position: 0,
            base_offset,
        }
    }

    pub(crate) fn read_bytes(&mut self, length: usize, field: &str) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| self.overflowed(field))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| self.truncated(field))?;
        self.position = end;
        Ok(bytes)
    }

    pub(crate) fn read_array<const N: usize>(&mut self, field: &str) -> Result<[u8; N]> {
        let mut array = [0_u8; N];
        array.copy_from_slice(self.read_bytes(N, field)?);
        Ok(array)
    }

    pub(crate) fn read_u16(&mut self, field: &str) -> Result<u16> {
        Ok(u16::from_le_bytes(self.read_array(field)?))
    }

    pub(crate) fn read_u32(&mut self, field: &str) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_array(field)?))
    }

    pub(crate) fn read_u64(&mut self, field: &str) -> Result<u64> {
        Ok(u64::from_le_bytes(self.read_array(field)?))
    }

    /// Advance past a field whose contents this crate does not interpret.
    pub(crate) fn skip(&mut self, length: usize, field: &str) -> Result<()> {
        self.read_bytes(length, field)?;
        Ok(())
    }

    fn offset(&self) -> Option<u64> {
        u64::try_from(self.position)
            .ok()
            .and_then(|position| self.base_offset.checked_add(position))
    }

    fn overflowed(&self, field: &str) -> Error {
        Error::corruption(
            format!("offset overflow while reading {field}"),
            self.offset(),
        )
    }

    fn truncated(&self, field: &str) -> Error {
        Error::corruption(format!("truncated {field}"), self.offset())
    }
}
