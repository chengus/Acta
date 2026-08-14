use crate::format::prologue::Prologue;

/// The result of a sequential Acta v0.2 structural validation.
///
/// A report is only produced for a file that contains at least its schema
/// frame, so [`frame_count`](Self::frame_count) is always one or more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationReport {
    format_version: (u16, u16),
    feature_flags: u64,
    frame_count: u64,
    file_size: u64,
    last_good_offset: u64,
    incomplete_tail: bool,
}

impl ValidationReport {
    /// The format version the prologue declares.
    pub fn format_version(&self) -> (u16, u16) {
        self.format_version
    }

    /// The prologue feature flags. Bit zero is `ROW_IDS`.
    pub fn feature_flags(&self) -> u64 {
        self.feature_flags
    }

    /// The number of complete frames, including the schema frame.
    pub fn frame_count(&self) -> u64 {
        self.frame_count
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// The byte after the last complete frame.
    ///
    /// A writer resumes appending here, and a repair tool may truncate here.
    pub fn last_good_offset(&self) -> u64 {
        self.last_good_offset
    }

    /// Whether the file ends inside a frame that is still being appended.
    pub fn incomplete_tail(&self) -> bool {
        self.incomplete_tail
    }

    pub(crate) fn complete(prologue: Prologue, frame_count: u64, file_size: u64) -> Self {
        Self {
            format_version: prologue.format_version,
            feature_flags: prologue.feature_flags,
            frame_count,
            file_size,
            last_good_offset: file_size,
            incomplete_tail: false,
        }
    }

    pub(crate) fn from_snapshot(
        format_version: (u16, u16),
        feature_flags: u64,
        frame_count: u64,
        file_size: u64,
        last_good_offset: u64,
        incomplete_tail: bool,
    ) -> Self {
        Self {
            format_version,
            feature_flags,
            frame_count,
            file_size,
            last_good_offset,
            incomplete_tail,
        }
    }

    pub(crate) fn with_incomplete_tail(
        prologue: Prologue,
        frame_count: u64,
        file_size: u64,
        last_good_offset: u64,
    ) -> Self {
        Self {
            format_version: prologue.format_version,
            feature_flags: prologue.feature_flags,
            frame_count,
            file_size,
            last_good_offset,
            incomplete_tail: true,
        }
    }
}
