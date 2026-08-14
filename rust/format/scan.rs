//! The one forward walk over an Acta file that every entry point shares.
//!
//! Specification section 13.1 describes a single discovery procedure: validate
//! the prologue and schema frame, then read frames in order until the file ends
//! or a frame is not yet complete. The validator and the reader want different
//! things from that walk, but they must agree about what the file contains, so
//! the walk itself lives here and they differ only in what they record.

use std::fs::File;
use std::path::Path;

use crate::error::{Error, ErrorContext, Result};
use crate::limits::Limits;
use crate::schema::Schema;

use super::constants::{
    FIRST_DATA_FRAME_SEQUENCE, PROLOGUE_SIZE, ROW_IDS_FEATURE, SCHEMA_FRAME_SEQUENCE,
};
use super::data_frame::{self, DataFrameMetadata};
use super::frame::{self, FrameMetadata, FrameRead};
use super::prologue::{self, Prologue};
use super::schema_frame;

/// An open file whose prologue and schema frame have been validated.
pub(crate) struct FileScan {
    file: File,
    file_size: u64,
    limits: Limits,
    prologue: Prologue,
    schema: Schema,
    first_data_frame_offset: u64,
}

/// Where a completed walk stopped, and why.
pub(crate) struct Walk {
    /// Complete frames in the file, counting the schema frame.
    pub(crate) frame_count: u64,
    /// The sequence number the next appended data frame must carry.
    ///
    /// Counted alongside `frame_count` rather than derived from it. The two
    /// agree only while every frame after the schema frame is a data frame,
    /// and section 14 reserves checkpoint frames, which would occupy a
    /// sequence number without being a data block. A writer that continued
    /// from a count would then commit a wrong sequence into a frame.
    pub(crate) next_sequence: u64,
    /// The byte after the last complete frame.
    pub(crate) last_good_offset: u64,
    /// Whether the file ends inside a frame that is still being appended.
    pub(crate) incomplete_tail: bool,
    /// The next implicit row ID after all complete data frames, when enabled.
    pub(crate) next_row_id: Option<u64>,
    /// Bytes of complete frames this walk streamed to verify their commit
    /// trailers.
    ///
    /// Counted here because only the walk knows which frames it actually
    /// read: a resumed walk reads the frames after its resume point and
    /// nothing before it, so a caller that reported the whole file extent
    /// would overstate an incremental walk by the size of everything it
    /// deliberately skipped. Bytes of an incomplete tail are excluded; they
    /// are not committed and the walk may read them again.
    pub(crate) frame_bytes_scanned: u64,
}

/// The boundary an incremental walk resumes from.
///
/// A refresh must discover only the frames appended after a snapshot's last
/// committed frame, so it carries that snapshot's continuation state instead
/// of restarting at the schema frame: the byte after the last committed
/// frame, the sequence number the next data frame must carry, the next
/// implicit base row ID when the file enables them, and the frame count the
/// snapshot had already reached. Every field is a checked continuation of the
/// prior walk, never a value derived from the file being walked.
pub(crate) struct ResumePoint {
    /// The byte after the last frame the prior walk committed.
    pub(crate) offset: u64,
    /// The sequence number the next data frame must carry.
    pub(crate) sequence: u64,
    /// The base row ID the next data block must carry, when row IDs are on.
    pub(crate) expected_base_row_id: Option<u64>,
    /// The complete frame count the prior walk reached, schema frame included.
    pub(crate) frame_count: u64,
}

impl FileScan {
    /// Open `path` and validate everything that precedes its first data frame.
    ///
    /// The file extent is captured once, here. Every later read is bounded by
    /// it, so a file that grows during the walk cannot extend the snapshot and
    /// a file that shrinks fails as an I/O error rather than silently.
    pub(crate) fn open(path: &Path, limits: Limits) -> Result<Self> {
        let file = File::open(path)
            .map_err(|error| Error::io(error, None).with_context(ErrorContext::File))?;
        Self::from_file(file, limits)
    }

    /// Build a scan over an already-open handle.
    ///
    /// The writer lends its locked append handle here and takes it back with
    /// [`Self::into_file`]. Scanning the handle rather than reopening the path
    /// means discovery observes exactly the bytes the appends will extend, and
    /// leaves no window between validating a path and writing to it.
    pub(crate) fn from_file(mut file: File, limits: Limits) -> Result<Self> {
        let file_size = file
            .metadata()
            .map(|metadata| metadata.len())
            .map_err(|error| Error::io(error, None).with_context(ErrorContext::File))?;
        let prologue = prologue::read_from_file(&mut file, file_size)?;

        let schema_offset = PROLOGUE_SIZE as u64;
        let frame = match frame::read_frame(
            &mut file,
            file_size,
            schema_offset,
            SCHEMA_FRAME_SEQUENCE,
            limits,
        )? {
            FrameRead::Complete(frame) => frame,
            // A tail is only recoverable once the schema frame itself is
            // complete; before that the file declares no columns and nothing
            // can be read from it.
            FrameRead::IncompleteTail => return Err(incomplete_schema_frame(schema_offset)),
        };
        let schema = schema_frame::parse(&mut file, &frame, limits)?;
        let first_data_frame_offset = next_offset(frame.frame_offset, frame.total_length)?;

        Ok(Self {
            file,
            file_size,
            limits,
            prologue,
            schema,
            first_data_frame_offset,
        })
    }

    pub(crate) fn prologue(&self) -> Prologue {
        self.prologue
    }

    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Return the handle this scan was built from.
    ///
    /// The writer opens and locks one handle, lends it here for discovery, and
    /// takes it back to append with. No descriptor is ever duplicated, so the
    /// lock never depends on how a platform treats closing a duplicate.
    pub(crate) fn into_file(self) -> File {
        self.file
    }

    /// Read every complete data frame in order, passing each to `visit`.
    ///
    /// Sequence continuity and the implicit row-ID chain are section 4 and
    /// section 8 invariants of the file, not of any one caller, so they are
    /// enforced here. A frame that is present but damaged is an error; a frame
    /// the file ends inside is an interrupted append and ends the walk.
    pub(crate) fn walk_data_frames(
        &mut self,
        visit: impl FnMut(&FrameMetadata, &DataFrameMetadata) -> Result<()>,
    ) -> Result<Walk> {
        self.walk_data_frames_from(self.initial_resume_point(), visit)
    }

    /// Resume a data-frame walk from a previously committed boundary.
    ///
    /// This is the one discovery procedure behind [`Reader::refresh`]: it
    /// validates every frame after the resume point exactly as a fresh walk
    /// would, carrying forward the expected sequence and base row ID, so a
    /// snapshot can extend itself without rescanning the frames it already
    /// committed. Everything after the resume point — completeness, sequence,
    /// row-ID continuity, schema agreement, and the walk's own limits — is
    /// still the same strict walk.
    pub(crate) fn walk_data_frames_from(
        &mut self,
        resume: ResumePoint,
        mut visit: impl FnMut(&FrameMetadata, &DataFrameMetadata) -> Result<()>,
    ) -> Result<Walk> {
        let mut offset = resume.offset;
        let mut sequence = resume.sequence;
        let mut frame_count = resume.frame_count;
        let mut expected_base_row_id = resume.expected_base_row_id;
        let mut incomplete_tail = false;
        let mut frame_bytes_scanned = 0_u64;

        while offset < self.file_size {
            let frame = match frame::read_frame(
                &mut self.file,
                self.file_size,
                offset,
                sequence,
                self.limits,
            )? {
                FrameRead::Complete(frame) => frame,
                FrameRead::IncompleteTail => {
                    incomplete_tail = true;
                    break;
                }
            };
            let block = data_frame::parse(
                &mut self.file,
                &frame,
                &self.schema,
                self.prologue.feature_flags,
                expected_base_row_id,
            )?;

            if let Some(base) = block.base_row_id {
                expected_base_row_id =
                    Some(base.checked_add(block.row_count).ok_or_else(|| {
                        Error::corruption("base row ID overflow", Some(frame.frame_offset))
                            .with_context(ErrorContext::Frame {
                                sequence: frame.sequence,
                            })
                    })?);
            }
            visit(&frame, &block)?;

            offset = next_offset(offset, frame.total_length)?;
            let (Some(advanced_sequence), Some(advanced_count), Some(advanced_bytes)) = (
                sequence.checked_add(1),
                frame_count.checked_add(1),
                frame_bytes_scanned.checked_add(frame.total_length),
            ) else {
                return Err(
                    Error::corruption("frame sequence overflow", Some(frame.frame_offset))
                        .with_context(ErrorContext::File),
                );
            };
            sequence = advanced_sequence;
            frame_count = advanced_count;
            frame_bytes_scanned = advanced_bytes;
        }

        Ok(Walk {
            frame_count,
            next_sequence: sequence,
            last_good_offset: offset,
            incomplete_tail,
            next_row_id: expected_base_row_id,
            frame_bytes_scanned,
        })
    }

    fn initial_resume_point(&self) -> ResumePoint {
        ResumePoint {
            offset: self.first_data_frame_offset,
            sequence: FIRST_DATA_FRAME_SEQUENCE,
            expected_base_row_id: self.row_ids_enabled().then_some(0_u64),
            frame_count: 1,
        }
    }

    fn row_ids_enabled(&self) -> bool {
        self.prologue.feature_flags & ROW_IDS_FEATURE != 0
    }
}

fn next_offset(offset: u64, length: u64) -> Result<u64> {
    offset.checked_add(length).ok_or_else(|| {
        Error::corruption("next frame offset overflow", Some(offset))
            .with_context(ErrorContext::File)
    })
}

fn incomplete_schema_frame(offset: u64) -> Error {
    Error::incomplete_tail(
        "the file ends before its schema frame is complete",
        Some(offset),
    )
    .with_context(ErrorContext::Frame {
        sequence: SCHEMA_FRAME_SEQUENCE,
    })
}
