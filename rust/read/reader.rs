//! Synchronous discovery of a stable metadata snapshot.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, ErrorContext, Result};
use crate::format::constants::TRAILER_SIZE;
use crate::format::data_frame;
use crate::format::frame::{self, FrameRead};
use crate::format::scan::{FileScan, ResumePoint};
use crate::limits::Limits;
use crate::schema::Schema;

use super::block::{BlockMetadata, PrimaryBounds};
use super::budget::ScanBudget;
use super::decode;
use super::identity::{self, FileIdentity};
use super::scan::Scan;
use super::tail::Tail;

/// The width of the opaque file ID a v0.2 prologue carries.
pub const FILE_ID_SIZE: usize = 16;

/// File-level metadata retained by a [`Reader`] snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetadata {
    format_version: (u16, u16),
    feature_flags: u64,
    file_id: [u8; FILE_ID_SIZE],
    schema_id: u64,
    file_size: u64,
    last_good_offset: u64,
    /// Complete frames in the snapshot, counting the schema frame. Carried
    /// from the shared walk rather than recomputed, so full validation and
    /// structural validation cannot drift apart if a future frame type stops
    /// contributing exactly one data block per frame.
    frame_count: u64,
    block_count: u64,
    total_rows: u64,
    incomplete_tail: bool,
    /// The sequence number the next appended data frame must carry.
    ///
    /// Carried from the walk rather than derived from `frame_count`. The two
    /// agree only while every frame after the schema frame is a data frame,
    /// and section 14 reserves checkpoint frames, which would occupy a
    /// sequence number without contributing a data block. A refresh that
    /// continued from a count would then expect the wrong sequence.
    next_sequence: u64,
    /// The base row ID the next appended data block must carry, when the file
    /// enables implicit row IDs. Also carried from the walk rather than
    /// re-derived from `total_rows`.
    next_row_id: Option<u64>,
    /// The commit trailer of the last frame this snapshot committed, exactly
    /// as it was stored when the snapshot committed it.
    ///
    /// This is how a refresh knows the committed bytes under it are still its
    /// own. See [`Reader::expect_own_committed_bytes`].
    commit_anchor: [u8; TRAILER_SIZE],
}

impl FileMetadata {
    /// The format version declared by the file prologue.
    pub fn format_version(&self) -> (u16, u16) {
        self.format_version
    }

    /// The prologue feature flags.
    pub fn feature_flags(&self) -> u64 {
        self.feature_flags
    }

    /// The opaque file ID from the prologue. It is identity metadata, not a
    /// content hash.
    pub fn file_id(&self) -> &[u8; FILE_ID_SIZE] {
        &self.file_id
    }

    /// The schema ID shared by the schema and data frames.
    pub fn schema_id(&self) -> u64 {
        self.schema_id
    }

    /// The file length observed when the reader opened it.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// The byte after the last complete committed frame.
    pub fn last_good_offset(&self) -> u64 {
        self.last_good_offset
    }

    /// The number of complete data blocks in this snapshot.
    pub fn block_count(&self) -> u64 {
        self.block_count
    }

    /// Complete frames in this snapshot, counting the schema frame.
    ///
    /// Internal: this exists so full validation can report the same frame
    /// count the structural walk produced instead of deriving one from the
    /// block count.
    pub(crate) fn frame_count(&self) -> u64 {
        self.frame_count
    }

    /// The checked sum of the row counts of all complete data blocks.
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Whether a final append ended before its next frame was committed.
    ///
    /// False means the snapshot ends exactly after its last committed frame.
    pub fn incomplete_tail(&self) -> bool {
        self.incomplete_tail
    }
}

/// A metadata-first, open-time snapshot of an Acta v0.2 file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reader {
    path: PathBuf,
    limits: Limits,
    schema: Arc<Schema>,
    file_metadata: FileMetadata,
    blocks: Vec<BlockMetadata>,
    identity: FileIdentity,
}

/// What one [`Reader::refresh`] added to a snapshot.
///
/// The counts describe only the frames discovered by that call, never the
/// frames the snapshot already held, so the sum of a report's
/// [`blocks_added`](Self::blocks_added) across refreshes is the total number
/// of blocks the refresh path has contributed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RefreshReport {
    blocks_added: u64,
    rows_added: u64,
    previous_file_size: u64,
    observed_file_size: u64,
    incomplete_tail: bool,
    /// Bytes of newly committed frames this refresh streamed to verify their
    /// commit trailers. Internal: a [`Tail`] folds it into its own metrics so
    /// the byte count it reports covers discovery as well as decoding.
    frame_bytes_scanned: u64,
}

impl RefreshReport {
    /// Complete data frames discovered and added by this refresh.
    pub fn blocks_added(&self) -> u64 {
        self.blocks_added
    }

    /// The checked sum of the row counts of the added blocks.
    pub fn rows_added(&self) -> u64 {
        self.rows_added
    }

    /// The file extent this snapshot held before the refresh.
    pub fn previous_file_size(&self) -> u64 {
        self.previous_file_size
    }

    /// The file extent the refresh observed.
    pub fn observed_file_size(&self) -> u64 {
        self.observed_file_size
    }

    /// Whether the refresh left a physically incomplete frame after the last
    /// complete committed frame.
    ///
    /// True here does not mean the snapshot is damaged: an interrupted append
    /// exposes no block and a later refresh may see that tail become a
    /// complete committed frame.
    pub fn incomplete_tail(&self) -> bool {
        self.incomplete_tail
    }

    /// Bytes of newly committed frames this refresh streamed to verify their
    /// commit trailers.
    ///
    /// Internal: a [`Tail`] folds this into its own metrics so the byte count
    /// it reports covers discovery as well as decoding.
    pub(crate) fn frame_bytes_scanned(&self) -> u64 {
        self.frame_bytes_scanned
    }
}

impl Reader {
    /// Open a file and discover its complete committed schema and data frames.
    ///
    /// The file extent is captured before scanning, so later appends do not
    /// alter this reader; reopen the path to obtain a newer snapshot. A final
    /// frame the file ends inside is an interrupted append: it is excluded from
    /// the snapshot and reported by [`FileMetadata::incomplete_tail`]. A frame
    /// that is present in full but damaged is corruption and fails the open.
    ///
    /// Section 6.2 permits a metadata-only open to defer the frame body CRC.
    /// This reader deliberately does not: every committed frame is checksummed
    /// here, which costs one pass over the file and in exchange makes an
    /// interrupted append distinguishable from damage at open time rather than
    /// at first read.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_limits(path, Limits::default())
    }

    /// Open a file with explicit frame, metadata, and snapshot limits.
    pub fn open_with_limits<P: AsRef<Path>>(path: P, limits: Limits) -> Result<Self> {
        let path = path.as_ref().to_owned();
        // The identity is captured before discovery so refresh can later tell
        // the file this snapshot came from from whatever else now sits at the
        // path, without holding a descriptor for the life of the reader.
        let identity = FileIdentity::capture(&path)
            .map_err(|error| Error::io(error, None).with_context(ErrorContext::File))?;
        let mut scan = FileScan::open(&path, limits)?;

        let mut blocks: Vec<BlockMetadata> = Vec::new();
        let mut total_rows = 0_u64;
        let walk = scan.walk_data_frames(|frame, block| {
            let offset = Some(frame.frame_offset);
            let block_count = u64::try_from(blocks.len()).map_err(|_| {
                Error::resource_limit("block count does not fit the configured limit", offset)
                    .with_context(ErrorContext::File)
            })?;
            if block_count >= limits.max_blocks() {
                return Err(Error::resource_limit(
                    format!(
                        "data block count exceeds the {}-block limit",
                        limits.max_blocks()
                    ),
                    offset,
                )
                .with_context(ErrorContext::File));
            }
            blocks.try_reserve(1).map_err(|_| {
                Error::resource_limit("unable to reserve block metadata", offset)
                    .with_context(ErrorContext::File)
            })?;
            total_rows = total_rows.checked_add(block.row_count).ok_or_else(|| {
                Error::corruption("total row count overflow", offset)
                    .with_context(ErrorContext::File)
            })?;
            blocks.push(BlockMetadata::new(
                frame.sequence,
                frame.frame_offset,
                frame.total_length,
                block.row_count,
                block.base_row_id,
                block
                    .primary_bounds
                    .map(|(minimum, maximum)| PrimaryBounds::new(minimum, maximum)),
                block.ts_sorted,
            ));
            Ok(())
        })?;

        let prologue = scan.prologue();
        let block_count = u64::try_from(blocks.len()).map_err(|_| {
            Error::resource_limit("block count does not fit the configured limit", None)
                .with_context(ErrorContext::File)
        })?;
        let file_size = scan.file_size();
        let schema = Arc::new(scan.schema().clone());
        let schema_id = schema.schema_id();
        // The schema frame is always complete by the time the walk runs, so a
        // committed boundary always follows at least one frame and there is
        // always an anchor to capture, even for a file with no data blocks.
        let commit_anchor =
            frame::read_commit_trailer(&mut scan.into_file(), file_size, walk.last_good_offset)?;
        let file_metadata = FileMetadata {
            format_version: prologue.format_version,
            feature_flags: prologue.feature_flags,
            file_id: prologue.file_id,
            schema_id,
            file_size,
            last_good_offset: walk.last_good_offset,
            frame_count: walk.frame_count,
            block_count,
            total_rows,
            incomplete_tail: walk.incomplete_tail,
            next_sequence: walk.next_sequence,
            next_row_id: walk.next_row_id,
            commit_anchor,
        };

        Ok(Self {
            path,
            limits,
            schema,
            file_metadata,
            blocks,
            identity,
        })
    }

    /// Extend this snapshot with frames committed since it was opened or last
    /// refreshed.
    ///
    /// Refresh starts at this reader's own committed boundary — its last good
    /// offset, expected next sequence, and expected next implicit row ID — and
    /// validates every frame it discovers beyond that boundary exactly as
    /// [`Self::open`] validates a whole file: prefix and prefix CRC, bounded
    /// lengths and checked arithmetic, header, payload, trailer, and body CRC,
    /// frame type and sequence, schema ID, block metadata, implicit row-ID
    /// continuity, and the configured resource limits. Newly discovered blocks
    /// are appended to the snapshot in exact file/sequence order.
    ///
    /// The snapshot is mutated only after the discovery pass succeeds, so a
    /// failed refresh leaves every field and block vector unchanged. A refresh
    /// that finds no growth succeeds with zero additions; one that finds only
    /// an incomplete next frame succeeds with zero additions and reports
    /// `incomplete_tail`. Repeated refreshes never add the same frame twice,
    /// and a no-growth refresh leaves the reported file size unchanged.
    ///
    /// A physically incomplete tail is an interrupted append, not corruption,
    /// and may become a complete committed frame by the next refresh. A later
    /// refresh also accepts a safe Stage 8b repair that shrank only that
    /// uncommitted tail back to the previous `last_good_offset`, and it clears
    /// the tail. A file that shrank below the committed boundary is refused
    /// with [`ErrorKind::FileTruncated`](crate::ErrorKind::FileTruncated).
    ///
    /// # Refusing another file at the same path
    ///
    /// A refresh will not extend this snapshot from a file that is not the one
    /// it came from, and it checks that in two ways because neither alone is
    /// enough. It compares the path's current file-system identity against the
    /// identity captured at open, which catches a replacement that unlinked
    /// and recreated the path. And, because an in-place truncate-and-rewrite
    /// keeps that identity while replacing every byte, it re-reads the commit
    /// trailer of the last frame this snapshot committed — its length,
    /// sequence, body CRC, own CRC, and commit magic — and refuses unless it
    /// still matches. On growth it also compares the prologue and schema it
    /// re-reads against the ones this snapshot holds. Any mismatch is
    /// [`ErrorKind::FileReplaced`](crate::ErrorKind::FileReplaced), never a
    /// silent adoption, and none of it depends on the v0.2 file ID, which the
    /// deterministic writer stores as a non-unique zero.
    ///
    /// One boundary is worth stating plainly. A snapshot holding no data
    /// blocks has only its prologue and schema frame to be recognised by, so a
    /// replacement whose prologue and schema are byte-identical is accepted —
    /// but such a file agrees with everything the snapshot has exposed, so
    /// nothing it already reported can be contradicted.
    ///
    /// Refresh never truncates, repairs, or acquires the writer lock.
    ///
    /// ```no_run
    /// # use acta::Reader;
    /// let mut reader = Reader::open("data.acta")?;
    /// let report = reader.refresh()?;
    /// println!("{} new block(s)", report.blocks_added());
    /// # Ok::<(), acta::Error>(())
    /// ```
    pub fn refresh(&mut self) -> Result<RefreshReport> {
        let previous_file_size = self.file_metadata.file_size;
        let boundary = self.file_metadata.last_good_offset;
        self.identity.expect_unchanged(&self.path)?;

        let mut file = File::open(&self.path)
            .map_err(|error| Error::io(error, None).with_context(ErrorContext::File))?;
        let observed_size = file
            .metadata()
            .map(|metadata| metadata.len())
            .map_err(|error| Error::io(error, None).with_context(ErrorContext::File))?;
        if observed_size < boundary {
            return Err(Error::file_truncated(
                format!(
                    "the file is {observed_size} bytes, below the committed boundary {boundary}"
                ),
                Some(observed_size),
            )
            .with_context(ErrorContext::File));
        }
        self.expect_own_committed_bytes(&mut file, observed_size)?;

        if observed_size == boundary {
            // No growth, or a Stage 8b repair has already removed the
            // uncommitted tail back to this boundary. Either way the file ends
            // exactly at the last complete frame, whose trailer the check
            // above has just confirmed is still this snapshot's own, so the
            // snapshot needs only its extent and tail state corrected; the
            // committed counts are already what a walk would report.
            self.file_metadata.file_size = observed_size;
            self.file_metadata.incomplete_tail = false;
            return Ok(RefreshReport {
                blocks_added: 0,
                rows_added: 0,
                previous_file_size,
                observed_file_size: observed_size,
                incomplete_tail: false,
                frame_bytes_scanned: 0,
            });
        }

        // A successful discovery pass stages new blocks here and only commits
        // them, together with the updated file metadata, once the whole walk
        // has validated. A failure anywhere leaves this reader untouched.
        let mut staged: Vec<BlockMetadata> = Vec::new();
        let mut total_rows = self.file_metadata.total_rows;
        // The handle the checks above used is the handle discovery reads, so
        // there is no second open and no window between the two.
        let mut scan = FileScan::from_file(file, self.limits)?;
        self.expect_own_file_header(&scan)?;
        let walk = scan.walk_data_frames_from(self.resume_point(), |frame, block| {
            let offset = Some(frame.frame_offset);
            let staged_count = u64::try_from(staged.len()).map_err(|_| {
                Error::resource_limit("block count does not fit the configured limit", offset)
                    .with_context(ErrorContext::File)
            })?;
            let total_count = self
                .file_metadata
                .block_count
                .checked_add(staged_count)
                .ok_or_else(|| {
                    Error::corruption("block count overflow", offset)
                        .with_context(ErrorContext::File)
                })?;
            if total_count >= self.limits.max_blocks() {
                return Err(Error::resource_limit(
                    format!(
                        "data block count exceeds the {}-block limit",
                        self.limits.max_blocks()
                    ),
                    offset,
                )
                .with_context(ErrorContext::File));
            }
            staged.try_reserve(1).map_err(|_| {
                Error::resource_limit("unable to reserve block metadata", offset)
                    .with_context(ErrorContext::File)
            })?;
            total_rows = total_rows.checked_add(block.row_count).ok_or_else(|| {
                Error::corruption("total row count overflow", offset)
                    .with_context(ErrorContext::File)
            })?;
            staged.push(BlockMetadata::new(
                frame.sequence,
                frame.frame_offset,
                frame.total_length,
                block.row_count,
                block.base_row_id,
                block
                    .primary_bounds
                    .map(|(minimum, maximum)| PrimaryBounds::new(minimum, maximum)),
                block.ts_sorted,
            ));
            Ok(())
        })?;

        let blocks_added = u64::try_from(staged.len()).map_err(|_| {
            Error::resource_limit("block count does not fit the configured limit", None)
                .with_context(ErrorContext::File)
        })?;
        let rows_added = total_rows - self.file_metadata.total_rows;
        let block_count = self
            .file_metadata
            .block_count
            .checked_add(blocks_added)
            .ok_or_else(|| {
                Error::corruption("block count overflow", Some(walk.last_good_offset))
                    .with_context(ErrorContext::File)
            })?;
        // The walk bounded itself by the extent this scan captured, so the
        // snapshot records that extent rather than the one stat'd before it:
        // a file that grew between the two is described by what was read.
        let file_size = scan.file_size();
        let prologue = scan.prologue();
        let schema_id = scan.schema().schema_id();
        let commit_anchor =
            frame::read_commit_trailer(&mut scan.into_file(), file_size, walk.last_good_offset)?;
        let file_metadata = FileMetadata {
            format_version: prologue.format_version,
            feature_flags: prologue.feature_flags,
            file_id: prologue.file_id,
            schema_id,
            file_size,
            last_good_offset: walk.last_good_offset,
            frame_count: walk.frame_count,
            block_count,
            total_rows,
            incomplete_tail: walk.incomplete_tail,
            next_sequence: walk.next_sequence,
            next_row_id: walk.next_row_id,
            commit_anchor,
        };

        // Everything that can fail has failed by now, so the snapshot changes
        // in one step: a caller never observes extended blocks described by
        // metadata that predates them.
        self.blocks.extend(staged);
        self.file_metadata = file_metadata;

        Ok(RefreshReport {
            blocks_added,
            rows_added,
            previous_file_size,
            observed_file_size: file_size,
            incomplete_tail: walk.incomplete_tail,
            frame_bytes_scanned: walk.frame_bytes_scanned,
        })
    }

    /// Refuse to continue unless the last frame this snapshot committed is
    /// still the frame at its committed boundary.
    ///
    /// File-system identity cannot see an in-place truncate-and-rewrite, which
    /// keeps the inode and replaces every byte, so identity alone would let a
    /// refresh resume a walk inside a stranger's file and append its frames to
    /// this snapshot. Sequence numbers cannot separate the two either: every
    /// Acta file numbers its data frames from one, and the deterministic
    /// writer gives them all the same zero file ID.
    ///
    /// The commit trailer can. It carries the frame's total length, sequence
    /// number, body CRC, its own CRC, and the commit magic, so two frames
    /// agree on all thirty-two bytes only if they are the same committed
    /// frame. Re-reading just those bytes costs one read rather than a pass
    /// over the file, which matters because a tail does this on every poll.
    ///
    /// The caller has already established that the file reaches the committed
    /// boundary, so a failure to read the trailer at all is a real I/O
    /// failure rather than evidence about which file this is, and is reported
    /// as one.
    fn expect_own_committed_bytes(&self, file: &mut File, file_size: u64) -> Result<()> {
        let anchor =
            frame::read_commit_trailer(file, file_size, self.file_metadata.last_good_offset)?;
        if anchor != self.file_metadata.commit_anchor {
            return Err(identity::replaced(&self.path));
        }
        Ok(())
    }

    /// Refuse to continue unless the prologue and schema a growing file
    /// presents are the ones this snapshot was built from.
    ///
    /// The committed-bytes check above already rejects a replacement, so this
    /// is defence in depth rather than the primary guard — but it is defence
    /// worth having, because without it a refresh would validate newly
    /// discovered frames against whatever schema the file now declares and
    /// then record that schema's identity while [`Self::schema`] still
    /// returned the old one, leaving a reader whose own two halves disagree.
    fn expect_own_file_header(&self, scan: &FileScan) -> Result<()> {
        let prologue = scan.prologue();
        let matches = prologue.format_version == self.file_metadata.format_version
            && prologue.feature_flags == self.file_metadata.feature_flags
            && prologue.file_id == self.file_metadata.file_id
            && scan.schema() == &*self.schema;
        if !matches {
            return Err(identity::replaced(&self.path));
        }
        Ok(())
    }

    /// Start a live tail over this reader.
    ///
    /// The tail begins after the blocks this snapshot already holds and polls
    /// the path for newly committed frames without reopening or rescanning the
    /// committed prefix. Existing blocks remain available through
    /// [`Self::scan`].
    ///
    /// A poll returns `None` when nothing is committed yet, which is never the
    /// end of the stream, so a tail is followed until the caller decides to
    /// stop rather than until it runs out — that is why this loop is not a
    /// `while let`.
    ///
    /// ```no_run
    /// # use acta::Reader;
    /// # fn keep_following() -> bool { true }
    /// # fn wait_a_while() {}
    /// let mut reader = Reader::open("data.acta")?;
    /// let mut tail = reader.tail().project(["value"])?;
    /// while keep_following() {
    ///     match tail.poll_next()? {
    ///         // one newly committed matching block per poll
    ///         Some(batch) => println!("{} new row(s)", batch.row_count()),
    ///         // nothing committed yet; the caller chooses how long to wait
    ///         None => wait_a_while(),
    ///     }
    /// }
    /// # Ok::<(), acta::Error>(())
    /// ```
    pub fn tail(&mut self) -> Tail<'_> {
        Tail::new(self)
    }

    /// The immutable schema reconstructed from the schema frame.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// File-level metadata captured at open time.
    pub fn file_metadata(&self) -> &FileMetadata {
        &self.file_metadata
    }

    /// Complete data blocks in file/sequence order.
    pub fn blocks(&self) -> &[BlockMetadata] {
        &self.blocks
    }

    /// The checked sum of rows in all complete blocks.
    pub fn total_rows(&self) -> u64 {
        self.file_metadata.total_rows()
    }

    /// The shared schema handle, so a scan can hand the same allocation to
    /// every block decode instead of cloning the schema per block.
    pub(crate) fn schema_handle(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn limits(&self) -> Limits {
        self.limits
    }

    /// The continuation state a refresh resumes from: this snapshot's last
    /// committed byte offset, the sequence number the next data frame must
    /// carry, the next implicit base row ID when the file enables them, and
    /// the frame count reached so far.
    ///
    /// Every field is carried forward from the walk that produced this
    /// snapshot rather than re-derived from the counters beside it. The two
    /// would agree today, but only because every frame after the schema frame
    /// is a data frame; section 14 reserves checkpoint frames, which would
    /// take a sequence number without contributing a block or a row.
    pub(crate) fn resume_point(&self) -> ResumePoint {
        ResumePoint {
            offset: self.file_metadata.last_good_offset,
            sequence: self.file_metadata.next_sequence,
            expected_base_row_id: self.file_metadata.next_row_id,
            frame_count: self.file_metadata.frame_count,
        }
    }

    /// Lazily decode the complete blocks in this snapshot in file order.
    ///
    /// The iterator reads one block only when its item is requested and
    /// yields [`Result<crate::RecordBatch>`] so decode and I/O failures are
    /// reported during iteration. The scan is isolated from later appends:
    /// it uses the file extent and committed block list captured by this
    /// reader when it was opened.
    ///
    /// By default every column is returned in schema order.
    /// [`Scan::project`] restricts and reorders the columns, and
    /// [`Scan::primary_range`] restricts the rows, pruning whole blocks by
    /// their stored bounds before reading them. Both are configured against
    /// the captured schema and fail immediately rather than during iteration.
    pub fn scan(&self) -> Scan<'_> {
        Scan::new(self)
    }

    /// Decode one complete data block in schema column order.
    ///
    /// The index is the zero-based position returned by [`Self::blocks`],
    /// which is also file and sequence order; it is not the frame sequence
    /// number, which starts at one. An index past the last block is a caller
    /// mistake rather than a defect in the file, so it is reported as
    /// [`ErrorKind::InvalidArgument`](crate::ErrorKind::InvalidArgument).
    ///
    /// Decoding uses the file extent captured by [`Self::open`], so a later
    /// append cannot become part of this snapshot.
    pub fn read_block(&self, index: usize) -> Result<crate::RecordBatch> {
        let mut file = File::open(&self.path)
            .map_err(|error| Error::io(error, None).with_context(ErrorContext::File))?;
        self.decode_block_at(&mut file, index)
    }

    /// Decode one complete data block using an already-open file handle.
    ///
    /// Shared by [`Self::read_block`], which opens the file per call, and
    /// [`Scan`], which opens it once and reuses the handle across the scan.
    fn decode_block_at(&self, file: &mut File, index: usize) -> Result<crate::RecordBatch> {
        let frame = self.frame_at(file, index)?;
        let layout = data_frame::read_layout(
            file,
            &frame,
            &self.schema,
            self.file_metadata.feature_flags,
            self.expected_base_row_id(index),
            self.limits,
        )?;
        decode::decode_block(
            file,
            self.file_metadata.file_size,
            &frame,
            &layout,
            Arc::clone(&self.schema),
            self.limits,
        )
    }

    /// Decode a selected set of columns from one complete data block. The
    /// scan module owns planning, pruning, and row filtering; this helper only
    /// performs the same frame/layout validation and physical stream decode as
    /// the full-column path, with a shared cumulative scan budget.
    pub(crate) fn decode_selected_block_at(
        &self,
        file: &mut File,
        index: usize,
        selection: &decode::Selection<'_>,
        scan_budget: &mut ScanBudget,
    ) -> Result<decode::DecodedBlock> {
        let frame = self.frame_at(file, index)?;
        // Reading the frame streamed its whole body to check the commit
        // trailer, and the layout read below re-reads the block header. Both
        // are bytes this scan spent on this block, whatever it goes on to
        // decode from it.
        scan_budget.record_bytes_read(frame.total_length)?;
        scan_budget.record_bytes_read(frame.header_length)?;

        let layout = data_frame::read_selected_layout(
            file,
            &frame,
            &self.schema,
            self.file_metadata.feature_flags,
            self.expected_base_row_id(index),
            self.limits,
            &self.selected_column_ids(selection)?,
        )?;
        decode::decode_selected_block(
            file,
            self.file_metadata.file_size,
            &frame,
            &layout,
            selection,
            Some(scan_budget),
            self.limits,
        )
    }

    /// The column IDs a selection reads, in no particular order.
    fn selected_column_ids(&self, selection: &decode::Selection<'_>) -> Result<Vec<u32>> {
        let mut ids = Vec::new();
        ids.try_reserve(selection.selected.len().saturating_add(1))
            .map_err(|_| Error::resource_limit("unable to allocate selected column IDs", None))?;
        let positions = selection.selected.iter().copied().chain(selection.primary);
        for position in positions {
            let column = self
                .schema
                .columns()
                .get(position)
                .ok_or_else(|| Error::internal("column index is outside the schema"))?;
            if !ids.contains(&column.id()) {
                ids.push(column.id());
            }
        }
        Ok(ids)
    }

    /// Read and validate the frame holding the block at `index`.
    fn frame_at(&self, file: &mut File, index: usize) -> Result<frame::FrameMetadata> {
        let block = self.blocks.get(index).ok_or_else(|| {
            Error::invalid_argument(format!(
                "block index {index} is past the {} blocks in this snapshot",
                self.blocks.len()
            ))
        })?;
        match frame::read_frame(
            file,
            self.file_metadata.file_size,
            block.file_offset(),
            block.sequence(),
            self.limits,
        )? {
            FrameRead::Complete(frame) => Ok(frame),
            FrameRead::IncompleteTail => Err(Error::io(
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "snapshot frame is no longer complete",
                ),
                Some(block.file_offset()),
            )
            .with_context(ErrorContext::File)),
        }
    }

    /// The base row ID the block at `index` must declare, when the file
    /// enables implicit row IDs.
    ///
    /// The discovery walk already proved this value is the checked sum of the
    /// row counts of every block before it, and refused the file otherwise, so
    /// the stored value *is* that sum. Re-deriving it here would add no
    /// guarantee and would make a scan quadratic in its block count — which a
    /// long-lived [`Tail`] feels directly, since it accumulates blocks for as
    /// long as it follows.
    fn expected_base_row_id(&self, index: usize) -> Option<u64> {
        self.blocks.get(index).and_then(BlockMetadata::base_row_id)
    }
}
