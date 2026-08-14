//! Writer lifecycle, buffering coordination, and sink handling.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;

use crate::batch::RecordBatch;
use crate::error::{Error, ErrorContext, Result};
use crate::format::constants::{
    FIRST_BASE_ROW_ID, FIRST_DATA_FRAME_SEQUENCE, ROW_IDS_FEATURE, UNAVAILABLE_BASE_ROW_ID,
};
use crate::format::scan::FileScan;
use crate::limits::Limits;
use crate::lock::acquire_writer_lock;
use crate::schema::Schema;

use super::api::{WriteAccounting, WriteSummary, WriterOptions, WriterStatistics};
use super::buffer::{BlockBuffer, block_size_with_statistics};
use super::encode::{build_data_frame_with_statistics, validate_encoding};
use super::framing::{build_prologue, build_schema_frame, write_initial};
use super::input::{validate_batch_input, validate_options, validate_schema};
use super::invalid_batch;

/// The file operations a [`Writer`] performs.
///
/// Naming them lets a test substitute a sink that fails on demand, which is the
/// only way to reach the poisoned state without an unwritable disk. Production
/// always writes to a [`File`].
pub(super) trait Sink: Write + Send + Sync {
    fn sync(&mut self) -> io::Result<()>;
}

impl Sink for File {
    fn sync(&mut self) -> io::Result<()> {
        self.sync_all()
    }
}

/// The continuation point shared by newly-created and reopened append
/// sessions. `file_length` is the last complete physical file boundary; a
/// failed write poisons the writer before this state can be advanced.
///
/// Every write goes to a handle opened in append mode, so this is accounting
/// and framing state rather than a seek target: an offset that fell behind the
/// real end of the file could misreport a length, but it could never place a
/// new frame over a committed one.
#[derive(Debug, Clone, Copy)]
pub(super) struct AppendState {
    pub(super) file_length: u64,
    pub(super) next_sequence: u64,
    pub(super) next_row_id: u64,
}

impl AppendState {
    pub(super) fn new(file_length: u64) -> Self {
        Self {
            file_length,
            next_sequence: FIRST_DATA_FRAME_SEQUENCE,
            next_row_id: FIRST_BASE_ROW_ID,
        }
    }
}

/// A synchronous buffered writer for Acta v0.2 append sessions.
///
/// [`Writer::create`] exclusively creates its path and [`Writer::open`] resumes
/// an existing complete file. They differ only in how the first
/// the append state is established: `create` writes a prologue and schema frame
/// and starts at sequence one, while `open` reconstructs the schema and
/// continuation point from the file. From there both hold the same locked
/// handle and enter the same append engine, which buffers appended nonempty
/// [`RecordBatch`] values until a row or byte target causes a data frame to be
/// published. `flush` publishes the final partial buffer, while `sync` and
/// `finish` additionally request durability. A partial write or durability
/// failure poisons the writer; dropping it performs no explicit I/O and may
/// discard only uncommitted buffered rows.
///
/// Every write is an operating-system append, so a writer can only ever extend
/// a file. No path in this type truncates, rewrites, or repairs committed
/// bytes.
///
/// ```
/// use std::sync::Arc;
/// use acta::{
///     Array, Column, LogicalType, PrimitiveArray, RecordBatch, Schema, Writer, WriterOptions,
/// };
///
/// let schema = Schema::new(
///     1,
///     vec![Column::new(1, "value", LogicalType::Int64, false)],
///     None,
/// );
/// let batch = RecordBatch::try_new(
///     Arc::new(schema.clone()),
///     vec![Array::Int64(PrimitiveArray::new(vec![1, 2, 3], None))],
///     3,
/// )?;
///
/// let path = std::env::temp_dir().join("acta-writer-doc-example.acta");
/// let _ = std::fs::remove_file(&path);
///
/// let mut writer = Writer::create(&path, schema, WriterOptions::default())?;
/// writer.append(batch)?;
/// let summary = writer.finish()?;
/// assert_eq!(summary.rows_written(), 3);
/// assert_eq!(summary.blocks_written(), 1);
///
/// let _ = std::fs::remove_file(&path);
/// # Ok::<(), acta::Error>(())
/// ```
#[must_use = "a Writer must be finished or explicitly dropped"]
pub struct Writer {
    sink: Box<dyn Sink>,
    schema: Arc<Schema>,
    options: WriterOptions,
    state: AppendState,
    buffer: BlockBuffer,
    published_rows: u64,
    published_bytes: u64,
    durable_rows: u64,
    durable_bytes: u64,
    blocks_written: u64,
    poisoned: bool,
}

impl Writer {
    /// Exclusively create `path`, write its prologue and schema frame, and
    /// return a writer ready for data blocks.
    ///
    /// The path must not already exist. This never opens, appends to, or
    /// overwrites an existing file, and it never removes one: only a file this
    /// call itself created can be cleaned up, and only when initializing it
    /// fails.
    ///
    /// # Writer exclusion
    ///
    /// The returned writer holds a cooperative exclusive lock on the file until
    /// it is finished or dropped. A second `acta` writer on the same file, in
    /// this process or another, fails immediately with
    /// [`ErrorKind::WriterLocked`](crate::ErrorKind::WriterLocked) instead of
    /// waiting. Readers never take the lock and are never blocked by it: it is
    /// advisory on Unix, and on Windows it covers a single byte past the end of
    /// the addressable file rather than the data.
    ///
    /// Its limits are worth stating plainly. Specification section 14 defers
    /// writer-locking protocols to a later format version, so this is a
    /// convention among `acta` writers rather than part of the format; no other
    /// Acta implementation participates in it, and no lock constrains a process
    /// that simply opens the path and writes. It is also unreliable on network
    /// filesystems, where advisory locks are emulated or absent. Targets that
    /// are neither Unix nor Windows have no lock primitive here at all, and
    /// both this method and [`Self::open`] refuse to construct a writer there.
    pub fn create<P: AsRef<Path>>(path: P, schema: Schema, options: WriterOptions) -> Result<Self> {
        validate_schema(&schema)?;
        validate_options(options)?;
        validate_encoding(&schema, options.encoding)?;
        let feature_flags = if options.row_ids { ROW_IDS_FEATURE } else { 0 };
        let prologue = build_prologue(feature_flags);
        let schema_frame = build_schema_frame(&schema)?;
        let file_length = u64::try_from(prologue.len() + schema_frame.len()).map_err(|_| {
            Error::resource_limit("initial Acta file length does not fit this platform", None)
                .with_context(ErrorContext::File)
        })?;

        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .open(path)
            .map_err(|error| Error::io(error, None).with_context(ErrorContext::File))?;
        // `create_new` succeeded, so every arm below owns a path this call
        // brought into existence a moment ago. No preexisting file can reach
        // one of them.
        if let Err(error) = acquire_writer_lock(&file) {
            // A writer that wins the lock in the window between `create_new`
            // and this call finds an empty file, fails its own discovery, and
            // releases. Both callers then fail and neither leaves a file
            // behind. Closing the window would need an atomic create-and-lock
            // that no portable API offers.
            drop(file);
            let _ = std::fs::remove_file(path);
            return Err(error);
        }
        if let Err(error) = write_initial(&mut file, &prologue, &schema_frame) {
            // Leaving the partial file behind would turn a transient failure
            // into a permanent one: creation is exclusive, so the natural retry
            // would find the path already taken.
            drop(file);
            let _ = std::fs::remove_file(path);
            return Err(error);
        }

        Ok(Self::from_append_session(
            Box::new(file),
            schema,
            options,
            AppendState::new(file_length),
        ))
    }

    /// Open an existing complete Acta file and continue appending to it.
    ///
    /// The file is authoritative. Its schema is reconstructed and returned by
    /// [`Self::schema`], and `options.row_ids` must agree with the row-ID
    /// feature its prologue declares, because reopening can neither enable nor
    /// disable that feature. Codec, encoding, statistics, and block-size
    /// options apply to the blocks this session writes and leave existing
    /// blocks and file-level features untouched.
    ///
    /// The path must already exist; this never creates one, and there is no
    /// open-or-create behavior.
    ///
    /// # What opening validates
    ///
    /// Opening performs structural and whole-frame validation in one linear
    /// pass: the prologue, the schema frame, and for every complete frame its
    /// prefix, header, trailer, and body CRC, along with sequence continuity
    /// and the implicit row-ID chain. It is *not*
    /// [`ValidationLevel::Full`](crate::ValidationLevel::Full) — no stream is
    /// decoded and no statistic is verified. Because every committed byte is
    /// checksummed, the cost is proportional to the size of the file, and a
    /// long series of small appends pays it once per session.
    ///
    /// A file ending inside an unfinished frame is refused with
    /// [`ErrorKind::IncompleteTail`](crate::ErrorKind::IncompleteTail) rather
    /// than truncated, and damage to a frame that is present in full stays
    /// [`ErrorKind::Corruption`](crate::ErrorKind::Corruption). Recovery is a
    /// separate, explicit operation. No failure path here writes, truncates, or
    /// repairs a single byte.
    ///
    /// # Writer exclusion
    ///
    /// This takes the same cooperative exclusive lock as [`Self::create`], with
    /// the same scope and the same limits; see that method. The lock is taken
    /// before any discovery, and the handle it is taken on is the handle this
    /// writer appends with, so there is no window between validating a path and
    /// writing to it.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use acta::{
    ///     Array, Column, LogicalType, PrimitiveArray, RecordBatch, Schema, Writer, WriterOptions,
    /// };
    ///
    /// let schema = Schema::new(
    ///     1,
    ///     vec![Column::new(1, "value", LogicalType::Int64, false)],
    ///     None,
    /// );
    /// let path = std::env::temp_dir().join("acta-writer-open-doc-example.acta");
    /// let _ = std::fs::remove_file(&path);
    /// Writer::create(&path, schema, WriterOptions::default())?.finish()?;
    ///
    /// let mut writer = Writer::open(&path, WriterOptions::default())?;
    /// // The reconstructed schema is what new batches must match.
    /// let schema = Arc::clone(writer.schema());
    /// let batch = RecordBatch::try_new(
    ///     schema,
    ///     vec![Array::Int64(PrimitiveArray::new(vec![1, 2, 3], None))],
    ///     3,
    /// )?;
    /// writer.append(batch)?;
    /// let summary = writer.finish()?;
    /// assert_eq!(summary.rows_written(), 3);
    /// assert_eq!(summary.last_sequence(), Some(1));
    ///
    /// let _ = std::fs::remove_file(&path);
    /// # Ok::<(), acta::Error>(())
    /// ```
    pub fn open<P: AsRef<Path>>(path: P, options: WriterOptions) -> Result<Self> {
        Self::open_internal(path.as_ref(), None, Limits::default(), options)
    }

    /// Open an existing complete Acta file for append and require its schema to
    /// equal `expected_schema` exactly.
    ///
    /// Equality covers the schema ID, the column count and order, and every
    /// column's ID, name, logical type and type parameters, and nullability,
    /// along with the primary-column selection. A difference in any of them
    /// fails with
    /// [`ErrorKind::SchemaMismatch`](crate::ErrorKind::SchemaMismatch) before
    /// the file is walked and before a writer exists, so no byte is written.
    ///
    /// This is a convenience over [`Self::open`] followed by comparing
    /// [`Self::schema`]; everything [`Self::open`] documents applies here.
    pub fn open_with_schema<P: AsRef<Path>>(
        path: P,
        expected_schema: &Schema,
        options: WriterOptions,
    ) -> Result<Self> {
        Self::open_internal(
            path.as_ref(),
            Some(expected_schema),
            Limits::default(),
            options,
        )
    }

    /// Open an existing complete Acta file for append under explicit [`Limits`].
    ///
    /// The bounds apply to the declared sizes this call reads out of the
    /// existing file, exactly as they do for
    /// [`Reader::open_with_limits`](crate::Reader::open_with_limits). Without
    /// this, a file whose frames exceed [`Limits::default`] is readable but not
    /// appendable. The blocks this writer goes on to produce are still bounded
    /// by the format defaults, so it cannot emit a frame that an ordinary
    /// reader would refuse.
    ///
    /// To combine a schema guard with custom limits, open with this method and
    /// compare [`Self::schema`] before appending.
    pub fn open_with_limits<P: AsRef<Path>>(
        path: P,
        limits: Limits,
        options: WriterOptions,
    ) -> Result<Self> {
        Self::open_internal(path.as_ref(), None, limits, options)
    }

    fn open_internal(
        path: &Path,
        expected_schema: Option<&Schema>,
        limits: Limits,
        options: WriterOptions,
    ) -> Result<Self> {
        validate_options(options)?;

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .map_err(|error| Error::io(error, None).with_context(ErrorContext::File))?;
        acquire_writer_lock(&file)?;

        // One handle for the whole operation: opened, locked, lent to discovery
        // below, and taken back to append with. It is never duplicated and the
        // path is never reopened, so discovery reads exactly the bytes the
        // appends will extend.
        let mut scan = FileScan::from_file(file, limits)?;

        // The prologue and schema frame answer every cheap guard, so all of
        // them run before the walk reads the rest of the file. A caller that
        // named the wrong schema or the wrong row-ID option learns so without
        // paying for a checksum pass over gigabytes.
        let schema = scan.schema().clone();
        if let Some(expected_schema) = expected_schema {
            if expected_schema != &schema {
                return Err(Error::schema_mismatch(
                    "the expected schema does not match the existing file",
                )
                .with_context(ErrorContext::Header));
            }
        }
        validate_schema(&schema)?;
        validate_encoding(&schema, options.encoding)?;
        let row_ids_enabled = scan.prologue().feature_flags & ROW_IDS_FEATURE != 0;
        if options.row_ids != row_ids_enabled {
            return Err(super::invalid_option(
                "WriterOptions::row_ids must match the existing file",
            ));
        }

        let walk = scan.walk_data_frames(|_frame, _block| Ok(()))?;
        if walk.incomplete_tail {
            return Err(Error::incomplete_tail(
                "cannot append to a file with an incomplete tail; recover it explicitly first",
                Some(walk.last_good_offset),
            )
            .with_context(ErrorContext::File));
        }

        let file_size = scan.file_size();
        let file = scan.into_file();
        let file_length = file
            .metadata()
            .map(|metadata| metadata.len())
            .map_err(|error| Error::io(error, None).with_context(ErrorContext::File))?;
        // A complete walk always ends exactly at the extent it captured, so
        // this compares that extent against the file as it stands now. A length
        // that moved means something outside this crate's lock wrote to the
        // file while it was being opened.
        if file_length != file_size {
            return Err(Error::io(
                io::Error::other("the file changed while it was being opened for append"),
                Some(file_length),
            )
            .with_context(ErrorContext::File));
        }
        Ok(Self::from_append_session(
            Box::new(file),
            schema,
            options,
            AppendState {
                file_length,
                next_sequence: walk.next_sequence,
                next_row_id: walk.next_row_id.unwrap_or(FIRST_BASE_ROW_ID),
            },
        ))
    }

    /// Enter the append engine at an arbitrary continuation point over a test
    /// sink, which is how a reopened session is reached without a real file.
    #[cfg(test)]
    pub(super) fn new(
        sink: Box<dyn Sink>,
        schema: Schema,
        options: WriterOptions,
        state: AppendState,
    ) -> Self {
        Self::from_append_session(sink, schema, options, state)
    }

    /// The schema every batch appended to this writer must match.
    ///
    /// For a writer from [`Self::open`] this is the schema reconstructed from
    /// the file, which is authoritative. Clone it to build compatible
    /// [`RecordBatch`] values:
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # let writer: acta::Writer = unimplemented!();
    /// let schema = Arc::clone(writer.schema());
    /// ```
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Enter the one append engine used by both public initialization paths.
    fn from_append_session(
        sink: Box<dyn Sink>,
        schema: Schema,
        options: WriterOptions,
        state: AppendState,
    ) -> Self {
        let buffer = BlockBuffer::new_with_statistics(schema.column_count(), options.statistics);
        Self {
            sink,
            schema: Arc::new(schema),
            options,
            state,
            buffer,
            // Every counter below describes this session alone and so starts at
            // zero even for a reopened file. The file-global continuation point
            // is `state`, which `open` reconstructs from the existing bytes.
            published_rows: 0,
            published_bytes: 0,
            durable_rows: 0,
            durable_bytes: 0,
            blocks_written: 0,
            poisoned: false,
        }
    }

    /// Append one nonempty batch to the bounded block buffer.
    ///
    /// The batch schema must exactly match [`Self::schema`], which is the
    /// schema passed to [`Self::create`] or, for a reopened writer, the one
    /// reconstructed from the file.
    /// Batch-shape and value errors are returned as
    /// [`crate::ErrorKind::InvalidArgument`] before anything is buffered, so a
    /// rejected batch leaves the writer exactly as it found it.
    ///
    /// Reaching a block target publishes a frame from inside this call, so an
    /// append is not all-or-nothing against I/O. A failed publication poisons
    /// the writer and reports the failure, but blocks published earlier in the
    /// same call stay on disk and stay counted: compare
    /// [`WriteAccounting::total_rows`] across the call to see how much of the
    /// batch was accepted. A poisoned writer refuses further work, so the
    /// unaccepted rows are not retried.
    pub fn append(&mut self, batch: RecordBatch) -> Result<()> {
        self.ensure_healthy()?;
        if batch.schema() != self.schema.as_ref() {
            return Err(invalid_batch(
                "the batch schema does not match the writer schema",
            ));
        }
        if batch.row_count() == 0 {
            return Err(invalid_batch("an Acta data block cannot be empty"));
        }

        // Validate the whole input before buffering any of it, so a batch this
        // writer rejects cannot leave a partially accepted prefix behind. Only
        // a failed publication can, which the documentation above states.
        validate_batch_input(&self.schema, &batch)?;

        // A batch that fits whole is moved into the buffer. Slicing it would
        // copy every value for nothing, and this is the ordinary case.
        if self.fitting_prefix(&batch, 0)? == batch.row_count() {
            self.buffer.push(&self.schema, batch);
            return self.publish_if_complete();
        }

        let total = batch.row_count();
        let mut consumed = 0;
        while consumed < total {
            let take = self.fitting_prefix(&batch, consumed)?;
            if take == 0 {
                self.publish_buffer()?;
                continue;
            }
            let end = consumed + take;
            self.buffer.push(&self.schema, batch.slice(consumed, end));
            consumed = end;
            self.publish_if_complete()?;
        }
        Ok(())
    }

    /// Publish buffered rows as a data frame and flush the operating-system
    /// file handle. This does not request durable storage.
    pub fn flush(&mut self) -> Result<()> {
        self.ensure_healthy()?;
        self.publish_buffer()?;
        if let Err(error) = self.sink.flush() {
            return self.poison_io(error, self.state.file_length);
        }
        Ok(())
    }

    /// Flush and request durable storage for all bytes written so far.
    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        if let Err(error) = self.sink.sync() {
            return self.poison_io(error, self.state.file_length);
        }
        self.durable_rows = self.published_rows;
        self.durable_bytes = self.published_bytes;
        Ok(())
    }

    /// Flush, synchronize, consume the writer, and return write accounting.
    ///
    /// This is the only way to end a writer without losing buffered rows,
    /// because `Drop` performs no I/O. A failure here therefore ends the file
    /// at its last published block and discards any rows still buffered along
    /// with the writer.
    pub fn finish(mut self) -> Result<WriteSummary> {
        self.sync()?;
        Ok(WriteSummary {
            rows_written: self.published_rows,
            blocks_written: self.blocks_written,
            bytes_written: self.state.file_length,
            last_sequence: (self.blocks_written != 0).then_some(self.state.next_sequence - 1),
            accounting: self.accounting(),
        })
    }

    /// The current buffered, published, durable, and total data accounting.
    pub fn accounting(&self) -> WriteAccounting {
        let buffered_rows = self.buffer.row_count();
        let buffered_bytes = self.buffer.frame_bytes();
        WriteAccounting {
            buffered_rows,
            buffered_bytes,
            published_rows: self.published_rows,
            published_bytes: self.published_bytes,
            durable_rows: self.durable_rows,
            durable_bytes: self.durable_bytes,
            total_rows: self.published_rows.saturating_add(buffered_rows),
            total_bytes: self.published_bytes.saturating_add(buffered_bytes),
        }
    }

    /// How many rows from `start` the block being assembled can still take.
    ///
    /// Zero means the buffer must be published first. A single row that no
    /// empty block could ever hold is reported as an error instead, so the
    /// caller cannot loop publishing an empty buffer.
    fn fitting_prefix(&self, batch: &RecordBatch, start: usize) -> Result<usize> {
        let capacity = self
            .options
            .row_block_target
            .saturating_sub(self.buffer.row_count());
        let max_rows =
            (batch.row_count() - start).min(usize::try_from(capacity).unwrap_or(usize::MAX));
        if max_rows == 0 {
            return Ok(0);
        }

        let mut footprints = self.buffer.footprints().to_vec();
        let mut rows = self.buffer.row_count();
        let mut accepted = 0;
        for offset in 0..max_rows {
            for (footprint, array) in footprints.iter_mut().zip(batch.columns()) {
                footprint.add_row(array, start + offset);
            }
            rows += 1;
            let size = block_size_with_statistics(
                &self.schema,
                &footprints,
                rows,
                self.options.statistics,
            );
            if !size.within_format_limits(rows) {
                break;
            }
            // A row too large for the byte target is still written, as a block
            // of its own, rather than split across blocks or refused.
            let unavoidably_oversize = self.buffer.is_empty() && offset == 0;
            if size.frame_bytes > self.options.byte_block_target && !unavoidably_oversize {
                break;
            }
            accepted = offset + 1;
        }

        if accepted == 0 && self.buffer.is_empty() {
            return Err(self.unwritable_row_error(batch, start));
        }
        Ok(accepted)
    }

    /// Explain why an empty block cannot hold even one row.
    ///
    /// The statistics policy is charged against the frame header budget, so a
    /// schema the encoding alone could write can become unwritable once a wide
    /// `fixed_binary` column has to carry a min/max pair twice its width.
    /// Naming that case separately keeps the caller from hunting for an
    /// oversize row that does not exist, and points at the option that caused
    /// it. This runs only on the failure path, where one more pricing pass
    /// costs nothing.
    fn unwritable_row_error(&self, batch: &RecordBatch, start: usize) -> Error {
        // The buffer is empty here, so its footprints are all zero and this
        // prices exactly the one row that was refused.
        let mut footprints = self.buffer.footprints().to_vec();
        for (footprint, array) in footprints.iter_mut().zip(batch.columns()) {
            footprint.add_row(array, start);
        }
        let without_statistics =
            block_size_with_statistics(&self.schema, &footprints, 1, WriterStatistics::None);
        if without_statistics.within_format_limits(1) {
            return invalid_batch(
                "the block statistics this writer would generate do not fit the frame \
                 header limit; this schema needs WriterStatistics::None",
            );
        }
        invalid_batch("a single row exceeds the largest block this format version allows")
    }

    /// Publish the buffer once it has reached either configured target.
    fn publish_if_complete(&mut self) -> Result<()> {
        if self.buffer.row_count() >= self.options.row_block_target
            || self.buffer.frame_bytes() >= self.options.byte_block_target
        {
            return self.publish_buffer();
        }
        Ok(())
    }

    /// Write the buffered rows as one data frame, and clear the buffer only
    /// once those bytes have reached the sink.
    fn publish_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let raw_bytes = self.buffer.frame_bytes();
        let row_count = self.buffer.row_count();
        let base_row_id = if self.options.row_ids {
            self.state.next_row_id
        } else {
            UNAVAILABLE_BASE_ROW_ID
        };
        let frame = build_data_frame_with_statistics(
            &self.schema,
            &self.buffer.rows(),
            self.options,
            base_row_id,
            self.state.next_sequence,
        )?;
        let frame_length = u64::try_from(frame.len()).map_err(|_| {
            Error::resource_limit("data frame length does not fit this platform", None)
                .with_context(ErrorContext::Frame {
                    sequence: self.state.next_sequence,
                })
        })?;
        if let Err(error) = self.sink.write_all(&frame) {
            return self.poison_io(error, self.state.file_length);
        }
        self.buffer.clear();
        self.commit(frame_length, raw_bytes, row_count)
    }

    /// Advance the accounting for a frame that is already on disk.
    ///
    /// Every counter here describes committed bytes, so any one of them
    /// overflowing leaves the writer unable to describe its own file. That is
    /// the same inability to continue safely that a partial write causes, and
    /// it is treated the same way. Keeping the four boundaries together also
    /// keeps a half-advanced writer from existing at all.
    fn commit(&mut self, frame_length: u64, raw_bytes: u64, row_count: u64) -> Result<()> {
        let advanced_row_id = if self.options.row_ids {
            self.state.next_row_id.checked_add(row_count)
        } else {
            Some(self.state.next_row_id)
        };
        let (
            Some(file_length),
            Some(blocks_written),
            Some(next_sequence),
            Some(next_row_id),
            Some(published_rows),
            Some(published_bytes),
        ) = (
            self.state.file_length.checked_add(frame_length),
            self.blocks_written.checked_add(1),
            self.state.next_sequence.checked_add(1),
            advanced_row_id,
            self.published_rows.checked_add(row_count),
            self.published_bytes.checked_add(raw_bytes),
        )
        else {
            self.poisoned = true;
            return Err(Error::resource_limit(
                "the writer can no longer account for the bytes it has written",
                Some(self.state.file_length),
            )
            .with_context(ErrorContext::File));
        };
        self.state.file_length = file_length;
        self.blocks_written = blocks_written;
        self.state.next_sequence = next_sequence;
        self.state.next_row_id = next_row_id;
        self.published_rows = published_rows;
        self.published_bytes = published_bytes;
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<()> {
        if self.poisoned {
            return Err(
                Error::poisoned("the writer cannot continue after a partial I/O failure")
                    .with_context(ErrorContext::File),
            );
        }
        Ok(())
    }

    fn poison_io<T>(&mut self, error: io::Error, offset: u64) -> Result<T> {
        self.poisoned = true;
        Err(Error::io(error, Some(offset)).with_context(ErrorContext::File))
    }
}

/// The sink is opaque, so the accounting is what a debug rendering can show.
impl fmt::Debug for Writer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Writer")
            .field("schema", &self.schema)
            .field("options", &self.options)
            .field("state", &self.state)
            .field("buffered_rows", &self.buffer.row_count())
            .field("buffered_bytes", &self.buffer.frame_bytes())
            .field("published_rows", &self.published_rows)
            .field("published_bytes", &self.published_bytes)
            .field("durable_rows", &self.durable_rows)
            .field("durable_bytes", &self.durable_bytes)
            .field("blocks_written", &self.blocks_written)
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}
