//! Live, synchronous tail following over a refreshed reader.

use std::fs::File;

use crate::error::{Error, ErrorContext, Result};

use super::budget::{ScanBudget, ScanMetrics};
use super::reader::Reader;
use super::scan::ScanPlan;

/// A live tail over the frames appended after a reader's snapshot.
///
/// A tail is the mutable counterpart of a snapshot [`Scan`](crate::Scan): it
/// begins after the blocks the reader already holds, and
/// [`poll_next`](Self::poll_next) refreshes that reader and decodes one newly
/// committed matching block per call. Polling is synchronous and never blocks:
/// it sleeps on nothing, spawns no thread, and owns no timer, so a call that
/// has nothing new returns `Ok(None)` immediately and the
/// caller decides how long to wait before polling again. `None` means "pending
/// now", never a permanent end of stream, so a tail does not implement
/// [`Iterator`] or [`FusedIterator`](std::iter::FusedIterator).
///
/// `None` means only that, too. A block that prunes on its bounds or filters
/// down to no rows is consumed inside the poll that reaches it, and the poll
/// carries on to the next committed block, so a caller that waits on `None`
/// is never waiting on data that has already arrived. Neither is a batch of
/// zero rows ever reported in place of one.
///
/// A physically incomplete frame is never exposed: the tail leaves it
/// undispatched and reports it through `incomplete_tail`, and a later poll
/// sees it once a writer completes it.
///
/// Projection, reordered projection, empty projection, primary ranges, file
/// order, and the treatment of a per-block decode error all match
/// [`Scan`](crate::Scan), and the tail's cumulative scan limits apply across
/// its whole lifetime rather than per poll. Cancellation is dropping the tail
/// or stopping the polls; dropping performs no I/O and leaks no thread or
/// handle.
///
/// The tail borrows its reader, so the reader cannot be refreshed through
/// [`Reader::refresh`] while one of its tails exists. That is the same
/// snapshot safety boundary [`Scan`](crate::Scan) already draws, kept on
/// purpose rather than bypassed with interior mutation.
#[derive(Debug)]
pub struct Tail<'reader> {
    reader: &'reader mut Reader,
    plan: ScanPlan,
    next_block: usize,
    file: Option<File>,
    budget: ScanBudget,
}

impl<'reader> Tail<'reader> {
    /// Begin tailing after the reader's current committed blocks.
    pub(crate) fn new(reader: &'reader mut Reader) -> Self {
        let limits = reader.limits();
        Self {
            next_block: reader.blocks().len(),
            plan: ScanPlan::new(reader.schema()),
            reader,
            file: None,
            budget: ScanBudget::new(limits.max_rows_per_scan(), limits.max_decoded_scan_bytes()),
        }
    }

    /// Select columns by exact schema name, preserving the requested order.
    ///
    /// This has exactly [`Scan::project`](crate::Scan::project)'s semantics:
    /// the requested order becomes the batch column order, an empty list
    /// yields zero-column batches that still carry their row counts, an
    /// unknown or repeated name fails here rather than during polling, and
    /// calling this again replaces the whole projection.
    pub fn project<I, S>(mut self, columns: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.plan.project(self.reader.schema(), columns)?;
        Ok(self)
    }

    /// Configure a typed half-open primary range, `[start, end)`.
    ///
    /// This has exactly [`Scan::primary_range`](crate::Scan::primary_range)'s
    /// semantics: the range type must match the schema's primary column and an
    /// empty range is legal. Blocks whose stored bounds cannot intersect the
    /// range are polled through without a batch, and no global primary
    /// ordering is ever inferred: a future block may overlap this range even
    /// when none of the current blocks do.
    pub fn primary_range(mut self, range: crate::PrimaryRange) -> Result<Self> {
        self.plan.primary_range(self.reader.schema(), range)?;
        Ok(self)
    }

    /// Keep committed block order and row order within each block explicit.
    /// This is currently the tail's only ordering mode, matching
    /// [`Scan::file_order`](crate::Scan::file_order).
    pub fn file_order(self) -> Self {
        self
    }

    /// Synchronously discover and decode the next newly committed matching
    /// block, without sleeping or blocking.
    ///
    /// A newly committed block is returned as `Ok(Some(batch))`. `Ok(None)`
    /// means the file currently holds no committed block this tail has not
    /// already dealt with — "pending now", not the end of the stream, since
    /// polling again later may yield data. Blocks that pruning or range
    /// filtering removes are consumed on the way, so `None` is never returned
    /// with matching work still queued and a caller may safely wait on it.
    ///
    /// A refresh or decode failure is returned as [`Err`]. A per-block decode
    /// failure consumes that block and the tail continues with the next
    /// committed block on a later poll, exactly as a snapshot
    /// [`Scan`](crate::Scan) yields the damaged block as one error and keeps
    /// going. A refresh failure leaves the tail's position untouched and is
    /// retried by the next poll.
    ///
    /// Until a poll returns a decoded block it keeps refreshing the reader,
    /// which keeps the file extent and commit boundary current. Once
    /// undispatched blocks exist, polling decodes them first and only
    /// refreshes again when they are exhausted, so no block is skipped and
    /// none is decoded twice.
    pub fn poll_next(&mut self) -> Result<Option<crate::RecordBatch>> {
        loop {
            // Drain, rather than return, the blocks that yield nothing: a
            // pruned or fully filtered block is work this poll completed, and
            // reporting it as `None` would tell the caller to wait for data
            // that is already on disk.
            while self.next_block < self.reader.blocks().len() {
                if let Some(batch) = self.decode_next()? {
                    return Ok(Some(batch));
                }
            }
            let report = self.reader.refresh()?;
            // Refresh reads and checksums every frame it discovers, which is
            // this tail's I/O whether or not the block is later decoded.
            self.budget
                .record_bytes_read(report.frame_bytes_scanned())?;
            if report.blocks_added() == 0 {
                return Ok(None);
            }
        }
    }

    /// Return aggregate planning, stream, byte, and row counters collected
    /// across this tail's lifetime.
    ///
    /// [`ScanMetrics::bytes_read`] covers both halves of a tail's work: the
    /// frames each refresh streamed to verify their commit trailers, and the
    /// bytes the decodes then read. A tail therefore reports more bytes than a
    /// [`Scan`](crate::Scan) over the same blocks, because a scan never has to
    /// discover them. The row and decoded-byte allowances from
    /// [`Limits`](crate::Limits) still bound decoding only; discovery is
    /// bounded by the file, not by the scan.
    pub fn metrics(&self) -> ScanMetrics {
        self.budget.metrics()
    }

    /// Decode the next undispatched block, or report that it yielded nothing.
    ///
    /// `Ok(None)` here means only "this block produced no batch"; deciding
    /// what that means for the caller is [`Self::poll_next`]'s job.
    fn decode_next(&mut self) -> Result<Option<crate::RecordBatch>> {
        // The block is consumed before it is decoded, exactly as a snapshot
        // [`Scan`](crate::Scan) consumes each block before reading it, so a
        // decode failure is yielded as one error for that block and the tail
        // continues with the next committed block on a later poll.
        let index = self.next_block;
        self.next_block += 1;
        let block = self.reader.blocks()[index].clone();
        let pruned = self.plan.should_prune(&block);
        self.budget.record_block(pruned)?;
        if pruned {
            return Ok(None);
        }

        // Built from the fields it needs rather than from `&self`, so the file
        // handle and the budget below stay independently borrowable.
        let selection = self.plan.selection(self.reader.schema_handle());

        let file = match &mut self.file {
            Some(file) => file,
            None => match File::open(self.reader.path()) {
                Ok(file) => self.file.insert(file),
                Err(error) => {
                    return Err(Error::io(error, None).with_context(ErrorContext::File));
                }
            },
        };

        self.budget.charge_rows(block.row_count())?;
        let decoded =
            self.reader
                .decode_selected_block_at(file, index, &selection, &mut self.budget)?;
        let super::decode::DecodedBlock {
            batch,
            primary_values,
            primary_sorted,
        } = decoded;

        let Some(range) = self.plan.range else {
            self.budget.record_rows(batch.row_count())?;
            return Ok(Some(batch));
        };
        let Some(primary_values) = primary_values else {
            return Err(Error::internal(
                "range tail did not decode its primary column",
            ));
        };
        match self
            .plan
            .filter_batch(primary_sorted, batch, &primary_values, range)?
        {
            Some(batch) => {
                self.budget.record_rows(batch.row_count())?;
                Ok(Some(batch))
            }
            None => Ok(None),
        }
    }
}
