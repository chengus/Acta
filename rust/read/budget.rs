//! The memory allowance one block decode spends.

use crate::error::{Error, ErrorContext, Result};

/// A running allowance of decoded bytes, shared by every column of one block.
///
/// Every declaration a block makes about itself is small: a row count, an
/// element count, a stream length. Decoding multiplies them, and a decoded
/// block holds all of its columns at once, so bounding each declaration alone
/// leaves the total unbounded. Each buffer a decode materializes is charged
/// here before it is allocated.
///
/// The allowance is spent, not borrowed: releasing a buffer does not refund
/// it. That makes one decode's cost a single number the caller can reason
/// about instead of a peak that depends on drop order.
#[derive(Debug)]
pub(crate) struct Budget<'scan> {
    remaining: u64,
    scan: Option<&'scan mut ScanBudget>,
}

impl<'scan> Budget<'scan> {
    pub(crate) fn new(allowance: u64) -> Self {
        Self {
            remaining: allowance,
            scan: None,
        }
    }

    pub(crate) fn with_scan(mut self, scan: &'scan mut ScanBudget) -> Self {
        self.scan = Some(scan);
        self
    }

    /// Charge for `count` values of `width` bytes each.
    pub(crate) fn charge_elements(&mut self, count: usize, width: usize) -> Result<()> {
        let bytes = count.checked_mul(width).ok_or_else(exceeded)?;
        self.charge(bytes)
    }

    /// Charge for one buffer of `bytes` bytes.
    pub(crate) fn charge(&mut self, bytes: usize) -> Result<()> {
        let bytes = u64::try_from(bytes).map_err(|_| exceeded())?;
        if self.remaining < bytes {
            return Err(exceeded());
        }
        // The scan allowance is only present on the lazy scan path.
        if let Some(scan) = self.scan.as_deref_mut() {
            if scan.remaining_bytes < bytes {
                return Err(scan_exceeded());
            }
        }
        self.remaining -= bytes;
        if let Some(scan) = self.scan.as_deref_mut() {
            scan.remaining_bytes -= bytes;
        }
        Ok(())
    }

    /// Record one decoded stream of `bytes` stored bytes.
    pub(crate) fn record_stream(&mut self, bytes: u64) -> Result<()> {
        if let Some(scan) = self.scan.as_deref_mut() {
            scan.record_stream(bytes)?;
        }
        Ok(())
    }

    /// Record bytes this scan read from the file that are not stream payloads:
    /// frame envelopes, block headers, and statistics.
    pub(crate) fn record_bytes_read(&mut self, bytes: u64) -> Result<()> {
        if let Some(scan) = self.scan.as_deref_mut() {
            scan.record_bytes_read(bytes)?;
        }
        Ok(())
    }
}

/// Aggregate work counters for one scan. These expose only logical scan
/// accounting; physical descriptor tables and decoder state remain private.
///
/// The two byte counters answer different questions and are deliberately kept
/// apart. [`Self::stream_bytes_decoded`] is the work projection removes: the
/// stored size of the streams the scan actually decoded. [`Self::bytes_read`]
/// is what the scan cost the file system, and it is larger, because reading a
/// block verifies the whole frame body against its commit trailer before any
/// stream is decoded. Projection narrows the first number, not the second.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanMetrics {
    blocks_considered: u64,
    blocks_pruned: u64,
    streams_decoded: u64,
    stream_bytes_decoded: u64,
    bytes_read: u64,
    rows_returned: u64,
}

impl ScanMetrics {
    /// Number of committed blocks examined by the scan planner.
    pub fn blocks_considered(&self) -> u64 {
        self.blocks_considered
    }

    /// Number of blocks skipped using primary bounds before stream decoding.
    pub fn blocks_pruned(&self) -> u64 {
        self.blocks_pruned
    }

    /// Number of physical streams successfully decoded.
    pub fn streams_decoded(&self) -> u64 {
        self.streams_decoded
    }

    /// Stored byte count of the streams this scan decoded.
    ///
    /// This is the measure projection and pruning reduce. It excludes every
    /// byte read for framing, block headers, and statistics; for the total
    /// cost of the scan use [`Self::bytes_read`].
    pub fn stream_bytes_decoded(&self) -> u64 {
        self.stream_bytes_decoded
    }

    /// Total bytes this scan read from the file.
    ///
    /// This counts every byte of every candidate block the scan touched: the
    /// frame envelope streamed to verify the body checksum, the block header,
    /// the decoded streams, and any statistics that were verified. Pruned
    /// blocks contribute nothing. It is a byte count taken from the lengths
    /// the reader actually reads, not an estimate from the file size.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Number of rows returned in yielded batches.
    pub fn rows_returned(&self) -> u64 {
        self.rows_returned
    }
}

/// The cumulative allowances owned by one lazy scan.
#[derive(Debug)]
pub(crate) struct ScanBudget {
    remaining_rows: u64,
    remaining_bytes: u64,
    metrics: ScanMetrics,
}

impl ScanBudget {
    pub(crate) fn new(rows: u64, bytes: u64) -> Self {
        Self {
            remaining_rows: rows,
            remaining_bytes: bytes,
            metrics: ScanMetrics::default(),
        }
    }

    pub(crate) fn charge_rows(&mut self, rows: u64) -> Result<()> {
        self.remaining_rows = self
            .remaining_rows
            .checked_sub(rows)
            .ok_or_else(scan_rows_exceeded)?;
        Ok(())
    }

    pub(crate) fn record_block(&mut self, pruned: bool) -> Result<()> {
        self.metrics.blocks_considered = self
            .metrics
            .blocks_considered
            .checked_add(1)
            .ok_or_else(metrics_overflow)?;
        if pruned {
            self.metrics.blocks_pruned = self
                .metrics
                .blocks_pruned
                .checked_add(1)
                .ok_or_else(metrics_overflow)?;
        }
        Ok(())
    }

    pub(crate) fn record_stream(&mut self, bytes: u64) -> Result<()> {
        self.metrics.streams_decoded = self
            .metrics
            .streams_decoded
            .checked_add(1)
            .ok_or_else(metrics_overflow)?;
        self.metrics.stream_bytes_decoded = self
            .metrics
            .stream_bytes_decoded
            .checked_add(bytes)
            .ok_or_else(metrics_overflow)?;
        self.record_bytes_read(bytes)
    }

    pub(crate) fn record_bytes_read(&mut self, bytes: u64) -> Result<()> {
        self.metrics.bytes_read = self
            .metrics
            .bytes_read
            .checked_add(bytes)
            .ok_or_else(metrics_overflow)?;
        Ok(())
    }

    pub(crate) fn record_rows(&mut self, rows: usize) -> Result<()> {
        let rows = u64::try_from(rows).map_err(|_| metrics_overflow())?;
        self.metrics.rows_returned = self
            .metrics
            .rows_returned
            .checked_add(rows)
            .ok_or_else(metrics_overflow)?;
        Ok(())
    }

    pub(crate) fn metrics(&self) -> ScanMetrics {
        self.metrics
    }
}

fn exceeded() -> Error {
    Error::resource_limit(
        "decoding this block exceeds the configured decoded-byte limit",
        None,
    )
    .with_context(ErrorContext::Payload)
}

fn scan_exceeded() -> Error {
    Error::resource_limit(
        "the scan exceeds the configured cumulative decoded-byte limit",
        None,
    )
    .with_context(ErrorContext::Payload)
}

fn scan_rows_exceeded() -> Error {
    Error::resource_limit("the scan exceeds the configured cumulative row limit", None)
        .with_context(ErrorContext::Payload)
}

fn metrics_overflow() -> Error {
    Error::resource_limit("scan metrics arithmetic overflow", None)
}

#[cfg(test)]
mod tests {
    use super::Budget;
    use crate::ErrorKind;

    #[test]
    fn charges_accumulate_across_calls() {
        let mut budget = Budget::new(16);
        budget.charge(8).expect("the first charge fits");

        assert_eq!(
            budget
                .charge(9)
                .expect_err("the second charge exceeds the allowance")
                .kind(),
            ErrorKind::ResourceLimit
        );
    }

    #[test]
    fn an_element_count_that_overflows_is_a_resource_limit() {
        let mut budget = Budget::new(u64::MAX);

        assert_eq!(
            budget
                .charge_elements(usize::MAX, 16)
                .expect_err("the element byte count overflows")
                .kind(),
            ErrorKind::ResourceLimit
        );
    }
}
