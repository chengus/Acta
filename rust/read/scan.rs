//! Lazy scan planning, pruning, and row filtering.

use std::fs::File;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::schema::{LogicalType, Schema};

use super::block::BlockMetadata;
use super::budget::{ScanBudget, ScanMetrics};
use super::reader::Reader;

/// A typed half-open range over the schema's primary column.
///
/// The two variants are separate types rather than one integer range because
/// the endpoints mean different things: a timestamp endpoint is a raw value in
/// the column's stored unit, and a `date32` endpoint is a signed day number.
/// A range is only accepted against a primary column of the matching kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrimaryRange {
    /// A range over a `timestamp64` primary column, in its stored unit.
    Timestamp {
        /// The first value the range includes.
        start: i64,
        /// The first value past the range.
        end: i64,
    },
    /// A range over a `date32` primary column, in signed days.
    Date32 {
        /// The first day the range includes.
        start: i32,
        /// The first day past the range.
        end: i32,
    },
}

impl PrimaryRange {
    /// Construct a range of raw values in the primary timestamp column's
    /// declared storage unit: `[start, end)`.
    pub fn timestamp(start: i64, end: i64) -> Self {
        Self::Timestamp { start, end }
    }

    /// Construct a half-open range of signed `date32` day values: `[start,
    /// end)`.
    pub fn date32(start: i32, end: i32) -> Self {
        Self::Date32 { start, end }
    }

    fn bounds(self) -> (i64, i64) {
        match self {
            Self::Timestamp { start, end } => (start, end),
            Self::Date32 { start, end } => (i64::from(start), i64::from(end)),
        }
    }
}

/// The projection and primary-range configuration shared by a snapshot
/// [`Scan`] and a live [`Tail`](crate::Tail).
///
/// Both consumers walk the same committed [`BlockMetadata`] list and decode
/// the same blocks, so projection, ordering, and pruning decisions live here
/// once instead of drifting apart in two iterators. The plan knows nothing
/// about which blocks exist; its caller decides that.
#[derive(Debug)]
pub(crate) struct ScanPlan {
    pub(crate) projection: Vec<usize>,
    pub(crate) output_schema: Arc<Schema>,
    pub(crate) range: Option<PrimaryRange>,
}

impl ScanPlan {
    /// A plan over every column of `schema`, in schema order.
    pub(crate) fn new(schema: &Schema) -> Self {
        let projection: Vec<usize> = (0..schema.column_count()).collect();
        let output_schema = projected_schema(schema, &projection);
        Self {
            projection,
            output_schema,
            range: None,
        }
    }

    /// Select columns by exact schema name, preserving the requested order.
    ///
    /// The requested order becomes the batch column order, and projected
    /// columns keep their schema IDs. An empty list is valid and yields
    /// zero-column batches that still carry their row counts. An unknown name,
    /// or the same name twice, is a caller mistake and fails here. Calling
    /// this again replaces the whole projection.
    pub(crate) fn project<I, S>(&mut self, schema: &Schema, columns: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut projection = Vec::new();
        for name in columns {
            let name = name.as_ref();
            let index = schema
                .columns()
                .iter()
                .position(|column| column.name() == name)
                .ok_or_else(|| {
                    Error::invalid_argument(format!("unknown projected column {name}"))
                })?;
            if projection.contains(&index) {
                return Err(Error::invalid_argument(format!(
                    "projected column {name} was requested more than once"
                )));
            }
            projection.push(index);
        }
        self.output_schema = projected_schema(schema, &projection);
        self.projection = projection;
        Ok(())
    }

    /// Configure a typed half-open primary range, `[start, end)`.
    ///
    /// A missing primary column, a range whose type does not match it, and a
    /// start greater than its end are all rejected here, against the captured
    /// schema, before any block is read. An empty range where `start == end`
    /// is legal and returns no rows without decoding anything. Calling this
    /// again replaces the range.
    pub(crate) fn primary_range(&mut self, schema: &Schema, range: PrimaryRange) -> Result<()> {
        let (start, end) = range.bounds();
        if start > end {
            return Err(Error::invalid_argument(
                "primary range start must not be greater than its end",
            ));
        }
        let primary = schema.primary_column().ok_or_else(|| {
            Error::invalid_argument("primary ranges require a schema primary column")
        })?;
        let compatible = matches!(
            (range, primary.logical_type()),
            (
                PrimaryRange::Timestamp { .. },
                LogicalType::Timestamp { .. }
            ) | (PrimaryRange::Date32 { .. }, LogicalType::Date32)
        );
        if !compatible {
            return Err(Error::invalid_argument(format!(
                "primary range type does not match primary column {}",
                primary.name()
            )));
        }
        self.range = Some(range);
        Ok(())
    }

    /// The one column a consumer may decode without projecting it: the
    /// primary, when a range needs its values to filter rows.
    fn internal_primary(&self, schema: &Schema) -> Option<usize> {
        super::decode::primary_index(schema)
            .filter(|index| self.range.is_some() || self.projection.contains(index))
    }

    /// The decode selection this plan requests for one block.
    pub(crate) fn selection<'a>(
        &'a self,
        source_schema: &'a Arc<Schema>,
    ) -> super::decode::Selection<'a> {
        super::decode::Selection {
            source_schema: Arc::clone(source_schema),
            output_schema: Arc::clone(&self.output_schema),
            selected: &self.projection,
            primary: self.internal_primary(source_schema),
        }
    }

    /// Whether a block's stored primary bounds exclude every range value.
    pub(crate) fn should_prune(&self, block: &BlockMetadata) -> bool {
        let Some(range) = self.range else {
            return false;
        };
        let (start, end) = range.bounds();
        if start == end {
            return true;
        }
        let Some(bounds) = block.primary_bounds() else {
            return false;
        };
        bounds.max() < start || bounds.min() >= end
    }

    /// Keep the rows of one decoded block that the range covers.
    ///
    /// The choice between a binary search and a linear pass is made from what
    /// the decode established about the values in hand, not from block
    /// metadata captured earlier. The two are separate reads of the file, and
    /// a binary search over values that are not really ordered would return
    /// arbitrary boundaries rather than an error.
    pub(crate) fn filter_batch(
        &self,
        primary_sorted: bool,
        batch: crate::RecordBatch,
        primary_values: &[i64],
        range: PrimaryRange,
    ) -> Result<Option<crate::RecordBatch>> {
        let (start, end) = range.bounds();
        if start == end {
            return Ok(None);
        }
        if primary_sorted {
            let first = primary_values.partition_point(|value| *value < start);
            let last = primary_values.partition_point(|value| *value < end);
            if first == last {
                return Ok(None);
            }
            return Ok(Some(batch.slice(first, last)));
        }

        let mut indices = Vec::new();
        indices.try_reserve(primary_values.len()).map_err(|_| {
            Error::resource_limit("unable to allocate unsorted range selection", None)
        })?;
        indices.extend(
            primary_values
                .iter()
                .enumerate()
                .filter_map(|(index, value)| (*value >= start && *value < end).then_some(index)),
        );
        if indices.is_empty() {
            return Ok(None);
        }
        Ok(Some(batch.take(&indices)))
    }
}

/// A lazy, sequential scan over the committed blocks in a reader snapshot.
///
/// Configuration validates only the captured schema. Frame bodies and streams
/// are not read until iteration reaches a block that survives planning.
///
/// # Errors during iteration
///
/// A failure is yielded as an item rather than ending the scan, and the scan
/// then continues with the next block: one damaged block does not hide the
/// blocks after it. Two consequences are worth planning for. A caller that
/// wants to stop at the first failure has to stop itself. And a scan that has
/// exhausted a cumulative allowance from [`Limits`](crate::Limits) fails every
/// remaining candidate block in turn, because the allowance stays spent, so
/// such a scan yields an error per remaining block rather than one error.
///
/// [`Self::metrics`] stays readable across all of this, and reports the work
/// done up to that point.
#[derive(Debug)]
pub struct Scan<'reader> {
    pub(crate) reader: &'reader Reader,
    pub(crate) next_index: usize,
    pub(crate) file: Option<File>,
    pub(crate) plan: ScanPlan,
    pub(crate) budget: ScanBudget,
}

impl<'reader> Scan<'reader> {
    pub(crate) fn new(reader: &'reader Reader) -> Self {
        Self {
            reader,
            next_index: 0,
            file: None,
            plan: ScanPlan::new(reader.schema()),
            budget: ScanBudget::new(
                reader.limits().max_rows_per_scan(),
                reader.limits().max_decoded_scan_bytes(),
            ),
        }
    }

    /// Select columns by exact schema name, preserving the requested order.
    ///
    /// The requested order becomes the batch column order, and projected
    /// columns keep their schema IDs. An empty list is valid and yields
    /// zero-column batches that still carry their row counts. An unknown name,
    /// or the same name twice, is a caller mistake and fails here rather than
    /// during iteration. Calling this again replaces the whole projection.
    ///
    /// ```no_run
    /// # use acta::Reader;
    /// let reader = Reader::open("data.acta")?;
    /// for batch in reader.scan().project(["timestamp", "value"])? {
    ///     let batch = batch?;
    ///     assert_eq!(batch.schema().column_count(), 2);
    /// }
    /// # Ok::<(), acta::Error>(())
    /// ```
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
    /// A missing primary column, a range whose type does not match it, and a
    /// start greater than its end are all rejected here, against the captured
    /// schema, before any block is read. An empty range where `start == end`
    /// is legal and returns no rows without decoding anything. The primary
    /// column is decoded to filter rows whether or not it is projected, but it
    /// is only returned when it is. Calling this again replaces the range.
    ///
    /// ```no_run
    /// # use acta::{PrimaryRange, Reader};
    /// let reader = Reader::open("data.acta")?;
    /// let scan = reader
    ///     .scan()
    ///     .project(["value"])?
    ///     .primary_range(PrimaryRange::timestamp(1_700_000_000_000_000, 1_700_003_600_000_000))?;
    /// # Ok::<(), acta::Error>(())
    /// ```
    pub fn primary_range(mut self, range: PrimaryRange) -> Result<Self> {
        self.plan.primary_range(self.reader.schema(), range)?;
        Ok(self)
    }

    /// Keep committed block order and row order within each block explicit.
    /// This is currently the scan's only ordering mode.
    pub fn file_order(self) -> Self {
        self
    }

    /// The number of blocks left that pruning has not already excluded.
    ///
    /// This is an upper bound on the items the scan can still yield, not a
    /// count of them: a block whose bounds overlap the range may still hold no
    /// matching row, and is then skipped without an item. That is also why
    /// [`Scan`] is not an
    /// [`ExactSizeIterator`](std::iter::ExactSizeIterator) and why this is not
    /// called `len`.
    pub fn remaining_candidate_blocks(&self) -> usize {
        self.remaining_candidates()
    }

    /// Return aggregate planning, stream, byte, and row counters collected so
    /// far. Counters remain readable after an iterator has yielded an item
    /// error; later blocks remain independently iterable.
    pub fn metrics(&self) -> ScanMetrics {
        self.budget.metrics()
    }

    fn remaining_candidates(&self) -> usize {
        self.reader.blocks()[self.next_index..]
            .iter()
            .filter(|block| !self.plan.should_prune(block))
            .count()
    }
}

impl Iterator for Scan<'_> {
    type Item = Result<crate::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_index < self.reader.blocks().len() {
            let index = self.next_index;
            self.next_index += 1;
            let block = &self.reader.blocks()[index];
            let pruned = self.plan.should_prune(block);
            if let Err(error) = self.budget.record_block(pruned) {
                return Some(Err(error));
            }
            if pruned {
                continue;
            }

            // Built from the fields it needs rather than from `&self`, so the
            // file handle and the budget below stay independently borrowable.
            let selection = self.plan.selection(self.reader.schema_handle());

            let file = match &mut self.file {
                Some(file) => file,
                None => match File::open(self.reader.path()) {
                    Ok(file) => self.file.insert(file),
                    Err(error) => {
                        return Some(Err(
                            Error::io(error, None).with_context(crate::ErrorContext::File)
                        ));
                    }
                },
            };

            if let Err(error) = self.budget.charge_rows(block.row_count()) {
                return Some(Err(error));
            }

            let decoded =
                self.reader
                    .decode_selected_block_at(file, index, &selection, &mut self.budget);
            match decoded {
                Err(error) => return Some(Err(error)),
                Ok(decoded) => {
                    let super::decode::DecodedBlock {
                        batch,
                        primary_values,
                        primary_sorted,
                    } = decoded;
                    let Some(range) = self.plan.range else {
                        if let Err(error) = self.budget.record_rows(batch.row_count()) {
                            return Some(Err(error));
                        }
                        return Some(Ok(batch));
                    };
                    let Some(primary_values) = primary_values else {
                        return Some(Err(Error::internal(
                            "range scan did not decode its primary column",
                        )));
                    };
                    match self
                        .plan
                        .filter_batch(primary_sorted, batch, &primary_values, range)
                    {
                        Ok(Some(batch)) => {
                            if let Err(error) = self.budget.record_rows(batch.row_count()) {
                                return Some(Err(error));
                            }
                            return Some(Ok(batch));
                        }
                        Ok(None) => continue,
                        Err(error) => return Some(Err(error)),
                    }
                }
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.remaining_candidates()))
    }
}

impl std::iter::FusedIterator for Scan<'_> {}

fn projected_schema(schema: &Schema, projection: &[usize]) -> Arc<Schema> {
    let columns = projection
        .iter()
        .map(|&index| schema.columns()[index].clone())
        .collect();
    let primary = schema.primary_column_id().filter(|id| {
        projection
            .iter()
            .any(|&index| schema.columns()[index].id() == *id)
    });
    Arc::new(Schema::new(schema.schema_id(), columns, primary))
}
