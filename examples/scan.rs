//! Project and filter a snapshot with a lazy scan.
//!
//! Run with:
//!
//!     cargo run --example scan -- spec/v0.2/fixtures/nyc_taxi_3_rows/nyc_taxi_3_rows.acta
//!
//! The example works with any Acta file. It projects the first two columns,
//! and, when the file has a primary timestamp/date column, scans the complete
//! range described by the file's block metadata.

use std::path::PathBuf;

use acta::{LogicalType, PrimaryRange, Reader, Result};

fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from("spec/v0.2/fixtures/nyc_taxi_3_rows/nyc_taxi_3_rows.acta")
        });
    let reader = Reader::open(&path)?;

    println!("{}", path.display());
    println!("schema {}:", reader.schema().schema_id());
    for column in reader.schema().columns() {
        println!("  {}: {:?}", column.name(), column.logical_type());
    }

    let projection: Vec<&str> = reader
        .schema()
        .columns()
        .iter()
        .take(2)
        .map(|column| column.name())
        .collect();
    let mut scan = reader.scan().project(projection.iter().copied())?;

    if let Some(range) = full_primary_range(&reader) {
        scan = scan.primary_range(range)?.file_order();
        println!("using the file's primary range");
    }

    let mut rows = 0;
    for batch in scan.by_ref() {
        let batch = batch?;
        println!(
            "batch: {} row(s), {} projected column(s)",
            batch.row_count(),
            batch.schema().column_count()
        );
        for row in 0..batch.row_count() {
            let values: Vec<_> = batch
                .columns()
                .iter()
                .map(|column| column.value_at(row))
                .collect();
            println!("  {values:?}");
        }
        rows += batch.row_count();
    }

    let metrics = scan.metrics();
    println!(
        "scanned {rows} row(s); pruned {} block(s); decoded {} stream(s)",
        metrics.blocks_pruned(),
        metrics.streams_decoded(),
    );
    Ok(())
}

fn full_primary_range(reader: &Reader) -> Option<PrimaryRange> {
    let primary = reader.schema().primary_column()?;
    let mut bounds: Option<(i64, i64)> = None;
    for block in reader.blocks() {
        if let Some(block) = block.primary_bounds() {
            bounds = Some(match bounds {
                Some((minimum, maximum)) => (minimum.min(block.min()), maximum.max(block.max())),
                None => (block.min(), block.max()),
            });
        }
    }
    let bounds = bounds?;
    let end = bounds.1.checked_add(1)?;
    match primary.logical_type() {
        LogicalType::Timestamp { .. } => Some(PrimaryRange::timestamp(bounds.0, end)),
        LogicalType::Date32 => {
            let start = i32::try_from(bounds.0).ok()?;
            let end = i32::try_from(end).ok()?;
            Some(PrimaryRange::date32(start, end))
        }
        _ => None,
    }
}
