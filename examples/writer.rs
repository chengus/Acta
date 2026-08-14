//! Buffered ingestion: many small appends, bounded blocks, and optional
//! Zstandard compression.
//!
//! Run with `cargo run --example writer`.

use std::sync::Arc;

use acta::{
    Array, Column, LogicalType, PrimitiveArray, Reader, RecordBatch, Schema, TimeUnit, TimeZone,
    TimestampArray, WriteSummary, Writer, WriterCodec, WriterOptions,
};

const ROWS: i64 = 5_000;

fn main() -> Result<(), acta::Error> {
    let schema = schema();

    let raw = write("acta-writer-example-raw.acta", &schema, WriterCodec::None)?;
    println!(
        "raw:        {} rows in {} blocks, {} bytes on disk",
        raw.rows_written(),
        raw.blocks_written(),
        raw.bytes_written()
    );

    // Zstandard compresses each raw stream independently, so the same rows and
    // the same block layout produce a smaller file. Without the default `zstd`
    // feature the writer refuses the codec instead, so this half is skipped.
    #[cfg(feature = "zstd")]
    {
        let compressed = write(
            "acta-writer-example-zstd.acta",
            &schema,
            WriterCodec::Zstandard,
        )?;
        println!(
            "zstandard:  {} rows in {} blocks, {} bytes on disk",
            compressed.rows_written(),
            compressed.blocks_written(),
            compressed.bytes_written()
        );
    }

    Ok(())
}

fn schema() -> Schema {
    Schema::new(
        1,
        vec![
            Column::new(
                1,
                "time",
                LogicalType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: TimeZone::Utc,
                },
                false,
            ),
            Column::new(2, "reading", LogicalType::Int64, false),
        ],
        Some(1),
    )
}

/// Ingest `ROWS` rows one small batch at a time, letting the row target decide
/// where blocks begin and end.
fn write(name: &str, schema: &Schema, codec: WriterCodec) -> Result<WriteSummary, acta::Error> {
    let path = std::env::temp_dir().join(name);
    let _ = std::fs::remove_file(&path);

    let options = WriterOptions::default()
        .with_row_block_target(1_024)
        .with_byte_block_target(4 * 1024 * 1024)
        .with_codec(codec);
    let mut writer = Writer::create(&path, schema.clone(), options)?;

    for start in (0..ROWS).step_by(100) {
        writer.append(batch(schema, start..(start + 100).min(ROWS))?)?;

        // Appended rows are buffered until a target is reached, so the counts
        // published and still buffered move independently.
        if start == 0 {
            let accounting = writer.accounting();
            println!(
                "after the first append: {} buffered, {} published, {} total rows",
                accounting.buffered_rows(),
                accounting.published_rows(),
                accounting.total_rows()
            );
        }
    }

    // `flush` publishes the final partial block without asking for durability;
    // `finish` does that and synchronizes. `Drop` alone would write nothing.
    writer.flush()?;
    let summary = writer.finish()?;

    let rows: usize = Reader::open(&path)?
        .scan()
        .map(|batch| Ok(batch?.row_count()))
        .sum::<Result<usize, acta::Error>>()?;
    assert_eq!(rows as u64, summary.rows_written());

    let _ = std::fs::remove_file(&path);
    Ok(summary)
}

fn batch(schema: &Schema, rows: std::ops::Range<i64>) -> Result<RecordBatch, acta::Error> {
    let values: Vec<i64> = rows.collect();
    let row_count = values.len();
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Timestamp(TimestampArray::new(
                values.iter().map(|row| row * 1_000).collect(),
                None,
                TimeUnit::Microsecond,
                TimeZone::Utc,
            )),
            Array::Int64(PrimitiveArray::new(values, None)),
        ],
        row_count,
    )
}
