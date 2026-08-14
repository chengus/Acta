//! Reader refresh and live tail following.
//!
//! Demonstrates the three ways a reader relates to an append-only file:
//!
//! - a snapshot [`Scan`](acta::Scan) over the blocks the reader already holds;
//! - a mutable [`Reader::refresh`] that extends the snapshot in place with
//!   frames committed since it opened;
//! - a live [`Tail`](acta::Tail) that polls for newly committed frames
//!   synchronously, one block per poll, without sleeping or blocking.
//!
//! Run with:
//!
//!     cargo run --example refresh_tail
//!
//! The example creates its own file in the system temporary directory, appends
//! to it through a writer, and follows it with a tail, then removes it.

use std::sync::Arc;

use acta::{
    Array, Column, LogicalType, PrimitiveArray, Reader, RecordBatch, Schema, Writer, WriterOptions,
};

fn int64_batch(schema: &Schema, values: Vec<i64>) -> RecordBatch {
    let row_count = values.len();
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Int64(PrimitiveArray::new(values, None))],
        row_count,
    )
    .expect("well-formed batch")
}

fn main() -> acta::Result<()> {
    let path = std::env::temp_dir().join("acta-refresh-tail-example.acta");
    let _ = std::fs::remove_file(&path);

    let schema = Schema::new(
        1,
        vec![Column::new(1, "value", LogicalType::Int64, false)],
        None,
    );
    let mut writer = Writer::create(&path, schema.clone(), WriterOptions::default())?;
    writer.append(int64_batch(&schema, vec![1, 2, 3]))?;
    // The block target keeps these rows buffered, so the file on disk still
    // ends after the schema frame. The reader snapshot below therefore holds
    // zero blocks, and the flush that follows becomes the first tailed block.
    let mut reader = Reader::open(&path)?;
    println!(
        "snapshot before flush: {} block(s), {} row(s)",
        reader.blocks().len(),
        reader.total_rows()
    );

    // 1. Manual refresh: publish a frame and extend the snapshot in place.
    writer.flush()?;
    let report = reader.refresh()?;
    println!(
        "refresh added {} block(s) / {} row(s), tail {}",
        report.blocks_added(),
        report.rows_added(),
        report.incomplete_tail()
    );

    // 2. Scan the refreshed snapshot (old blocks + the one just discovered).
    let mut scanned = 0;
    for batch in reader.scan() {
        scanned += batch?.row_count();
    }
    println!("scan over the refreshed snapshot: {scanned} row(s)");

    // 3. Live tail: append after the snapshot and poll for the new block.
    //
    // A poll returns None when nothing is committed yet, which is never the
    // end of the stream, so the loop is bounded by how long this example
    // wants to follow rather than by the tail running out. Writing it as
    // `while let Some(batch) = tail.poll_next()?` would instead stop at the
    // first quiet moment and call that the end.
    let mut tail = reader.tail();
    writer.append(int64_batch(&schema, vec![4, 5]))?;
    let _summary = writer.finish()?;
    let mut idle_polls = 0;
    while idle_polls < 3 {
        match tail.poll_next()? {
            Some(batch) => {
                println!("tailed a batch of {} row(s)", batch.row_count());
                idle_polls = 0;
            }
            // Nothing committed right now. A real follower waits here for as
            // long as it likes; polling neither blocks nor sleeps for it.
            None => idle_polls += 1,
        }
    }
    println!("tail is pending; dropping it cancels the follow");

    let _ = std::fs::remove_file(&path);
    Ok(())
}
