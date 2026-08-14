//! Open a file, inspect its snapshot metadata, and decode every block.
//!
//! Run with:
//!
//!     cargo run --example reader -- spec/v0.2/fixtures/minimal/minimal.acta

use acta::Reader;

fn main() -> acta::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "spec/v0.2/fixtures/minimal/minimal.acta".to_owned());

    // `open` captures a snapshot of the file's extent and committed blocks;
    // a later append to the same path will not be visible through it.
    let reader = Reader::open(&path)?;
    let metadata = reader.file_metadata();
    println!(
        "{path}: {} block(s), {} row(s), schema id {}",
        metadata.block_count(),
        reader.total_rows(),
        metadata.schema_id(),
    );

    // `scan` decodes one block per iteration, in file order, and yields a
    // `Result` per item so a damaged block doesn't stop the rest of the scan.
    for (index, block) in reader.scan().enumerate() {
        let batch = block?;
        println!("block {index}: {} row(s)", batch.row_count());

        // Prints every decoded row with no limit, as raw column values with
        // no knowledge of this file's schema. That's fine for a small
        // fixture; point this at a large file and it will print (and hold
        // in memory, one block at a time) every row it has.
        for row in 0..batch.row_count() {
            let values: Vec<_> = batch.columns().iter().map(|c| c.value_at(row)).collect();
            println!("  row {row}: {values:?}");
        }
    }

    Ok(())
}
