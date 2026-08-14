//! Create a file, reopen it, and append another batch.
//!
//! Run with `cargo run --example append`.

use std::sync::Arc;

use acta::{
    Array, Column, LogicalType, PrimitiveArray, Reader, RecordBatch, Schema, Writer, WriterOptions,
};

fn main() -> acta::Result<()> {
    let path =
        std::env::temp_dir().join(format!("acta-append-example-{}.acta", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let schema = Schema::new(
        1,
        vec![Column::new(1, "value", LogicalType::Int64, false)],
        None,
    );

    let mut writer = Writer::create(&path, schema.clone(), WriterOptions::default())?;
    writer.append(batch(&schema, &[1, 2, 3]))?;
    let first = writer.finish()?;
    println!(
        "created {} block(s) with {} row(s)",
        first.blocks_written(),
        first.rows_written()
    );

    let mut writer = Writer::open(&path, WriterOptions::default())?;
    let reopened_schema = Arc::clone(writer.schema());
    writer.append(batch(reopened_schema.as_ref(), &[4, 5]))?;
    let second = writer.finish()?;
    println!(
        "appended {} block(s) with {} row(s)",
        second.blocks_written(),
        second.rows_written()
    );

    let reader = Reader::open(&path)?;
    let mut values = Vec::new();
    for batch in reader.scan() {
        let batch = batch?;
        if let Some(Array::Int64(values_array)) = batch.column(0) {
            values.extend_from_slice(values_array.values());
        } else {
            unreachable!("the value column has the schema's Int64 type");
        }
    }
    println!("file now contains {values:?}");

    let _ = std::fs::remove_file(&path);
    Ok(())
}

fn batch(schema: &Schema, values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Array::Int64(PrimitiveArray::new(values.to_vec(), None))],
        values.len(),
    )
    .expect("the example batch matches its schema")
}
