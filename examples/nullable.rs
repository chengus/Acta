//! Write and read nullable typed columns.
//!
//! Run with `cargo run --example nullable`.
//!
//! Nullability is a property of the schema. The array keeps dense value slots
//! plus a validity bitmap, and the reader restores nulls at their logical row
//! positions.

use std::sync::Arc;

use acta::{
    Array, Column, LogicalType, PrimitiveArray, Reader, RecordBatch, Schema, Utf8Array, Writer,
    WriterOptions,
};

fn main() -> acta::Result<()> {
    let path =
        std::env::temp_dir().join(format!("acta-nullable-example-{}.acta", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let schema = Schema::new(
        1,
        vec![
            Column::new(1, "temperature", LogicalType::Float64, true),
            Column::new(2, "status", LogicalType::Utf8, false),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Array::Float64(PrimitiveArray::new(
                vec![20.5, 0.0, 21.25],
                Some(vec![true, false, true]),
            )),
            Array::Utf8(Utf8Array::new(
                vec!["ok".into(), "missing".into(), "ok".into()],
                None,
            )),
        ],
        3,
    )?;

    let mut writer = Writer::create(&path, schema, WriterOptions::default())?;
    writer.append(batch)?;
    let _ = writer.finish()?;

    let reader = Reader::open(&path)?;
    for batch in reader.scan() {
        let batch = batch?;
        for row in 0..batch.row_count() {
            println!(
                "row {row}: temperature={:?}, status={:?}",
                batch
                    .column_by_name("temperature")
                    .expect("temperature")
                    .value_at(row),
                batch
                    .column_by_name("status")
                    .expect("status")
                    .value_at(row),
            );
        }
    }

    let _ = std::fs::remove_file(&path);
    Ok(())
}
