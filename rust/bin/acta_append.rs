//! Test helper: append one frame to an existing Acta file from a new process.
//!
//! Stage 9's tail must follow a writer that runs in a different process from
//! the reading process, which the library integration tests cannot exercise
//! directly. This small binary exists so those tests can spawn a real writer:
//! it opens the path with [`Writer::open`](acta::Writer::open), appends one
//! batch of `rows` int64 values, and finishes, publishing exactly one frame.
//! It is a test fixture, not a supported command-line surface.

use std::sync::Arc;

use acta::{Array, PrimitiveArray, RecordBatch, Writer, WriterOptions};

fn main() -> acta::Result<()> {
    let mut arguments = std::env::args().skip(1);
    let Some(path) = arguments.next() else {
        eprintln!("usage: acta_append <path> <rows> [first-value]");
        std::process::exit(2);
    };
    let rows: usize = arguments
        .next()
        .expect("usage: acta_append <path> <rows> [first-value]")
        .parse()
        .expect("rows must be a count");
    let first_value: i64 = arguments
        .next()
        .map(|value| value.parse().expect("first-value must be an integer"))
        .unwrap_or(0);

    let mut writer = Writer::open(path, WriterOptions::default())?;
    // The helper targets the single non-nullable `int64` schema the tail tests
    // build; a file with any other schema is refused by the writer's own batch
    // check, which is the correct behavior for a fixture.
    let schema = Arc::clone(writer.schema());
    let batch = RecordBatch::try_new(
        schema,
        vec![Array::Int64(PrimitiveArray::new(
            (0..rows).map(|row| first_value + row as i64).collect(),
            None,
        ))],
        rows,
    )?;
    writer.append(batch)?;
    let _summary = writer.finish()?;
    Ok(())
}
