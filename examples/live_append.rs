//! Concurrent appending and live tailing from two threads.
//!
//! Run with:
//!
//!     cargo run --example live_append
//!
//! The reader establishes its tail before the writer starts. The writer then
//! publishes one update at a time, while the reader prints each newly
//! committed row. The example is deliberately bounded so it exits on its own.

use std::error::Error;
use std::io;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use acta::{
    Array, Column, LogicalType, Reader, RecordBatch, Schema, TimeUnit, TimeZone, TimestampArray,
    Utf8Array, Writer, WriterOptions,
};

const UPDATE_COUNT: usize = 30;
const UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const EXAMPLE_TIMEOUT: Duration = Duration::from_secs(10);

type AnyError = Box<dyn Error + Send + Sync>;

fn main() -> Result<(), AnyError> {
    let path = std::env::temp_dir().join(format!(
        "acta-live-append-example-{}.acta",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);

    let schema = Schema::new(
        1,
        vec![
            Column::new(
                1,
                "last_update",
                LogicalType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: TimeZone::Utc,
                },
                false,
            ),
            Column::new(2, "message", LogicalType::Utf8, false),
        ],
        Some(1),
    );

    // Publish the schema before either worker starts. The writer thread later
    // reopens this file for append while the reader keeps its snapshot open.
    let _ = Writer::create(&path, schema, WriterOptions::default())?.finish()?;

    let (tail_ready_tx, tail_ready_rx) = mpsc::channel();

    let reader_path = path.clone();
    let reader_thread = thread::spawn(move || -> Result<(), AnyError> {
        let mut reader = Reader::open(&reader_path)?;
        let mut tail = reader.tail();
        tail_ready_tx.send(())?;

        let deadline = Instant::now() + EXAMPLE_TIMEOUT;
        let mut updates_seen = 0;
        while updates_seen < UPDATE_COUNT {
            match tail.poll_next()? {
                Some(batch) => {
                    print_batch(&batch)?;
                    updates_seen += batch.row_count();
                }
                None if Instant::now() < deadline => thread::sleep(POLL_INTERVAL),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("received only {updates_seen} of {UPDATE_COUNT} updates"),
                    )
                    .into());
                }
            }
        }

        println!("reader: received all {updates_seen} updates");
        Ok(())
    });

    let writer_path = path.clone();
    let writer_thread = thread::spawn(move || -> Result<(), AnyError> {
        // Do not append until the other thread has opened the file and created
        // its tail. This makes every row below a live update, not snapshot data.
        tail_ready_rx.recv()?;

        let mut writer = Writer::open(&writer_path, WriterOptions::default())?;
        let schema = Arc::clone(writer.schema());
        for update in 1..=UPDATE_COUNT {
            let last_update = unix_timestamp_micros()?;
            let message = format!("update {update}");
            writer.append(update_batch(&schema, last_update, message.clone())?)?;

            // append() is buffered. flush() commits a frame so the tailing
            // reader can discover this update immediately.
            writer.flush()?;
            println!("writer: committed {message}");
            thread::sleep(UPDATE_INTERVAL);
        }
        let _ = writer.finish()?;
        Ok(())
    });

    let writer_result = writer_thread
        .join()
        .map_err(|_| io::Error::other("writer thread panicked"))?;
    let reader_result = reader_thread
        .join()
        .map_err(|_| io::Error::other("reader thread panicked"))?;

    let _ = std::fs::remove_file(&path);
    writer_result?;
    reader_result?;
    Ok(())
}

fn update_batch(
    schema: &Arc<Schema>,
    last_update: i64,
    message: String,
) -> acta::Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Array::Timestamp(TimestampArray::new(
                vec![last_update],
                None,
                TimeUnit::Microsecond,
                TimeZone::Utc,
            )),
            Array::Utf8(Utf8Array::new(vec![message], None)),
        ],
        1,
    )
}

fn print_batch(batch: &RecordBatch) -> Result<(), AnyError> {
    let Some(Array::Timestamp(last_updates)) = batch.column(0) else {
        return Err(io::Error::other("last_update did not decode as timestamp64").into());
    };
    let Some(Array::Utf8(messages)) = batch.column(1) else {
        return Err(io::Error::other("message did not decode as UTF-8").into());
    };

    for row in 0..batch.row_count() {
        let last_update = format_utc_time(last_updates.values()[row]);
        println!(
            "reader: last_update={last_update}, message={}",
            messages.values()[row]
        );
    }
    Ok(())
}

fn format_utc_time(timestamp_micros: i64) -> String {
    const MICROS_PER_SECOND: i64 = 1_000_000;
    const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

    let whole_seconds = timestamp_micros.div_euclid(MICROS_PER_SECOND);
    let micros = timestamp_micros.rem_euclid(MICROS_PER_SECOND);
    let seconds_today = whole_seconds.rem_euclid(SECONDS_PER_DAY);
    let hours = seconds_today / (60 * 60);
    let minutes = seconds_today / 60 % 60;
    let seconds = seconds_today % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}.{micros:06} UTC")
}

fn unix_timestamp_micros() -> Result<i64, AnyError> {
    // Unix time is UTC; the schema's TimeZone::Utc annotation preserves that
    // interpretation when this timestamp is decoded.
    let micros = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
    Ok(i64::try_from(micros)?)
}
