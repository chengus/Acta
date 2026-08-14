//! Fuzz the metadata reader with arbitrary bytes.
//!
//! ```text
//! cargo +nightly fuzz run read -- -max_len=8192
//! ```
//!
//! The validator target covers the same walk, but only the reader reaches the
//! schema descriptor and block header parsers, builds the public schema, and
//! retains per-block metadata. Seeding the corpus from `spec/v0.2/fixtures`
//! gives the fuzzer valid framing to mutate rather than making it rediscover
//! the prologue magic and CRCs.
//!
//! Every accepted file is also decoded. The descriptor tables, the transforms,
//! and the codecs are reached only that way, since they are private to the
//! crate; a file the fuzzer mutates into a new stream shape exercises them
//! without a second entry point.
//!
//! Decoding happens twice over: once through `read_block`, which reads whole
//! blocks, and once through a scan carrying a projection and a primary range
//! derived from the same bytes. The second pass reaches the planner, the
//! pruning arithmetic, and the row filters, none of which the first one
//! touches.

#![no_main]

use std::sync::OnceLock;

use libfuzzer_sys::fuzz_target;
use tempfile::NamedTempFile;

/// One scratch file for the whole run, so each case costs a single rewrite.
static SCRATCH: OnceLock<NamedTempFile> = OnceLock::new();

fuzz_target!(|data: &[u8]| {
    let scratch = SCRATCH.get_or_init(|| NamedTempFile::new().expect("scratch file"));
    std::fs::write(scratch.path(), data).expect("write scratch file");

    // Any verdict is acceptable. The properties under test are that no input
    // panics or hangs, that an accepted file is described truthfully, and that
    // the reader and the validator never disagree about it.
    let opened = acta::Reader::open(scratch.path());
    let validated = acta::validate(scratch.path());

    match (opened, validated) {
        (Ok(reader), Ok(report)) => {
            let metadata = reader.file_metadata();
            assert_eq!(metadata.file_size(), data.len() as u64);
            assert!(metadata.last_good_offset() <= metadata.file_size());
            assert_eq!(metadata.block_count(), reader.blocks().len() as u64);
            assert_eq!(report.frame_count(), metadata.block_count() + 1);
            assert_eq!(report.last_good_offset(), metadata.last_good_offset());
            assert_eq!(report.incomplete_tail(), metadata.incomplete_tail());

            assert!(reader.schema().column_count() > 0);
            let rows: Option<u64> = reader
                .blocks()
                .iter()
                .try_fold(0_u64, |total, block| total.checked_add(block.row_count()));
            assert_eq!(rows, Some(reader.total_rows()));

            // Any verdict is acceptable here too. A decoded block must describe
            // itself truthfully: one array per schema column, and one logical
            // position per declared row in every one of them.
            for index in 0..reader.blocks().len() {
                let Ok(batch) = reader.read_block(index) else {
                    continue;
                };
                assert_eq!(batch.row_count() as u64, reader.blocks()[index].row_count());
                assert_eq!(batch.columns().len(), reader.schema().column_count());
                assert!(
                    batch
                        .columns()
                        .iter()
                        .all(|array| array.len() == batch.row_count())
                );
            }

            scan_arbitrarily(&reader, data);
        }
        (Err(opened), Err(validated)) => assert_eq!(opened.kind(), validated.kind()),
        _ => panic!("the reader and the validator disagreed about this file"),
    }
});

/// Scan the file with a projection and a range chosen from its own bytes.
///
/// Any verdict is acceptable, including a configuration that this schema
/// rejects. What is not acceptable is a panic, or a batch that contradicts the
/// projection it was asked for.
fn scan_arbitrarily(reader: &acta::Reader, data: &[u8]) {
    let seed = data
        .iter()
        .fold(0_u64, |seed, byte| seed.rotate_left(7) ^ u64::from(*byte));
    let names: Vec<String> = reader
        .schema()
        .columns()
        .iter()
        .map(|column| column.name().to_owned())
        .collect();

    // A subset of the columns, rotated so the requested order is rarely the
    // schema order, and sometimes empty.
    let mut projection: Vec<&str> = names
        .iter()
        .enumerate()
        .filter(|(index, _)| seed >> (index % 64) & 1 == 1)
        .map(|(_, name)| name.as_str())
        .collect();
    if !projection.is_empty() {
        let rotation = seed as usize % projection.len();
        projection.rotate_left(rotation);
    }

    let Ok(scan) = reader.scan().project(&projection) else {
        return;
    };
    let (first, second) = (seed as i64, seed.rotate_left(32) as i64);
    let range = acta::PrimaryRange::timestamp(first.min(second), first.max(second));
    let scan = match scan.primary_range(range) {
        Ok(scan) => scan,
        // No primary column, or one this range cannot address.
        Err(_) => reader.scan().project(&projection).expect("checked above"),
    };

    let mut returned = 0_u64;
    for batch in scan {
        let Ok(batch) = batch else {
            continue;
        };
        assert_eq!(batch.schema().column_count(), projection.len());
        assert_eq!(batch.columns().len(), projection.len());
        assert!(
            batch
                .columns()
                .iter()
                .all(|array| array.len() == batch.row_count())
        );
        for (position, name) in projection.iter().enumerate() {
            assert_eq!(&batch.schema().columns()[position].name(), name);
        }
        returned = returned
            .checked_add(batch.row_count() as u64)
            .expect("a scan cannot return more rows than a u64 counts");
        assert!(returned <= reader.total_rows(), "a scan invented rows");
    }
}
