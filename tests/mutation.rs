//! Structure-aware mutation of a valid file.
//!
//! Hand-written corruption tests only cover the failures somebody thought of.
//! These cases assert the properties that must hold for *every* input: neither
//! entry point panics, any report they produce describes a prefix of the file
//! they were given, the two never disagree about whether a file is well formed,
//! and any block that does decode describes itself truthfully.

mod common;

use acta::Reader;
use common::{TemporaryFile, expect_valid, reference_fixture};

/// Enough iterations to exercise every field of the fixture many times over,
/// while keeping the test fast enough to run on every build.
const ITERATIONS: usize = 4096;

#[test]
fn random_single_byte_edits_never_panic() {
    let original = reference_fixture();
    let mut random = Random::new(0x0000_ac7a_0002);

    for iteration in 0..ITERATIONS {
        let mut bytes = original.clone();
        let index = random.below(bytes.len());
        bytes[index] ^= random.byte() | 1;

        assert_both_entry_points_agree(&bytes, iteration);
    }
}

#[test]
fn random_multi_byte_edits_never_panic() {
    let original = reference_fixture();
    let mut random = Random::new(0x5eed_1234);

    for iteration in 0..ITERATIONS {
        let mut bytes = original.clone();
        for _ in 0..random.below(8) + 1 {
            let index = random.below(bytes.len());
            bytes[index] = random.byte();
        }

        assert_both_entry_points_agree(&bytes, iteration);
    }
}

#[test]
fn random_truncations_never_panic() {
    let original = reference_fixture();
    let mut random = Random::new(0xdead_beef);

    for iteration in 0..ITERATIONS {
        let cut = random.below(original.len() + 1);
        assert_both_entry_points_agree(&original[..cut], iteration);
    }
}

#[test]
fn arbitrary_bytes_never_panic() {
    let mut random = Random::new(0xfeed_face);

    for iteration in 0..ITERATIONS {
        let length = random.below(512);
        let bytes: Vec<u8> = (0..length).map(|_| random.byte()).collect();

        assert_both_entry_points_agree(&bytes, iteration);
    }
}

/// The reader and the validator share one parser, so for any input at all they
/// must reach the same verdict and describe the same extent of the file.
fn assert_both_entry_points_agree(bytes: &[u8], iteration: usize) {
    let file = TemporaryFile::new("mutated", bytes);
    let report = acta::validate(file.path());
    let reader = Reader::open(file.path());

    match (report, reader) {
        (Ok(report), Ok(reader)) => {
            assert_report_describes_a_prefix(&report, bytes.len(), iteration);
            let metadata = reader.file_metadata();
            assert_eq!(
                report.frame_count(),
                metadata.block_count() + 1,
                "iteration {iteration}: frame counts disagree"
            );
            assert_eq!(
                report.last_good_offset(),
                metadata.last_good_offset(),
                "iteration {iteration}: valid extents disagree"
            );
            assert_eq!(
                report.incomplete_tail(),
                metadata.incomplete_tail(),
                "iteration {iteration}: tail classifications disagree"
            );
            assert_decoded_blocks_are_consistent(&reader, iteration);
        }
        (Err(report), Err(reader)) => assert_eq!(
            report.kind(),
            reader.kind(),
            "iteration {iteration}: {report} versus {reader}"
        ),
        (report, reader) => panic!(
            "iteration {iteration}: one entry point accepted this file and the other did not \
             ({report:?} versus {reader:?})"
        ),
    }
}

/// Decoding may refuse any block. What it may not do is panic, or hand back a
/// batch whose shape contradicts the metadata the same snapshot reported.
fn assert_decoded_blocks_are_consistent(reader: &Reader, iteration: usize) {
    for index in 0..reader.blocks().len() {
        let Ok(batch) = reader.read_block(index) else {
            continue;
        };
        assert_eq!(
            batch.row_count() as u64,
            reader.blocks()[index].row_count(),
            "iteration {iteration}: block {index} decoded a different row count"
        );
        assert_eq!(
            batch.columns().len(),
            reader.schema().column_count(),
            "iteration {iteration}: block {index} decoded a different column count"
        );
        for array in batch.columns() {
            assert_eq!(
                array.len(),
                batch.row_count(),
                "iteration {iteration}: block {index} decoded a short array"
            );
        }
    }
}

#[test]
fn a_growing_file_never_loses_its_committed_frames() {
    let original = reference_fixture();
    let data_frame = common::data_frame_offset(&original);

    for length in data_frame..=original.len() {
        let report = expect_valid("growing", &original[..length]);
        assert!(
            report.last_good_offset() >= data_frame as u64,
            "the schema frame was lost at length {length}"
        );
    }
}

#[test]
fn validation_does_not_modify_the_file() {
    let original = reference_fixture();
    let file = TemporaryFile::new("read-only", &original);

    let _ = acta::validate(file.path());

    assert_eq!(std::fs::read(file.path()).unwrap(), original);
}

fn assert_report_describes_a_prefix(
    report: &acta::ValidationReport,
    length: usize,
    iteration: usize,
) {
    assert_eq!(report.file_size(), length as u64, "iteration {iteration}");
    assert!(
        report.last_good_offset() <= length as u64,
        "iteration {iteration} validated past the end of the file"
    );
    assert!(
        report.frame_count() >= 1,
        "iteration {iteration} reported a file with no schema frame"
    );
}

/// A xorshift generator, so every run mutates the same bytes in the same order.
struct Random {
    state: u64,
}

impl Random {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state
    }

    fn byte(&mut self) -> u8 {
        self.next() as u8
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}
