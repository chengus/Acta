//! Explicit recovery planning and narrowly bounded incomplete-tail repair.

mod common;

use std::fs;
use std::sync::Arc;

use acta::{
    Array, Column, ErrorKind, Limits, LogicalType, PrimitiveArray, Reader, RecordBatch,
    RecoveryAction, Schema, ValidationLevel, ValidationOptions, Writer, WriterOptions,
};
use common::{
    PREFIX_SIZE, TemporaryFile, data_frame_offset, reference_fixture, with_appended_frame,
};

#[test]
fn complete_files_need_no_recovery() {
    let bytes = reference_fixture();
    let schema_end = data_frame_offset(&bytes);
    for candidate in [&bytes[..schema_end], &bytes] {
        let file = TemporaryFile::new("recovery-complete", candidate);
        let plan = acta::inspect_recovery(file.path()).unwrap();

        assert_eq!(plan.action(), RecoveryAction::None);
        assert_eq!(plan.file_size(), candidate.len() as u64);
        assert_eq!(plan.last_good_offset(), candidate.len() as u64);
        assert_eq!(plan.bytes_to_remove(), 0);
        assert!(!plan.requires_repair());
    }
}

#[test]
fn every_cut_inside_a_valid_appended_frame_has_the_exact_plan() {
    let complete = reference_fixture();
    let appended = with_appended_frame(&complete, data_frame_offset(&complete), 2);

    for cut in complete.len() + 1..appended.len() {
        let file = TemporaryFile::new("recovery-cut", &appended[..cut]);
        let before = fs::read(file.path()).unwrap();
        let plan = acta::inspect_recovery(file.path()).unwrap();

        assert_eq!(plan.action(), RecoveryAction::TruncateIncompleteTail);
        assert_eq!(plan.file_size(), cut as u64);
        assert_eq!(plan.last_good_offset(), complete.len() as u64);
        assert_eq!(plan.bytes_to_remove(), (cut - complete.len()) as u64);
        assert!(plan.requires_repair());
        assert_eq!(fs::read(file.path()).unwrap(), before);
    }
}

#[test]
fn repair_removes_only_the_incomplete_tail_and_preserves_the_prefix() {
    let complete = reference_fixture();
    let appended = with_appended_frame(&complete, data_frame_offset(&complete), 2);
    let frame_length = appended.len() - complete.len();
    let cuts = [
        complete.len() + 1,
        complete.len() + PREFIX_SIZE,
        complete.len() + frame_length / 2,
        appended.len() - 1,
    ];

    for cut in cuts {
        let file = TemporaryFile::new("recovery-repair", &appended[..cut]);
        let summary = acta::repair_incomplete_tail(file.path()).unwrap();

        assert_eq!(summary.original_file_size(), cut as u64);
        assert_eq!(summary.repaired_file_size(), complete.len() as u64);
        assert_eq!(summary.bytes_removed(), (cut - complete.len()) as u64);
        assert_eq!(fs::read(file.path()).unwrap(), complete);
        assert_eq!(
            acta::inspect_recovery(file.path()).unwrap().action(),
            RecoveryAction::None
        );
        acta::validate_with_options(
            file.path(),
            ValidationOptions::default().with_level(ValidationLevel::Full),
        )
        .unwrap();
        Reader::open(file.path()).unwrap();
        let _ = Writer::open(file.path(), WriterOptions::default())
            .unwrap()
            .finish()
            .unwrap();
    }
}

#[test]
fn complete_corruption_and_incomplete_schema_are_refused_without_mutation() {
    let mut corrupt = reference_fixture();
    let frame = data_frame_offset(&corrupt);
    corrupt[frame] ^= 0xff;
    let corrupt_file = TemporaryFile::new("recovery-corrupt", &corrupt);
    assert_eq!(
        acta::inspect_recovery(corrupt_file.path())
            .unwrap_err()
            .kind(),
        ErrorKind::Corruption
    );
    assert_eq!(
        acta::repair_incomplete_tail(corrupt_file.path())
            .unwrap_err()
            .kind(),
        ErrorKind::Corruption
    );
    assert_eq!(fs::read(corrupt_file.path()).unwrap(), corrupt);

    let schema_end = data_frame_offset(&reference_fixture());
    let incomplete_schema = TemporaryFile::new(
        "recovery-schema-cut",
        &reference_fixture()[..schema_end - 1],
    );
    assert_eq!(
        acta::inspect_recovery(incomplete_schema.path())
            .unwrap_err()
            .kind(),
        ErrorKind::IncompleteTail
    );
    let before = fs::read(incomplete_schema.path()).unwrap();
    assert_eq!(
        acta::repair_incomplete_tail(incomplete_schema.path())
            .unwrap_err()
            .kind(),
        ErrorKind::IncompleteTail
    );
    assert_eq!(fs::read(incomplete_schema.path()).unwrap(), before);
}

#[test]
fn complete_file_repair_is_invalid_and_missing_paths_are_not_created() {
    let file = TemporaryFile::new("recovery-complete-repair", &reference_fixture());
    let before = fs::read(file.path()).unwrap();
    assert_eq!(
        acta::repair_incomplete_tail(file.path())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(fs::read(file.path()).unwrap(), before);

    let missing = std::env::temp_dir().join("acta-recovery-definitely-missing.acta");
    let _ = fs::remove_file(&missing);
    assert_eq!(
        acta::repair_incomplete_tail(&missing).unwrap_err().kind(),
        ErrorKind::Io
    );
    assert!(!missing.exists());
}

#[test]
fn custom_limits_apply_to_recovery_discovery() {
    let file = TemporaryFile::new("recovery-limits", &reference_fixture());
    let limits = Limits::default().with_max_frame_payload_length(0);

    assert_eq!(
        acta::inspect_recovery_with_limits(file.path(), limits)
            .unwrap_err()
            .kind(),
        ErrorKind::ResourceLimit
    );
}

#[cfg(unix)]
#[test]
fn inspection_opens_read_only_files() {
    let file = TemporaryFile::new("recovery-read-only", &reference_fixture());
    let mut permissions = fs::metadata(file.path()).unwrap().permissions();
    permissions.set_readonly(true);
    fs::set_permissions(file.path(), permissions).unwrap();

    assert_eq!(
        acta::inspect_recovery(file.path()).unwrap().action(),
        RecoveryAction::None
    );
}

#[test]
fn repair_rechecks_a_stale_plan_and_uses_the_current_tail() {
    let complete = reference_fixture();
    let mut current = complete.clone();
    current.push(0);
    let file = TemporaryFile::new("recovery-stale-plan", &current);
    let plan = acta::inspect_recovery(file.path()).unwrap();
    assert!(plan.requires_repair());

    let mut grown = fs::read(file.path()).unwrap();
    grown.push(0);
    fs::write(file.path(), grown).unwrap();

    let summary = acta::repair_incomplete_tail(file.path()).unwrap();
    assert_eq!(summary.bytes_removed(), 2);
    assert_eq!(fs::read(file.path()).unwrap(), complete);
}

#[test]
fn an_active_writer_blocks_repair_but_not_readers() {
    let file = TemporaryFile::new("recovery-lock", &[]);
    fs::remove_file(file.path()).unwrap();
    let schema = Schema::new(
        900,
        vec![Column::new(1, "value", LogicalType::Int64, false)],
        None,
    );
    let writer = Writer::create(file.path(), schema, WriterOptions::default()).unwrap();

    assert_eq!(
        acta::repair_incomplete_tail(file.path())
            .unwrap_err()
            .kind(),
        ErrorKind::WriterLocked
    );
    Reader::open(file.path()).unwrap();
    drop(writer);
}

#[test]
fn repair_preserves_row_id_and_sequence_continuation_for_append() {
    let file = TemporaryFile::new("recovery-row-ids", &[]);
    fs::remove_file(file.path()).unwrap();
    let schema = Schema::new(
        901,
        vec![Column::new(1, "value", LogicalType::Int64, false)],
        None,
    );
    let options = WriterOptions::default()
        .with_row_ids(true)
        .with_row_block_target(2);
    let mut writer = Writer::create(file.path(), schema.clone(), options).unwrap();
    writer
        .append(
            RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Array::Int64(PrimitiveArray::new(vec![10, 20], None))],
                2,
            )
            .unwrap(),
        )
        .unwrap();
    let _ = writer.finish().unwrap();

    let mut bytes = fs::read(file.path()).unwrap();
    bytes.push(0);
    fs::write(file.path(), bytes).unwrap();
    acta::repair_incomplete_tail(file.path()).unwrap();

    let mut reopened = Writer::open(file.path(), options).unwrap();
    reopened
        .append(
            RecordBatch::try_new(
                Arc::new(schema),
                vec![Array::Int64(PrimitiveArray::new(vec![30], None))],
                1,
            )
            .unwrap(),
        )
        .unwrap();
    let _ = reopened.finish().unwrap();

    let reader = Reader::open(file.path()).unwrap();
    assert_eq!(
        reader
            .blocks()
            .iter()
            .map(|block| (block.sequence(), block.base_row_id()))
            .collect::<Vec<_>>(),
        [(1, Some(0)), (2, Some(2))]
    );
}
