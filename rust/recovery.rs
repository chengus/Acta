//! Point-in-time recovery inspection and explicit incomplete-tail repair.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use crate::error::{Error, ErrorContext, Result};
use crate::format::scan::FileScan;
use crate::limits::Limits;
use crate::lock::acquire_writer_lock;

/// The action a recovery inspection recommends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecoveryAction {
    /// The file ends at a complete, structurally validated frame boundary.
    None,
    /// The file ends inside one final data frame and may be truncated safely.
    TruncateIncompleteTail,
}

/// A read-only, point-in-time recovery decision for an Acta file.
///
/// Inspection captures the file length when it opens the file and validates
/// only that snapshot. The plan must not be treated as an authorization to
/// truncate later: [`repair_incomplete_tail`] independently opens, locks, and
/// rescans the current file before changing anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RecoveryPlan {
    action: RecoveryAction,
    file_size: u64,
    last_good_offset: u64,
    bytes_to_remove: u64,
}

impl RecoveryPlan {
    /// The action supported by this plan.
    pub fn action(&self) -> RecoveryAction {
        self.action
    }

    /// The file length observed by inspection.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// The byte after the last complete, validated frame.
    pub fn last_good_offset(&self) -> u64 {
        self.last_good_offset
    }

    /// The number of bytes a matching repair would remove.
    pub fn bytes_to_remove(&self) -> u64 {
        self.bytes_to_remove
    }

    /// Whether the snapshot contains a repairable incomplete data-frame tail.
    pub fn requires_repair(&self) -> bool {
        self.action == RecoveryAction::TruncateIncompleteTail
    }
}

/// The result of successfully repairing one incomplete final data frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RecoverySummary {
    original_file_size: u64,
    repaired_file_size: u64,
    bytes_removed: u64,
}

impl RecoverySummary {
    /// The file length observed before the repair.
    pub fn original_file_size(&self) -> u64 {
        self.original_file_size
    }

    /// The file length after truncation and post-repair validation.
    pub fn repaired_file_size(&self) -> u64 {
        self.repaired_file_size
    }

    /// The number of bytes removed from the incomplete tail.
    pub fn bytes_removed(&self) -> u64 {
        self.bytes_removed
    }
}

/// Inspect recovery state without opening the file for writing.
///
/// This is a point-in-time snapshot. It never acquires the writer lock and
/// never changes file bytes. A plan that requests repair is only a description
/// of the snapshot; the repair operation independently locks and revalidates
/// the current file.
///
/// The scan is the structural, whole-frame pass
/// [`Writer::open`](crate::Writer::open) performs rather than
/// [`ValidationLevel::Full`](crate::ValidationLevel::Full), so a plan reporting
/// a complete file says the frames and their chains are intact, not that every
/// stream decodes.
pub fn inspect_recovery<P: AsRef<Path>>(path: P) -> Result<RecoveryPlan> {
    inspect_recovery_with_limits(path, Limits::default())
}

/// Inspect recovery state under caller-supplied structural limits.
pub fn inspect_recovery_with_limits<P: AsRef<Path>>(
    path: P,
    limits: Limits,
) -> Result<RecoveryPlan> {
    let scan = FileScan::open(path.as_ref(), limits)?;
    let (_file, plan) = discover(scan)?;
    Ok(plan)
}

/// Repair a verified incomplete final data frame.
///
/// Repair is destructive but narrowly bounded: it can remove only the bytes
/// after the last complete frame found by a strict structural scan. It refuses
/// complete corruption, an incomplete schema frame, and an already complete
/// file. The current file is opened read/write, locked before discovery,
/// rescanned on that same handle, and scanned once more after truncation. Both
/// scans are the structural, whole-frame pass
/// [`Writer::open`](crate::Writer::open) performs rather than
/// [`ValidationLevel::Full`](crate::ValidationLevel::Full): no stream is
/// decoded and no statistic is verified.
///
/// # Writer exclusion and its limits
///
/// This takes the same cooperative exclusive lock as
/// [`Writer::create`](crate::Writer::create), with the same scope and the same
/// limits; see that method. Those limits matter more here than they do for an
/// append, because this operation deletes bytes. The lock is a convention among
/// `acta` writers rather than part of the format, so it constrains neither
/// another implementation nor a process that simply opens the path, and it is
/// unreliable on network filesystems. The physical length is rechecked
/// immediately before the truncation and a length that moved is refused, but no
/// portable API makes that check and the truncation a single atomic step: a
/// writer that does not participate in the lock can still commit a frame inside
/// that window and lose it.
///
/// # Failure after truncation
///
/// Every failure before the truncation leaves the file byte for byte as it was,
/// and every one of them says so. A failure can also follow the truncation —
/// the synchronization, the rescan, or the post-repair validation — and every
/// one of those instead says the tail was already removed, whatever its
/// [`ErrorKind`](crate::ErrorKind). The two sets of messages never overlap, so
/// a caller can always tell which side of the mutation it is on. After a
/// post-truncation error the file may already be shorter while its durability
/// or its structure is unconfirmed.
pub fn repair_incomplete_tail<P: AsRef<Path>>(path: P) -> Result<RecoverySummary> {
    repair_incomplete_tail_with_limits(path, Limits::default())
}

/// Repair a verified incomplete final data frame under structural `limits`.
pub fn repair_incomplete_tail_with_limits<P: AsRef<Path>>(
    path: P,
    limits: Limits,
) -> Result<RecoverySummary> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.as_ref())
        .map_err(|error| Error::io(error, None).with_context(ErrorContext::File))?;
    acquire_writer_lock(&file)?;
    repair_locked_file(file, limits, &FileRepairOperations)
}

/// The file operations recovery performs around its one mutation.
///
/// Naming them lets a test report a length that moved underneath a plan, or
/// truncate to the wrong length, or fail either call, which is the only way to
/// reach those paths without a disk that misbehaves. Production always acts on
/// the locked [`File`].
trait RepairOperations {
    fn physical_length(&self, file: &File) -> io::Result<u64>;
    fn set_len(&self, file: &File, length: u64) -> io::Result<()>;
    fn sync(&self, file: &File) -> io::Result<()>;
}

struct FileRepairOperations;

impl RepairOperations for FileRepairOperations {
    fn physical_length(&self, file: &File) -> io::Result<u64> {
        file.metadata().map(|metadata| metadata.len())
    }

    fn set_len(&self, file: &File, length: u64) -> io::Result<()> {
        file.set_len(length)
    }

    fn sync(&self, file: &File) -> io::Result<()> {
        file.sync_all()
    }
}

fn discover(mut scan: FileScan) -> Result<(File, RecoveryPlan)> {
    let walk = scan.walk_data_frames(|_frame, _block| Ok(()))?;
    let plan = RecoveryPlan::from_walk(
        scan.file_size(),
        walk.last_good_offset,
        walk.incomplete_tail,
    )?;
    Ok((scan.into_file(), plan))
}

fn repair_locked_file(
    file: File,
    limits: Limits,
    operations: &dyn RepairOperations,
) -> Result<RecoverySummary> {
    let scan = FileScan::from_file(file, limits)?;
    let (file, plan) = discover(scan)?;
    let current_size = operations
        .physical_length(&file)
        .map_err(|error| Error::io(error, None).with_context(ErrorContext::File))?;
    if current_size != plan.file_size {
        return Err(Error::io(
            io::Error::other("the file changed while recovery was being inspected"),
            Some(current_size),
        )
        .with_context(ErrorContext::File));
    }
    if !plan.requires_repair() {
        return Err(Error::invalid_argument(
            "the file is complete; there is no incomplete tail to repair",
        )
        .with_context(ErrorContext::File));
    }

    // The truncation is the point the file's state changes, so the failure on
    // either side of it says which side it is on. A caller that cannot tell
    // them apart cannot know whether the committed bytes it had a moment ago
    // are still all there.
    operations
        .set_len(&file, plan.last_good_offset)
        .map_err(|error| {
            Error::io(error, Some(plan.last_good_offset))
                .with_message_prefix(BEFORE_TRUNCATION)
                .with_context(ErrorContext::File)
        })?;

    // Synchronization is not the only step left — the file is rescanned and
    // revalidated after it — and any of those can fail on a disk that stops
    // cooperating or against a writer that ignored the lock. They all report a
    // file that is already shorter, so they all carry the same marker.
    finish_repair(file, limits, &plan, operations)
        .map_err(|error| error.with_message_prefix(AFTER_TRUNCATION))
}

/// The marker every failure that leaves the file untouched carries.
const BEFORE_TRUNCATION: &str = "the incomplete tail was not removed and the file is unchanged";

/// The marker every failure after the truncation carries, whatever its kind.
const AFTER_TRUNCATION: &str =
    "the incomplete tail was already removed, so the file may already be shorter than it was";

/// Everything after the one mutation: synchronize, rescan, and validate.
fn finish_repair(
    file: File,
    limits: Limits,
    plan: &RecoveryPlan,
    operations: &dyn RepairOperations,
) -> Result<RecoverySummary> {
    operations.sync(&file).map_err(|error| {
        Error::io(error, Some(plan.last_good_offset))
            .with_message_prefix("the repaired file could not be synchronized")
            .with_context(ErrorContext::File)
    })?;

    let post_scan = FileScan::from_file(file, limits)?;
    let (file, post_plan) = discover(post_scan)?;
    if post_plan.action != RecoveryAction::None
        || post_plan.file_size != plan.last_good_offset
        || post_plan.last_good_offset != plan.last_good_offset
    {
        return Err(Error::corruption(
            "post-repair validation did not produce the expected complete file",
            Some(plan.last_good_offset),
        )
        .with_context(ErrorContext::File));
    }
    drop(file);

    let bytes_removed = plan
        .file_size
        .checked_sub(post_plan.file_size)
        .ok_or_else(|| {
            Error::corruption(
                "repaired file grew beyond its original size",
                Some(post_plan.file_size),
            )
            .with_context(ErrorContext::File)
        })?;
    Ok(RecoverySummary {
        original_file_size: plan.file_size,
        repaired_file_size: post_plan.file_size,
        bytes_removed,
    })
}

impl RecoveryPlan {
    fn from_walk(file_size: u64, last_good_offset: u64, incomplete_tail: bool) -> Result<Self> {
        let bytes_to_remove = if incomplete_tail {
            if last_good_offset >= file_size {
                return Err(Error::corruption(
                    "an incomplete tail does not extend beyond the last complete frame",
                    Some(last_good_offset),
                )
                .with_context(ErrorContext::File));
            }
            file_size.checked_sub(last_good_offset).ok_or_else(|| {
                Error::corruption(
                    "the last complete frame is beyond the captured file extent",
                    Some(last_good_offset),
                )
                .with_context(ErrorContext::File)
            })?
        } else {
            if last_good_offset != file_size {
                return Err(Error::corruption(
                    "a complete recovery walk did not reach the file extent",
                    Some(last_good_offset),
                )
                .with_context(ErrorContext::File));
            }
            0
        };
        let action = if incomplete_tail {
            RecoveryAction::TruncateIncompleteTail
        } else {
            RecoveryAction::None
        };
        Ok(Self {
            action,
            file_size,
            last_good_offset,
            bytes_to_remove,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Operations that behave normally except where a test asks otherwise.
    #[derive(Default)]
    struct FailingOperations {
        /// Reported instead of the real length, to move the file underneath a
        /// plan that has already been discovered.
        length: Option<u64>,
        fail_length: bool,
        fail_set_len: bool,
        fail_sync: bool,
        /// Added to the verified target, to truncate somewhere the plan did not
        /// authorize.
        length_delta: i64,
    }

    impl RepairOperations for FailingOperations {
        fn physical_length(&self, file: &File) -> io::Result<u64> {
            if self.fail_length {
                return Err(io::Error::other("injected metadata failure"));
            }
            match self.length {
                Some(length) => Ok(length),
                None => file.metadata().map(|metadata| metadata.len()),
            }
        }

        fn set_len(&self, file: &File, length: u64) -> io::Result<()> {
            if self.fail_set_len {
                return Err(io::Error::other("injected set_len failure"));
            }
            file.set_len(length.wrapping_add(self.length_delta as u64))
        }

        fn sync(&self, file: &File) -> io::Result<()> {
            if self.fail_sync {
                return Err(io::Error::other("injected sync failure"));
            }
            file.sync_all()
        }
    }

    #[test]
    fn plan_requires_removal_only_for_an_incomplete_data_tail() {
        let complete = RecoveryPlan::from_walk(10, 10, false).unwrap();
        assert_eq!(complete.action(), RecoveryAction::None);
        assert_eq!(complete.bytes_to_remove(), 0);

        let incomplete = RecoveryPlan::from_walk(14, 10, true).unwrap();
        assert_eq!(incomplete.action(), RecoveryAction::TruncateIncompleteTail);
        assert!(incomplete.requires_repair());
        assert_eq!(incomplete.bytes_to_remove(), 4);
    }

    #[test]
    fn injected_set_len_failure_preserves_the_file() {
        let fixture = Fixture::incomplete();
        let error = fixture.repair(FailingOperations {
            fail_set_len: true,
            ..FailingOperations::default()
        });
        assert_eq!(error.kind(), crate::ErrorKind::Io);
        assert!(error.message().starts_with(BEFORE_TRUNCATION), "{error}");
        assert!(!error.message().contains(AFTER_TRUNCATION), "{error}");
        assert_eq!(fixture.length(), fixture.original_size);
        assert_eq!(fixture.bytes(), fixture.original_bytes);
    }

    #[test]
    fn injected_sync_failure_does_not_claim_durability() {
        let fixture = Fixture::incomplete();
        let error = fixture.repair(FailingOperations {
            fail_sync: true,
            ..FailingOperations::default()
        });
        assert_eq!(error.kind(), crate::ErrorKind::Io);
        // The failures on either side of the truncation leave opposite states,
        // so a caller has to be able to tell them apart from the error alone.
        assert!(error.message().starts_with(AFTER_TRUNCATION), "{error}");
        assert!(!error.message().contains(BEFORE_TRUNCATION), "{error}");
        assert!(fixture.length() < fixture.original_size);
    }

    #[test]
    fn a_length_that_moved_under_the_plan_refuses_before_truncating() {
        for reported in [0, 1, u64::MAX] {
            let fixture = Fixture::incomplete();
            let error = fixture.repair(FailingOperations {
                length: Some(reported),
                ..FailingOperations::default()
            });
            assert_eq!(error.kind(), crate::ErrorKind::Io);
            assert!(error.message().contains("changed"), "{error}");
            assert_eq!(fixture.bytes(), fixture.original_bytes);
        }
    }

    #[test]
    fn an_unreadable_length_refuses_before_truncating() {
        let fixture = Fixture::incomplete();
        let error = fixture.repair(FailingOperations {
            fail_length: true,
            ..FailingOperations::default()
        });
        assert_eq!(error.kind(), crate::ErrorKind::Io);
        assert_eq!(fixture.bytes(), fixture.original_bytes);
    }

    /// The last guard before success is reported: a truncation that lands
    /// anywhere but the verified boundary must not be called a repair.
    ///
    /// This failure is a `Corruption`, not the `Io` a failed synchronization
    /// produces, and it still arrives after the file has been shortened, so it
    /// has to carry the same marker: the distinction a caller needs is which
    /// side of the mutation the failure is on, not which kind it is.
    #[test]
    fn a_wrong_truncation_target_fails_post_repair_validation() {
        for delta in [-64_i64, -8, -1, 1, 8, 64] {
            let fixture = Fixture::incomplete();
            let error = fixture.repair(FailingOperations {
                length_delta: delta,
                ..FailingOperations::default()
            });
            assert_eq!(error.kind(), crate::ErrorKind::Corruption, "delta {delta}");
            assert!(
                error.message().starts_with(AFTER_TRUNCATION),
                "delta {delta}: {error}"
            );
            assert!(!error.message().contains(BEFORE_TRUNCATION), "{error}");
        }
    }

    /// A file that removes itself, so a failing assertion cannot leave one
    /// behind for the next run to collide with.
    struct Fixture {
        path: PathBuf,
        original_bytes: Vec<u8>,
        original_size: u64,
    }

    impl Fixture {
        /// The reference fixture with one byte of an unfinished frame after it.
        fn incomplete() -> Self {
            static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

            let mut bytes = include_bytes!("../spec/v0.2/fixtures/minimal/minimal.acta").to_vec();
            bytes.push(0);
            let path = std::env::temp_dir().join(format!(
                "acta-recovery-unit-{}-{}.acta",
                std::process::id(),
                NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_file(&path);
            std::fs::write(&path, &bytes).unwrap();
            Self {
                path,
                original_size: bytes.len() as u64,
                original_bytes: bytes,
            }
        }

        /// Run the production repair over a locked handle and expect a failure.
        fn repair(&self, operations: FailingOperations) -> Error {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path)
                .unwrap();
            acquire_writer_lock(&file).unwrap();
            match repair_locked_file(file, Limits::default(), &operations) {
                Ok(summary) => panic!("repair unexpectedly succeeded: {summary:?}"),
                Err(error) => error,
            }
        }

        fn length(&self) -> u64 {
            std::fs::metadata(&self.path).unwrap().len()
        }

        fn bytes(&self) -> Vec<u8> {
            std::fs::read(&self.path).unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
