//! The cooperative writer lock shared by append and recovery operations.

use std::fs::File;
use std::io;

use crate::error::{Error, ErrorContext, Result};

/// Take this crate's cooperative exclusive writer lock on `file`.
///
/// Contention is reported as [`ErrorKind::WriterLocked`](crate::ErrorKind), so
/// a caller can retry it without reading a platform error number, while any
/// other failure stays an ordinary I/O error.
pub(crate) fn acquire_writer_lock(file: &File) -> Result<()> {
    match lock_exclusive_nonblocking(file) {
        Ok(()) => Ok(()),
        Err(error) if is_lock_contention(&error) => Err(Error::writer_locked(
            "another writer already holds the exclusive lock on this file",
        )
        .with_context(ErrorContext::File)),
        Err(error) => Err(Error::io(error, None).with_context(ErrorContext::File)),
    }
}

/// Explicitly release a Unix writer lock before a successfully finished writer
/// returns to its caller.
///
/// Closing the file also releases the lock, but making the successful handoff
/// explicit ensures an immediately reopened writer never observes the previous
/// session's lock. Other supported platforms retain their close-on-drop
/// behavior.
#[cfg(unix)]
pub(crate) fn release_writer_lock(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    unsafe extern "C" {
        fn flock(file_descriptor: i32, operation: i32) -> i32;
    }

    const LOCK_UN: i32 = 8;
    // SAFETY: `as_raw_fd` returns the live descriptor owned by `file`, and
    // flock does not retain any pointer supplied by the caller.
    let result = unsafe { flock(file.as_raw_fd(), LOCK_UN) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Whether a failed lock request means another writer holds the lock.
#[cfg(unix)]
fn is_lock_contention(error: &io::Error) -> bool {
    // `flock` reports a held lock as EWOULDBLOCK, which is EAGAIN on every
    // target this crate builds for.
    error.kind() == io::ErrorKind::WouldBlock
}

#[cfg(windows)]
fn is_lock_contention(error: &io::Error) -> bool {
    /// `ERROR_LOCK_VIOLATION`, which `LockFileEx` returns for a held range.
    const LOCK_VIOLATION: i32 = 33;

    error.kind() == io::ErrorKind::WouldBlock || error.raw_os_error() == Some(LOCK_VIOLATION)
}

#[cfg(not(any(unix, windows)))]
fn is_lock_contention(_error: &io::Error) -> bool {
    false
}

/// Request the platform's exclusive, nonblocking lock on the handle the append
/// engine will retain.
///
/// The crate's MSRV predates `std::fs::File`'s own locking methods, so the
/// small platform calls live here instead of adding a dependency solely for
/// this operation. The request never blocks, so a second writer fails promptly
/// rather than waiting behind the first.
fn lock_exclusive_nonblocking(file: &File) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;

        unsafe extern "C" {
            fn flock(file_descriptor: i32, operation: i32) -> i32;
        }

        const LOCK_EX: i32 = 2;
        const LOCK_NB: i32 = 4;
        // SAFETY: `as_raw_fd` returns the live descriptor owned by `file`, and
        // flock does not retain any pointer supplied by the caller.
        let result = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(windows)]
    {
        use std::ffi::c_void;
        use std::mem::zeroed;
        use std::os::windows::io::AsRawHandle;

        #[repr(C)]
        struct Overlapped {
            internal: usize,
            internal_high: usize,
            offset: u32,
            offset_high: u32,
            event: *mut c_void,
        }

        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn LockFileEx(
                file: *mut c_void,
                flags: u32,
                reserved: u32,
                bytes_to_lock_low: u32,
                bytes_to_lock_high: u32,
                overlapped: *mut Overlapped,
            ) -> i32;
        }

        const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x0000_0002;
        const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x0000_0001;
        /// The single byte this lock covers, chosen one below the largest
        /// addressable offset so it can never overlap file content.
        ///
        /// Windows byte-range locks are mandatory, not advisory: a lock placed
        /// over the data itself would make `ReadFile` through any other handle
        /// fail with `ERROR_LOCK_VIOLATION`, stopping every reader — and the
        /// `acta` command line — for as long as a writer is open. Locking a
        /// range past the end of the file is explicitly permitted and still
        /// excludes other writers, which all contend for this same byte.
        const LOCK_OFFSET: u64 = u64::MAX - 1;

        // SAFETY: The zeroed OVERLAPPED is only used as the OS call's storage
        // for an exclusive byte-range lock. The handle is owned by `file` and
        // remains alive for the entire writer session.
        let mut overlapped: Overlapped = unsafe { zeroed() };
        overlapped.offset = LOCK_OFFSET as u32;
        overlapped.offset_high = (LOCK_OFFSET >> 32) as u32;
        // SAFETY: The handle and OVERLAPPED pointer are valid for this call;
        // the OS copies no Rust-owned data beyond the call's duration.
        let result = unsafe {
            LockFileEx(
                file.as_raw_handle(),
                LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                0,
                1,
                0,
                &mut overlapped,
            )
        };
        if result != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = file;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "exclusive writer locks are unavailable on this target",
        ))
    }
}
