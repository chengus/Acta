//! File identity for refresh, without a handle retained by the reader.
//!
//! [`Reader::refresh`] must never silently adopt a different file that
//! happens to be valid Acta with the same schema and length, so it needs an
//! identity check that detects a path replacement. The v0.2 file ID is the
//! wrong tool: the deterministic writer stores an opaque zero ID, so it is
//! not unique. Reopening the path to revalidate would be a race. Instead a
//! refresh compares the path's current file-system identity against the
//! identity captured when the reader opened.
//!
//! This is only half of the answer, and deliberately so. File-system identity
//! answers "is this the same file?", which a replacement that unlinks and
//! recreates the path cannot survive — but an in-place truncate-and-rewrite
//! (`File::create`, `fs::write`, a shell `>`, `cp` onto an existing path)
//! keeps the very same inode while replacing every byte, and no identity of
//! this kind can see it. The other half lives in [`Reader::refresh`], which
//! re-reads the commit trailer of the last frame it committed and refuses to
//! continue unless the committed bytes under it are still its own. Identity
//! catches the cheap case cheaply; the trailer catches the rest. Neither is
//! sufficient alone.
//!
//! On Unix the identity is the device and inode pair. On Windows it is the
//! volume serial number and file index from `GetFileInformationByHandle`,
//! called directly rather than through `std::os::windows::fs::MetadataExt`,
//! whose `volume_serial_number` and `file_index` are still behind the unstable
//! `windows_by_handle` feature and so are unavailable to this crate's stable
//! MSRV. That mirrors [`crate::lock`], which declares the one Win32 call it
//! needs for the same reason. On other targets a length-plus-modification-time
//! fingerprint stands in: it is weaker, but the commit-trailer check does not
//! depend on it, so a replacement there is still refused by the bytes rather
//! than by the fingerprint.
//!
//! The check is a point-in-time comparison. The window between capturing the
//! new identity and reading the new bytes is no larger than it was for the
//! reader's own open, and every byte read inside it is validated exactly as
//! initial discovery validates it.

use std::io;
use std::path::Path;

use crate::error::{Error, ErrorContext, Result};

/// Identity of the file at a path at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileIdentity {
    #[cfg(unix)]
    Unix { device: u64, inode: u64 },
    #[cfg(windows)]
    Windows { volume: u32, index: u64 },
    #[cfg(not(any(unix, windows)))]
    Other { length: u64, modified: Option<u64> },
}

impl FileIdentity {
    /// Capture the identity of the file `path` names right now.
    ///
    /// A path that names nothing at this instant has no identity to capture
    /// and fails as an I/O error, which is what the caller should report for
    /// a file that disappeared.
    pub(crate) fn capture(path: &Path) -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            let metadata = std::fs::metadata(path)?;
            Ok(Self::Unix {
                device: metadata.dev(),
                inode: metadata.ino(),
            })
        }
        #[cfg(windows)]
        {
            // The Win32 call needs an open handle rather than a path, so this
            // opens one for the duration of the call and closes it again. The
            // reader still retains no descriptor between refreshes.
            let file = std::fs::File::open(path)?;
            Self::from_handle(&file)
        }
        #[cfg(not(any(unix, windows)))]
        {
            use std::time::SystemTime;

            let metadata = std::fs::metadata(path)?;
            Ok(Self::Other {
                length: metadata.len(),
                modified: metadata
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
                    .map(|duration| duration.as_secs()),
            })
        }
    }

    /// Refuse the current file at `path` if it is not the file this identity
    /// was captured from.
    ///
    /// A mismatch is reported as [`ErrorKind::FileReplaced`](crate::ErrorKind)
    /// with a context naming the path, so a caller can distinguish a replaced
    /// path from corruption of the file it holds. A path that no longer
    /// exists is an ordinary I/O error instead, which keeps the two failure
    /// shapes easy to tell apart.
    pub(crate) fn expect_unchanged(self, path: &Path) -> Result<()> {
        let current = FileIdentity::capture(path)
            .map_err(|error| Error::io(error, None).with_context(ErrorContext::File))?;
        if current == self {
            return Ok(());
        }
        Err(replaced(path))
    }

    #[cfg(windows)]
    fn from_handle(file: &std::fs::File) -> io::Result<Self> {
        use std::ffi::c_void;
        use std::mem::zeroed;
        use std::os::windows::io::AsRawHandle;

        #[repr(C)]
        struct FileTime {
            low: u32,
            high: u32,
        }

        #[repr(C)]
        struct ByHandleFileInformation {
            file_attributes: u32,
            creation_time: FileTime,
            last_access_time: FileTime,
            last_write_time: FileTime,
            volume_serial_number: u32,
            file_size_high: u32,
            file_size_low: u32,
            number_of_links: u32,
            file_index_high: u32,
            file_index_low: u32,
        }

        // `BY_HANDLE_FILE_INFORMATION` is ten 32-bit fields; a mismatch here
        // would mean the declaration below no longer describes what the OS
        // writes, which is worth failing the build over rather than reading.
        const _: () = assert!(size_of::<ByHandleFileInformation>() == 52);

        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn GetFileInformationByHandle(
                file: *mut c_void,
                information: *mut ByHandleFileInformation,
            ) -> i32;
        }

        // SAFETY: `as_raw_handle` returns the live handle owned by `file`, and
        // the call only fills in the caller-owned structure below. Nothing
        // Rust owns outlives the call.
        let mut information: ByHandleFileInformation = unsafe { zeroed() };
        let result = unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut information) };
        if result == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self::Windows {
            volume: information.volume_serial_number,
            index: (u64::from(information.file_index_high) << 32)
                | u64::from(information.file_index_low),
        })
    }
}

/// The one shape a refusal to adopt the file at `path` takes.
pub(crate) fn replaced(path: &Path) -> Error {
    Error::file_replaced(format!(
        "the file at {} is not the file this reader opened; it was replaced \
         and refresh refuses to adopt it",
        path.display()
    ))
    .with_context(ErrorContext::File)
}
