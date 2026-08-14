//! The crate-wide error type.
//!
//! Error kinds follow one rule, so callers can act on them:
//!
//! - a field whose value contradicts a requirement of Acta v0.2 is
//!   [`ErrorKind::Corruption`];
//! - a well-formed field whose value belongs to a format or frame envelope this
//!   crate does not implement is one of the `Unsupported*` kinds, because such a
//!   file may be perfectly valid for a reader that does implement it;
//! - a declared size above a configured bound is [`ErrorKind::ResourceLimit`];
//! - a file that ends before its schema frame is complete is
//!   [`ErrorKind::IncompleteTail`], because a later append may complete it;
//! - a call that asks for something the snapshot does not contain is
//!   [`ErrorKind::InvalidArgument`], because the file is not implicated;
//! - a caller-supplied expected schema that differs from the file's own is
//!   [`ErrorKind::SchemaMismatch`], because neither side is wrong on its own
//!   and a caller acts on this differently from a malformed argument.
//!
//! A writer that cannot safely continue after a partial I/O failure reports
//! [`ErrorKind::Poisoned`]. A writer that cannot take the crate's cooperative
//! exclusive lock because another writer holds it reports
//! [`ErrorKind::WriterLocked`], which is separated from [`ErrorKind::Io`] so
//! contention is retryable without inspecting a platform error number. A
//! writer that builds a frame contradicting its own invariants reports
//! [`ErrorKind::Internal`], which never describes a file.
//!
//! A long-lived reader whose path now names a different file than the one it
//! opened reports [`ErrorKind::FileReplaced`]. The replacement file may be
//! perfectly valid on its own; the kind exists so a caller can distinguish
//! "the path changed identity underneath the reader" from corruption of the
//! file the reader holds. A path that still names the reader's own file but
//! has lost committed bytes reports [`ErrorKind::FileTruncated`], which is the
//! same distinction drawn one step further: the file is neither damaged nor a
//! stranger, it is simply shorter than the snapshot that describes it.
//!
//! Reserved fields never produce an error. Specification section 2 requires
//! readers to ignore them, and every reserved field lies inside a CRC-covered
//! region, so corruption there is already caught by the surrounding checksum.

use std::error::Error as StdError;
use std::fmt;
use std::io;

/// The result type returned by Acta operations.
pub type Result<T> = std::result::Result<T, Error>;

/// The broad category of an Acta failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// An operating-system I/O operation failed.
    Io,
    /// The bytes violate an implemented v0.2 invariant.
    Corruption,
    /// The file declares a format version this crate does not implement.
    UnsupportedVersion,
    /// The file declares a feature bit this v0.2 implementation does not know.
    UnsupportedFeature,
    /// The file declares a frame type or envelope version outside this stage.
    UnsupportedFrame,
    /// A declared size exceeds the configured [`crate::Limits`].
    ResourceLimit,
    /// The file ends before its schema frame is complete.
    IncompleteTail,
    /// The call asked for something this snapshot does not contain.
    InvalidArgument,
    /// A caller-supplied expected schema differs from the file's own schema.
    SchemaMismatch,
    /// Another writer already holds this crate's cooperative exclusive lock.
    WriterLocked,
    /// A writer has encountered a partial I/O failure and cannot continue.
    Poisoned,
    /// The path a long-lived reader refreshes now names a different file than
    /// the one its snapshot came from.
    FileReplaced,
    /// The path a long-lived reader refreshes still names its own file, but
    /// that file has shrunk below the committed boundary the snapshot holds.
    ///
    /// A repair that removed only an uncommitted tail shrinks the file back to
    /// exactly that boundary and is accepted; this kind reports a file that
    /// went below it and so no longer contains frames the reader already
    /// committed.
    FileTruncated,
    /// A writer built something that contradicts its own invariants, which is a
    /// bug in this crate rather than a problem with any file.
    Internal,
}

/// The region of a file an error was detected in.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorContext {
    /// The file as a whole, rather than one of its structures.
    File,
    Prologue,
    Frame {
        sequence: u64,
    },
    Prefix,
    Header,
    Payload,
    Trailer,
}

/// An Acta error with a category, optional file offset, and parsing context.
pub struct Error {
    kind: ErrorKind,
    message: String,
    offset: Option<u64>,
    context: Vec<ErrorContext>,
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// The absolute file offset of the field or region that failed the check.
    ///
    /// A check over a single field reports that field's offset. A check over a
    /// region, such as a CRC, reports the first byte of the covered region.
    pub fn offset(&self) -> Option<u64> {
        self.offset
    }

    /// The regions containing the failure, innermost first.
    pub fn context(&self) -> &[ErrorContext] {
        &self.context
    }

    pub(crate) fn corruption(message: impl Into<String>, offset: Option<u64>) -> Self {
        Self::new(ErrorKind::Corruption, message, offset, None)
    }

    pub(crate) fn unsupported_version(message: impl Into<String>, offset: Option<u64>) -> Self {
        Self::new(ErrorKind::UnsupportedVersion, message, offset, None)
    }

    pub(crate) fn unsupported_feature(message: impl Into<String>, offset: Option<u64>) -> Self {
        Self::new(ErrorKind::UnsupportedFeature, message, offset, None)
    }

    pub(crate) fn unsupported_frame(message: impl Into<String>, offset: Option<u64>) -> Self {
        Self::new(ErrorKind::UnsupportedFrame, message, offset, None)
    }

    pub(crate) fn resource_limit(message: impl Into<String>, offset: Option<u64>) -> Self {
        Self::new(ErrorKind::ResourceLimit, message, offset, None)
    }

    pub(crate) fn incomplete_tail(message: impl Into<String>, offset: Option<u64>) -> Self {
        Self::new(ErrorKind::IncompleteTail, message, offset, None)
    }

    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument, message, None, None)
    }

    pub(crate) fn schema_mismatch(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::SchemaMismatch, message, None, None)
    }

    pub(crate) fn writer_locked(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::WriterLocked, message, None, None)
    }

    pub(crate) fn poisoned(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Poisoned, message, None, None)
    }

    pub(crate) fn file_replaced(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::FileReplaced, message, None, None)
    }

    pub(crate) fn file_truncated(message: impl Into<String>, offset: Option<u64>) -> Self {
        Self::new(ErrorKind::FileTruncated, message, offset, None)
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message, None, None)
    }

    pub(crate) fn io(error: io::Error, offset: Option<u64>) -> Self {
        Self::new(
            ErrorKind::Io,
            error.to_string(),
            offset,
            Some(Box::new(error)),
        )
    }

    pub(crate) fn with_context(mut self, context: ErrorContext) -> Self {
        self.context.push(context);
        self
    }

    /// Name the structure an error came from, for a check that could not name
    /// it itself.
    ///
    /// Some checks are deliberately unaware of their caller: the codecs do not
    /// know which column they are decompressing, and should not have to. The
    /// caller that does know prefixes the message rather than restating it.
    pub(crate) fn with_message_prefix(mut self, prefix: impl AsRef<str>) -> Self {
        self.message = format!("{}: {}", prefix.as_ref(), self.message);
        self
    }

    fn new(
        kind: ErrorKind,
        message: impl Into<String>,
        offset: Option<u64>,
        source: Option<Box<dyn StdError + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            offset,
            context: Vec::new(),
            source,
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Error")
            .field("kind", &self.kind)
            .field("message", &self.message)
            .field("offset", &self.offset)
            .field("context", &self.context)
            .field(
                "source",
                &self.source.as_ref().map(|source| source.to_string()),
            )
            .finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.kind, self.message)?;
        if let Some(offset) = self.offset {
            write!(formatter, " at file offset 0x{offset:x}")?;
        }
        if !self.context.is_empty() {
            write!(formatter, " (context: ")?;
            for (index, context) in self.context.iter().enumerate() {
                if index != 0 {
                    write!(formatter, ", ")?;
                }
                write!(formatter, "{context:?}")?;
            }
            write!(formatter, ")")?;
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_ref()
            .map(|source| &**source as &(dyn StdError + 'static))
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Io => "I/O error",
            Self::Corruption => "corruption",
            Self::UnsupportedVersion => "unsupported format version",
            Self::UnsupportedFeature => "unsupported feature",
            Self::UnsupportedFrame => "unsupported frame",
            Self::ResourceLimit => "resource limit exceeded",
            Self::IncompleteTail => "incomplete tail",
            Self::InvalidArgument => "invalid argument",
            Self::SchemaMismatch => "schema mismatch",
            Self::WriterLocked => "writer locked",
            Self::Poisoned => "poisoned writer",
            Self::FileReplaced => "file replaced",
            Self::FileTruncated => "file truncated",
            Self::Internal => "internal writer error",
        };
        formatter.write_str(label)
    }
}
