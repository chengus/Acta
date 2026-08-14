//! Compression codecs defined by Acta v0.2.

use std::io;

use crate::error::{Error, ErrorContext, Result};

/// Decompress one independent Zstandard stream into exactly `expected_length`
/// bytes.
///
/// The declared transformed length decides how much memory this call reserves,
/// so a caller must charge it against its decode allowance first. The
/// reservation is then fallible and the codec is given no room beyond it: a
/// stream that expands past its declaration fails rather than growing the
/// buffer, and one that stops short fails the final length check.
pub(crate) fn zstd(stored: &[u8], expected_length: usize) -> Result<Vec<u8>> {
    let mut decoded = Vec::new();
    decoded.try_reserve_exact(expected_length).map_err(|_| {
        Error::resource_limit("unable to allocate the decompressed stream", None)
            .with_context(ErrorContext::Payload)
    })?;

    zstd::bulk::Decompressor::new()
        .and_then(|mut decompressor| decompressor.decompress_to_buffer(stored, &mut decoded))
        .map_err(invalid_stream)?;

    if decoded.len() != expected_length {
        return Err(Error::corruption(
            format!(
                "decompressed stream length {} does not match {expected_length}",
                decoded.len()
            ),
            None,
        )
        .with_context(ErrorContext::Payload));
    }
    Ok(decoded)
}

fn invalid_stream(error: io::Error) -> Error {
    Error::corruption(format!("invalid Zstandard stream: {error}"), None)
        .with_context(ErrorContext::Payload)
}

#[cfg(test)]
mod tests {
    use super::zstd;

    #[test]
    fn zstandard_round_trip_honors_the_declared_length() {
        let source = b"acta stream data";
        let compressed = zstd::bulk::compress(source, 1).expect("compression should succeed");

        assert_eq!(
            zstd(&compressed, source.len()).expect("decompression should succeed"),
            source
        );
    }

    #[test]
    fn a_stream_longer_than_its_declaration_is_rejected() {
        let source = b"acta stream data";
        let compressed = zstd::bulk::compress(source, 1).expect("compression should succeed");

        let error = zstd(&compressed, source.len() - 1).expect_err("the buffer is too small");

        assert_eq!(error.kind(), crate::ErrorKind::Corruption);
    }
}
