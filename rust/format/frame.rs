use std::fs::File;
use std::io::Read;

use crate::crc32c::{Crc32c, checksum};
use crate::error::{Error, ErrorContext, Result};
use crate::limits::Limits;

use super::constants::{
    CHECKPOINT_FRAME_TYPE, COMMIT_MAGIC, DATA_BLOCK_HEADER_SIZE, DATA_FRAME_TYPE, FRAME_ALIGNMENT,
    FRAME_FLAGS, FRAME_MAGIC, FRAME_VERSION, MAGIC_SIZE, PREFIX_CRC_FIELD,
    PREFIX_FRAME_FLAGS_FIELD, PREFIX_FRAME_TYPE_FIELD, PREFIX_FRAME_VERSION_FIELD,
    PREFIX_HEADER_LENGTH_FIELD, PREFIX_PAYLOAD_LENGTH_FIELD, PREFIX_RESERVED_SIZE,
    PREFIX_SEQUENCE_FIELD, PREFIX_SIZE, SCHEMA_FRAME_HEADER_SIZE, SCHEMA_FRAME_SEQUENCE,
    SCHEMA_FRAME_TYPE, TRAILER_COMMIT_MAGIC_FIELD, TRAILER_CRC_FIELD, TRAILER_SEQUENCE_FIELD,
    TRAILER_SIZE,
};
use super::cursor::Cursor;
use super::{field_offset, read_exact_at, seek};

/// How much of a frame body is checksummed per read.
const CHECKSUM_CHUNK_SIZE: usize = 16 * 1024;

/// The outcome of reading the frame that begins at a known offset.
pub(crate) enum FrameRead {
    /// The frame is structurally complete. Carries its total length in bytes.
    Complete(FrameMetadata),
    /// The file ends before this frame is complete.
    IncompleteTail,
}

/// A frame whose generic prefix, header, payload, trailer, and CRCs have all
/// been validated. Frame-specific parsers consume these borrowed coordinates;
/// they do not need to duplicate prefix arithmetic.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FrameMetadata {
    pub(crate) frame_type: u16,
    pub(crate) sequence: u64,
    pub(crate) frame_offset: u64,
    pub(crate) header_offset: u64,
    pub(crate) header_length: u64,
    pub(crate) payload_offset: u64,
    pub(crate) payload_length: u64,
    pub(crate) total_length: u64,
}

/// Read and fully validate the frame at `offset`.
///
/// A frame prefix that is present in full is validated in full, exactly as
/// specification section 13.1 orders the steps: the prefix is validated before
/// the frame is measured against the file extent. A frame whose prefix is not
/// yet entirely present, or whose declared length runs past the end of the
/// file, is an interrupted append rather than a defect.
pub(crate) fn read_frame(
    file: &mut File,
    file_size: u64,
    offset: u64,
    expected_sequence: u64,
    limits: Limits,
) -> Result<FrameRead> {
    let remaining = remaining_bytes(file_size, offset)?;
    if remaining < PREFIX_SIZE as u64 {
        return Ok(FrameRead::IncompleteTail);
    }

    let mut prefix_bytes = [0_u8; PREFIX_SIZE];
    read_exact_at(file, offset, &mut prefix_bytes)
        .map_err(|error| error.with_context(ErrorContext::Prefix))?;
    let prefix = parse_prefix(&prefix_bytes, offset, expected_sequence, limits)?;

    if prefix.total_length > remaining {
        return Ok(FrameRead::IncompleteTail);
    }

    let regions = FrameRegions::locate(offset, &prefix)?;
    verify_body(file, &prefix_bytes, &prefix, &regions)?;

    Ok(FrameRead::Complete(FrameMetadata {
        frame_type: prefix.frame_type,
        sequence: prefix.sequence,
        frame_offset: regions.frame_offset,
        header_offset: regions.header_offset,
        header_length: regions.header_length,
        payload_offset: regions.payload_offset,
        payload_length: regions.payload_length,
        total_length: prefix.total_length,
    }))
}

/// Read, exactly as stored, the commit trailer of the frame that ends at
/// `end_offset`.
///
/// The trailer is the whole committed identity of a frame in thirty-two
/// bytes: its total length, its sequence number, its body CRC, the trailer's
/// own CRC, and the commit magic. Two frames from different files agree on all
/// of it only if they hold the same bytes, so comparing a retained trailer
/// against the one at the same boundary today tells a snapshot whether the
/// bytes it already committed are still the bytes under it — without
/// re-reading a single frame body.
///
/// This deliberately does not validate what it read. The caller is comparing
/// it against a trailer that was validated when it was captured, so a trailer
/// that no longer parses simply fails that comparison, which is the answer the
/// caller wants rather than a second, differently shaped error.
pub(crate) fn read_commit_trailer(
    file: &mut File,
    file_size: u64,
    end_offset: u64,
) -> Result<[u8; TRAILER_SIZE]> {
    let trailer_offset = end_offset
        .checked_sub(TRAILER_SIZE as u64)
        .filter(|_| end_offset <= file_size)
        .ok_or_else(|| {
            Error::corruption(
                "the committed boundary does not follow a complete frame trailer",
                Some(end_offset),
            )
            .with_context(ErrorContext::File)
        })?;
    let mut bytes = [0_u8; TRAILER_SIZE];
    read_exact_at(file, trailer_offset, &mut bytes)
        .map_err(|error| error.with_context(ErrorContext::Trailer))?;
    Ok(bytes)
}

fn remaining_bytes(file_size: u64, offset: u64) -> Result<u64> {
    file_size.checked_sub(offset).ok_or_else(|| {
        Error::corruption("frame offset is beyond the file extent", Some(offset))
            .with_context(ErrorContext::File)
    })
}

/// A validated frame prefix.
#[derive(Debug, Clone, Copy)]
struct FramePrefix {
    frame_type: u16,
    header_length: u64,
    payload_length: u64,
    header_crc: u32,
    sequence: u64,
    total_length: u64,
}

/// The prefix exactly as stored, before any of it is trusted.
struct PrefixFields {
    magic: [u8; MAGIC_SIZE],
    frame_type: u16,
    frame_version: u16,
    frame_flags: u32,
    header_length: u64,
    payload_length: u64,
    sequence: u64,
    header_crc: u32,
    prefix_crc: u32,
}

fn parse_prefix(
    bytes: &[u8; PREFIX_SIZE],
    offset: u64,
    expected_sequence: u64,
    limits: Limits,
) -> Result<FramePrefix> {
    let fields = verify_prefix_integrity(bytes, offset)
        .map_err(|error| error.with_context(ErrorContext::Prefix))?;

    // The prefix CRC has now validated, so the stored sequence number is
    // trustworthy enough to name the frame in any remaining error.
    verify_prefix_fields(&fields, offset, expected_sequence, limits).map_err(|error| {
        error
            .with_context(ErrorContext::Frame {
                sequence: fields.sequence,
            })
            .with_context(ErrorContext::Prefix)
    })
}

/// Establish that the prefix is an undamaged Acta frame prefix.
///
/// Section 6.1 requires the prefix CRC to be validated before either declared
/// length is trusted, so nothing else may be checked ahead of it.
fn verify_prefix_integrity(bytes: &[u8; PREFIX_SIZE], offset: u64) -> Result<PrefixFields> {
    let fields = decode_prefix(bytes, offset)?;

    if fields.magic != FRAME_MAGIC {
        return Err(Error::corruption("bad frame magic", Some(offset)));
    }
    let covered = bytes.get(..PREFIX_CRC_FIELD).ok_or_else(|| {
        Error::corruption("frame prefix CRC coverage is out of bounds", Some(offset))
    })?;
    if fields.prefix_crc != checksum(covered) {
        return Err(Error::corruption("bad frame prefix CRC32C", Some(offset)));
    }

    Ok(fields)
}

fn verify_prefix_fields(
    fields: &PrefixFields,
    offset: u64,
    expected_sequence: u64,
    limits: Limits,
) -> Result<FramePrefix> {
    check_envelope(fields, offset)?;
    check_frame_type(fields.frame_type, expected_sequence, offset)?;
    check_sequence(fields.sequence, expected_sequence, offset)?;
    check_header_length(fields, offset)?;
    check_payload_length(fields, offset)?;
    check_limits(fields, offset, limits)?;

    Ok(FramePrefix {
        frame_type: fields.frame_type,
        header_length: fields.header_length,
        payload_length: fields.payload_length,
        header_crc: fields.header_crc,
        sequence: fields.sequence,
        total_length: total_frame_length(fields, offset)?,
    })
}

fn decode_prefix(bytes: &[u8; PREFIX_SIZE], offset: u64) -> Result<PrefixFields> {
    let mut cursor = Cursor::new(bytes, offset);
    let magic = cursor.read_array::<MAGIC_SIZE>("frame magic")?;
    let frame_type = cursor.read_u16("frame type")?;
    let frame_version = cursor.read_u16("frame version")?;
    let frame_flags = cursor.read_u32("frame flags")?;
    let header_length = u64::from(cursor.read_u32("frame header length")?);
    cursor.skip(PREFIX_RESERVED_SIZE, "frame prefix reserved field")?;
    let payload_length = cursor.read_u64("frame payload length")?;
    let sequence = cursor.read_u64("frame sequence number")?;
    let header_crc = cursor.read_u32("frame header CRC32C")?;
    let prefix_crc = cursor.read_u32("frame prefix CRC32C")?;

    Ok(PrefixFields {
        magic,
        frame_type,
        frame_version,
        frame_flags,
        header_length,
        payload_length,
        sequence,
        header_crc,
        prefix_crc,
    })
}

fn check_envelope(fields: &PrefixFields, offset: u64) -> Result<()> {
    if fields.frame_version != FRAME_VERSION {
        return Err(Error::unsupported_frame(
            format!(
                "unsupported frame envelope version {}",
                fields.frame_version
            ),
            field_offset(offset, PREFIX_FRAME_VERSION_FIELD),
        ));
    }
    if fields.frame_flags != FRAME_FLAGS {
        return Err(Error::unsupported_frame(
            format!("unsupported frame flags 0x{:x}", fields.frame_flags),
            field_offset(offset, PREFIX_FRAME_FLAGS_FIELD),
        ));
    }
    Ok(())
}

/// Reject frames this crate cannot interpret before any of the body is read.
fn check_frame_type(frame_type: u16, expected_sequence: u64, offset: u64) -> Result<()> {
    let expected = expected_frame_type(expected_sequence);
    if frame_type == expected {
        return Ok(());
    }

    let field = field_offset(offset, PREFIX_FRAME_TYPE_FIELD);
    if frame_type == CHECKPOINT_FRAME_TYPE {
        return Err(Error::unsupported_frame(
            "checkpoint frames are reserved for a later format minor version",
            field,
        ));
    }
    Err(Error::corruption(
        format!("expected frame type {expected}, found {frame_type}"),
        field,
    ))
}

/// Section 4 gives sequence zero to the schema frame and numbers data frames
/// contiguously from one, so the expected type follows from the sequence.
fn expected_frame_type(sequence: u64) -> u16 {
    if sequence == SCHEMA_FRAME_SEQUENCE {
        SCHEMA_FRAME_TYPE
    } else {
        DATA_FRAME_TYPE
    }
}

fn check_sequence(sequence: u64, expected_sequence: u64, offset: u64) -> Result<()> {
    if sequence != expected_sequence {
        return Err(Error::corruption(
            format!("expected frame sequence {expected_sequence}, found {sequence}"),
            field_offset(offset, PREFIX_SEQUENCE_FIELD),
        ));
    }
    Ok(())
}

fn check_header_length(fields: &PrefixFields, offset: u64) -> Result<()> {
    let field = field_offset(offset, PREFIX_HEADER_LENGTH_FIELD);
    let length = fields.header_length;

    if length % FRAME_ALIGNMENT != 0 {
        return Err(Error::corruption(
            format!("unaligned frame header length {length}"),
            field,
        ));
    }
    if fields.frame_type == SCHEMA_FRAME_TYPE && length != SCHEMA_FRAME_HEADER_SIZE {
        return Err(Error::corruption(
            format!("a schema frame header is {SCHEMA_FRAME_HEADER_SIZE} bytes, found {length}"),
            field,
        ));
    }
    if fields.frame_type == DATA_FRAME_TYPE && length < DATA_BLOCK_HEADER_SIZE {
        return Err(Error::corruption(
            format!(
                "a data frame header starts with a {DATA_BLOCK_HEADER_SIZE}-byte block header, \
                 found {length} header bytes"
            ),
            field,
        ));
    }
    Ok(())
}

fn check_payload_length(fields: &PrefixFields, offset: u64) -> Result<()> {
    if fields.payload_length % FRAME_ALIGNMENT != 0 {
        return Err(Error::corruption(
            format!("unaligned frame payload length {}", fields.payload_length),
            field_offset(offset, PREFIX_PAYLOAD_LENGTH_FIELD),
        ));
    }
    Ok(())
}

fn check_limits(fields: &PrefixFields, offset: u64, limits: Limits) -> Result<()> {
    if fields.header_length > limits.max_frame_header_length() {
        return Err(Error::resource_limit(
            format!(
                "frame header length {} exceeds the {}-byte limit",
                fields.header_length,
                limits.max_frame_header_length()
            ),
            field_offset(offset, PREFIX_HEADER_LENGTH_FIELD),
        ));
    }
    if fields.payload_length > limits.max_frame_payload_length() {
        return Err(Error::resource_limit(
            format!(
                "frame payload length {} exceeds the {}-byte limit",
                fields.payload_length,
                limits.max_frame_payload_length()
            ),
            field_offset(offset, PREFIX_PAYLOAD_LENGTH_FIELD),
        ));
    }
    Ok(())
}

fn total_frame_length(fields: &PrefixFields, offset: u64) -> Result<u64> {
    (PREFIX_SIZE as u64)
        .checked_add(fields.header_length)
        .and_then(|length| length.checked_add(fields.payload_length))
        .and_then(|length| length.checked_add(TRAILER_SIZE as u64))
        .ok_or_else(|| Error::corruption("frame length overflow", Some(offset)))
}

/// Where each part of a frame lives in the file.
///
/// Locating every region once keeps the frame's boundary arithmetic in one
/// place instead of spreading it across the checks that consume it.
struct FrameRegions {
    frame_offset: u64,
    header_offset: u64,
    header_length: u64,
    payload_offset: u64,
    payload_length: u64,
    trailer_offset: u64,
}

impl FrameRegions {
    fn locate(frame_offset: u64, prefix: &FramePrefix) -> Result<Self> {
        let overflowed = || {
            Error::corruption("frame region offset overflow", Some(frame_offset)).with_context(
                ErrorContext::Frame {
                    sequence: prefix.sequence,
                },
            )
        };

        let header_offset = frame_offset
            .checked_add(PREFIX_SIZE as u64)
            .ok_or_else(overflowed)?;
        let payload_offset = header_offset
            .checked_add(prefix.header_length)
            .ok_or_else(overflowed)?;
        let trailer_offset = payload_offset
            .checked_add(prefix.payload_length)
            .ok_or_else(overflowed)?;

        Ok(Self {
            frame_offset,
            header_offset,
            header_length: prefix.header_length,
            payload_offset,
            payload_length: prefix.payload_length,
            trailer_offset,
        })
    }
}

fn verify_body(
    file: &mut File,
    prefix_bytes: &[u8; PREFIX_SIZE],
    prefix: &FramePrefix,
    regions: &FrameRegions,
) -> Result<()> {
    let frame = || ErrorContext::Frame {
        sequence: prefix.sequence,
    };

    let mut trailer_bytes = [0_u8; TRAILER_SIZE];
    read_exact_at(file, regions.trailer_offset, &mut trailer_bytes).map_err(|error| {
        error
            .with_context(frame())
            .with_context(ErrorContext::Trailer)
    })?;
    let body_crc = parse_trailer(&trailer_bytes, prefix, regions.trailer_offset)?;

    let checksums =
        checksum_frame(file, prefix_bytes, regions).map_err(|error| error.with_context(frame()))?;

    if checksums.header != prefix.header_crc {
        return Err(
            Error::corruption("bad frame header CRC32C", Some(regions.header_offset))
                .with_context(frame())
                .with_context(ErrorContext::Header),
        );
    }
    if checksums.body != body_crc {
        return Err(
            Error::corruption("bad frame body CRC32C", Some(regions.frame_offset))
                .with_context(frame()),
        );
    }
    Ok(())
}

fn parse_trailer(bytes: &[u8; TRAILER_SIZE], prefix: &FramePrefix, offset: u64) -> Result<u32> {
    validate_trailer(bytes, prefix, offset).map_err(|error| {
        error
            .with_context(ErrorContext::Frame {
                sequence: prefix.sequence,
            })
            .with_context(ErrorContext::Trailer)
    })
}

fn validate_trailer(bytes: &[u8; TRAILER_SIZE], prefix: &FramePrefix, offset: u64) -> Result<u32> {
    let mut cursor = Cursor::new(bytes, offset);
    let total_length = cursor.read_u64("trailer total frame length")?;
    let sequence = cursor.read_u64("trailer sequence number")?;
    let body_crc = cursor.read_u32("trailer body CRC32C")?;
    let trailer_crc = cursor.read_u32("trailer CRC32C")?;
    let commit_magic = cursor.read_array::<MAGIC_SIZE>("commit magic")?;

    if total_length != prefix.total_length {
        return Err(Error::corruption(
            format!(
                "trailer total length {total_length} does not match prefix {}",
                prefix.total_length
            ),
            Some(offset),
        ));
    }
    if sequence != prefix.sequence {
        return Err(Error::corruption(
            format!(
                "trailer sequence {sequence} does not match prefix {}",
                prefix.sequence
            ),
            field_offset(offset, TRAILER_SEQUENCE_FIELD),
        ));
    }
    if commit_magic != COMMIT_MAGIC {
        return Err(Error::corruption(
            "bad frame commit magic",
            field_offset(offset, TRAILER_COMMIT_MAGIC_FIELD),
        ));
    }
    if trailer_crc != trailer_checksum(bytes, offset)? {
        return Err(Error::corruption(
            "bad frame trailer CRC32C",
            field_offset(offset, TRAILER_CRC_FIELD),
        ));
    }

    Ok(body_crc)
}

/// Section 6.2: the trailer CRC covers bytes `[0, 20)` then `[24, 32)`.
fn trailer_checksum(bytes: &[u8; TRAILER_SIZE], offset: u64) -> Result<u32> {
    let out_of_bounds = || Error::corruption("trailer CRC coverage is out of bounds", Some(offset));

    let mut checksum = Crc32c::new();
    checksum.update(bytes.get(..TRAILER_CRC_FIELD).ok_or_else(out_of_bounds)?);
    checksum.update(
        bytes
            .get(TRAILER_COMMIT_MAGIC_FIELD..)
            .ok_or_else(out_of_bounds)?,
    );
    Ok(checksum.finish())
}

struct FrameChecksums {
    header: u32,
    body: u32,
}

/// Compute the header and body CRCs in one pass over the frame.
///
/// Section 6.1 covers the padded header, and section 6.2 covers the stored
/// prefix, header, and payload. The two regions overlap, so both accumulators
/// are fed from the same reads and every byte is read exactly once.
fn checksum_frame(
    file: &mut File,
    prefix_bytes: &[u8; PREFIX_SIZE],
    regions: &FrameRegions,
) -> Result<FrameChecksums> {
    let mut header = Crc32c::new();
    let mut body = Crc32c::new();
    body.update(prefix_bytes);

    let mut reader = RegionReader::new();
    reader.read(
        file,
        regions.header_offset,
        regions.header_length,
        |chunk| {
            header.update(chunk);
            body.update(chunk);
        },
    )?;
    reader.read(
        file,
        regions.payload_offset,
        regions.payload_length,
        |chunk| body.update(chunk),
    )?;

    Ok(FrameChecksums {
        header: header.finish(),
        body: body.finish(),
    })
}

/// Streams file regions of any size through a fixed buffer.
struct RegionReader {
    buffer: Vec<u8>,
}

impl RegionReader {
    fn new() -> Self {
        Self {
            buffer: vec![0_u8; CHECKSUM_CHUNK_SIZE],
        }
    }

    fn read(
        &mut self,
        file: &mut File,
        offset: u64,
        length: u64,
        mut visit: impl FnMut(&[u8]),
    ) -> Result<()> {
        if length == 0 {
            return Ok(());
        }

        seek(file, offset)?;
        let mut remaining = length;
        while remaining != 0 {
            let chunk_length = remaining.min(self.buffer.len() as u64) as usize;
            let chunk = &mut self.buffer[..chunk_length];
            file.read_exact(chunk)
                .map_err(|error| Error::io(error, Some(offset)))?;
            visit(chunk);
            remaining -= chunk_length as u64;
        }
        Ok(())
    }
}
