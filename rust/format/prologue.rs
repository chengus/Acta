use crate::crc32c::checksum;
use crate::error::{Error, ErrorContext, Result};

use super::constants::{
    FILE_ID_SIZE, FILE_MAGIC, FORMAT_MAJOR, FORMAT_MINOR, MAGIC_SIZE, PROLOGUE_CRC_FIELD,
    PROLOGUE_FEATURE_FLAGS_FIELD, PROLOGUE_FORMAT_VERSION_FIELD, PROLOGUE_RESERVED_SIZE,
    PROLOGUE_SCHEMA_FRAME_OFFSET_FIELD, PROLOGUE_SIZE, PROLOGUE_SIZE_FIELD, SCHEMA_FRAME_OFFSET,
    SUPPORTED_FEATURES,
};
use super::cursor::Cursor;
use super::read_exact_at;

/// The parts of a validated prologue the rest of the crate needs.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Prologue {
    pub(crate) format_version: (u16, u16),
    pub(crate) feature_flags: u64,
    pub(crate) file_id: [u8; FILE_ID_SIZE],
}

/// The prologue exactly as stored, before any of it is trusted.
struct PrologueFields {
    magic: [u8; MAGIC_SIZE],
    format_major: u16,
    format_minor: u16,
    prologue_size: u32,
    feature_flags: u64,
    file_id: [u8; FILE_ID_SIZE],
    schema_frame_offset: u64,
    stored_crc: u32,
}

pub(crate) fn parse(bytes: &[u8; PROLOGUE_SIZE]) -> Result<Prologue> {
    validate(bytes).map_err(|error| error.with_context(ErrorContext::Prologue))
}

/// Read and parse the fixed prologue after the caller has captured the file
/// extent. Both validator and reader use this entry point so only the byte
/// parser below defines prologue semantics.
pub(crate) fn read_from_file(file: &mut std::fs::File, file_size: u64) -> Result<Prologue> {
    if file_size < PROLOGUE_SIZE as u64 {
        return Err(Error::corruption(
            format!("file is shorter than the {PROLOGUE_SIZE}-byte prologue"),
            Some(file_size),
        )
        .with_context(ErrorContext::Prologue));
    }
    let mut bytes = [0_u8; PROLOGUE_SIZE];
    read_exact_at(file, 0, &mut bytes)
        .map_err(|error| error.with_context(ErrorContext::Prologue))?;
    parse(&bytes)
}

fn validate(bytes: &[u8; PROLOGUE_SIZE]) -> Result<Prologue> {
    let fields = decode(bytes)?;

    check_magic(&fields)?;
    check_crc(bytes, &fields)?;
    check_format_version(&fields)?;
    check_prologue_size(&fields)?;
    check_feature_flags(&fields)?;
    check_schema_frame_offset(&fields)?;

    Ok(Prologue {
        format_version: (fields.format_major, fields.format_minor),
        feature_flags: fields.feature_flags,
        file_id: fields.file_id,
    })
}

fn decode(bytes: &[u8; PROLOGUE_SIZE]) -> Result<PrologueFields> {
    let mut cursor = Cursor::new(bytes, 0);
    let magic = cursor.read_array::<MAGIC_SIZE>("file magic")?;
    let format_major = cursor.read_u16("format major")?;
    let format_minor = cursor.read_u16("format minor")?;
    let prologue_size = cursor.read_u32("prologue size")?;
    let feature_flags = cursor.read_u64("feature flags")?;
    let file_id = cursor.read_array::<FILE_ID_SIZE>("file ID")?;
    let schema_frame_offset = cursor.read_u64("schema frame offset")?;
    cursor.skip(PROLOGUE_RESERVED_SIZE, "prologue reserved bytes")?;
    let stored_crc = cursor.read_u32("prologue CRC32C")?;

    Ok(PrologueFields {
        magic,
        format_major,
        format_minor,
        prologue_size,
        feature_flags,
        file_id,
        schema_frame_offset,
        stored_crc,
    })
}

fn check_magic(fields: &PrologueFields) -> Result<()> {
    if fields.magic != FILE_MAGIC {
        return Err(Error::corruption("bad file magic", Some(0)));
    }
    Ok(())
}

fn check_crc(bytes: &[u8; PROLOGUE_SIZE], fields: &PrologueFields) -> Result<()> {
    let covered = bytes
        .get(..PROLOGUE_CRC_FIELD)
        .ok_or_else(|| Error::corruption("prologue CRC coverage is out of bounds", Some(0)))?;
    if fields.stored_crc != checksum(covered) {
        return Err(Error::corruption("bad prologue CRC32C", Some(0)));
    }
    Ok(())
}

fn check_format_version(fields: &PrologueFields) -> Result<()> {
    let version = (fields.format_major, fields.format_minor);
    if version == (FORMAT_MAJOR, FORMAT_MINOR) {
        return Ok(());
    }
    Err(Error::unsupported_version(
        format!(
            "unsupported Acta format version ({}, {}); this crate supports \
             ({FORMAT_MAJOR}, {FORMAT_MINOR})",
            fields.format_major, fields.format_minor
        ),
        Some(PROLOGUE_FORMAT_VERSION_FIELD as u64),
    ))
}

fn check_prologue_size(fields: &PrologueFields) -> Result<()> {
    if u64::from(fields.prologue_size) != PROLOGUE_SIZE as u64 {
        return Err(Error::corruption(
            format!(
                "prologue size is {PROLOGUE_SIZE} in v0.2, found {}",
                fields.prologue_size
            ),
            Some(PROLOGUE_SIZE_FIELD as u64),
        ));
    }
    Ok(())
}

fn check_feature_flags(fields: &PrologueFields) -> Result<()> {
    let unknown = fields.feature_flags & !SUPPORTED_FEATURES;
    if unknown != 0 {
        return Err(Error::unsupported_feature(
            format!("unknown prologue feature bits 0x{unknown:x}"),
            Some(PROLOGUE_FEATURE_FLAGS_FIELD as u64),
        ));
    }
    Ok(())
}

fn check_schema_frame_offset(fields: &PrologueFields) -> Result<()> {
    if fields.schema_frame_offset != SCHEMA_FRAME_OFFSET {
        return Err(Error::corruption(
            format!(
                "schema frame offset is {SCHEMA_FRAME_OFFSET} in v0.2, found {}",
                fields.schema_frame_offset
            ),
            Some(PROLOGUE_SCHEMA_FRAME_OFFSET_FIELD as u64),
        ));
    }
    Ok(())
}
