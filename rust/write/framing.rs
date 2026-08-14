//! Prologue, schema-frame, frame-envelope, and wire-writing helpers.

use std::fs::File;
use std::io::Write;

use crate::crc32c::{Crc32c, checksum};
use crate::error::{Error, ErrorContext, Result};
use crate::format::constants::*;
use crate::limits::Limits;
use crate::schema::{LogicalType, Schema, TimeUnit, TimeZone};

use super::{internal, invalid_schema, resource};

pub(super) fn build_prologue(feature_flags: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(PROLOGUE_SIZE);
    bytes.extend_from_slice(&FILE_MAGIC);
    push_u16(&mut bytes, FORMAT_MAJOR);
    push_u16(&mut bytes, FORMAT_MINOR);
    push_u32(&mut bytes, PROLOGUE_SIZE as u32);
    push_u64(&mut bytes, feature_flags);
    // A fixed identity makes Stage 6 serialization byte-for-byte
    // deterministic. The ID is opaque metadata, not a content hash.
    bytes.extend_from_slice(&[0; FILE_ID_SIZE]);
    push_u64(&mut bytes, SCHEMA_FRAME_OFFSET);
    bytes.extend_from_slice(&[0; PROLOGUE_RESERVED_SIZE]);
    let crc = checksum(&bytes);
    push_u32(&mut bytes, crc);
    debug_assert_eq!(bytes.len(), PROLOGUE_SIZE);
    bytes
}

pub(super) fn build_schema_frame(schema: &Schema) -> Result<Vec<u8>> {
    let mut header = Vec::with_capacity(SCHEMA_FRAME_HEADER_SIZE as usize);
    push_u64(&mut header, schema.schema_id());
    push_u32(
        &mut header,
        u32::try_from(schema.column_count())
            .map_err(|_| invalid_schema("schema column count exceeds uint32::MAX"))?,
    );
    push_u32(
        &mut header,
        schema.primary_column_id().unwrap_or(NO_PRIMARY_COLUMN_ID),
    );
    push_u32(&mut header, 0);
    push_u32(&mut header, 0);

    // Section 7 requires descriptors in schema order, which is the order a
    // reader reports the columns in. Only a data frame's column table is sorted
    // by column ID.
    let mut payload = Vec::new();
    for column in schema.columns() {
        let (type_id, parameters) = type_id_and_parameters(column.logical_type())?;
        if u64::try_from(parameters.len()).unwrap_or(u64::MAX)
            > Limits::default().max_schema_field_length()
        {
            return Err(resource(
                "type parameters exceed the default schema field limit",
            ));
        }
        let name = column.name().as_bytes();
        let content_length = (SCHEMA_DESCRIPTOR_SIZE as usize)
            .checked_add(name.len())
            .and_then(|length| length.checked_add(parameters.len()))
            .ok_or_else(|| invalid_schema("schema descriptor length overflows"))?;
        let descriptor_length = padded_length(content_length)?;
        let descriptor_length_u32 = u32::try_from(descriptor_length)
            .map_err(|_| invalid_schema("schema descriptor exceeds uint32::MAX"))?;
        push_u32(&mut payload, descriptor_length_u32);
        push_u32(&mut payload, column.id());
        push_u16(&mut payload, type_id);
        push_u16(
            &mut payload,
            if column.is_nullable() {
                NULLABLE_COLUMN_FLAG
            } else {
                0
            },
        );
        push_u32(
            &mut payload,
            u32::try_from(name.len())
                .map_err(|_| invalid_schema("column name exceeds uint32::MAX"))?,
        );
        push_u32(
            &mut payload,
            u32::try_from(parameters.len())
                .map_err(|_| invalid_schema("type parameters exceed uint32::MAX"))?,
        );
        push_u32(&mut payload, 0);
        payload.extend_from_slice(name);
        payload.extend_from_slice(&parameters);
        payload.resize(payload.len() + descriptor_length - content_length, 0);
    }
    if u64::try_from(payload.len()).unwrap_or(u64::MAX)
        > Limits::default().max_frame_payload_length()
    {
        return Err(resource(
            "the schema frame payload exceeds the default frame limit",
        ));
    }
    build_frame(SCHEMA_FRAME_TYPE, SCHEMA_FRAME_SEQUENCE, &header, &payload)
}

pub(super) fn type_id_and_parameters(logical_type: &LogicalType) -> Result<(u16, Vec<u8>)> {
    let (type_id, parameters) = match logical_type {
        LogicalType::Bool => (TYPE_BOOL, Vec::new()),
        LogicalType::Int8 => (TYPE_INT8, Vec::new()),
        LogicalType::Int16 => (TYPE_INT16, Vec::new()),
        LogicalType::Int32 => (TYPE_INT32, Vec::new()),
        LogicalType::Int64 => (TYPE_INT64, Vec::new()),
        LogicalType::UInt8 => (TYPE_UINT8, Vec::new()),
        LogicalType::UInt16 => (TYPE_UINT16, Vec::new()),
        LogicalType::UInt32 => (TYPE_UINT32, Vec::new()),
        LogicalType::UInt64 => (TYPE_UINT64, Vec::new()),
        LogicalType::Float32 => (TYPE_FLOAT32, Vec::new()),
        LogicalType::Float64 => (TYPE_FLOAT64, Vec::new()),
        LogicalType::Decimal { precision, scale } => {
            if !(1..=18).contains(precision) {
                return Err(invalid_schema("decimal precision must be between 1 and 18"));
            }
            let mut bytes = Vec::with_capacity(8);
            push_u16(&mut bytes, *precision);
            push_u16(&mut bytes, *scale as u16);
            push_u32(&mut bytes, 0);
            (TYPE_DECIMAL64, bytes)
        }
        LogicalType::Timestamp { unit, timezone } => {
            let unit = match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 1,
                TimeUnit::Microsecond => 2,
                TimeUnit::Nanosecond => 3,
            };
            let (mode, name) = match timezone {
                TimeZone::Naive => (0, ""),
                TimeZone::Utc => (1, ""),
                TimeZone::Iana(name) if !name.is_empty() => (2, name.as_str()),
                TimeZone::Iana(_) => {
                    return Err(invalid_schema("an IANA timezone name cannot be empty"));
                }
            };
            let mut bytes = Vec::new();
            bytes.push(unit);
            bytes.push(mode);
            bytes.extend_from_slice(&[0; 2]);
            push_u32(
                &mut bytes,
                u32::try_from(name.len())
                    .map_err(|_| invalid_schema("timezone name exceeds uint32::MAX"))?,
            );
            bytes.extend_from_slice(name.as_bytes());
            pad_to_alignment(&mut bytes);
            (TYPE_TIMESTAMP64, bytes)
        }
        LogicalType::Utf8 => (TYPE_UTF8, Vec::new()),
        LogicalType::Categorical { ordered } => {
            let mut bytes = vec![0; 8];
            bytes[0] = u8::from(*ordered);
            (TYPE_CATEGORICAL, bytes)
        }
        LogicalType::Binary => (TYPE_BINARY, Vec::new()),
        LogicalType::FixedBinary { byte_width } => {
            if *byte_width == 0 {
                return Err(invalid_schema("fixed_binary width must be nonzero"));
            }
            let mut bytes = Vec::with_capacity(8);
            push_u32(&mut bytes, *byte_width);
            push_u32(&mut bytes, 0);
            (TYPE_FIXED_BINARY, bytes)
        }
        LogicalType::Date32 => (TYPE_DATE32, Vec::new()),
    };
    Ok((type_id, parameters))
}
pub(super) fn build_frame(
    frame_type: u16,
    sequence: u64,
    header: &[u8],
    payload: &[u8],
) -> Result<Vec<u8>> {
    if header.len() % FRAME_ALIGNMENT as usize != 0 || payload.len() % FRAME_ALIGNMENT as usize != 0
    {
        return Err(internal("the writer built an unaligned frame region"));
    }
    let header_length =
        u32::try_from(header.len()).map_err(|_| resource("frame header exceeds uint32::MAX"))?;
    let payload_length =
        u64::try_from(payload.len()).map_err(|_| resource("frame payload exceeds uint64::MAX"))?;
    let total_length = (PREFIX_SIZE as u64)
        .checked_add(u64::from(header_length))
        .and_then(|length| length.checked_add(payload_length))
        .and_then(|length| length.checked_add(TRAILER_SIZE as u64))
        .ok_or_else(|| resource("frame length overflows uint64"))?;

    let mut prefix = [0_u8; PREFIX_SIZE];
    prefix[..MAGIC_SIZE].copy_from_slice(&FRAME_MAGIC);
    prefix[PREFIX_FRAME_TYPE_FIELD..PREFIX_FRAME_TYPE_FIELD + 2]
        .copy_from_slice(&frame_type.to_le_bytes());
    prefix[PREFIX_FRAME_VERSION_FIELD..PREFIX_FRAME_VERSION_FIELD + 2]
        .copy_from_slice(&FRAME_VERSION.to_le_bytes());
    prefix[PREFIX_FRAME_FLAGS_FIELD..PREFIX_FRAME_FLAGS_FIELD + 4]
        .copy_from_slice(&FRAME_FLAGS.to_le_bytes());
    prefix[PREFIX_HEADER_LENGTH_FIELD..PREFIX_HEADER_LENGTH_FIELD + 4]
        .copy_from_slice(&header_length.to_le_bytes());
    prefix[PREFIX_PAYLOAD_LENGTH_FIELD..PREFIX_PAYLOAD_LENGTH_FIELD + 8]
        .copy_from_slice(&payload_length.to_le_bytes());
    prefix[PREFIX_SEQUENCE_FIELD..PREFIX_SEQUENCE_FIELD + 8]
        .copy_from_slice(&sequence.to_le_bytes());
    prefix[PREFIX_CRC_FIELD - 4..PREFIX_CRC_FIELD].copy_from_slice(&checksum(header).to_le_bytes());
    let prefix_crc = checksum(&prefix[..PREFIX_CRC_FIELD]);
    prefix[PREFIX_CRC_FIELD..PREFIX_CRC_FIELD + 4].copy_from_slice(&prefix_crc.to_le_bytes());

    let mut body_crc = Crc32c::new();
    body_crc.update(&prefix);
    body_crc.update(header);
    body_crc.update(payload);
    let body_crc = body_crc.finish();

    let mut trailer = [0_u8; TRAILER_SIZE];
    trailer[..8].copy_from_slice(&total_length.to_le_bytes());
    trailer[TRAILER_SEQUENCE_FIELD..TRAILER_SEQUENCE_FIELD + 8]
        .copy_from_slice(&sequence.to_le_bytes());
    trailer[TRAILER_CRC_FIELD - 4..TRAILER_CRC_FIELD].copy_from_slice(&body_crc.to_le_bytes());
    trailer[TRAILER_COMMIT_MAGIC_FIELD..].copy_from_slice(&COMMIT_MAGIC);
    let mut trailer_crc = Crc32c::new();
    trailer_crc.update(&trailer[..TRAILER_CRC_FIELD]);
    trailer_crc.update(&trailer[TRAILER_COMMIT_MAGIC_FIELD..]);
    trailer[TRAILER_CRC_FIELD..TRAILER_CRC_FIELD + 4]
        .copy_from_slice(&trailer_crc.finish().to_le_bytes());

    let mut frame = Vec::new();
    frame
        .try_reserve_exact(
            usize::try_from(total_length)
                .map_err(|_| resource("frame length does not fit this platform"))?,
        )
        .map_err(|_| resource("unable to reserve the frame to write"))?;
    frame.extend_from_slice(&prefix);
    frame.extend_from_slice(header);
    frame.extend_from_slice(payload);
    frame.extend_from_slice(&trailer);
    Ok(frame)
}

pub(super) fn write_initial(file: &mut File, prologue: &[u8], schema_frame: &[u8]) -> Result<()> {
    file.write_all(prologue)
        .map_err(|error| Error::io(error, Some(0)).with_context(ErrorContext::File))?;
    file.write_all(schema_frame).map_err(|error| {
        Error::io(error, Some(PROLOGUE_SIZE as u64)).with_context(ErrorContext::File)
    })?;
    Ok(())
}
pub(super) fn padded_length(length: usize) -> Result<usize> {
    length
        .checked_add(FRAME_ALIGNMENT as usize - 1)
        .map(|length| length & !(FRAME_ALIGNMENT as usize - 1))
        .ok_or_else(|| resource("aligned length overflows usize"))
}

pub(super) fn pad_to_alignment(bytes: &mut Vec<u8>) {
    let remainder = bytes.len() % FRAME_ALIGNMENT as usize;
    if remainder != 0 {
        bytes.resize(bytes.len() + FRAME_ALIGNMENT as usize - remainder, 0);
    }
}
pub(super) fn push_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn push_i64(bytes: &mut Vec<u8>, value: i64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}
