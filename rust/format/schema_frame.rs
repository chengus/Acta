//! Checked parsing of the v0.2 schema frame.

use std::collections::HashSet;
use std::fs::File;

use crate::error::{Error, ErrorContext, Result};
use crate::limits::Limits;
use crate::schema::{Column, LogicalType, Schema, TimeUnit, TimeZone};

use super::constants::{
    FRAME_ALIGNMENT, NO_PRIMARY_COLUMN_ID, NULLABLE_COLUMN_FLAG, SCHEMA_COLUMN_COUNT_FIELD,
    SCHEMA_DESCRIPTOR_COLUMN_ID_FIELD, SCHEMA_DESCRIPTOR_FLAGS_FIELD,
    SCHEMA_DESCRIPTOR_LENGTH_FIELD, SCHEMA_DESCRIPTOR_NAME_LENGTH_FIELD,
    SCHEMA_DESCRIPTOR_PARAMETERS_LENGTH_FIELD, SCHEMA_DESCRIPTOR_RESERVED_SIZE,
    SCHEMA_DESCRIPTOR_SIZE, SCHEMA_DESCRIPTOR_TYPE_FIELD, SCHEMA_FLAGS_FIELD,
    SCHEMA_FRAME_HEADER_SIZE, SCHEMA_ID_FIELD, SCHEMA_PRIMARY_COLUMN_ID_FIELD,
    SCHEMA_RESERVED_SIZE, TYPE_BINARY, TYPE_BOOL, TYPE_CATEGORICAL, TYPE_DATE32, TYPE_DECIMAL64,
    TYPE_FIXED_BINARY, TYPE_FLOAT32, TYPE_FLOAT64, TYPE_INT8, TYPE_INT16, TYPE_INT32, TYPE_INT64,
    TYPE_TIMESTAMP64, TYPE_UINT8, TYPE_UINT16, TYPE_UINT32, TYPE_UINT64, TYPE_UTF8,
};
use super::cursor::Cursor;
use super::frame::FrameMetadata;
use super::{field_offset, read_exact_at};

const DESCRIPTOR_PREFIX_SIZE: usize = 24;
const DESCRIPTOR_PREFIX_LENGTH: u64 = DESCRIPTOR_PREFIX_SIZE as u64;

/// Section 7 pads each type-parameter record to the frame alignment, and the
/// stored type-parameter length includes that padding.
const PARAMETER_ALIGNMENT: usize = FRAME_ALIGNMENT as usize;

const TIMESTAMP_PARAMETER_PREFIX_SIZE: usize = 8;
const DECIMAL_PARAMETER_SIZE: usize = 8;
const CATEGORICAL_PARAMETER_SIZE: usize = 8;
const FIXED_BINARY_PARAMETER_SIZE: usize = 8;

/// Parse and validate the schema frame after its generic framing has passed.
pub(crate) fn parse(file: &mut File, frame: &FrameMetadata, limits: Limits) -> Result<Schema> {
    check_frame_type(frame)?;

    let fields = read_header(file, frame)?;
    check_header(&fields, frame, limits)?;

    let columns = read_columns(file, frame, fields.column_count, limits)?;
    let primary_column_id = resolve_primary(&columns, &fields, frame)?;

    Ok(Schema::new(fields.schema_id, columns, primary_column_id))
}

fn check_frame_type(frame: &FrameMetadata) -> Result<()> {
    if frame.frame_type != super::constants::SCHEMA_FRAME_TYPE {
        return Err(
            Error::corruption("expected a schema frame", Some(frame.frame_offset)).with_context(
                ErrorContext::Frame {
                    sequence: frame.sequence,
                },
            ),
        );
    }
    Ok(())
}

fn check_header(fields: &SchemaHeaderFields, frame: &FrameMetadata, limits: Limits) -> Result<()> {
    let header_error = |error| schema_error(error, ErrorContext::Header);
    let at = |field| field_offset(frame.header_offset, field);

    if fields.schema_id == 0 {
        return Err(header_error(Error::corruption(
            "schema ID must be nonzero",
            at(SCHEMA_ID_FIELD),
        )));
    }
    if fields.column_count == 0 {
        return Err(header_error(Error::corruption(
            "schema column count must be nonzero",
            at(SCHEMA_COLUMN_COUNT_FIELD),
        )));
    }
    if fields.flags != 0 {
        return Err(header_error(Error::corruption(
            "schema flags are nonzero in v0.2",
            at(SCHEMA_FLAGS_FIELD),
        )));
    }
    if u64::from(fields.column_count) > limits.max_schema_columns() {
        return Err(header_error(Error::resource_limit(
            format!(
                "schema column count {} exceeds the {}-column limit",
                fields.column_count,
                limits.max_schema_columns()
            ),
            at(SCHEMA_COLUMN_COUNT_FIELD),
        )));
    }

    // Every descriptor occupies at least its own prefix, so the payload places a
    // tighter bound on the column count than the configured limit does. Checking
    // it before the columns are read keeps their reservation from trusting a
    // count the file cannot possibly justify.
    let declared_capacity = frame.payload_length / SCHEMA_DESCRIPTOR_SIZE;
    if u64::from(fields.column_count) > declared_capacity {
        return Err(header_error(Error::corruption(
            format!(
                "schema declares {} columns but its {}-byte payload holds at most \
                 {declared_capacity}",
                fields.column_count, frame.payload_length
            ),
            at(SCHEMA_COLUMN_COUNT_FIELD),
        )));
    }
    Ok(())
}

/// Read exactly `column_count` descriptors, which must fill the payload.
fn read_columns(
    file: &mut File,
    frame: &FrameMetadata,
    column_count: u32,
    limits: Limits,
) -> Result<Vec<Column>> {
    let mut columns = ColumnSet::with_capacity(column_count)?;
    let mut payload_position = 0_u64;

    for _ in 0..column_count {
        let descriptor = parse_descriptor(file, frame, payload_position, limits)?;
        payload_position = descriptor.next_position;
        columns.insert(descriptor)?;
    }

    if payload_position != frame.payload_length {
        return Err(schema_error(
            Error::corruption(
                format!(
                    "schema descriptors consume {payload_position} payload bytes, expected {}",
                    frame.payload_length
                ),
                Some(frame.payload_offset),
            ),
            ErrorContext::Payload,
        ));
    }
    Ok(columns.into_columns())
}

/// Resolve the section 7 primary selection against the declared columns.
///
/// Column ID zero is the sentinel for a schema without a primary timestamp
/// column, so it never identifies a descriptor.
fn resolve_primary(
    columns: &[Column],
    fields: &SchemaHeaderFields,
    frame: &FrameMetadata,
) -> Result<Option<u32>> {
    if fields.primary_column_id == NO_PRIMARY_COLUMN_ID {
        return Ok(None);
    }

    let invalid = |message| {
        schema_error(
            Error::corruption(
                message,
                field_offset(frame.header_offset, SCHEMA_PRIMARY_COLUMN_ID_FIELD),
            ),
            ErrorContext::Header,
        )
    };
    let column = columns
        .iter()
        .find(|column| column.id() == fields.primary_column_id)
        .ok_or_else(|| invalid("primary timestamp column ID is not declared"))?;

    if !is_primary_candidate(column) {
        return Err(invalid(
            "primary column must be a non-nullable timestamp or date32 column",
        ));
    }
    Ok(Some(fields.primary_column_id))
}

fn is_primary_candidate(column: &Column) -> bool {
    !column.is_nullable()
        && matches!(
            column.logical_type(),
            LogicalType::Timestamp { .. } | LogicalType::Date32
        )
}

/// The columns accepted so far, and the identity of each one.
///
/// Section 7 requires column IDs and names to be unique, but imposes no order
/// on the descriptors themselves; only the data-frame column table of section
/// 8.1 is sorted. Uniqueness is therefore established through hash sets rather
/// than a scan over the columns already accepted, which would cost time
/// quadratic in the column count of a wide schema.
struct ColumnSet {
    columns: Vec<Column>,
    ids: HashSet<u32>,
    names: HashSet<String>,
}

impl ColumnSet {
    fn with_capacity(column_count: u32) -> Result<Self> {
        let capacity = usize::try_from(column_count).map_err(|_| {
            schema_error(
                Error::resource_limit("schema column count does not fit this platform", None),
                ErrorContext::Header,
            )
        })?;
        let unavailable = |_| {
            schema_error(
                Error::resource_limit("unable to reserve schema column metadata", None),
                ErrorContext::Header,
            )
        };

        let mut columns = Vec::new();
        columns.try_reserve_exact(capacity).map_err(unavailable)?;
        let mut ids = HashSet::new();
        ids.try_reserve(capacity).map_err(unavailable)?;
        let mut names = HashSet::new();
        names.try_reserve(capacity).map_err(unavailable)?;

        Ok(Self {
            columns,
            ids,
            names,
        })
    }

    fn insert(&mut self, descriptor: Descriptor) -> Result<()> {
        if descriptor.column_id == 0 {
            return Err(schema_error(
                Error::corruption(
                    "schema column ID must be nonzero",
                    field_offset(descriptor.offset, SCHEMA_DESCRIPTOR_COLUMN_ID_FIELD),
                ),
                ErrorContext::Payload,
            ));
        }
        if !self.ids.insert(descriptor.column_id) {
            return Err(schema_error(
                Error::corruption(
                    "schema column ID is duplicated",
                    field_offset(descriptor.offset, SCHEMA_DESCRIPTOR_COLUMN_ID_FIELD),
                ),
                ErrorContext::Payload,
            ));
        }
        if !self.names.insert(descriptor.name.clone()) {
            return Err(schema_error(
                Error::corruption(
                    "schema column name is duplicated",
                    field_offset(descriptor.offset, SCHEMA_DESCRIPTOR_NAME_LENGTH_FIELD),
                ),
                ErrorContext::Payload,
            ));
        }

        self.columns.push(Column::new(
            descriptor.column_id,
            descriptor.name,
            descriptor.logical_type,
            descriptor.nullable,
        ));
        Ok(())
    }

    /// The columns in the order the schema frame declares them.
    fn into_columns(self) -> Vec<Column> {
        self.columns
    }
}

#[derive(Debug, Clone, Copy)]
struct SchemaHeaderFields {
    schema_id: u64,
    column_count: u32,
    primary_column_id: u32,
    flags: u32,
}

fn read_header(file: &mut File, frame: &FrameMetadata) -> Result<SchemaHeaderFields> {
    debug_assert_eq!(frame.header_length, SCHEMA_FRAME_HEADER_SIZE);
    let mut bytes = [0_u8; SCHEMA_FRAME_HEADER_SIZE as usize];
    read_exact_at(file, frame.header_offset, &mut bytes)
        .map_err(|error| schema_error(error, ErrorContext::Header))?;
    parse_header(&bytes, frame.header_offset)
        .map_err(|error| schema_error(error, ErrorContext::Header))
}

fn parse_header(
    bytes: &[u8; SCHEMA_FRAME_HEADER_SIZE as usize],
    offset: u64,
) -> Result<SchemaHeaderFields> {
    let mut cursor = Cursor::new(bytes, offset);
    let fields = SchemaHeaderFields {
        schema_id: cursor.read_u64("schema ID")?,
        column_count: cursor.read_u32("schema column count")?,
        primary_column_id: cursor.read_u32("primary timestamp column ID")?,
        flags: cursor.read_u32("schema flags")?,
    };
    cursor.skip(SCHEMA_RESERVED_SIZE, "schema reserved field")?;
    Ok(fields)
}

struct Descriptor {
    offset: u64,
    next_position: u64,
    column_id: u32,
    nullable: bool,
    name: String,
    logical_type: LogicalType,
}

struct DescriptorFields {
    length: u64,
    column_id: u32,
    type_id: u16,
    flags: u16,
    name_length: u64,
    parameters_length: u64,
}

fn parse_descriptor(
    file: &mut File,
    frame: &FrameMetadata,
    payload_position: u64,
    limits: Limits,
) -> Result<Descriptor> {
    let place = locate_descriptor(frame, payload_position)?;

    let mut bytes = [0_u8; DESCRIPTOR_PREFIX_SIZE];
    read_exact_at(file, place.offset, &mut bytes)
        .map_err(|error| schema_error(error, ErrorContext::Payload))?;
    let fields = parse_descriptor_fields(&bytes, place.offset)?;
    check_descriptor_fields(&fields, &place, limits)?;

    let name_offset = checked_offset(
        place.offset,
        DESCRIPTOR_PREFIX_LENGTH,
        "schema name offset overflow",
    )?;
    let name = read_name(file, name_offset, fields.name_length, limits)?;

    let parameters_offset = checked_offset(
        name_offset,
        fields.name_length,
        "schema type-parameter offset overflow",
    )?;
    let parameters = read_allocated(
        file,
        parameters_offset,
        fields.parameters_length,
        "type parameters",
        limits.max_schema_field_length(),
    )?;
    let logical_type = parse_type(fields.type_id, &parameters, parameters_offset, place.offset)?;

    let next_position = payload_position.checked_add(fields.length).ok_or_else(|| {
        schema_error(
            Error::corruption("schema payload position overflow", Some(place.offset)),
            ErrorContext::Payload,
        )
    })?;

    Ok(Descriptor {
        offset: place.offset,
        next_position,
        column_id: fields.column_id,
        nullable: fields.flags & NULLABLE_COLUMN_FLAG != 0,
        name,
        logical_type,
    })
}

/// Where a descriptor starts, and how much payload is left for it.
struct DescriptorPlace {
    offset: u64,
    remaining: u64,
}

fn locate_descriptor(frame: &FrameMetadata, payload_position: u64) -> Result<DescriptorPlace> {
    let offset = checked_offset(
        frame.payload_offset,
        payload_position,
        "schema descriptor offset overflow",
    )?;
    let payload_error = |message| {
        schema_error(
            Error::corruption(message, Some(offset)),
            ErrorContext::Payload,
        )
    };

    let remaining = frame
        .payload_length
        .checked_sub(payload_position)
        .ok_or_else(|| payload_error("schema descriptor offset exceeds payload"))?;
    if remaining < SCHEMA_DESCRIPTOR_SIZE {
        return Err(payload_error(
            "schema payload ends inside a column descriptor prefix",
        ));
    }
    Ok(DescriptorPlace { offset, remaining })
}

fn check_descriptor_fields(
    fields: &DescriptorFields,
    place: &DescriptorPlace,
    limits: Limits,
) -> Result<()> {
    let payload_error = |error| schema_error(error, ErrorContext::Payload);
    let at = |field| field_offset(place.offset, field);

    if fields.length < SCHEMA_DESCRIPTOR_SIZE || fields.length % FRAME_ALIGNMENT != 0 {
        return Err(payload_error(Error::corruption(
            format!("invalid schema descriptor length {}", fields.length),
            at(SCHEMA_DESCRIPTOR_LENGTH_FIELD),
        )));
    }
    if fields.length > place.remaining {
        return Err(payload_error(Error::corruption(
            "schema descriptor extends beyond its payload",
            at(SCHEMA_DESCRIPTOR_LENGTH_FIELD),
        )));
    }
    if fields.flags & !NULLABLE_COLUMN_FLAG != 0 {
        return Err(payload_error(Error::corruption(
            "unknown schema column flags",
            at(SCHEMA_DESCRIPTOR_FLAGS_FIELD),
        )));
    }

    let content_length = DESCRIPTOR_PREFIX_LENGTH
        .checked_add(fields.name_length)
        .and_then(|length| length.checked_add(fields.parameters_length))
        .ok_or_else(|| {
            payload_error(Error::corruption(
                "schema descriptor content length overflow",
                Some(place.offset),
            ))
        })?;
    if content_length > fields.length {
        return Err(payload_error(Error::corruption(
            "schema name and parameters exceed descriptor length",
            at(SCHEMA_DESCRIPTOR_LENGTH_FIELD),
        )));
    }

    check_field_length(
        fields.name_length,
        "column name",
        at(SCHEMA_DESCRIPTOR_NAME_LENGTH_FIELD),
        limits,
    )?;
    check_field_length(
        fields.parameters_length,
        "type-parameter",
        at(SCHEMA_DESCRIPTOR_PARAMETERS_LENGTH_FIELD),
        limits,
    )
}

fn check_field_length(length: u64, field: &str, offset: Option<u64>, limits: Limits) -> Result<()> {
    if length > limits.max_schema_field_length() {
        return Err(schema_error(
            Error::resource_limit(
                format!(
                    "{field} length {length} exceeds the {}-byte limit",
                    limits.max_schema_field_length()
                ),
                offset,
            ),
            ErrorContext::Payload,
        ));
    }
    Ok(())
}

fn read_name(file: &mut File, offset: u64, length: u64, limits: Limits) -> Result<String> {
    let bytes = read_allocated(
        file,
        offset,
        length,
        "column name",
        limits.max_schema_field_length(),
    )?;
    String::from_utf8(bytes).map_err(|_| {
        schema_error(
            Error::corruption("column name is not valid UTF-8", Some(offset)),
            ErrorContext::Payload,
        )
    })
}

fn parse_descriptor_fields(
    bytes: &[u8; DESCRIPTOR_PREFIX_SIZE],
    offset: u64,
) -> Result<DescriptorFields> {
    let mut cursor = Cursor::new(bytes, offset);
    let fields = DescriptorFields {
        length: u64::from(cursor.read_u32("schema descriptor length")?),
        column_id: cursor.read_u32("schema column ID")?,
        type_id: cursor.read_u16("schema logical type ID")?,
        flags: cursor.read_u16("schema column flags")?,
        name_length: u64::from(cursor.read_u32("schema name length")?),
        parameters_length: u64::from(cursor.read_u32("schema type-parameter length")?),
    };
    cursor.skip(
        SCHEMA_DESCRIPTOR_RESERVED_SIZE,
        "schema descriptor reserved field",
    )?;
    Ok(fields)
}

fn parse_type(
    type_id: u16,
    parameters: &[u8],
    parameters_offset: u64,
    descriptor_offset: u64,
) -> Result<LogicalType> {
    let invalid_length = || {
        schema_error(
            Error::corruption(
                format!(
                    "invalid parameter length {} for logical type {type_id}",
                    parameters.len()
                ),
                field_offset(descriptor_offset, SCHEMA_DESCRIPTOR_PARAMETERS_LENGTH_FIELD),
            ),
            ErrorContext::Payload,
        )
    };

    match type_id {
        TYPE_BOOL => no_parameters(parameters, invalid_length, LogicalType::Bool),
        TYPE_INT8 => no_parameters(parameters, invalid_length, LogicalType::Int8),
        TYPE_INT16 => no_parameters(parameters, invalid_length, LogicalType::Int16),
        TYPE_INT32 => no_parameters(parameters, invalid_length, LogicalType::Int32),
        TYPE_INT64 => no_parameters(parameters, invalid_length, LogicalType::Int64),
        TYPE_UINT8 => no_parameters(parameters, invalid_length, LogicalType::UInt8),
        TYPE_UINT16 => no_parameters(parameters, invalid_length, LogicalType::UInt16),
        TYPE_UINT32 => no_parameters(parameters, invalid_length, LogicalType::UInt32),
        TYPE_UINT64 => no_parameters(parameters, invalid_length, LogicalType::UInt64),
        TYPE_FLOAT32 => no_parameters(parameters, invalid_length, LogicalType::Float32),
        TYPE_FLOAT64 => no_parameters(parameters, invalid_length, LogicalType::Float64),
        TYPE_UTF8 => no_parameters(parameters, invalid_length, LogicalType::Utf8),
        TYPE_BINARY => no_parameters(parameters, invalid_length, LogicalType::Binary),
        TYPE_DATE32 => no_parameters(parameters, invalid_length, LogicalType::Date32),
        TYPE_DECIMAL64 => parse_decimal(parameters, parameters_offset, invalid_length),
        TYPE_TIMESTAMP64 => parse_timestamp(parameters, parameters_offset, invalid_length),
        TYPE_CATEGORICAL => parse_categorical(parameters, parameters_offset, invalid_length),
        TYPE_FIXED_BINARY => parse_fixed_binary(parameters, parameters_offset, invalid_length),
        _ => Err(schema_error(
            Error::unsupported_frame(
                format!("unsupported logical type ID {type_id}"),
                field_offset(descriptor_offset, SCHEMA_DESCRIPTOR_TYPE_FIELD),
            ),
            ErrorContext::Payload,
        )),
    }
}

fn no_parameters<T>(parameters: &[u8], invalid_length: impl Fn() -> Error, value: T) -> Result<T> {
    if parameters.is_empty() {
        Ok(value)
    } else {
        Err(invalid_length())
    }
}

fn parse_decimal(
    parameters: &[u8],
    offset: u64,
    invalid_length: impl Fn() -> Error,
) -> Result<LogicalType> {
    if parameters.len() != DECIMAL_PARAMETER_SIZE {
        return Err(invalid_length());
    }
    let mut cursor = Cursor::new(parameters, offset);
    let precision = cursor.read_u16("decimal precision")?;
    let scale = cursor.read_u16("decimal scale")? as i16;
    let _reserved = cursor.read_u32("decimal reserved field")?;
    if !(1..=18).contains(&precision) {
        return Err(schema_error(
            Error::corruption("decimal precision must be between 1 and 18", Some(offset)),
            ErrorContext::Payload,
        ));
    }
    Ok(LogicalType::Decimal { precision, scale })
}

/// Decode a `timestamp64` parameter record.
///
/// Section 7 pads the record itself to the next eight-byte boundary after the
/// timezone name, so the stored type-parameter length is the eight-byte fixed
/// part plus the name rounded up to a multiple of eight. Descriptor padding is
/// separate and follows the parameters.
fn parse_timestamp(
    parameters: &[u8],
    offset: u64,
    invalid_length: impl Fn() -> Error,
) -> Result<LogicalType> {
    if parameters.len() < TIMESTAMP_PARAMETER_PREFIX_SIZE {
        return Err(invalid_length());
    }
    let mut cursor = Cursor::new(parameters, offset);
    let unit = match cursor.read_bytes(1, "timestamp unit")?[0] {
        0 => TimeUnit::Second,
        1 => TimeUnit::Millisecond,
        2 => TimeUnit::Microsecond,
        3 => TimeUnit::Nanosecond,
        value => {
            return Err(schema_error(
                Error::corruption(format!("unknown timestamp unit {value}"), Some(offset)),
                ErrorContext::Payload,
            ));
        }
    };
    let timezone_mode = cursor.read_bytes(1, "timestamp timezone mode")?[0];
    let _reserved = cursor.read_u16("timestamp reserved field")?;
    let name_length = usize::try_from(cursor.read_u32("timezone name length")?).map_err(|_| {
        schema_error(
            Error::corruption(
                "timezone name length does not fit this platform",
                Some(offset),
            ),
            ErrorContext::Payload,
        )
    })?;
    let name_start = TIMESTAMP_PARAMETER_PREFIX_SIZE;
    let name_end = name_start.checked_add(name_length).ok_or_else(|| {
        schema_error(
            Error::corruption("timezone name length overflows parameters", Some(offset)),
            ErrorContext::Payload,
        )
    })?;
    let expected_length = name_end
        .checked_next_multiple_of(PARAMETER_ALIGNMENT)
        .ok_or_else(|| {
            schema_error(
                Error::corruption("timestamp parameter length overflow", Some(offset)),
                ErrorContext::Payload,
            )
        })?;
    if expected_length != parameters.len() || name_end > parameters.len() {
        return Err(invalid_length());
    }
    let timezone = match timezone_mode {
        0 if name_length == 0 => TimeZone::Naive,
        1 if name_length == 0 => TimeZone::Utc,
        2 if name_length != 0 => {
            let name = std::str::from_utf8(&parameters[name_start..name_end]).map_err(|_| {
                schema_error(
                    Error::corruption("timezone name is not valid UTF-8", Some(offset)),
                    ErrorContext::Payload,
                )
            })?;
            TimeZone::Iana(name.to_owned())
        }
        _ => {
            return Err(schema_error(
                Error::corruption("invalid timestamp timezone mode or name", Some(offset)),
                ErrorContext::Payload,
            ));
        }
    };
    Ok(LogicalType::Timestamp { unit, timezone })
}

fn parse_categorical(
    parameters: &[u8],
    offset: u64,
    invalid_length: impl Fn() -> Error,
) -> Result<LogicalType> {
    if parameters.len() != CATEGORICAL_PARAMETER_SIZE {
        return Err(invalid_length());
    }
    if parameters[0] > 1 {
        return Err(schema_error(
            Error::corruption("categorical ordered flag must be zero or one", Some(offset)),
            ErrorContext::Payload,
        ));
    }
    Ok(LogicalType::Categorical {
        ordered: parameters[0] != 0,
    })
}

fn parse_fixed_binary(
    parameters: &[u8],
    offset: u64,
    invalid_length: impl Fn() -> Error,
) -> Result<LogicalType> {
    if parameters.len() != FIXED_BINARY_PARAMETER_SIZE {
        return Err(invalid_length());
    }
    let mut cursor = Cursor::new(parameters, offset);
    let width = cursor.read_u32("fixed_binary byte width")?;
    let _reserved = cursor.read_u32("fixed_binary reserved field")?;
    if width == 0 {
        return Err(schema_error(
            Error::corruption("fixed_binary width must be nonzero", Some(offset)),
            ErrorContext::Payload,
        ));
    }
    Ok(LogicalType::FixedBinary { byte_width: width })
}

fn read_allocated(
    file: &mut File,
    offset: u64,
    length: u64,
    field: &str,
    max_length: u64,
) -> Result<Vec<u8>> {
    if length > max_length {
        return Err(schema_error(
            Error::resource_limit(
                format!("{field} length {length} exceeds the {max_length}-byte limit"),
                Some(offset),
            ),
            ErrorContext::Payload,
        ));
    }
    let length = usize::try_from(length).map_err(|_| {
        schema_error(
            Error::resource_limit(format!("{field} does not fit this platform"), Some(offset)),
            ErrorContext::Payload,
        )
    })?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(|_| {
        schema_error(
            Error::resource_limit(format!("unable to allocate {field}"), Some(offset)),
            ErrorContext::Payload,
        )
    })?;
    bytes.resize(length, 0);
    read_exact_at(file, offset, &mut bytes)
        .map_err(|error| schema_error(error, ErrorContext::Payload))?;
    Ok(bytes)
}

fn checked_offset(base: u64, relative: u64, message: &str) -> Result<u64> {
    base.checked_add(relative).ok_or_else(|| {
        schema_error(
            Error::corruption(message, Some(base)),
            ErrorContext::Payload,
        )
    })
}

fn schema_error(error: Error, region: ErrorContext) -> Error {
    error
        .with_context(ErrorContext::Frame { sequence: 0 })
        .with_context(region)
}
