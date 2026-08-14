//! Human-readable rendering of the metadata a [`Reader`] snapshot exposes.
//!
//! Everything here goes through the public reader API, so what the command
//! prints is exactly what a library caller sees. The tool never parses Acta
//! bytes itself, which is what makes its output evidence about the reader.

use std::path::Path;

use acta::{BlockMetadata, FileMetadata, LogicalType, Reader, Schema, TimeUnit, TimeZone};

/// Printed in a table cell that has no value for this file.
const ABSENT: &str = "-";

/// Printed where a file-level field has nothing to report.
const NOTHING: &str = "none";

/// Printed in place of the block table when a file carries no data blocks.
const NO_BLOCKS: &str = "(no data blocks)";

/// Blank columns between two rendered table columns.
const COLUMN_GAP: usize = 2;

/// The only feature bit v0.2 assigns. [`Reader::open`] rejects every other one,
/// so a snapshot's flags can only ever be this bit or nothing.
const ROW_IDS_FEATURE: u64 = 1;

/// Open `path` and render its schema and block metadata.
pub fn inspect_path(path: &Path) -> acta::Result<String> {
    Ok(format_inspection(&Reader::open(path)?))
}

/// Render the file, schema, and block metadata of an open snapshot.
pub fn format_inspection(reader: &Reader) -> String {
    let (major, minor) = reader.file_metadata().format_version();
    let sections = [
        format!("Acta v{major}.{minor}"),
        section("File", &file_section(reader)),
        section("Schema", &schema_section(reader.schema())),
        section("Blocks", &block_section(reader.blocks())),
    ];
    format!("{}\n", sections.join("\n\n"))
}

fn section(title: &str, body: &str) -> String {
    format!("{title}\n{}\n{body}", "-".repeat(title.len()))
}

// ---------------------------------------------------------------------- file

fn file_section(reader: &Reader) -> String {
    let metadata = reader.file_metadata();
    field_list(&[
        ("file id", hexadecimal(metadata.file_id())),
        ("features", features(metadata.feature_flags())),
        ("schema id", metadata.schema_id().to_string()),
        ("size", format!("{} bytes", metadata.file_size())),
        ("blocks", metadata.block_count().to_string()),
        ("rows", metadata.total_rows().to_string()),
        ("primary", primary_column(reader.schema())),
        ("tail", tail(metadata)),
    ])
}

/// Label-and-value lines whose values start in one column.
fn field_list(fields: &[(&str, String)]) -> String {
    let width = fields
        .iter()
        .map(|(label, _)| label.chars().count())
        .max()
        .unwrap_or_default();

    fields
        .iter()
        .map(|(label, value)| {
            let label = format!("{label}:");
            format!("{label:<0$} {value}", width + 1)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn hexadecimal(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn features(feature_flags: u64) -> String {
    if feature_flags & ROW_IDS_FEATURE != 0 {
        return "ROW_IDS".to_owned();
    }
    NOTHING.to_owned()
}

fn primary_column(schema: &Schema) -> String {
    match schema.primary_column() {
        Some(column) => format!("{} (column {})", column.name(), column.id()),
        None => NOTHING.to_owned(),
    }
}

fn tail(metadata: &FileMetadata) -> String {
    if metadata.incomplete_tail() {
        return format!(
            "incomplete, resume at offset {}",
            metadata.last_good_offset()
        );
    }
    "complete".to_owned()
}

// -------------------------------------------------------------------- schema

fn schema_section(schema: &Schema) -> String {
    let mut table = Table::new(&["ID", "Name", "Type", "Nullable", "Primary"]);
    for column in schema.columns() {
        table.push(vec![
            column.id().to_string(),
            column.name().to_owned(),
            logical_type(column.logical_type()),
            yes_or_no(column.is_nullable()),
            yes_or_no(schema.primary_column_id() == Some(column.id())),
        ]);
    }
    table.render()
}

/// Name a logical type the way section 3 describes it, parameters included.
fn logical_type(logical_type: &LogicalType) -> String {
    match logical_type {
        LogicalType::Bool => "bool".to_owned(),
        LogicalType::Int8 => "int8".to_owned(),
        LogicalType::Int16 => "int16".to_owned(),
        LogicalType::Int32 => "int32".to_owned(),
        LogicalType::Int64 => "int64".to_owned(),
        LogicalType::UInt8 => "uint8".to_owned(),
        LogicalType::UInt16 => "uint16".to_owned(),
        LogicalType::UInt32 => "uint32".to_owned(),
        LogicalType::UInt64 => "uint64".to_owned(),
        LogicalType::Float32 => "float32".to_owned(),
        LogicalType::Float64 => "float64".to_owned(),
        LogicalType::Utf8 => "utf8".to_owned(),
        LogicalType::Binary => "binary".to_owned(),
        LogicalType::Date32 => "date32".to_owned(),
        LogicalType::Decimal { precision, scale } => {
            format!("decimal64(precision={precision}, scale={scale})")
        }
        LogicalType::Timestamp { unit, timezone } => {
            format!("timestamp64({}, {})", time_unit(unit), time_zone(timezone))
        }
        LogicalType::Categorical { ordered } => format!("categorical(ordered={ordered})"),
        LogicalType::FixedBinary { byte_width } => format!("fixed_binary({byte_width})"),
    }
}

fn time_unit(unit: &TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}

fn time_zone(timezone: &TimeZone) -> &str {
    match timezone {
        TimeZone::Naive => "naive",
        TimeZone::Utc => "UTC",
        TimeZone::Iana(name) => name,
    }
}

// -------------------------------------------------------------------- blocks

fn block_section(blocks: &[BlockMetadata]) -> String {
    if blocks.is_empty() {
        return NO_BLOCKS.to_owned();
    }

    let mut table = Table::new(&[
        "Seq",
        "Offset",
        "Bytes",
        "Rows",
        "Base Row",
        "Primary Min",
        "Primary Max",
        "Sorted",
    ]);
    for block in blocks {
        let (minimum, maximum) = primary_bounds(block);
        table.push(vec![
            block.sequence().to_string(),
            block.file_offset().to_string(),
            block.total_length().to_string(),
            block.row_count().to_string(),
            number_or_absent(block.base_row_id()),
            minimum,
            maximum,
            yes_or_no(block.ts_sorted()),
        ]);
    }
    table.render()
}

/// The block's declared bounds as stored: timestamp units or signed day counts.
///
/// Rendering them as instants would need the primary column's unit and
/// timezone, and a date is a calendar identity rather than an instant, so the
/// raw counts stay until a decoder can give them their type.
fn primary_bounds(block: &BlockMetadata) -> (String, String) {
    match block.primary_bounds() {
        Some(bounds) => (bounds.min().to_string(), bounds.max().to_string()),
        None => (ABSENT.to_owned(), ABSENT.to_owned()),
    }
}

fn number_or_absent(value: Option<u64>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => ABSENT.to_owned(),
    }
}

fn yes_or_no(value: bool) -> String {
    if value {
        return "yes".to_owned();
    }
    "no".to_owned()
}

// --------------------------------------------------------------------- table

/// A left-aligned text table sized to its own contents.
///
/// Widths follow the data, so output is deterministic for a given file and
/// needs no table-rendering dependency. The first row holds the headings, which
/// lets them size their columns like any other row.
struct Table {
    rows: Vec<Vec<String>>,
}

impl Table {
    fn new(headings: &[&str]) -> Self {
        Self {
            rows: vec![
                headings
                    .iter()
                    .map(|heading| (*heading).to_owned())
                    .collect(),
            ],
        }
    }

    fn push(&mut self, cells: Vec<String>) {
        self.rows.push(cells);
    }

    fn render(&self) -> String {
        let widths = self.column_widths();
        self.rows
            .iter()
            .map(|row| render_row(row, &widths))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Column names and values are UTF-8, so columns are sized in characters.
    fn column_widths(&self) -> Vec<usize> {
        let mut widths = vec![0; self.rows.first().map_or(0, Vec::len)];
        for row in &self.rows {
            for (width, cell) in widths.iter_mut().zip(row) {
                *width = (*width).max(cell.chars().count());
            }
        }
        widths
    }
}

/// Pad every cell, then drop the padding the final cell does not need.
fn render_row(cells: &[String], widths: &[usize]) -> String {
    cells
        .iter()
        .zip(widths)
        .map(|(cell, width)| format!("{cell:<width$}"))
        .collect::<Vec<_>>()
        .join(&" ".repeat(COLUMN_GAP))
        .trim_end()
        .to_owned()
}
