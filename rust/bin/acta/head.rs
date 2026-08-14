//! Human-readable rendering of the first logical rows in a snapshot.

use std::fmt::Write as _;
use std::path::Path;

use acta::{Reader, Result, ScalarValue, TimeUnit};

const COLUMN_GAP: usize = 2;

/// The widest cell `head` prints before eliding the rest of the value.
///
/// A v0.2 value may be up to `UINT32_MAX` bytes, and every cell in a column is
/// padded to the widest one, so an unbounded cell would set the width of the
/// whole table.
const MAX_CELL_CHARACTERS: usize = 64;

/// Marks a cell the width above cut short.
const ELISION: char = '…';

/// Open a snapshot and render up to row_limit rows in file order.
pub fn head_path(path: &Path, row_limit: usize) -> Result<String> {
    let reader = Reader::open(path)?;
    let mut table = Table::new(
        reader
            .schema()
            .columns()
            .iter()
            .map(|column| display_text(column.name()))
            .collect(),
    );
    let mut remaining = row_limit;

    for (block_index, _block) in reader.blocks().iter().enumerate() {
        if remaining == 0 {
            break;
        }
        let batch = reader.read_block(block_index)?;
        let rows = remaining.min(batch.row_count());
        for row in 0..rows {
            let cells = batch
                .columns()
                .iter()
                .map(|array| format_value(array.value_at(row)))
                .collect();
            table.push(cells);
        }
        remaining -= rows;
    }

    Ok(format!("{}\n", table.render()))
}

fn format_value(value: Option<ScalarValue<'_>>) -> String {
    match value {
        None => "null".to_owned(),
        Some(ScalarValue::Bool(value)) => value.to_string(),
        Some(ScalarValue::Int8(value)) => value.to_string(),
        Some(ScalarValue::Int16(value)) => value.to_string(),
        Some(ScalarValue::Int32(value)) => value.to_string(),
        Some(ScalarValue::Int64(value)) => value.to_string(),
        Some(ScalarValue::UInt8(value)) => value.to_string(),
        Some(ScalarValue::UInt16(value)) => value.to_string(),
        Some(ScalarValue::UInt32(value)) => value.to_string(),
        Some(ScalarValue::UInt64(value)) => value.to_string(),
        Some(ScalarValue::Float32(value)) => value.to_string(),
        Some(ScalarValue::Float64(value)) => value.to_string(),
        Some(ScalarValue::Decimal {
            unscaled, scale, ..
        }) => decimal_text(unscaled, scale),
        Some(ScalarValue::Timestamp { value, unit, .. }) => {
            format!("{value}{}", time_unit_suffix(unit))
        }
        Some(ScalarValue::Date32(value)) => value.to_string(),
        Some(ScalarValue::Utf8(value) | ScalarValue::Categorical(value)) => display_text(value),
        Some(ScalarValue::Binary(value) | ScalarValue::FixedBinary(value)) => hexadecimal(value),
        Some(_) => "<unsupported>".to_owned(),
    }
}

fn decimal_text(unscaled: i64, scale: i16) -> String {
    let negative = unscaled < 0;
    let magnitude = i128::from(unscaled).abs().to_string();
    let scale = i32::from(scale);
    let body = if scale <= 0 {
        let zeros = usize::try_from(-scale).unwrap_or_default();
        format!("{magnitude}{}", "0".repeat(zeros))
    } else {
        let scale = usize::try_from(scale).unwrap_or_default();
        if magnitude.len() <= scale {
            format!("0.{}{}", "0".repeat(scale - magnitude.len()), magnitude)
        } else {
            let split = magnitude.len() - scale;
            format!("{}.{}", &magnitude[..split], &magnitude[split..])
        }
    };
    if negative { format!("-{body}") } else { body }
}

fn time_unit_suffix(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}

/// Render only as many bytes as a cell can show. Two hexadecimal digits per
/// byte, so the elision point is half the cell width.
fn hexadecimal(bytes: &[u8]) -> String {
    let shown = bytes.len().min(MAX_CELL_CHARACTERS / 2);
    let mut output = String::from("0x");
    for byte in &bytes[..shown] {
        let _ = write!(output, "{byte:02x}");
    }
    if shown != bytes.len() {
        output.push(ELISION);
    }
    output
}

fn display_text(value: &str) -> String {
    let escaped: String = value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    elided(&escaped)
}

fn elided(value: &str) -> String {
    let mut characters = value.chars();
    let head: String = characters.by_ref().take(MAX_CELL_CHARACTERS).collect();
    if characters.next().is_none() {
        return head;
    }
    format!("{head}{ELISION}")
}

struct Table {
    rows: Vec<Vec<String>>,
}

impl Table {
    fn new(headings: Vec<String>) -> Self {
        Self {
            rows: vec![headings],
        }
    }

    fn push(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }

    fn render(&self) -> String {
        let widths = self.column_widths();
        self.rows
            .iter()
            .map(|row| {
                row.iter()
                    .zip(&widths)
                    .map(|(cell, width)| format!("{cell:<width$}"))
                    .collect::<Vec<_>>()
                    .join(&" ".repeat(COLUMN_GAP))
                    .trim_end()
                    .to_owned()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn column_widths(&self) -> Vec<usize> {
        let mut widths = self
            .rows
            .first()
            .map_or_else(Vec::new, |row| vec![0; row.len()]);
        for row in &self.rows {
            for (width, cell) in widths.iter_mut().zip(row) {
                *width = (*width).max(cell.chars().count());
            }
        }
        widths
    }
}
