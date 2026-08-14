//! Immutable public schema metadata reconstructed from a v0.2 schema frame.

/// The logical unit of a `timestamp64` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

/// The timezone annotation of a `timestamp64` column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeZone {
    Naive,
    Utc,
    Iana(String),
}

/// A v0.2 logical column type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Decimal { precision: u16, scale: i16 },
    Timestamp { unit: TimeUnit, timezone: TimeZone },
    Utf8,
    Categorical { ordered: bool },
    Binary,
    FixedBinary { byte_width: u32 },
    Date32,
}

/// One immutable column in a [`Schema`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    id: u32,
    name: String,
    logical_type: LogicalType,
    nullable: bool,
}

impl Column {
    /// Construct a column descriptor.
    ///
    /// The writer checks the complete schema invariants, including nonzero
    /// unique IDs, unique names, and primary-column compatibility, before it
    /// creates a file.
    pub fn new(
        id: u32,
        name: impl Into<String>,
        logical_type: LogicalType,
        nullable: bool,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            logical_type,
            nullable,
        }
    }

    /// The stable nonzero column ID stored in the file.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// The UTF-8 column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The column's logical type.
    pub fn logical_type(&self) -> &LogicalType {
        &self.logical_type
    }

    /// Whether the column may contain null values.
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
}

/// The fixed schema shared by every block in an Acta file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    schema_id: u64,
    columns: Vec<Column>,
    primary_column_id: Option<u32>,
}

impl Schema {
    /// Construct immutable schema metadata.
    ///
    /// Schema validation that depends on the file format is performed by
    /// [`crate::Writer::create`].
    pub fn new(schema_id: u64, columns: Vec<Column>, primary_column_id: Option<u32>) -> Self {
        Self {
            schema_id,
            columns,
            primary_column_id,
        }
    }

    /// The nonzero schema ID stored in the schema and data headers.
    pub fn schema_id(&self) -> u64 {
        self.schema_id
    }

    /// The columns in schema order, which is the order the file declares them
    /// and the order a reader reports them.
    ///
    /// Section 7 does not require that order to be sorted by column ID. Only a
    /// data frame's column table is sorted, and the writer sorts it there.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// The number of declared columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// The selected primary timestamp/date column, if the schema has one.
    pub fn primary_column(&self) -> Option<&Column> {
        self.primary_column_id.and_then(|id| self.column_by_id(id))
    }

    /// The selected primary timestamp/date column ID, if present.
    pub fn primary_column_id(&self) -> Option<u32> {
        self.primary_column_id
    }

    /// Find a column by its stable ID.
    pub fn column_by_id(&self, id: u32) -> Option<&Column> {
        self.columns.iter().find(|column| column.id == id)
    }

    /// Find a column by its exact UTF-8 name.
    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|column| column.name == name)
    }
}
