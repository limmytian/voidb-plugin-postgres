//! PostgreSQL service event types.
//!
//! Events are sent from the background service task back to the TUI plugin
//! via an unbounded mpsc channel. The TUI polls events via
//! `PostgresService::poll_event()` in its synchronous `Plugin::update()`.

use voidb_core::{ColumnInfo, Row};
use voidb_core::database::types::ForeignKeyInfo;

/// Top-level event enum for the PostgreSQL service.
///
/// Connection lifecycle events are at the top level.
/// Domain events are grouped into sub-enums matching the command domains.
#[derive(Debug)]
pub enum PostgresEvent {
    // --- Connection lifecycle ---
    /// Client created and connection verified.
    Connected,

    /// Client disconnected cleanly.
    Disconnected,

    // --- Domain events ---
    /// Schema introspection results.
    Schema(SchemaEvent),

    /// CRUD operation results.
    Crud(CrudEvent),

    /// Data loading results.
    Load(LoadEvent),

    /// Browser operation results.
    Browser(BrowserEvent),

    /// An error occurred during an operation.
    Error(String),
}

/// Schema introspection events.
#[derive(Debug)]
pub enum SchemaEvent {
    /// List of schemas in the database.
    Schemas(Vec<String>),

    /// Tables in a schema.
    Tables {
        schema: String,
        tables: Vec<TableMeta>,
    },

    /// Column metadata for a table.
    Columns {
        schema: String,
        table: String,
        columns: Vec<ColumnInfo>,
    },

    /// Index metadata for a table.
    Indexes {
        schema: String,
        table: String,
        indexes: Vec<IndexInfo>,
    },

    /// Foreign keys for a table.
    ForeignKeysLoaded {
        schema: String,
        table: String,
        foreign_keys: Vec<ForeignKeyInfo>,
    },

    /// Generated CREATE TABLE DDL for a table.
    CreateTableDdl {
        schema: String,
        table: String,
        ddl: String,
    },

    /// Combined structure info (columns + indexes + foreign keys) for the structure viewer.
    StructureLoaded {
        schema: String,
        table: String,
        columns: Vec<ColumnInfo>,
        indexes: Vec<IndexInfo>,
        foreign_keys: Vec<ForeignKeyInfo>,
    },
}

/// CRUD operation events.
#[derive(Debug)]
pub enum CrudEvent {
    /// Single query result (columns + rows).
    QueryResult {
        columns: Vec<ColumnInfo>,
        rows: Vec<Row>,
    },

    /// Multiple statement results.
    MultiQueryResult { results: Vec<StatementResult> },

    /// Cell edits and row deletions applied.
    ChangesSaved { affected_rows: u64 },

    /// A new row was inserted.
    RowInserted { new_row: Option<Row> },
}

/// Result of a single SQL statement within a multi-query execution.
#[derive(Debug)]
pub enum StatementResult {
    /// A SELECT result set.
    Select {
        columns: Vec<ColumnInfo>,
        rows: Vec<Row>,
    },
    /// An INSERT/UPDATE/DELETE result with affected row count.
    Affected(u64),
    /// Empty result set.
    Empty,
    /// Statement execution error.
    Error(String),
}

/// Data loading events.
#[derive(Debug)]
pub enum LoadEvent {
    /// A page of table data with total row count.
    TableDataLoaded {
        columns: Vec<ColumnInfo>,
        rows: Vec<Row>,
        total_count: Option<i64>,
    },

    /// Total row count for a table.
    RowCountLoaded { count: i64 },

    /// Primary key column names for a table.
    PrimaryKeysLoaded { pk_cols: Vec<String> },
}

/// Browser / DDL operation events.
///
/// Stub variants for Plan 01. Full implementation in Plan 03.
#[derive(Debug)]
pub enum BrowserEvent {
    /// Schema created successfully.
    SchemaCreated { name: String },

    /// Schema dropped successfully.
    SchemaDropped { name: String },

    /// Table dropped successfully.
    TableDropped { schema: String, table: String },

    /// Table renamed successfully.
    TableRenamed {
        schema: String,
        old_name: String,
        new_name: String,
    },

    /// Table duplicated successfully.
    TableDuplicated {
        schema: String,
        source: String,
        target: String,
    },

    /// Copy-tables operation completed.
    CopyTablesDone,

    /// Paste-tables operation completed.
    PasteTablesDone,

    /// Raw SQL executed in browser context.
    SqlExecuted { result: String },

    /// Columns loaded for the schema editor.
    ColumnsForEditorLoaded {
        schema: String,
        table: String,
        columns: Vec<ColumnInfo>,
    },

    /// Schema changes applied from the editor.
    SchemaChangesApplied { schema: String, table: String },

    /// Paste-database operation completed.
    PasteDatabaseDone,

    /// Generic error for browser operations.
    Error(String),
}

/// Table metadata for browser display.
#[derive(Debug, Clone)]
pub struct TableMeta {
    /// Table name.
    pub name: String,
    /// Table type (e.g., "BASE TABLE", "VIEW").
    pub table_type: String,
    /// Approximate row count (from pg_class reltuples).
    pub row_estimate: Option<i64>,
}

/// Index metadata.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Columns in the index.
    pub columns: Vec<String>,
    /// Whether the index enforces uniqueness.
    pub is_unique: bool,
    /// Index type (e.g., "btree", "hash", "gin", "gist").
    pub index_type: String,
}

/// Statement result type alias for multi-query results.
/// Re-exported for convenience.
pub use voidb_core::database::types::ForeignKeyInfo as CoreForeignKeyInfo;
