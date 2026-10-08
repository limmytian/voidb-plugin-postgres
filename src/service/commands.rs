//! PostgreSQL service command types.
//!
//! Commands are sent from the TUI plugin to the background service task
//! via an unbounded mpsc channel. Each domain (schema, CRUD, data loading,
//! browser) has its own sub-enum for organization.
//!
//! Key difference from MySQL: PostgreSQL uses `schema: String` instead of
//! `database: String` in sub-commands, since PG organizes objects by schema
//! within a single database.

use std::collections::HashMap;

use tokio::sync::oneshot;
use voidb_core::{CellValue, ColumnInfo, Row};

use crate::config::PostgresConfig;

/// Top-level command enum for the PostgreSQL service.
///
/// Connection lifecycle commands are at the top level.
/// Domain-specific commands are grouped into sub-enums.
#[derive(Debug)]
pub enum PostgresCommand {
    // --- Connection lifecycle ---
    /// Establish a connection from the given config.
    /// The oneshot reply signals success or failure.
    Connect {
        config: PostgresConfig,
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// Ping the server to verify the connection is alive.
    Ping {
        reply: oneshot::Sender<Result<(), String>>,
    },

    /// Disconnect and shut down the background task.
    Disconnect,

    // --- Domain sub-commands ---
    /// Schema introspection operations.
    Schema(SchemaCommand),

    /// CRUD operations (query, execute, save, insert).
    Crud(CrudCommand),

    /// Data loading operations (pagination, row count, PKs).
    Load(LoadCommand),

    /// Browser operations (DDL, table management).
    Browser(BrowserCommand),
}

/// Schema introspection commands.
///
/// PostgreSQL uses schemas (namespaces) within a database, unlike MySQL's
/// database-per-schema model. Commands accept `schema` parameters.
#[derive(Debug)]
pub enum SchemaCommand {
    /// List all schemas in the database (excluding system schemas).
    ListSchemas,

    /// List tables in a schema.
    ListTables { schema: String },

    /// List columns for a specific table.
    ListColumns { schema: String, table: String },

    /// List indexes for a specific table.
    ListIndexes { schema: String, table: String },

    /// List foreign keys for a specific table.
    ListForeignKeys { schema: String, table: String },

    /// Generate CREATE TABLE DDL for a table.
    ShowCreateTable { schema: String, table: String },

    /// Load full structure info (columns + indexes + foreign keys) for the structure viewer.
    LoadStructure { schema: String, table: String },
}

/// CRUD commands (query execution and data mutation).
#[derive(Debug)]
pub enum CrudCommand {
    /// Execute a single SQL query and return results.
    ExecuteQuery { sql: String },

    /// Execute multiple SQL statements sequentially.
    ExecuteMultiQuery { sql: String },

    /// Save pending cell edits and row deletions.
    SaveChanges {
        schema: String,
        table: String,
        columns: Vec<ColumnInfo>,
        rows: Vec<Row>,
        pk_columns: Vec<String>,
        pending: HashMap<(usize, usize), CellValue>,
        deleted: Vec<usize>,
    },

    /// Insert a new row into a table.
    InsertRow {
        schema: String,
        table: String,
        columns: Vec<String>,
        values: Vec<CellValue>,
    },
}

/// Data loading commands (pagination, counts, PKs).
#[derive(Debug)]
pub enum LoadCommand {
    /// Load a page of table data.
    LoadTableData {
        schema: String,
        table: String,
        page_size: usize,
        offset: usize,
    },

    /// Load the total row count for a table.
    LoadRowCount { schema: String, table: String },

    /// Load primary key column names for a table.
    LoadPrimaryKeys { schema: String, table: String },
}

/// Browser / DDL management commands.
///
/// Stub variants for Plan 01. Full implementation in Plan 03.
#[derive(Debug)]
pub enum BrowserCommand {
    /// Create a new schema.
    CreateSchema { name: String },

    /// Drop an existing schema.
    DropSchema { name: String },

    /// Drop a table from a schema.
    DropTable { schema: String, table: String },

    /// Rename a table.
    RenameTable {
        schema: String,
        table: String,
        new_name: String,
    },

    /// Duplicate a table (structure + optionally data).
    DuplicateTable {
        schema: String,
        table: String,
        new_name: String,
    },

    /// Copy tables from a source to internal clipboard.
    CopyTables {
        connection_id: String,
        schema: String,
        tables: Vec<String>,
        with_data: bool,
    },

    /// Copy an entire database/schema to internal clipboard.
    CopyDatabase {
        connection_id: String,
        schema: String,
        with_data: bool,
    },

    /// Paste previously copied tables into a target schema.
    PasteTables {
        target_schema: String,
        table_schemas: Vec<TableCopyData>,
    },

    /// Paste an entire database (all tables from clipboard).
    PasteDatabase {
        target_schema: String,
        source_schema: String,
        tables: Vec<String>,
        with_data: bool,
    },

    /// Execute raw SQL (for browser-level operations).
    ExecuteSql { sql: String },

    /// Load columns for the schema editor.
    LoadColumnsForEditor { schema: String, table: String },

    /// Apply schema changes from the schema editor.
    ApplySchemaChanges {
        schema: String,
        table: String,
        sql: Vec<String>,
    },
}

/// Data needed for a table copy/paste operation.
#[derive(Debug, Clone)]
pub struct TableCopyData {
    /// Name of the table to create.
    pub table_name: String,
    /// CREATE TABLE SQL statement.
    pub create_sql: String,
    /// Whether to copy data in addition to structure.
    pub with_data: bool,
}
