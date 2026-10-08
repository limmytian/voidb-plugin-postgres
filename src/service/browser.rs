//! Browser inner service.
//!
//! Handles `BrowserCommand` variants: schema/table DDL operations
//! (create, drop, rename, duplicate), table copy/paste operations,
//! schema editor support, and generic SQL execution.
//!
//! All identifier quoting uses `quote_ident()` from value_convert
//! (threat T-03-07). Copy operations write to the shared clipboard
//! via Arc.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_postgres::Client;

use voidb_core::{
    CellValue, ColumnInfo, DatabaseClipboard, DatabaseDialect, Row, TabManager,
    TableClipboard, TableSchema, VoidbClipboard,
};

use super::commands::{BrowserCommand, TableCopyData};
use super::events::{BrowserEvent, PostgresEvent};
use super::schema::SchemaService;
use super::value_convert::{decode_value, quote_ident};

/// Inner service for browser / DDL operations.
///
/// Holds a reference to the shared clipboard for copy operations.
/// Receives `&Client` per call from the background task (stateless pattern).
pub struct BrowserService {
    clipboard: Arc<tokio::sync::RwLock<Option<VoidbClipboard>>>,
}

impl BrowserService {
    /// Create a new browser service with clipboard access.
    pub fn new(clipboard: Arc<tokio::sync::RwLock<Option<VoidbClipboard>>>) -> Self {
        Self { clipboard }
    }

    /// Dispatch a `BrowserCommand` to the appropriate handler method.
    pub async fn handle(
        &self,
        client: &Client,
        cmd: BrowserCommand,
        event_tx: &mpsc::UnboundedSender<PostgresEvent>,
        tabs: &Arc<dyn TabManager>,
    ) {
        match cmd {
            BrowserCommand::CreateSchema { name } => {
                match self.create_schema(client, &name).await {
                    Ok(()) => {
                        let _ = event_tx
                            .send(PostgresEvent::Browser(BrowserEvent::SchemaCreated { name }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e));
                    }
                }
            }

            BrowserCommand::DropSchema { name } => {
                match self.drop_schema(client, &name).await {
                    Ok(()) => {
                        let _ = event_tx
                            .send(PostgresEvent::Browser(BrowserEvent::SchemaDropped { name }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e));
                    }
                }
            }

            BrowserCommand::DropTable { schema, table } => {
                match self.drop_table(client, &schema, &table).await {
                    Ok(()) => {
                        let _ = event_tx.send(PostgresEvent::Browser(
                            BrowserEvent::TableDropped { schema, table },
                        ));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e));
                    }
                }
            }

            BrowserCommand::RenameTable {
                schema,
                table,
                new_name,
            } => {
                match self.rename_table(client, &schema, &table, &new_name).await {
                    Ok(()) => {
                        let _ = event_tx.send(PostgresEvent::Browser(
                            BrowserEvent::TableRenamed {
                                schema,
                                old_name: table,
                                new_name,
                            },
                        ));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e));
                    }
                }
            }

            BrowserCommand::DuplicateTable {
                schema,
                table,
                new_name,
            } => {
                match self
                    .duplicate_table(client, &schema, &table, &new_name)
                    .await
                {
                    Ok(()) => {
                        let _ = event_tx.send(PostgresEvent::Browser(
                            BrowserEvent::TableDuplicated {
                                schema,
                                source: table,
                                target: new_name,
                            },
                        ));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e));
                    }
                }
            }

            BrowserCommand::CopyTables {
                connection_id,
                schema,
                tables,
                with_data,
            } => {
                match self
                    .copy_tables_to_clipboard(client, &connection_id, &schema, &tables, with_data)
                    .await
                {
                    Ok(()) => {
                        let _ = event_tx
                            .send(PostgresEvent::Browser(BrowserEvent::CopyTablesDone));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e));
                    }
                }
            }

            BrowserCommand::CopyDatabase {
                connection_id,
                schema,
                with_data,
            } => {
                match self
                    .copy_database_to_clipboard(client, &connection_id, &schema, with_data)
                    .await
                {
                    Ok(()) => {
                        let _ = event_tx
                            .send(PostgresEvent::Browser(BrowserEvent::CopyTablesDone));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e));
                    }
                }
            }

            BrowserCommand::PasteTables {
                target_schema,
                table_schemas,
            } => {
                match self
                    .paste_tables(client, &target_schema, &table_schemas)
                    .await
                {
                    Ok(()) => {
                        let _ = event_tx
                            .send(PostgresEvent::Browser(BrowserEvent::PasteTablesDone));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e));
                    }
                }
            }

            BrowserCommand::PasteDatabase {
                target_schema,
                source_schema,
                tables,
                with_data,
            } => {
                match self
                    .paste_database(
                        client,
                        &target_schema,
                        &source_schema,
                        &tables,
                        with_data,
                    )
                    .await
                {
                    Ok(()) => {
                        let _ = event_tx
                            .send(PostgresEvent::Browser(BrowserEvent::PasteDatabaseDone));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e));
                    }
                }
            }

            BrowserCommand::ExecuteSql { sql } => {
                match self.execute_sql(client, &sql).await {
                    Ok(result) => {
                        let _ = event_tx
                            .send(PostgresEvent::Browser(BrowserEvent::SqlExecuted { result }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e));
                    }
                }
            }

            BrowserCommand::LoadColumnsForEditor { schema, table } => {
                match self.load_columns_for_editor(client, &schema, &table).await {
                    Ok(columns) => {
                        let _ = event_tx.send(PostgresEvent::Browser(
                            BrowserEvent::ColumnsForEditorLoaded {
                                schema,
                                table,
                                columns,
                            },
                        ));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e));
                    }
                }
            }

            BrowserCommand::ApplySchemaChanges { schema, table, sql } => {
                match self.apply_schema_changes(client, &sql).await {
                    Ok(()) => {
                        let _ = event_tx.send(PostgresEvent::Browser(
                            BrowserEvent::SchemaChangesApplied { schema, table },
                        ));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e));
                    }
                }
            }
        }
        let _ = tabs.request_render();
    }

    // === DDL operations ===

    /// Create a new schema.
    async fn create_schema(&self, client: &Client, name: &str) -> Result<(), String> {
        let sql = format!("CREATE SCHEMA {}", quote_ident(name));
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Drop an existing schema with CASCADE.
    async fn drop_schema(&self, client: &Client, name: &str) -> Result<(), String> {
        let sql = format!("DROP SCHEMA {} CASCADE", quote_ident(name));
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Drop a table.
    async fn drop_table(
        &self,
        client: &Client,
        schema: &str,
        table: &str,
    ) -> Result<(), String> {
        let sql = format!(
            "DROP TABLE {}.{}",
            quote_ident(schema),
            quote_ident(table)
        );
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Rename a table within a schema.
    async fn rename_table(
        &self,
        client: &Client,
        schema: &str,
        table: &str,
        new_name: &str,
    ) -> Result<(), String> {
        // SET search_path so ALTER TABLE finds the table
        let set_path = format!("SET search_path TO {}", quote_ident(schema));
        client
            .execute(&set_path, &[])
            .await
            .map_err(|e| e.to_string())?;

        let sql = format!(
            "ALTER TABLE {} RENAME TO {}",
            quote_ident(table),
            quote_ident(new_name)
        );
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Duplicate a table (structure + data).
    async fn duplicate_table(
        &self,
        client: &Client,
        schema: &str,
        table: &str,
        new_name: &str,
    ) -> Result<(), String> {
        let fq_src = format!("{}.{}", quote_ident(schema), quote_ident(table));
        let fq_dst = format!("{}.{}", quote_ident(schema), quote_ident(new_name));

        // Create structure
        let create_sql = format!("CREATE TABLE {} (LIKE {} INCLUDING ALL)", fq_dst, fq_src);
        client
            .execute(&create_sql, &[])
            .await
            .map_err(|e| e.to_string())?;

        // Copy data
        let insert_sql = format!("INSERT INTO {} SELECT * FROM {}", fq_dst, fq_src);
        client
            .execute(&insert_sql, &[])
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    // === Copy/paste operations ===

    /// Copy tables to the shared clipboard.
    async fn copy_tables_to_clipboard(
        &self,
        client: &Client,
        connection_id: &str,
        schema: &str,
        tables: &[String],
        with_data: bool,
    ) -> Result<(), String> {
        let mut clips = Vec::new();

        for table in tables {
            let table_schema = self.describe_table(client, schema, table).await?;
            let create_sql = generate_create_table(&table_schema, schema);

            let data = if with_data {
                Some(self.read_table_data(client, schema, table).await?)
            } else {
                None
            };

            clips.push(TableClipboard {
                dialect: DatabaseDialect::PostgreSQL,
                source_connection: connection_id.to_string(),
                source_database: schema.to_string(),
                table_name: table.clone(),
                create_table_sql: create_sql,
                schema: table_schema,
                data,
            });
        }

        let clip = if clips.len() == 1 {
            VoidbClipboard::Table(Box::new(clips.into_iter().next().unwrap()))
        } else {
            VoidbClipboard::Tables(clips)
        };

        if let Ok(mut guard) = self.clipboard.try_write() {
            *guard = Some(clip);
        }

        Ok(())
    }

    /// Copy an entire schema to the shared clipboard.
    async fn copy_database_to_clipboard(
        &self,
        client: &Client,
        connection_id: &str,
        schema: &str,
        with_data: bool,
    ) -> Result<(), String> {
        // List all base tables
        let table_rows = client
            .query(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_type = 'BASE TABLE' \
                 ORDER BY table_name",
                &[&schema],
            )
            .await
            .map_err(|e| e.to_string())?;

        let table_names: Vec<String> = table_rows.iter().map(|r| r.get(0)).collect();

        let mut clips = Vec::new();
        for table in &table_names {
            let table_schema = self.describe_table(client, schema, table).await?;
            let create_sql = generate_create_table(&table_schema, schema);

            let data = if with_data {
                Some(self.read_table_data(client, schema, table).await?)
            } else {
                None
            };

            clips.push(TableClipboard {
                dialect: DatabaseDialect::PostgreSQL,
                source_connection: connection_id.to_string(),
                source_database: schema.to_string(),
                table_name: table.clone(),
                create_table_sql: create_sql,
                schema: table_schema,
                data,
            });
        }

        let db_clip = DatabaseClipboard {
            dialect: DatabaseDialect::PostgreSQL,
            source_connection: connection_id.to_string(),
            database_name: schema.to_string(),
            tables: clips,
        };

        if let Ok(mut guard) = self.clipboard.try_write() {
            *guard = Some(VoidbClipboard::Database(db_clip));
        }

        Ok(())
    }

    /// Paste tables from structured clipboard data.
    async fn paste_tables(
        &self,
        client: &Client,
        target_schema: &str,
        table_schemas: &[TableCopyData],
    ) -> Result<(), String> {
        for tcd in table_schemas {
            // Set search path for table creation
            let set_path = format!("SET search_path TO {}", quote_ident(target_schema));
            client
                .execute(&set_path, &[])
                .await
                .map_err(|e| e.to_string())?;

            // Execute CREATE TABLE
            client
                .execute(&tcd.create_sql, &[])
                .await
                .map_err(|e| format!("CREATE TABLE '{}' failed: {}", tcd.table_name, e))?;
        }

        Ok(())
    }

    /// Paste an entire database (create schema + tables).
    async fn paste_database(
        &self,
        client: &Client,
        target_schema: &str,
        source_schema: &str,
        tables: &[String],
        with_data: bool,
    ) -> Result<(), String> {
        // Create target schema if it doesn't exist
        let create_schema_sql = format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            quote_ident(target_schema)
        );
        client
            .execute(&create_schema_sql, &[])
            .await
            .map_err(|e| e.to_string())?;

        // For each table: get DDL from source schema, rewrite, execute
        for table in tables {
            let fq_src = format!("{}.{}", quote_ident(source_schema), quote_ident(table));
            let fq_dst = format!("{}.{}", quote_ident(target_schema), quote_ident(table));

            // Create table with structure
            let create_sql = format!("CREATE TABLE {} (LIKE {} INCLUDING ALL)", fq_dst, fq_src);
            if let Err(e) = client.execute(&create_sql, &[]).await {
                tracing::warn!("DDL for '{}' failed: {}", table, e);
                continue;
            }

            // Copy data if requested
            if with_data {
                let insert_sql = format!("INSERT INTO {} SELECT * FROM {}", fq_dst, fq_src);
                if let Err(e) = client.execute(&insert_sql, &[]).await {
                    tracing::warn!("Data copy for '{}' failed: {}", table, e);
                }
            }
        }

        Ok(())
    }

    // === SQL execution ===

    /// Execute raw SQL in browser context.
    async fn execute_sql(&self, client: &Client, sql: &str) -> Result<String, String> {
        let results = client
            .simple_query(sql)
            .await
            .map_err(|e| e.to_string())?;

        let mut total_affected = 0u64;
        let mut stmt_count = 0usize;
        for msg in &results {
            match msg {
                tokio_postgres::SimpleQueryMessage::CommandComplete(n) => {
                    stmt_count += 1;
                    total_affected += n;
                }
                tokio_postgres::SimpleQueryMessage::Row(_) => {}
                _ => {}
            }
        }

        Ok(format!(
            "OK ({} statement{}, {} affected)",
            stmt_count,
            if stmt_count == 1 { "" } else { "s" },
            total_affected
        ))
    }

    // === Schema editor support ===

    /// Load column metadata for the schema editor.
    ///
    /// Delegates to SchemaService::list_columns for consistency.
    async fn load_columns_for_editor(
        &self,
        client: &Client,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>, String> {
        SchemaService::list_columns(client, schema, table)
            .await
            .map_err(|e| e.to_string())
    }

    /// Apply schema changes from the schema editor.
    async fn apply_schema_changes(
        &self,
        client: &Client,
        statements: &[String],
    ) -> Result<(), String> {
        for stmt in statements {
            if stmt.starts_with("--") {
                continue;
            }
            client
                .execute(stmt.as_str(), &[])
                .await
                .map_err(|e| format!("Schema change failed: {}", e))?;
        }
        Ok(())
    }

    // === Private helpers ===

    /// Describe a table for clipboard operations.
    ///
    /// Returns column metadata and primary key information.
    async fn describe_table(
        &self,
        client: &Client,
        schema: &str,
        table: &str,
    ) -> Result<TableSchema, String> {
        let col_rows = client
            .query(
                "SELECT c.column_name, c.data_type, c.udt_name, c.is_nullable,
                        c.column_default, c.character_maximum_length
                 FROM information_schema.columns c
                 WHERE c.table_schema = $1 AND c.table_name = $2
                 ORDER BY c.ordinal_position",
                &[&schema, &table],
            )
            .await
            .map_err(|e| e.to_string())?;

        let pk_rows = client
            .query(
                "SELECT a.attname
                 FROM pg_index i
                 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                 WHERE i.indrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass
                   AND i.indisprimary",
                &[&schema, &table],
            )
            .await
            .map_err(|e| e.to_string())?;

        let pk_names: Vec<String> = pk_rows.iter().map(|r| r.get::<_, String>(0)).collect();

        let columns: Vec<ColumnInfo> = col_rows
            .iter()
            .map(|r| {
                let col_name: String = r.get(0);
                let data_type: String = r.get(1);
                let udt_name: String = r.get(2);
                let nullable_str: String = r.get(3);
                let default_val: Option<String> = r.try_get(4).ok().flatten();
                let max_len: Option<i32> = r.try_get(5).ok().flatten();

                let display_type = if data_type == "USER-DEFINED" {
                    udt_name.clone()
                } else if let Some(ml) = max_len {
                    format!("{}({})", data_type, ml)
                } else {
                    data_type.clone()
                };

                ColumnInfo {
                    name: col_name.clone(),
                    data_type: display_type,
                    nullable: nullable_str == "YES",
                    is_primary_key: pk_names.contains(&col_name),
                    default_value: default_val,
                    max_length: max_len.map(|v| v as u64),
                    extra: String::new(),
                }
            })
            .collect();

        Ok(TableSchema {
            database: schema.to_string(),
            table_name: table.to_string(),
            columns,
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            engine: None,
            comment: None,
        })
    }

    /// Read all rows from a table for copy operations.
    async fn read_table_data(
        &self,
        client: &Client,
        schema: &str,
        table: &str,
    ) -> Result<Vec<Row>, String> {
        let select_sql = format!(
            "SELECT * FROM {}.{}",
            quote_ident(schema),
            quote_ident(table)
        );
        let rows = client
            .query(&select_sql, &[])
            .await
            .map_err(|e| e.to_string())?;

        Ok(rows
            .iter()
            .map(|row| {
                let values: Vec<CellValue> =
                    (0..row.columns().len()).map(|i| decode_value(row, i)).collect();
                Row { values }
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Free-standing helpers
// ---------------------------------------------------------------------------

/// Generate CREATE TABLE DDL from a TableSchema.
fn generate_create_table(schema: &TableSchema, schema_name: &str) -> String {
    let mut sql = format!(
        "CREATE TABLE {}.{} (\n",
        quote_ident(schema_name),
        quote_ident(&schema.table_name)
    );

    let mut col_defs = Vec::new();
    let mut pk_cols = Vec::new();

    for col in &schema.columns {
        let mut def = format!("  {} {}", quote_ident(&col.name), col.data_type);
        if !col.nullable {
            def.push_str(" NOT NULL");
        }
        if let Some(default) = &col.default_value
            && !default.starts_with("nextval(")
        {
            def.push_str(&format!(" DEFAULT {}", default));
        }
        col_defs.push(def);
        if col.is_primary_key {
            pk_cols.push(quote_ident(&col.name));
        }
    }

    sql.push_str(&col_defs.join(",\n"));

    if !pk_cols.is_empty() {
        sql.push_str(&format!(",\n  PRIMARY KEY ({})", pk_cols.join(", ")));
    }

    sql.push_str("\n)");
    sql
}
