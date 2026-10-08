//! PostgreSQL service layer.
//!
//! This module provides `PostgresService`, the service facade for the PostgreSQL
//! plugin. It follows the service-layer convention established in Phase 1:
//!
//! - Background tokio task processes commands asynchronously
//! - `send()` dispatches commands via unbounded mpsc channel (non-blocking)
//! - `poll_event()` drains events via `try_recv()` (non-blocking)
//! - Render notifications fire after every event emission
//!
//! # Inner Services
//!
//! The background task delegates to domain-specific inner services:
//! - `SchemaService` -- schema/table/column/index introspection
//! - `CrudService` -- query execution, save changes, insert rows
//! - `DataLoaderService` -- paginated data loading, row counts, PKs
//! - `BrowserService` -- DDL management (stub, fleshed out in Plan 03)
//!
//! # Connection Lifecycle
//!
//! The service starts without a client. A `Connect` command creates the
//! client via `create_connection()`. On `Disconnect` (or sender drop),
//! the client is dropped, closing the connection.

pub mod browser;
pub mod commands;
pub mod connection;
pub mod crud;
pub mod data_loader;
pub mod events;
pub mod schema;
pub mod value_convert;

// Re-export all public types for ergonomic imports
pub use commands::{
    BrowserCommand, CrudCommand, LoadCommand, PostgresCommand, SchemaCommand, TableCopyData,
};
pub use events::{
    BrowserEvent, CrudEvent, IndexInfo, LoadEvent, PostgresEvent, SchemaEvent, StatementResult,
    TableMeta,
};
pub use voidb_core::database::types::ForeignKeyInfo;

use std::sync::Arc;

use tokio::sync::mpsc;
use voidb_core::database::types::ColumnInfo;

use crate::config::PostgresConfig;
use connection::create_connection;
use crud::CrudService;
use data_loader::DataLoaderService;
use schema::SchemaService;
use voidb_core::{TabManager, VoidbClipboard};

use browser::BrowserService;

/// Internal mode discriminator for PostgresService.
///
/// `Channel` mode is used by the TUI: commands are sent asynchronously
/// and results are polled. `Direct` mode is used by the CLI: async methods
/// return results directly.
enum ServiceMode {
    Channel {
        cmd_tx: mpsc::UnboundedSender<PostgresCommand>,
        event_rx: mpsc::UnboundedReceiver<PostgresEvent>,
        _task: tokio::task::JoinHandle<()>,
    },
    Direct {
        client: tokio_postgres::Client,
        _conn_task: tokio::task::JoinHandle<()>,
    },
}

/// PostgreSQL service facade.
///
/// Owns either channel-based TUI communication or a direct async client
/// depending on the mode.
///
/// # Send + Sync
///
/// `PostgresService` is `Send` but NOT `Sync` (because `UnboundedReceiver`
/// is `!Sync`). Plugin structs must wrap it in `std::sync::Mutex` to
/// satisfy `Plugin: Send + Sync`. Since `Plugin::update(&mut self)` has
/// exclusive access, the Mutex is never contended.
pub struct PostgresService {
    mode: ServiceMode,
}

impl PostgresService {
    /// Create a new PostgresService with a background processing task.
    ///
    /// The service starts without an active connection. Send a `Connect`
    /// command to establish the client.
    ///
    /// # Arguments
    ///
    /// * `_config` - PostgreSQL connection configuration (reserved for future use).
    /// * `tabs` - Tab manager for render notifications.
    /// * `runtime` - Shared tokio runtime handle for spawning the background task.
    /// * `_clipboard` - Shared clipboard for copy/paste operations (used by BrowserService).
    pub fn new(
        _config: PostgresConfig,
        tabs: Arc<dyn TabManager>,
        runtime: tokio::runtime::Handle,
        clipboard: Arc<tokio::sync::RwLock<Option<VoidbClipboard>>>,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<PostgresCommand>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<PostgresEvent>();

        let task = runtime.spawn(Self::background_task(cmd_rx, event_tx, tabs, clipboard));

        Self {
            mode: ServiceMode::Channel {
                cmd_tx,
                event_rx,
                _task: task,
            },
        }
    }

    /// Create a PostgresService in direct mode for CLI usage.
    ///
    /// Connects immediately via `create_connection()`, no background command loop.
    /// Use direct async methods (list_databases, execute_query, etc.) instead
    /// of send/poll_event.
    pub async fn new_direct(config: &PostgresConfig) -> Result<Self, String> {
        let client = create_connection(config).await?;
        // Verify connectivity
        client
            .simple_query("SELECT 1")
            .await
            .map_err(|e| format!("PostgreSQL connectivity check failed: {}", e))?;

        // create_connection already spawns the connection task internally,
        // but for direct mode we need a handle to keep the lifetime.
        // Since create_connection already spawned it, we use a no-op task.
        let conn_task = tokio::spawn(async {});

        Ok(Self {
            mode: ServiceMode::Direct {
                client,
                _conn_task: conn_task,
            },
        })
    }

    /// Send a command to the background service task (channel mode only).
    ///
    /// This is non-blocking -- safe to call from the synchronous
    /// `Plugin::update()` context.
    pub fn send(&self, cmd: PostgresCommand) {
        if let ServiceMode::Channel { ref cmd_tx, .. } = self.mode {
            let _ = cmd_tx.send(cmd);
        }
    }

    /// Poll for the next event from the service (channel mode only).
    ///
    /// Returns `Some(event)` if available, `None` otherwise.
    /// Non-blocking, suitable for calling from `Plugin::update()`.
    pub fn poll_event(&mut self) -> Option<PostgresEvent> {
        if let ServiceMode::Channel { ref mut event_rx, .. } = self.mode {
            event_rx.try_recv().ok()
        } else {
            None
        }
    }

    // === Direct mode async methods (CLI) ===

    /// List all databases (direct mode).
    pub async fn list_databases(&self) -> anyhow::Result<Vec<String>> {
        match &self.mode {
            ServiceMode::Direct { client, .. } => {
                let rows = client
                    .query(
                        "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname",
                        &[],
                    )
                    .await?;
                Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
            }
            _ => Err(anyhow::anyhow!("list_databases requires direct mode")),
        }
    }

    /// List user schemas (direct mode).
    pub async fn list_schemas(&self) -> anyhow::Result<Vec<String>> {
        match &self.mode {
            ServiceMode::Direct { client, .. } => SchemaService::list_schemas(client).await,
            _ => Err(anyhow::anyhow!("list_schemas requires direct mode")),
        }
    }

    /// List tables in a schema (direct mode).
    pub async fn list_tables(&self, schema: &str) -> anyhow::Result<Vec<TableMeta>> {
        match &self.mode {
            ServiceMode::Direct { client, .. } => SchemaService::list_tables(client, schema).await,
            _ => Err(anyhow::anyhow!("list_tables requires direct mode")),
        }
    }

    /// List columns of a table (direct mode).
    pub async fn list_columns(&self, schema: &str, table: &str) -> anyhow::Result<Vec<ColumnInfo>> {
        match &self.mode {
            ServiceMode::Direct { client, .. } => SchemaService::list_columns(client, schema, table).await,
            _ => Err(anyhow::anyhow!("list_columns requires direct mode")),
        }
    }

    /// List indexes of a table (direct mode).
    pub async fn list_indexes(&self, schema: &str, table: &str) -> anyhow::Result<Vec<IndexInfo>> {
        match &self.mode {
            ServiceMode::Direct { client, .. } => SchemaService::list_indexes(client, schema, table).await,
            _ => Err(anyhow::anyhow!("list_indexes requires direct mode")),
        }
    }

    /// List foreign keys of a table (direct mode).
    pub async fn list_foreign_keys(&self, schema: &str, table: &str) -> anyhow::Result<Vec<ForeignKeyInfo>> {
        match &self.mode {
            ServiceMode::Direct { client, .. } => SchemaService::list_foreign_keys(client, schema, table).await,
            _ => Err(anyhow::anyhow!("list_foreign_keys requires direct mode")),
        }
    }

    /// Show CREATE TABLE DDL (direct mode).
    pub async fn show_create_table(&self, schema: &str, table: &str) -> anyhow::Result<String> {
        match &self.mode {
            ServiceMode::Direct { client, .. } => SchemaService::show_create_table(client, schema, table).await,
            _ => Err(anyhow::anyhow!("show_create_table requires direct mode")),
        }
    }

    /// Load table structure: columns, indexes, foreign keys (direct mode).
    pub async fn describe_table(&self, schema: &str, table: &str) -> anyhow::Result<(Vec<ColumnInfo>, Vec<IndexInfo>, Vec<ForeignKeyInfo>)> {
        match &self.mode {
            ServiceMode::Direct { client, .. } => SchemaService::load_structure(client, schema, table).await,
            _ => Err(anyhow::anyhow!("describe_table requires direct mode")),
        }
    }

    /// Execute a SQL query, returning statement results (direct mode).
    pub async fn execute_query(&self, sql: &str) -> anyhow::Result<Vec<StatementResult>> {
        match &self.mode {
            ServiceMode::Direct { client, .. } => CrudService::execute_multi_query(client, sql).await,
            _ => Err(anyhow::anyhow!("execute_query requires direct mode")),
        }
    }

    /// Background task that processes commands and dispatches to inner services.
    ///
    /// The task owns the `tokio_postgres::Client` directly (not wrapped in
    /// Arc<Mutex>). Since it is the sole consumer of the command channel,
    /// there is no contention on the client.
    async fn background_task(
        mut cmd_rx: mpsc::UnboundedReceiver<PostgresCommand>,
        event_tx: mpsc::UnboundedSender<PostgresEvent>,
        tabs: Arc<dyn TabManager>,
        clipboard: Arc<tokio::sync::RwLock<Option<VoidbClipboard>>>,
    ) {
        // Client is created on Connect
        let mut client: Option<tokio_postgres::Client> = None;
        let browser_svc = BrowserService::new(clipboard);

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                // === Connection lifecycle ===
                PostgresCommand::Connect { config, reply } => {
                    match create_connection(&config).await {
                        Ok(c) => {
                            // Verify connection with a simple query
                            if let Err(e) = c.simple_query("SELECT 1").await {
                                let _ = reply.send(Err(e.to_string()));
                                let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                                let _ = tabs.request_render();
                                continue;
                            }

                            client = Some(c);
                            let _ = event_tx.send(PostgresEvent::Connected);
                            let _ = tabs.request_render();
                            let _ = reply.send(Ok(()));
                        }
                        Err(e) => {
                            let _ = reply.send(Err(e.clone()));
                            let _ = event_tx.send(PostgresEvent::Error(e));
                            let _ = tabs.request_render();
                        }
                    }
                }

                PostgresCommand::Ping { reply } => {
                    if let Some(ref c) = client {
                        match c.simple_query("SELECT 1").await {
                            Ok(_) => {
                                let _ = reply.send(Ok(()));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(e.to_string()));
                            }
                        }
                    } else {
                        let _ = reply.send(Err("Not connected".to_string()));
                    }
                }

                PostgresCommand::Disconnect => {
                    // Drop client to close the connection
                    drop(client.take());
                    let _ = event_tx.send(PostgresEvent::Disconnected);
                    let _ = tabs.request_render();
                    break;
                }

                // === Schema commands ===
                PostgresCommand::Schema(schema_cmd) => {
                    if let Some(ref c) = client {
                        Self::handle_schema(c, schema_cmd, &event_tx, &tabs).await;
                    } else {
                        let _ = event_tx.send(PostgresEvent::Error("Not connected".into()));
                        let _ = tabs.request_render();
                    }
                }

                // === CRUD commands ===
                PostgresCommand::Crud(crud_cmd) => {
                    if let Some(ref c) = client {
                        Self::handle_crud(c, crud_cmd, &event_tx, &tabs).await;
                    } else {
                        let _ = event_tx.send(PostgresEvent::Error("Not connected".into()));
                        let _ = tabs.request_render();
                    }
                }

                // === Load commands ===
                PostgresCommand::Load(load_cmd) => {
                    if let Some(ref c) = client {
                        Self::handle_load(c, load_cmd, &event_tx, &tabs).await;
                    } else {
                        let _ = event_tx.send(PostgresEvent::Error("Not connected".into()));
                        let _ = tabs.request_render();
                    }
                }

                // === Browser commands ===
                PostgresCommand::Browser(browser_cmd) => {
                    if let Some(ref c) = client {
                        browser_svc.handle(c, browser_cmd, &event_tx, &tabs).await;
                    } else {
                        let _ = event_tx.send(PostgresEvent::Error("Not connected".into()));
                        let _ = tabs.request_render();
                    }
                }
            }
        }

        // Loop exits on sender drop or Disconnect -- ensure client is cleaned up
        drop(client);
    }

    // === Schema command dispatch ===

    async fn handle_schema(
        client: &tokio_postgres::Client,
        cmd: SchemaCommand,
        event_tx: &mpsc::UnboundedSender<PostgresEvent>,
        tabs: &Arc<dyn TabManager>,
    ) {
        match cmd {
            SchemaCommand::ListSchemas => match SchemaService::list_schemas(client).await {
                Ok(schemas) => {
                    let _ = event_tx.send(PostgresEvent::Schema(SchemaEvent::Schemas(schemas)));
                }
                Err(e) => {
                    let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                }
            },

            SchemaCommand::ListTables { schema } => {
                match SchemaService::list_tables(client, &schema).await {
                    Ok(tables) => {
                        let _ = event_tx
                            .send(PostgresEvent::Schema(SchemaEvent::Tables { schema, tables }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                    }
                }
            }

            SchemaCommand::ListColumns { schema, table } => {
                match SchemaService::list_columns(client, &schema, &table).await {
                    Ok(columns) => {
                        let _ = event_tx.send(PostgresEvent::Schema(SchemaEvent::Columns {
                            schema,
                            table,
                            columns,
                        }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                    }
                }
            }

            SchemaCommand::ListIndexes { schema, table } => {
                match SchemaService::list_indexes(client, &schema, &table).await {
                    Ok(indexes) => {
                        let _ = event_tx.send(PostgresEvent::Schema(SchemaEvent::Indexes {
                            schema,
                            table,
                            indexes,
                        }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                    }
                }
            }

            SchemaCommand::ListForeignKeys { schema, table } => {
                match SchemaService::list_foreign_keys(client, &schema, &table).await {
                    Ok(foreign_keys) => {
                        let _ =
                            event_tx.send(PostgresEvent::Schema(SchemaEvent::ForeignKeysLoaded {
                                schema,
                                table,
                                foreign_keys,
                            }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                    }
                }
            }

            SchemaCommand::ShowCreateTable { schema, table } => {
                match SchemaService::show_create_table(client, &schema, &table).await {
                    Ok(ddl) => {
                        let _ =
                            event_tx.send(PostgresEvent::Schema(SchemaEvent::CreateTableDdl {
                                schema,
                                table,
                                ddl,
                            }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                    }
                }
            }

            SchemaCommand::LoadStructure { schema, table } => {
                match SchemaService::load_structure(client, &schema, &table).await {
                    Ok((columns, indexes, foreign_keys)) => {
                        let _ =
                            event_tx.send(PostgresEvent::Schema(SchemaEvent::StructureLoaded {
                                schema,
                                table,
                                columns,
                                indexes,
                                foreign_keys,
                            }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                    }
                }
            }
        }
        let _ = tabs.request_render();
    }

    // === CRUD command dispatch ===

    async fn handle_crud(
        client: &tokio_postgres::Client,
        cmd: CrudCommand,
        event_tx: &mpsc::UnboundedSender<PostgresEvent>,
        tabs: &Arc<dyn TabManager>,
    ) {
        match cmd {
            CrudCommand::ExecuteQuery { sql } => {
                // Use multi-query for single queries too -- consistent behavior
                match CrudService::execute_multi_query(client, &sql).await {
                    Ok(results) => {
                        if results.len() == 1 {
                            match results.into_iter().next().unwrap() {
                                StatementResult::Select { columns, rows } => {
                                    let _ = event_tx.send(PostgresEvent::Crud(
                                        CrudEvent::QueryResult { columns, rows },
                                    ));
                                }
                                StatementResult::Affected(n) => {
                                    let _ = event_tx.send(PostgresEvent::Crud(
                                        CrudEvent::MultiQueryResult {
                                            results: vec![StatementResult::Affected(n)],
                                        },
                                    ));
                                }
                                StatementResult::Empty => {
                                    let _ = event_tx.send(PostgresEvent::Crud(
                                        CrudEvent::QueryResult {
                                            columns: vec![],
                                            rows: vec![],
                                        },
                                    ));
                                }
                                StatementResult::Error(e) => {
                                    let _ = event_tx.send(PostgresEvent::Error(e));
                                }
                            }
                        } else {
                            let _ = event_tx.send(PostgresEvent::Crud(
                                CrudEvent::MultiQueryResult { results },
                            ));
                        }
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                    }
                }
            }

            CrudCommand::ExecuteMultiQuery { sql } => {
                match CrudService::execute_multi_query(client, &sql).await {
                    Ok(results) => {
                        let _ = event_tx
                            .send(PostgresEvent::Crud(CrudEvent::MultiQueryResult { results }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                    }
                }
            }

            CrudCommand::SaveChanges {
                schema,
                table,
                columns,
                rows,
                pk_columns,
                pending,
                deleted,
            } => {
                match CrudService::save_changes(
                    client,
                    &schema,
                    &table,
                    &columns,
                    &rows,
                    &pk_columns,
                    &pending,
                    &deleted,
                )
                .await
                {
                    Ok(affected_rows) => {
                        let _ = event_tx
                            .send(PostgresEvent::Crud(CrudEvent::ChangesSaved { affected_rows }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                    }
                }
            }

            CrudCommand::InsertRow {
                schema,
                table,
                columns,
                values,
            } => {
                match CrudService::insert_row(client, &schema, &table, &columns, &values).await {
                    Ok(new_row) => {
                        let _ = event_tx
                            .send(PostgresEvent::Crud(CrudEvent::RowInserted { new_row }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                    }
                }
            }
        }
        let _ = tabs.request_render();
    }

    // === Load command dispatch ===

    async fn handle_load(
        client: &tokio_postgres::Client,
        cmd: LoadCommand,
        event_tx: &mpsc::UnboundedSender<PostgresEvent>,
        tabs: &Arc<dyn TabManager>,
    ) {
        match cmd {
            LoadCommand::LoadTableData {
                schema,
                table,
                page_size,
                offset,
            } => {
                match DataLoaderService::load_table_data(client, &schema, &table, page_size, offset)
                    .await
                {
                    Ok((columns, rows, total_count)) => {
                        let _ = event_tx.send(PostgresEvent::Load(LoadEvent::TableDataLoaded {
                            columns,
                            rows,
                            total_count,
                        }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                    }
                }
            }

            LoadCommand::LoadRowCount { schema, table } => {
                match DataLoaderService::load_row_count(client, &schema, &table).await {
                    Ok(count) => {
                        let _ = event_tx
                            .send(PostgresEvent::Load(LoadEvent::RowCountLoaded { count }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                    }
                }
            }

            LoadCommand::LoadPrimaryKeys { schema, table } => {
                match DataLoaderService::load_primary_keys(client, &schema, &table).await {
                    Ok(pk_cols) => {
                        let _ = event_tx
                            .send(PostgresEvent::Load(LoadEvent::PrimaryKeysLoaded { pk_cols }));
                    }
                    Err(e) => {
                        let _ = event_tx.send(PostgresEvent::Error(e.to_string()));
                    }
                }
            }
        }
        let _ = tabs.request_render();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // === Send/Sync compile-time assertions ===

    fn assert_send<T: Send>() {}

    #[allow(dead_code)]
    fn assert_sync<T: Sync>() {}

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn test_postgres_command_is_send() {
        assert_send::<PostgresCommand>();
    }

    #[test]
    fn test_postgres_event_is_send() {
        assert_send::<PostgresEvent>();
    }

    #[test]
    fn test_postgres_service_is_send() {
        assert_send::<PostgresService>();
    }

    #[test]
    fn test_postgres_service_in_mutex_is_sync() {
        // PostgresService is Send but NOT Sync (UnboundedReceiver is !Sync).
        // Wrapping in Mutex<T> provides Sync when T: Send.
        // This proves the Plugin: Send + Sync pattern works.
        assert_send_sync::<std::sync::Mutex<PostgresService>>();
    }
}
