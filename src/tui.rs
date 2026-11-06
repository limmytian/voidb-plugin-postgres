//! Standalone PostgreSQL schema-aware data-inspector TUI (Requirement 83 pilot).
//!
//! The pilot follows the retained family TUI pattern (see the Redis pilot):
//! overlay-first routing, a non-modal error banner, per-view selection memory,
//! a persistent hint footer, full-screen scrollable viewers, confirm-quit,
//! loading markers, Length(4) header/status panels, fixture launch with
//! `build_postgres_tui_evidence`, a secret-leak scan, and secret-free preflight
//! JSON. All reads go through the existing `PostgresService` channel contract;
//! the TUI adds no backend operations.
//!
//! Scope is deliberately bounded to schema-aware read-only data triage:
//! schemas/tables browsing, bounded table-data paging with a typed row-detail
//! viewer, and table structure inspection (columns/indexes/foreign keys).
//! Mutations are deferred as `later candidate`: the service layer exposes
//! batch `SaveChanges`/`InsertRow` operations but no bounded single-row
//! delete, so this slice ships READ-ONLY and the CLI remains the supported
//! general path for writes.

use std::collections::VecDeque;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::oneshot;
use voidb_core::database::types::ForeignKeyInfo;
use voidb_core::{
    CellValue, ColumnInfo, Row, TabInfo, TabManager, TuiLaunchPlan, retained_tui_quality_gate,
};

use crate::config::PostgresConfig;
use crate::service::{
    IndexInfo, LoadCommand, LoadEvent, PostgresCommand, PostgresEvent, PostgresService,
    SchemaCommand, SchemaEvent, TableMeta,
};

const TABLES_HINTS: &str = "1 tables 2 structure j/k / filter s schema i data r refresh ? q quit";
const STRUCTURE_HINTS: &str = "1 tables 2 structure j/k r refresh ? q quit";
const VIEWER_LINE_LIMIT: usize = 2_000;
const VIEWER_BYTE_LIMIT: usize = 2 * 1024 * 1024;
const INPUT_LIMIT: usize = 128;
const PAGE_STEP: isize = 10;
/// Bounded page size sent as LIMIT to `LoadCommand::LoadTableData`; pages are
/// replaced, never appended, so the viewport stays bounded.
const PAGE_SIZE: usize = 10;
const CELL_CAP: usize = 32;
const DETAIL_CAP: usize = 200;

/// Where the TUI launch came from. Mirrors the family `RedisTuiSource`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresTuiSource {
    Profile,
    Connection,
    Fixture,
}

/// Launch description for the standalone PostgreSQL data TUI. Mirrors the
/// family launch shape.
#[derive(Debug, Clone)]
pub struct PostgresTuiLaunch {
    pub profile_label: String,
    pub config: Option<PostgresConfig>,
    pub source: PostgresTuiSource,
    pub fixture_path: Option<String>,
    pub purpose: String,
    pub readonly: bool,
    pub restore: bool,
    pub launch_plan: Option<TuiLaunchPlan>,
}

// ---------------------------------------------------------------------------
// Fixture model
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct PostgresTuiFixture {
    profile_label: Option<String>,
    server_label: String,
    database: String,
    status: Option<String>,
    target_error: Option<String>,
    #[serde(default)]
    error_tables: Vec<String>,
    error_message: Option<String>,
    #[serde(default)]
    schemas: Vec<String>,
    #[serde(default)]
    tables: Vec<FixtureTable>,
}

#[derive(Debug, Deserialize)]
struct FixtureTable {
    schema: String,
    name: String,
    #[serde(default)]
    table_type: String,
    row_estimate: Option<i64>,
    #[serde(default)]
    columns: Vec<FixtureColumn>,
    #[serde(default)]
    rows: Vec<Vec<Value>>,
    structure: Option<FixtureStructure>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureColumn {
    name: String,
    data_type: String,
    #[serde(default)]
    nullable: bool,
    #[serde(default)]
    is_primary_key: bool,
    default_value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FixtureStructure {
    #[serde(default)]
    columns: Vec<FixtureColumn>,
    #[serde(default)]
    indexes: Vec<FixtureIndex>,
    #[serde(default)]
    foreign_keys: Vec<FixtureForeignKey>,
}

#[derive(Debug, Deserialize)]
struct FixtureIndex {
    name: String,
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    is_unique: bool,
    #[serde(default)]
    index_type: String,
}

#[derive(Debug, Deserialize)]
struct FixtureForeignKey {
    name: String,
    #[serde(default)]
    columns: Vec<String>,
    referenced_table: String,
    #[serde(default)]
    referenced_columns: Vec<String>,
    #[serde(default)]
    on_update: String,
    #[serde(default)]
    on_delete: String,
}

/// Structure snapshot cached for the table the structure view shows.
#[derive(Debug, Clone)]
struct StructureView {
    columns: Vec<ColumnInfo>,
    indexes: Vec<IndexInfo>,
    foreign_keys: Vec<ForeignKeyInfo>,
}

/// In-memory fixture state. Fixture reads resolve synchronously against this
/// state so paging evidence stays deterministic without a server. Rows are
/// stored as the full bounded fixture row list and sliced with the same
/// LIMIT/OFFSET semantics the live `DataLoaderService` applies.
struct PostgresTuiData {
    profile_label: String,
    server_label: String,
    database: String,
    status: String,
    target_error: Option<String>,
    error_tables: Vec<String>,
    error_message: String,
    schemas: Vec<String>,
    tables: Vec<FixtureTableData>,
}

struct FixtureTableData {
    schema: String,
    name: String,
    table_type: String,
    row_estimate: Option<i64>,
    columns: Vec<ColumnInfo>,
    rows: Vec<Row>,
    structure: Option<StructureView>,
}

fn load_fixture(path: &str) -> Result<PostgresTuiData> {
    let text = fs::read_to_string(path).with_context(|| format!("read fixture {path}"))?;
    let fixture: PostgresTuiFixture =
        serde_json::from_str(&text).with_context(|| format!("parse fixture {path}"))?;

    let mut tables: Vec<FixtureTableData> = fixture
        .tables
        .into_iter()
        .map(|table| FixtureTableData {
            schema: table.schema,
            name: table.name,
            table_type: if table.table_type.is_empty() {
                "BASE TABLE".to_string()
            } else {
                table.table_type
            },
            row_estimate: table.row_estimate,
            columns: table.columns.iter().map(fixture_column_info).collect(),
            rows: convert_fixture_rows(&table.columns, &table.rows),
            structure: table.structure.map(|structure| StructureView {
                columns: structure.columns.iter().map(fixture_column_info).collect(),
                indexes: structure
                    .indexes
                    .iter()
                    .map(|index| IndexInfo {
                        name: index.name.clone(),
                        columns: index.columns.clone(),
                        is_unique: index.is_unique,
                        index_type: index.index_type.clone(),
                    })
                    .collect(),
                foreign_keys: structure
                    .foreign_keys
                    .iter()
                    .map(|fk| ForeignKeyInfo {
                        name: fk.name.clone(),
                        columns: fk.columns.clone(),
                        referenced_table: fk.referenced_table.clone(),
                        referenced_columns: fk.referenced_columns.clone(),
                        on_update: fk.on_update.clone(),
                        on_delete: fk.on_delete.clone(),
                    })
                    .collect(),
            }),
        })
        .collect();
    // Mirror the live service ordering (`ORDER BY table_name` per schema).
    tables.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));

    Ok(PostgresTuiData {
        profile_label: fixture
            .profile_label
            .unwrap_or_else(|| "fixture-postgres".to_string()),
        server_label: fixture.server_label,
        database: fixture.database,
        status: fixture
            .status
            .unwrap_or_else(|| "fixture postgres operations ready".to_string()),
        target_error: fixture.target_error,
        error_tables: fixture.error_tables,
        error_message: fixture
            .error_message
            .unwrap_or_else(|| "target error: fixture read rejected".to_string()),
        schemas: fixture.schemas,
        tables,
    })
}

fn fixture_column_info(column: &FixtureColumn) -> ColumnInfo {
    ColumnInfo {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
        is_primary_key: column.is_primary_key,
        default_value: column.default_value.clone(),
        max_length: None,
        extra: String::new(),
    }
}

/// Convert raw fixture JSON cells to typed `CellValue`s the same way the live
/// `value_convert::decode_value` path does for real rows: numbers by column
/// family, temporal strings, JSON/JSONB documents, PG array literals, UUIDs,
/// and explicit NULLs.
fn convert_fixture_rows(columns: &[FixtureColumn], rows: &[Vec<Value>]) -> Vec<Row> {
    rows.iter()
        .map(|cells| Row {
            values: cells
                .iter()
                .enumerate()
                .map(|(idx, raw)| {
                    let data_type = columns.get(idx).map(|c| c.data_type.as_str()).unwrap_or("");
                    fixture_cell(raw, data_type)
                })
                .collect(),
        })
        .collect()
}

fn fixture_cell(raw: &Value, data_type: &str) -> CellValue {
    let dt = data_type.to_lowercase();
    match raw {
        Value::Null => CellValue::Null,
        Value::Bool(b) => CellValue::Bool(*b),
        Value::Number(number) => {
            if dt.contains("int") || dt.contains("serial") {
                number
                    .as_i64()
                    .map(CellValue::Int)
                    .unwrap_or_else(|| CellValue::Text(number.to_string()))
            } else {
                number
                    .as_f64()
                    .map(CellValue::Float)
                    .unwrap_or_else(|| CellValue::Text(number.to_string()))
            }
        }
        Value::String(text) => fixture_string_cell(text, &dt),
        // JSON/JSONB documents and PG array literals serialized by the loader.
        Value::Array(items) => {
            if dt.contains("[]") {
                CellValue::Text(pg_array_literal(items))
            } else {
                CellValue::Json(raw.to_string())
            }
        }
        Value::Object(_) => CellValue::Json(raw.to_string()),
    }
}

fn fixture_string_cell(text: &str, dt: &str) -> CellValue {
    if dt.contains("json") {
        CellValue::Json(text.to_string())
    } else if dt == "date" {
        CellValue::Date(text.to_string())
    } else if dt.contains("timestamp") {
        CellValue::DateTime(text.to_string())
    } else if dt == "time" || dt == "timetz" {
        CellValue::Time(text.to_string())
    } else if dt == "uuid" {
        CellValue::Uuid(text.to_string())
    } else {
        // text/varchar/numeric-as-string/PG array literals all render as text,
        // matching the live fallback decode for unknown types.
        CellValue::Text(text.to_string())
    }
}

fn pg_array_literal(items: &[Value]) -> String {
    let rendered: Vec<String> = items
        .iter()
        .map(|item| match item {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        })
        .collect();
    format!("{{{}}}", rendered.join(","))
}

impl PostgresTuiData {
    fn find_table(&self, schema: &str, table: &str) -> Option<&FixtureTableData> {
        self.tables
            .iter()
            .find(|candidate| candidate.schema == schema && candidate.name == table)
    }

    fn is_error_table(&self, schema: &str, table: &str) -> bool {
        self.error_tables
            .iter()
            .any(|qualified| qualified == &format!("{schema}.{table}"))
    }

    /// Resolve one LIMIT/OFFSET page the way `DataLoaderService::load_table_data`
    /// does: bounded slice plus total count. Erroring tables reject before any
    /// row is produced so the TUI proves a target error keeps it responsive.
    fn resolve_page(
        &self,
        schema: &str,
        table: &str,
        offset: usize,
        page_size: usize,
    ) -> std::result::Result<(Vec<ColumnInfo>, Vec<Row>, i64), String> {
        if self.is_error_table(schema, table) {
            return Err(self.error_message.clone());
        }
        let Some(found) = self.find_table(schema, table) else {
            return Err(format!(
                "target error: no fixture table {schema}.{table} (fixture)"
            ));
        };
        let rows = found
            .rows
            .iter()
            .skip(offset)
            .take(page_size)
            .cloned()
            .collect();
        Ok((found.columns.clone(), rows, found.rows.len() as i64))
    }

    fn resolve_structure(
        &self,
        schema: &str,
        table: &str,
    ) -> std::result::Result<StructureView, String> {
        if self.is_error_table(schema, table) {
            return Err(self.error_message.clone());
        }
        let Some(found) = self.find_table(schema, table) else {
            return Err(format!(
                "target error: no fixture table {schema}.{table} (fixture)"
            ));
        };
        found.structure.clone().ok_or_else(|| {
            format!("target error: no fixture structure for table {schema}.{table} (fixture)")
        })
    }
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum View {
    Tables,
    Structure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Browser,
    Filter,
}

/// Full-screen or popup surfaces stacked on top of the browser. Input is
/// routed to the active overlay first, so each surface owns its key space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Overlay {
    None,
    Help,
    DataViewer,
    RowDetail,
    SchemaPrompt,
    ConfirmQuit,
}

/// Bounded text buffer for viewer bodies (same contract as the family TUIs).
struct BoundedLines {
    lines: VecDeque<String>,
    bytes: usize,
    dropped_lines: usize,
    max_lines: usize,
    max_bytes: usize,
}

pub fn write_postgres_tui_preflight(launch: &PostgresTuiLaunch) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&preflight_value(launch))?
    );
    Ok(())
}

pub fn build_postgres_tui_evidence(launch: &PostgresTuiLaunch) -> Result<Value> {
    let data = if let Some(path) = &launch.fixture_path {
        load_fixture(path)?
    } else {
        let config = launch
            .config
            .as_ref()
            .context("postgres tui evidence requires a profile, connection, or fixture")?;
        config_data(launch.profile_label.clone(), config)
    };

    let mut evidence = json!({
        "schema_version": 1,
        "kind": "postgres_tui_fixture_evidence",
        "quality_gate": retained_tui_quality_gate(
            "postgres",
            &["fixture-postgres-ops", "fixture-postgres-local"],
            &["target error"],
            80,
            24,
            10_000
        ),
        "preflight": preflight_value(launch),
        "transcript": {
            "profile_label": data.profile_label,
            "server_label": data.server_label,
            "database": data.database,
            "status": data.status,
            "target_error": data.target_error,
            "schemas": data.schemas,
            "tables": data.tables.iter().map(table_value).collect::<Vec<_>>(),
            "viewer_bounds": {
                "max_lines": VIEWER_LINE_LIMIT,
                "max_bytes": VIEWER_BYTE_LIMIT,
                "page_size": PAGE_SIZE,
                "cell_render_cap": CELL_CAP
            }
        },
        "mutation_transcript": [
            {
                "slice": "read_only",
                "staged_plans": "none; the TUI exposes no mutation keys",
                "service_mutations_available": [
                    "CrudCommand::SaveChanges (batch grid save: pending cell edits plus row-deletion indices over a full page snapshot, requires PK columns)",
                    "CrudCommand::InsertRow"
                ],
                "why_not_shipped": "no bounded single-row delete operation exists in the service layer; wiring a destructive pilot mutation through the batch grid-save surface would need its own confirmation and redaction evidence",
                "outcome": "later candidate",
                "readonly_launch_behavior": "accepted and reported in preflight; there is no mutation surface to block"
            }
        ],
        "coverage": [
            "startup",
            "tables_structure_tabs",
            "schema_switch_prompt",
            "tables_client_filter_match_count",
            "table_paging_offset_history",
            "data_viewer_filter_match_count",
            "row_detail_viewer",
            "typed_cell_rendering",
            "structure_viewer_columns_indexes_fks",
            "empty_table_state",
            "partial_page_state",
            "truncated_page_state",
            "non_modal_error_banner",
            "target_error_responsive",
            "readonly_no_mutation_surface",
            "quit_confirmation",
            "resize",
            "quit_restore",
            "secret_leak_scan"
        ],
        "deferred": {
            "server_view": "no server-info command exists in the postgres service layer (redis's ServerCommand has no postgres counterpart); deferred as later candidate",
            "mutation_workflows": ["row_delete", "cell_edit", "insert_row"],
            "outcome": "later candidate within the accepted pilot",
            "supported_mutations": [],
            "cli_remains_supported_general_path": true
        },
        "paging_policy": {
            "semantics": "one bounded LIMIT/OFFSET page at a time via LoadCommand::LoadTableData; the next page is fetched at offset+page_size and pages are replaced, never appended; the previous offset is kept in a history stack for back navigation",
            "total_count": "COUNT(*) ships with every page from the existing service so partial and truncated states are visible",
            "fixture_paging": "LIMIT/OFFSET emulated over the fixture row list so paging evidence stays deterministic"
        },
        "mutation_policy": {
            "slice": "read_only",
            "staged_plans": "none",
            "rationale": "the service exposes batch SaveChanges/InsertRow but no bounded single-row delete; slice 1 ships read-only and all mutations stay later candidate",
            "readonly_launch": "accepted and reported in preflight; no mutation surface exists to block",
            "cli_write_path": "postgres query remains the supported write path"
        },
        "secret_leak_scan": null
    });

    let rendered = serde_json::to_string(&evidence)?;
    let markers = secret_leak_markers(&rendered);
    evidence["secret_leak_scan"] = json!({
        "passed": markers.is_empty(),
        "marker_count": markers.len(),
        "markers": markers
    });
    Ok(evidence)
}

pub async fn run_postgres_tui(launch: PostgresTuiLaunch) -> Result<()> {
    let mut app = PostgresTuiApp::new(launch)?;
    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, &mut app);
    app.shutdown();
    ratatui::restore();
    result
}

fn preflight_value(launch: &PostgresTuiLaunch) -> Value {
    let fixture_data = launch
        .fixture_path
        .as_ref()
        .and_then(|path| load_fixture(path).ok());
    let connection = launch
        .config
        .as_ref()
        .map(connection_value)
        .or_else(|| {
            fixture_data.as_ref().map(|data| {
                json!({
                    "kind": "postgres",
                    "server_label": data.server_label,
                    "database": data.database,
                    "secret_material": "redacted"
                })
            })
        })
        .unwrap_or_else(|| {
            json!({
                "kind": "postgres",
                "server_label": "fixture",
                "database": "fixture",
                "secret_material": "redacted"
            })
        });
    let database = launch
        .config
        .as_ref()
        .map(|config| config.database.clone())
        .or_else(|| fixture_data.as_ref().map(|data| data.database.clone()))
        .unwrap_or_default();
    let launch_plan = launch.launch_plan.as_ref().map(|plan| {
        json!({
            "schema_version": plan.schema_version,
            "plugin_id": plan.plugin_id,
            "command": plan.command,
            "args": plan.args,
            "profile": plan.profile,
            "purpose": plan.purpose,
            "readonly": plan.readonly,
            "restore": plan.restore,
            "raw_input": plan.raw_input,
            "credential_ref_count": plan.credential_grant.credential_refs.len(),
            "credential_grant_id": plan.credential_grant.id,
            "redaction": plan.redaction
        })
    });

    json!({
        "ok": true,
        "command": "postgres tui",
        "plugin_id": "postgres",
        "profile_label": fixture_data
            .as_ref()
            .map(|data| data.profile_label.clone())
            .unwrap_or_else(|| launch.profile_label.clone()),
        "source": launch.source,
        "purpose": launch.purpose,
        "readonly": launch.readonly,
        "restore": launch.restore,
        "raw_input": false,
        "fixture": launch.fixture_path.is_some(),
        "database": database,
        "connection": connection,
        "privacy": {
            "diagnostics_include_password": false,
            "diagnostics_include_connection_url": false,
            "diagnostics_include_row_values": false
        },
        "stream_bounds": {
            "data_viewer": {
                "max_lines": VIEWER_LINE_LIMIT,
                "max_bytes": VIEWER_BYTE_LIMIT,
                "page_size": PAGE_SIZE
            }
        },
        "service_boundary": "PostgresService::Channel",
        "modes": [
            "tables",
            "structure",
            "filter",
            "schema_prompt",
            "data_viewer",
            "row_detail_viewer",
            "confirm_quit",
            "target_error"
        ],
        "mutation_policy": {
            "slice": "read_only",
            "mutations": []
        },
        "launch_plan": launch_plan
    })
}

fn run_loop(terminal: &mut ratatui::DefaultTerminal, app: &mut PostgresTuiApp) -> Result<()> {
    // Startup auto-load (standalone TUI acceptance): the primary view must
    // show live data without requiring a manual refresh key.
    if app.service.is_some() {
        app.refresh_view();
    }
    terminal.draw(|frame| app.draw(frame))?;
    loop {
        let mut dirty = app.drain_service();
        if app.should_quit {
            return Ok(());
        }

        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key) => {
                    app.handle_key(key);
                    dirty = true;
                }
                Event::Resize(cols, rows) => {
                    app.status = format!("resized postgres view to {cols}x{rows}");
                    dirty = true;
                }
                _ => {}
            }
        }
        if dirty {
            terminal.draw(|frame| app.draw(frame))?;
        }
    }
}

fn config_data(profile_label: String, config: &PostgresConfig) -> PostgresTuiData {
    PostgresTuiData {
        profile_label,
        server_label: format!("{}:{}", config.host, config.port),
        database: config.database.clone(),
        status: "connecting through PostgresService channel mode".to_string(),
        target_error: None,
        error_tables: Vec::new(),
        error_message: "target error: fixture read rejected".to_string(),
        schemas: Vec::new(),
        tables: Vec::new(),
    }
}

fn connection_value(config: &PostgresConfig) -> Value {
    json!({
        "kind": "postgres",
        "host_label": config.host,
        "port": config.port,
        "database": config.database,
        "auth": if config.password.is_empty() { "none" } else { "password" },
        "username_present": !config.username.is_empty(),
        "ssl_mode": config.ssl_mode,
        "secret_material": "redacted"
    })
}

struct PostgresTuiApp {
    profile_label: String,
    server_label: String,
    database: String,
    source: PostgresTuiSource,
    purpose: String,
    readonly: bool,
    restore: bool,
    view: View,
    /// Active schema; empty until schemas load in live mode.
    schema: String,
    schema_names: Vec<String>,
    schemas_loaded: bool,
    schemas_pending: bool,
    tables: Vec<TableMeta>,
    tables_loaded: bool,
    tables_pending: bool,
    selection: usize,
    filter: String,
    mode: Mode,
    overlay: Overlay,
    error_banner: Option<String>,
    // === Table data viewer state (one bounded page at a time) ===
    data_schema: String,
    data_table: String,
    data_columns: Vec<ColumnInfo>,
    data_rows: Vec<Row>,
    data_total: Option<i64>,
    data_offset: usize,
    /// Previous page offsets so `b` can walk back without losing context.
    data_history: Vec<usize>,
    data_page_index: usize,
    data_pending: bool,
    data_selection: usize,
    data_filter: String,
    viewer_filtering: bool,
    // === Row detail viewer state ===
    detail_title: String,
    detail_lines: BoundedLines,
    detail_scroll: usize,
    // === Structure view state ===
    structure_request: Option<(String, String)>,
    structure_view: Option<StructureView>,
    structure_pending: bool,
    structure_selection: usize,
    // === Schema prompt state ===
    schema_input: String,
    schema_choice: usize,
    fixture: Option<PostgresTuiData>,
    service: Option<PostgresService>,
    status: String,
    should_quit: bool,
    render_quit: Arc<AtomicBool>,
}

impl PostgresTuiApp {
    fn new(launch: PostgresTuiLaunch) -> Result<Self> {
        let data = if let Some(path) = &launch.fixture_path {
            load_fixture(path)?
        } else {
            let config = launch
                .config
                .as_ref()
                .context("postgres tui requires a profile, connection, or fixture")?;
            config_data(launch.profile_label.clone(), config)
        };

        let render_quit = Arc::new(AtomicBool::new(false));
        let service = if launch.fixture_path.is_none() {
            let config = launch
                .config
                .clone()
                .context("postgres tui requires config outside fixture mode")?;
            let tabs = Arc::new(StandalonePostgresTabManager::new(render_quit.clone()));
            let clipboard: Arc<tokio::sync::RwLock<Option<voidb_core::VoidbClipboard>>> =
                Arc::new(tokio::sync::RwLock::new(None));
            let runtime = tokio::runtime::Handle::current();
            let service = PostgresService::new(config.clone(), tabs, runtime, clipboard);
            let (reply, _reply_rx) = oneshot::channel();
            service.send(PostgresCommand::Connect { config, reply });
            Some(service)
        } else {
            None
        };

        let is_fixture = launch.fixture_path.is_some();
        let mut app = Self {
            profile_label: data.profile_label.clone(),
            server_label: data.server_label.clone(),
            database: data.database.clone(),
            source: launch.source,
            purpose: launch.purpose,
            readonly: launch.readonly,
            restore: launch.restore,
            view: View::Tables,
            schema: String::new(),
            schema_names: Vec::new(),
            schemas_loaded: false,
            schemas_pending: false,
            tables: Vec::new(),
            tables_loaded: false,
            tables_pending: false,
            selection: 0,
            filter: String::new(),
            mode: Mode::Browser,
            overlay: Overlay::None,
            error_banner: data.target_error.clone(),
            data_schema: String::new(),
            data_table: String::new(),
            data_columns: Vec::new(),
            data_rows: Vec::new(),
            data_total: None,
            data_offset: 0,
            data_history: Vec::new(),
            data_page_index: 1,
            data_pending: false,
            data_selection: 0,
            data_filter: String::new(),
            viewer_filtering: false,
            detail_title: String::new(),
            detail_lines: BoundedLines::new(VIEWER_LINE_LIMIT, VIEWER_BYTE_LIMIT),
            detail_scroll: 0,
            structure_request: None,
            structure_view: None,
            structure_pending: false,
            structure_selection: 0,
            schema_input: String::new(),
            schema_choice: 0,
            fixture: if is_fixture { Some(data) } else { None },
            service,
            status: String::new(),
            should_quit: false,
            render_quit,
        };
        if is_fixture {
            // Fixture mode loads schemas and the first table list eagerly so
            // the first frame already shows the browser.
            app.request_schemas();
        }
        if let Some(error) = app
            .fixture
            .as_ref()
            .and_then(|fixture| fixture.target_error.clone())
        {
            app.error_banner = Some(error.clone());
            app.status = format!("{} | {}", app.status, safe_error_summary(&error));
        }
        Ok(app)
    }

    fn shutdown(&mut self) {
        if let Some(service) = &self.service {
            service.send(PostgresCommand::Disconnect);
        }
    }

    fn drain_service(&mut self) -> bool {
        let mut changed = false;
        if self.render_quit.load(Ordering::SeqCst) {
            changed |= !self.should_quit;
            self.should_quit = true;
        }
        let Some(mut service) = self.service.take() else {
            return changed;
        };
        while let Some(event) = service.poll_event() {
            changed = true;
            self.handle_service_event(event);
        }
        self.service = Some(service);
        changed
    }

    fn handle_service_event(&mut self, event: PostgresEvent) {
        match event {
            PostgresEvent::Connected => {
                self.status = "connected; loading schemas".to_string();
                self.request_schemas();
            }
            PostgresEvent::Disconnected => {
                self.status = "disconnected".to_string();
            }
            PostgresEvent::Schema(SchemaEvent::Schemas(schemas)) => {
                self.schemas_pending = false;
                self.schemas_loaded = true;
                self.schema_names = schemas;
                if !self.schema_names.iter().any(|name| name == &self.schema) {
                    self.schema = self.schema_names.first().cloned().unwrap_or_default();
                    self.tables.clear();
                    self.tables_loaded = false;
                    self.selection = 0;
                }
                self.request_tables();
            }
            PostgresEvent::Schema(SchemaEvent::Tables { schema, tables }) => {
                self.tables_pending = false;
                if schema == self.schema {
                    self.tables = tables;
                    self.tables_loaded = true;
                    self.trim_selection();
                    self.status = format!(
                        "loaded {} tables in schema {}",
                        self.tables.len(),
                        self.schema
                    );
                }
            }
            PostgresEvent::Schema(SchemaEvent::StructureLoaded {
                schema,
                table,
                columns,
                indexes,
                foreign_keys,
            }) => {
                self.structure_pending = false;
                let matches = self
                    .structure_request
                    .as_ref()
                    .is_some_and(|(s, t)| s == &schema && t == &table);
                if matches {
                    self.structure_view = Some(StructureView {
                        columns,
                        indexes,
                        foreign_keys,
                    });
                    self.structure_selection = 0;
                    self.status = format!("structure loaded for {schema}.{table}");
                }
            }
            // Columns/Indexes/ForeignKeysLoaded/CreateTableDdl belong to other
            // surfaces; the pilot never sends those commands.
            PostgresEvent::Schema(_) => {}
            PostgresEvent::Load(LoadEvent::TableDataLoaded {
                columns,
                rows,
                total_count,
            }) => {
                self.data_pending = false;
                self.data_columns = columns;
                self.data_rows = rows;
                self.data_total = total_count;
                self.set_data_selection(0);
                self.status = format!(
                    "loaded page {} of {}.{} (offset {}): {} rows of {} total",
                    self.data_page_index,
                    self.data_schema,
                    self.data_table,
                    self.data_offset,
                    self.data_rows.len(),
                    total_count
                        .map(|t| t.to_string())
                        .unwrap_or_else(|| "?".into()),
                );
            }
            PostgresEvent::Load(LoadEvent::RowCountLoaded { count }) => {
                self.data_total = Some(count);
            }
            PostgresEvent::Load(_) => {}
            // The read-only pilot sends no Crud/Browser commands; ignore their
            // events so unrelated service traffic never disturbs the browser.
            PostgresEvent::Crud(_) | PostgresEvent::Browser(_) => {}
            PostgresEvent::Error(message) => {
                // Background and request failures surface as a non-modal
                // banner so a failing load never hijacks the browser.
                self.schemas_pending = false;
                self.tables_pending = false;
                self.data_pending = false;
                self.structure_pending = false;
                self.error_banner = Some(message);
                self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
            }
        }
    }

    // === Input routing ===

    fn handle_key(&mut self, key: KeyEvent) {
        // The error banner is informational: any key acknowledges it while
        // the key itself keeps its normal meaning, so background failures
        // never trap the operator in a modal.
        self.error_banner = None;
        match self.overlay {
            Overlay::None => {}
            Overlay::Help => {
                self.overlay = Overlay::None;
                self.status = "help closed".to_string();
                return;
            }
            Overlay::DataViewer => {
                self.handle_data_viewer_key(key);
                return;
            }
            Overlay::RowDetail => {
                self.handle_row_detail_key(key);
                return;
            }
            Overlay::SchemaPrompt => {
                self.handle_schema_prompt_key(key);
                return;
            }
            Overlay::ConfirmQuit => {
                self.handle_confirm_quit_key(key);
                return;
            }
        }
        if self.mode == Mode::Filter {
            self.handle_filter_key(key);
            return;
        }

        match key.code {
            KeyCode::Char('q') => self.request_quit(),
            KeyCode::Char('1') => self.set_view(View::Tables),
            KeyCode::Char('2') => self.set_view(View::Structure),
            KeyCode::Tab => {
                let next = match self.view {
                    View::Tables => View::Structure,
                    View::Structure => View::Tables,
                };
                self.set_view(next);
            }
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::PageUp => self.move_selection(-PAGE_STEP),
            KeyCode::PageDown => self.move_selection(PAGE_STEP),
            KeyCode::Home | KeyCode::Char('g') => self.set_selected(0),
            KeyCode::End | KeyCode::Char('G') => {
                let len = self.filtered_tables().len();
                self.set_selected(len.saturating_sub(1));
            }
            KeyCode::Enter | KeyCode::Char('i') => self.inspect_selected(),
            KeyCode::Char('/') => {
                if self.view == View::Tables {
                    self.mode = Mode::Filter;
                    self.status = "filter: type to narrow live, Enter apply, Esc clear".to_string();
                } else {
                    self.status = format!("postgres: {}", STRUCTURE_HINTS);
                }
            }
            KeyCode::Char('s') => self.open_schema_prompt(),
            KeyCode::Char('r') => self.refresh_view(),
            KeyCode::Char('?') => self.show_help(),
            KeyCode::Esc => {
                self.status = format!(
                    "postgres: {}",
                    match self.view {
                        View::Tables => TABLES_HINTS,
                        View::Structure => STRUCTURE_HINTS,
                    }
                );
            }
            _ => {
                // The read-only pilot binds no mutation keys; unknown keys
                // restate the hints so the operator never guesses.
                self.status = format!(
                    "postgres: {}",
                    match self.view {
                        View::Tables => TABLES_HINTS,
                        View::Structure => STRUCTURE_HINTS,
                    }
                );
            }
        }
    }

    fn handle_filter_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => {
                self.mode = Mode::Browser;
                self.set_selected(0);
                self.status = format!(
                    "filter applied: {} ({} matches)",
                    self.filter_label(),
                    self.filtered_tables().len()
                );
            }
            KeyCode::Esc => {
                self.filter.clear();
                self.mode = Mode::Browser;
                self.set_selected(0);
                self.status = "filter cleared".to_string();
            }
            KeyCode::Backspace => {
                self.filter.pop();
                self.set_selected(0);
            }
            KeyCode::Char(ch) => {
                if self.filter.len() < INPUT_LIMIT {
                    self.filter.push(ch);
                }
                self.set_selected(0);
            }
            _ => {}
        }
    }

    fn handle_data_viewer_key(&mut self, key: KeyEvent) {
        // While the viewer filter input is open every character belongs to
        // the filter, mirroring the family filter-mode routing.
        if self.viewer_filtering {
            match key.code {
                KeyCode::Esc => {
                    self.viewer_filtering = false;
                    self.data_filter.clear();
                    self.set_data_selection(0);
                    self.status = "viewer filter cleared".to_string();
                }
                KeyCode::Backspace => {
                    self.data_filter.pop();
                    self.set_data_selection(0);
                }
                KeyCode::Enter => {
                    self.viewer_filtering = false;
                    self.set_data_selection(0);
                    self.status = format!(
                        "viewer filter applied: '{}' ({} match(es) on this page)",
                        self.data_filter,
                        self.filtered_data_rows().len()
                    );
                }
                KeyCode::Char(ch) => {
                    if self.data_filter.len() < INPUT_LIMIT {
                        self.data_filter.push(ch);
                    }
                    self.set_data_selection(0);
                }
                _ => {}
            }
            return;
        }
        let len = self.filtered_data_rows().len();
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.overlay = Overlay::None;
                self.status = "data viewer closed".to_string();
            }
            KeyCode::Up | KeyCode::Char('k') => self.move_data_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_data_selection(1),
            KeyCode::PageUp => self.move_data_selection(-PAGE_STEP),
            KeyCode::PageDown => self.move_data_selection(PAGE_STEP),
            KeyCode::Home | KeyCode::Char('g') => self.set_data_selection(0),
            KeyCode::End | KeyCode::Char('G') => self.set_data_selection(len.saturating_sub(1)),
            KeyCode::Char('n') => self.next_data_page(),
            KeyCode::Char('b') => self.previous_data_page(),
            KeyCode::Char('/') => {
                self.viewer_filtering = true;
                self.status =
                    "viewer filter: type to narrow this page live, Enter applies, Esc clears"
                        .to_string();
            }
            KeyCode::Enter | KeyCode::Char('i') => self.open_row_detail(),
            // The read-only pilot binds no mutation keys inside the viewer;
            // unknown keys restate the viewer hints.
            _ => {
                self.status =
                    "viewer: j/k scroll / filter n next b back Enter detail Esc close".to_string();
            }
        }
    }

    fn handle_row_detail_key(&mut self, key: KeyEvent) {
        let max_scroll = self.detail_lines.len().saturating_sub(1);
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.overlay = Overlay::DataViewer;
                self.status = "row detail closed".to_string();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.detail_scroll = self.detail_scroll.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.detail_scroll = (self.detail_scroll + 1).min(max_scroll);
            }
            KeyCode::PageUp => {
                self.detail_scroll = self.detail_scroll.saturating_sub(PAGE_STEP as usize);
            }
            KeyCode::PageDown => {
                self.detail_scroll = (self.detail_scroll + PAGE_STEP as usize).min(max_scroll);
            }
            KeyCode::Home | KeyCode::Char('g') => self.detail_scroll = 0,
            KeyCode::End | KeyCode::Char('G') => self.detail_scroll = max_scroll,
            _ => {}
        }
    }

    fn handle_schema_prompt_key(&mut self, key: KeyEvent) {
        let candidates = self.filtered_schemas();
        match key.code {
            KeyCode::Esc => {
                self.schema_input.clear();
                self.overlay = Overlay::None;
                self.status = "schema switch cancelled".to_string();
            }
            KeyCode::Up => {
                if !candidates.is_empty() {
                    self.schema_choice = self
                        .schema_choice
                        .saturating_sub(1)
                        .min(candidates.len().saturating_sub(1));
                }
            }
            KeyCode::Down => {
                if !candidates.is_empty() {
                    self.schema_choice = (self.schema_choice + 1).min(candidates.len() - 1);
                }
            }
            KeyCode::Backspace => {
                self.schema_input.pop();
                self.schema_choice = 0;
            }
            KeyCode::Enter => {
                let chosen = candidates.get(self.schema_choice).cloned();
                if let Some(schema) = chosen {
                    self.schema_input.clear();
                    self.overlay = Overlay::None;
                    self.schema = schema.clone();
                    self.tables.clear();
                    self.tables_loaded = false;
                    self.selection = 0;
                    self.request_tables();
                    self.status = format!("schema set to {schema}; loading tables");
                } else {
                    self.status = "schema: no schema matches the typed filter".to_string();
                }
            }
            KeyCode::Char(ch) if self.schema_input.len() < INPUT_LIMIT => {
                self.schema_input.push(ch);
                self.schema_choice = 0;
            }
            _ => {}
        }
    }

    fn handle_confirm_quit_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                self.should_quit = true;
            }
            _ => {
                self.overlay = Overlay::None;
                self.status = "quit cancelled".to_string();
            }
        }
    }

    // === Selection and views ===

    fn selected(&self) -> usize {
        self.selection
    }

    fn set_selected(&mut self, value: usize) {
        let len = self.filtered_tables().len();
        self.selection = value.min(len.saturating_sub(1));
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.filtered_tables().len();
        if len == 0 {
            self.set_selected(0);
            return;
        }
        let last = len as isize - 1;
        let next = (self.selected() as isize + delta).clamp(0, last) as usize;
        self.set_selected(next);
    }

    fn trim_selection(&mut self) {
        self.set_selected(self.selected());
    }

    fn set_data_selection(&mut self, value: usize) {
        let len = self.filtered_data_rows().len();
        self.data_selection = value.min(len.saturating_sub(1));
    }

    fn move_data_selection(&mut self, delta: isize) {
        let len = self.filtered_data_rows().len();
        if len == 0 {
            self.set_data_selection(0);
            return;
        }
        let last = len as isize - 1;
        let next = (self.data_selection as isize + delta).clamp(0, last) as usize;
        self.set_data_selection(next);
    }

    fn set_view(&mut self, view: View) {
        if self.view == view {
            return;
        }
        self.view = view;
        self.status = format!(
            "view: {}",
            match view {
                View::Tables => "tables",
                View::Structure => "structure",
            }
        );
        if view == View::Structure {
            self.request_structure_for_selected();
        }
    }

    fn filtered_tables(&self) -> Vec<TableMeta> {
        let needle = self.filter.to_lowercase();
        self.tables
            .iter()
            .filter(|meta| needle.is_empty() || meta.name.to_lowercase().contains(&needle))
            .cloned()
            .collect()
    }

    fn filtered_schemas(&self) -> Vec<String> {
        let needle = self.schema_input.to_lowercase();
        self.schema_names
            .iter()
            .filter(|name| needle.is_empty() || name.to_lowercase().contains(&needle))
            .cloned()
            .collect()
    }

    /// Selected table as `(schema, table)` from the tables-view cursor.
    /// Callers gate on the active view themselves; this must also work while
    /// the structure view is showing so it can request the selected table.
    fn selected_table(&self) -> Option<(String, String)> {
        self.filtered_tables()
            .get(self.selected())
            .map(|meta| (self.schema.clone(), meta.name.clone()))
    }

    fn filter_label(&self) -> String {
        if self.filter.is_empty() {
            "none".to_string()
        } else {
            self.filter.clone()
        }
    }

    fn filtered_data_rows(&self) -> Vec<usize> {
        let needle = self.data_filter.to_lowercase();
        self.data_rows
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                needle.is_empty()
                    || row
                        .values
                        .iter()
                        .any(|value| render_cell(value).to_lowercase().contains(&needle))
            })
            .map(|(index, _)| index)
            .collect()
    }

    // === Requests ===

    fn request_schemas(&mut self) {
        if let Some(fixture) = &self.fixture {
            self.schema_names = fixture.schemas.clone();
            self.schemas_loaded = true;
            if self.schema.is_empty() {
                self.schema = fixture.schemas.first().cloned().unwrap_or_default();
            }
            self.request_tables();
        } else if let Some(service) = &self.service {
            self.schemas_pending = true;
            service.send(PostgresCommand::Schema(SchemaCommand::ListSchemas));
            self.status = "loading schemas".to_string();
        }
    }

    fn request_tables(&mut self) {
        if self.schema.is_empty() {
            return;
        }
        if let Some(fixture) = &self.fixture {
            self.tables = fixture
                .tables
                .iter()
                .filter(|table| table.schema == self.schema)
                .map(|table| TableMeta {
                    name: table.name.clone(),
                    table_type: table.table_type.clone(),
                    row_estimate: table.row_estimate,
                })
                .collect();
            self.tables_loaded = true;
            self.tables_pending = false;
            self.trim_selection();
            self.status = format!(
                "fixture loaded {} tables in schema {}",
                self.tables.len(),
                self.schema
            );
        } else if let Some(service) = &self.service {
            self.tables_pending = true;
            let schema = self.schema.clone();
            service.send(PostgresCommand::Schema(SchemaCommand::ListTables {
                schema,
            }));
        }
    }

    /// Send one bounded page request for the open table. Fixture mode
    /// resolves the page synchronously; live mode sends
    /// `LoadCommand::LoadTableData` with PAGE_SIZE and the requested offset.
    fn request_page(&mut self, offset: usize) {
        self.data_offset = offset;
        self.data_pending = true;
        if let Some(fixture) = &self.fixture {
            match fixture.resolve_page(&self.data_schema, &self.data_table, offset, PAGE_SIZE) {
                Ok((columns, rows, total)) => {
                    self.data_columns = columns;
                    self.data_rows = rows;
                    self.data_total = Some(total);
                    self.data_pending = false;
                    self.set_data_selection(0);
                    self.status = format!(
                        "fixture loaded page {} of {}.{} (offset {}): {} rows of {} total",
                        self.data_page_index,
                        self.data_schema,
                        self.data_table,
                        offset,
                        self.data_rows.len(),
                        total
                    );
                }
                Err(message) => {
                    self.data_pending = false;
                    self.data_rows.clear();
                    self.data_columns.clear();
                    self.data_total = None;
                    self.error_banner = Some(message);
                    self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
                }
            }
        } else if let Some(service) = &self.service {
            let schema = self.data_schema.clone();
            let table = self.data_table.clone();
            service.send(PostgresCommand::Load(LoadCommand::LoadTableData {
                schema,
                table,
                page_size: PAGE_SIZE,
                offset,
            }));
        } else {
            self.data_pending = false;
        }
    }

    fn request_structure_for_selected(&mut self) {
        let Some((schema, table)) = self.selected_table() else {
            self.status = "no table selected; switch to tables (1) and pick a table for structure"
                .to_string();
            return;
        };
        if self
            .structure_request
            .as_ref()
            .is_some_and(|(s, t)| s == &schema && t == &table)
            && (self.structure_view.is_some() || self.structure_pending)
        {
            return;
        }
        self.structure_request = Some((schema.clone(), table.clone()));
        self.structure_selection = 0;
        if let Some(fixture) = &self.fixture {
            match fixture.resolve_structure(&schema, &table) {
                Ok(view) => {
                    self.structure_view = Some(view);
                    self.structure_pending = false;
                    self.status = format!("fixture structure loaded for {schema}.{table}");
                }
                Err(message) => {
                    self.structure_view = None;
                    self.structure_pending = false;
                    self.error_banner = Some(message);
                    self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
                }
            }
        } else if let Some(service) = &self.service {
            self.structure_pending = true;
            self.structure_view = None;
            service.send(PostgresCommand::Schema(SchemaCommand::LoadStructure {
                schema,
                table,
            }));
            self.status = "loading structure".to_string();
        }
    }

    fn refresh_view(&mut self) {
        match self.view {
            View::Tables => {
                self.request_tables();
                self.status = format!("refreshing tables in schema {}", self.schema);
            }
            View::Structure => {
                self.structure_request = None;
                self.request_structure_for_selected();
            }
        }
    }

    // === Data viewer paging ===

    fn open_data_viewer(&mut self) {
        let Some((schema, table)) = self.selected_table() else {
            self.status = "select a table to inspect".to_string();
            return;
        };
        // Erroring tables fail before the viewer opens so the non-modal
        // banner proves the browser stays responsive.
        if let Some(fixture) = &self.fixture
            && fixture.is_error_table(&schema, &table)
        {
            let message = fixture.error_message.clone();
            self.error_banner = Some(message);
            self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
            return;
        }
        self.data_schema = schema;
        self.data_table = table;
        self.data_offset = 0;
        self.data_history.clear();
        self.data_page_index = 1;
        self.data_filter.clear();
        self.viewer_filtering = false;
        self.data_selection = 0;
        self.data_total = None;
        self.overlay = Overlay::DataViewer;
        self.request_page(0);
    }

    /// `n` inside the viewer: replace the page with the page at
    /// `offset + PAGE_SIZE`. Pages are replaced (not appended) so the
    /// viewport stays bounded; the previous offset is pushed onto a history
    /// stack for `b`.
    fn next_data_page(&mut self) {
        let page_was_full = self.data_rows.len() == PAGE_SIZE;
        let more_known = self
            .data_total
            .is_none_or(|total| self.data_offset + self.data_rows.len() < total as usize);
        if !page_was_full || !more_known {
            self.status = "end of table reached; press b to go back or r to refresh".to_string();
            return;
        }
        self.data_history.push(self.data_offset);
        self.data_page_index = self.data_page_index.saturating_add(1);
        self.set_data_selection(0);
        let next_offset = self.data_offset + PAGE_SIZE;
        self.request_page(next_offset);
    }

    fn previous_data_page(&mut self) {
        let Some(previous) = self.data_history.pop() else {
            self.status = "no previous page".to_string();
            return;
        };
        self.data_page_index = self.data_page_index.saturating_sub(1).max(1);
        self.set_data_selection(0);
        self.request_page(previous);
    }

    // === Row detail viewer ===

    fn open_row_detail(&mut self) {
        let rows = self.filtered_data_rows();
        let Some(position) = rows.get(self.data_selection) else {
            self.status = "no row selected; this page has no matching rows".to_string();
            return;
        };
        let Some(row) = self.data_rows.get(*position) else {
            self.status = "row unavailable".to_string();
            return;
        };
        self.detail_lines.clear();
        let absolute = self.data_offset + *position + 1;
        self.detail_title = format!(
            "{}.{} · row {absolute} of {}",
            self.data_schema,
            self.data_table,
            self.data_total
                .map(|total| total.to_string())
                .unwrap_or_else(|| "?".to_string())
        );
        for (index, column) in self.data_columns.iter().enumerate() {
            let value = row.values.get(index).unwrap_or(&CellValue::Null);
            self.detail_lines.push_line(format!(
                "{} ({}): {}",
                column.name,
                column.data_type,
                render_detail_cell(value)
            ));
        }
        if self.data_columns.is_empty() {
            self.detail_lines.push_line("(no columns)".to_string());
        }
        self.detail_scroll = 0;
        self.overlay = Overlay::RowDetail;
        self.status = format!("row detail: {absolute}");
    }

    // === Schema prompt / help / quit ===

    fn open_schema_prompt(&mut self) {
        if self.view != View::Tables {
            self.status = "switch to the tables view (1) to switch schemas".to_string();
            return;
        }
        self.schema_input.clear();
        let candidates = self.filtered_schemas();
        self.schema_choice = candidates
            .iter()
            .position(|name| name == &self.schema)
            .unwrap_or(0);
        self.overlay = Overlay::SchemaPrompt;
        self.status = "schema: type to filter, Up/Down to move, Enter switches".to_string();
    }

    fn inspect_selected(&mut self) {
        match self.view {
            View::Tables => self.open_data_viewer(),
            View::Structure => {
                self.status =
                    "structure view shows columns/indexes/FKs panels; press 1 to browse data"
                        .to_string();
            }
        }
    }

    fn request_quit(&mut self) {
        let work_active = self.schemas_pending
            || self.tables_pending
            || self.data_pending
            || self.structure_pending;
        if work_active {
            self.overlay = Overlay::ConfirmQuit;
            self.status = "quit? a fetch is active: y quits, any other key stays".to_string();
            return;
        }
        self.should_quit = true;
    }

    fn show_help(&mut self) {
        self.overlay = Overlay::Help;
        self.status = "help open; any key closes".to_string();
    }

    fn active_work_summary(&self) -> String {
        let mut parts = Vec::new();
        if self.schemas_pending {
            parts.push("schema fetch in flight".to_string());
        }
        if self.tables_pending {
            parts.push("table fetch in flight".to_string());
        }
        if self.data_pending {
            parts.push("table data fetch in flight".to_string());
        }
        if self.structure_pending {
            parts.push("structure fetch in flight".to_string());
        }
        if parts.is_empty() {
            "no active fetches (read-only pilot: no staged plans)".to_string()
        } else {
            parts.join(", ")
        }
    }

    // === Drawing ===

    fn draw(&self, frame: &mut Frame) {
        let area = frame.area();
        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                // Header and status each carry two content rows (title+tabs,
                // status+hints) on top of their borders.
                Constraint::Length(4),
                Constraint::Min(12),
                Constraint::Length(4),
            ])
            .split(area);

        frame.render_widget(self.header(), vertical[0]);
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(vertical[1]);
        match self.view {
            View::Tables => {
                frame.render_widget(self.table_list(body[0]), body[0]);
                frame.render_widget(self.details_panel(), body[1]);
            }
            View::Structure => {
                frame.render_widget(self.structure_columns(body[0]), body[0]);
                frame.render_widget(self.structure_indexes_panel(), body[1]);
            }
        }
        frame.render_widget(self.status_panel(), vertical[2]);

        match self.overlay {
            Overlay::None => {}
            Overlay::Help => self.draw_help(frame, area),
            Overlay::DataViewer => self.draw_data_viewer(frame, area),
            Overlay::RowDetail => self.draw_row_detail(frame, area),
            Overlay::SchemaPrompt => self.draw_schema_prompt(frame, area),
            Overlay::ConfirmQuit => self.draw_confirm_quit(frame, area),
        }
        if let Some(error) = &self.error_banner {
            self.draw_error_banner(frame, area, error);
        }
    }

    fn header(&self) -> Paragraph<'_> {
        let source = format!("{:?}", self.source).to_lowercase();
        let tables_mark = if self.tables_pending || self.schemas_pending {
            "\u{27f3}"
        } else {
            ""
        };
        let structure_mark = if self.structure_pending {
            "\u{27f3}"
        } else {
            ""
        };
        let tables_tab = format!("1:tables{tables_mark}");
        let structure_tab = format!("2:structure{structure_mark}");
        let tabs = match self.view {
            View::Tables => format!("[{tables_tab}] {structure_tab}"),
            View::Structure => format!("{tables_tab} [{structure_tab}]"),
        };
        Paragraph::new(vec![
            Line::from(vec![
                Span::styled(
                    "PostgreSQL Data TUI (pilot)",
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!(
                    "  profile={} server={} db={} schema={}",
                    self.profile_label,
                    self.server_label,
                    self.database,
                    self.schema_label()
                )),
            ]),
            Line::from(format!(
                "{}  source={} purpose={} readonly={} restore={} filter={} (read-only pilot)",
                tabs,
                source,
                self.purpose,
                self.readonly,
                self.restore,
                self.filter_label()
            )),
        ])
        .block(Block::default().borders(Borders::ALL))
    }

    fn schema_label(&self) -> String {
        if self.schema.is_empty() {
            "-".to_string()
        } else {
            self.schema.clone()
        }
    }

    fn table_list(&self, area: Rect) -> Paragraph<'_> {
        let items = self.filtered_tables();
        let visible = area.height.saturating_sub(2).max(1) as usize;
        let start = window_start(self.selected(), visible, items.len());
        let lines = items
            .iter()
            .enumerate()
            .skip(start)
            .take(visible)
            .map(|(idx, meta)| {
                let marker = if idx == self.selected() { ">" } else { " " };
                let style = if idx == self.selected() {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Green)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Line::from(Span::styled(
                    format!(
                        "{marker} {:<34} {:<12} {:>10}",
                        meta.name,
                        meta.table_type,
                        meta.row_estimate
                            .map(|rows| rows.to_string())
                            .unwrap_or_else(|| "-".to_string()),
                    ),
                    style,
                ))
            })
            .collect::<Vec<_>>();
        Paragraph::new(if !lines.is_empty() {
            lines
        } else if self.tables_pending || self.schemas_pending {
            vec![Line::from("loading\u{2026}")]
        } else {
            vec![Line::from(
                "No tables in this schema. Press s to switch schemas or r to refresh.",
            )]
        })
        .block(
            Block::default()
                .title(format!(
                    " tables ({}/{}) schema={} ",
                    self.selected().saturating_add(1).min(items.len().max(1)),
                    items.len(),
                    self.schema_label()
                ))
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false })
    }

    fn details_panel(&self) -> Paragraph<'_> {
        let mut lines = match self.filtered_tables().get(self.selected()) {
            Some(meta) => vec![
                Line::from(format!("table: {}", meta.name)),
                Line::from(format!(
                    "type: {}  rows(estimate): {}",
                    meta.table_type,
                    meta.row_estimate
                        .map(|rows| rows.to_string())
                        .unwrap_or_else(|| "-".to_string())
                )),
                Line::from(format!(
                    "schema: {}  db: {}",
                    self.schema_label(),
                    self.database
                )),
                Line::from("i/Enter data viewer  2 structure  s schema"),
            ],
            None => vec![Line::from(
                "Select a table to page its rows. Press s to switch schemas.",
            )],
        };
        lines.push(Line::from(""));
        lines.push(Line::from(format!(
            "viewer page {} offset={} rows={} filter='{}'",
            self.data_page_index,
            self.data_offset,
            self.data_rows.len(),
            self.data_filter
        )));
        if self.readonly {
            lines.push(Line::from(Span::styled(
                "readonly launch: no mutation surface",
                Style::default().fg(Color::DarkGray),
            )));
        }

        Paragraph::new(lines)
            .block(Block::default().title(" Details ").borders(Borders::ALL))
            .wrap(Wrap { trim: false })
    }

    fn structure_columns(&self, area: Rect) -> Paragraph<'_> {
        let Some(view) = &self.structure_view else {
            let hint = if self.structure_pending {
                "loading\u{2026}"
            } else {
                "No structure loaded. Select a table in tables view (1), then press 2."
            };
            return Paragraph::new(vec![Line::from(hint)]).block(
                Block::default()
                    .title(format!(
                        " columns · {}.{} ",
                        self.structure_label().0,
                        self.structure_label().1
                    ))
                    .borders(Borders::ALL),
            );
        };
        let count = view.columns.len();
        let visible = area.height.saturating_sub(2).max(1) as usize;
        let start = window_start(self.structure_selection, visible, count);
        let mut lines: Vec<Line> = view
            .columns
            .iter()
            .enumerate()
            .skip(start)
            .take(visible)
            .map(|(idx, column)| {
                let marker = if idx == self.structure_selection {
                    ">"
                } else {
                    " "
                };
                let style = if idx == self.structure_selection {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Green)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Line::from(Span::styled(
                    format!(
                        "{marker} {:<22} {:<20} {:<4} {:<4} {}",
                        column.name,
                        column.data_type,
                        if column.nullable { "Y" } else { "N" },
                        if column.is_primary_key { "PRI" } else { "" },
                        column.default_value.as_deref().unwrap_or("")
                    ),
                    style,
                ))
            })
            .collect();
        if lines.is_empty() {
            lines.push(Line::from("(no columns)"));
        }
        Paragraph::new(lines)
            .block(
                Block::default()
                    .title(format!(
                        " columns ({}/{}) · {} ",
                        (self.structure_selection + 1).min(count.max(1)),
                        count,
                        self.structure_label_title()
                    ))
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false })
    }

    fn structure_label(&self) -> (String, String) {
        match &self.structure_request {
            Some((schema, table)) => (schema.clone(), table.clone()),
            None => ("-".to_string(), "-".to_string()),
        }
    }

    fn structure_label_title(&self) -> String {
        let (schema, table) = self.structure_label();
        format!("{schema}.{table}")
    }

    fn structure_indexes_panel(&self) -> Paragraph<'_> {
        let mut lines: Vec<Line> = Vec::new();
        match &self.structure_view {
            Some(view) => {
                lines.push(Line::from(Span::styled(
                    "indexes",
                    Style::default().add_modifier(Modifier::BOLD),
                )));
                if view.indexes.is_empty() {
                    lines.push(Line::from("  (none)"));
                }
                for index in &view.indexes {
                    lines.push(Line::from(format!(
                        "  {} ({}) [{}]",
                        index.name,
                        index.columns.join(", "),
                        if index.is_unique {
                            "UNIQUE".to_string()
                        } else {
                            index.index_type.clone()
                        }
                    )));
                }
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "foreign keys",
                    Style::default().add_modifier(Modifier::BOLD),
                )));
                if view.foreign_keys.is_empty() {
                    lines.push(Line::from("  (none)"));
                }
                for fk in &view.foreign_keys {
                    lines.push(Line::from(format!(
                        "  {} ({}) -> {} ({}) on_update={} on_delete={}",
                        fk.name,
                        fk.columns.join(", "),
                        fk.referenced_table,
                        fk.referenced_columns.join(", "),
                        fk.on_update,
                        fk.on_delete
                    )));
                }
            }
            None => {
                lines.push(Line::from(
                    "Structure loads here for the selected table (r refreshes).",
                ));
            }
        }
        Paragraph::new(lines)
            .block(
                Block::default()
                    .title(" Indexes & FKs ")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false })
    }

    fn status_panel(&self) -> Paragraph<'_> {
        let hints = if self.mode == Mode::Filter {
            format!(
                "filter: {}\u{258f} Enter apply \u{b7} Esc clear \u{b7} {} match(es)",
                self.filter,
                self.filtered_tables().len()
            )
        } else {
            match self.view {
                View::Tables => TABLES_HINTS.to_string(),
                View::Structure => STRUCTURE_HINTS.to_string(),
            }
        };
        Paragraph::new(vec![
            Line::from(format!(
                "{} | schema={} | tables {}/{} | viewer page {} offset={} rows={} | pending schemas={} tables={} data={} structure={}",
                self.status,
                self.schema_label(),
                self.filtered_tables().len(),
                self.tables.len(),
                self.data_page_index,
                self.data_offset,
                self.data_rows.len(),
                self.schemas_pending,
                self.tables_pending,
                self.data_pending,
                self.structure_pending,
            )),
            Line::from(Span::styled(hints, Style::default().fg(Color::DarkGray))),
        ])
        .block(Block::default().title(" Status ").borders(Borders::ALL))
    }

    fn draw_help(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(78, 84, area);
        frame.render_widget(Clear, popup);
        let help = vec![
            Line::from(Span::styled(
                "Navigation",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("1 tables browser \u{b7} 2 structure (Tab cycles)"),
            Line::from("j/k or arrows move \u{b7} PgUp/PgDn page \u{b7} g/G home/end"),
            Line::from("/ client-side filter on the table list (live, Esc clears)"),
            Line::from(Span::styled(
                "Schemas & tables",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("s schema prompt (type to filter, Enter switches)"),
            Line::from("r refresh current view"),
            Line::from(Span::styled(
                "Inspect (read-only)",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("i/Enter: full-screen table data viewer, one bounded page at a time"),
            Line::from(
                "viewer: j/k PgUp/PgDn scroll \u{b7} n next page \u{b7} b back \u{b7} / filter \u{b7} Enter row detail \u{b7} Esc close",
            ),
            Line::from(
                "row detail: one typed line per column (NULL/jsonb/arrays render as stored)",
            ),
            Line::from("structure view: columns list + indexes/foreign-keys panel"),
            Line::from(Span::styled(
                "Scope",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("read-only pilot: no mutation keys are bound; writes stay in the CLI"),
            Line::from("3 server view: deferred (no server-info command in the service layer)"),
            Line::from("q: quit (asks for confirmation while a fetch is active)"),
            Line::from("errors appear in a non-modal banner; any key acknowledges"),
        ];
        frame.render_widget(
            Paragraph::new(help)
                .block(Block::default().title(" Help ").borders(Borders::ALL))
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_data_viewer(&self, frame: &mut Frame, area: Rect) {
        // 84% height matches the family viewers so the status panel's text
        // rows stay visible below the popup while the overlay is open.
        let popup = centered_rect(88, 84, area);
        frame.render_widget(Clear, popup);
        let rows = self.filtered_data_rows();
        let visible = popup.height.saturating_sub(4).max(1) as usize;
        let start = window_start(self.data_selection, visible, rows.len());

        let mut body: Vec<Line> = Vec::new();
        if !self.data_columns.is_empty() {
            let header: Vec<String> = self.data_columns.iter().map(|c| c.name.clone()).collect();
            body.push(Line::from(Span::styled(
                format!("  {}", header.join(" | ")),
                Style::default().add_modifier(Modifier::BOLD),
            )));
        }
        if rows.is_empty() {
            if self.data_pending {
                body.push(Line::from("loading\u{2026}"));
            } else if self.data_rows.is_empty() {
                body.push(Line::from(format!(
                    "(no rows in {}.{})",
                    self.data_schema, self.data_table
                )));
            } else {
                body.push(Line::from(format!(
                    "No rows match '{}'. Press Esc in the filter to clear.",
                    self.data_filter
                )));
            }
        } else {
            for (position, row_index) in rows.iter().enumerate().skip(start).take(visible) {
                let marker = if position == self.data_selection {
                    ">"
                } else {
                    " "
                };
                let style = if position == self.data_selection {
                    Style::default().fg(Color::Black).bg(Color::Green)
                } else if position % 2 == 1 {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default()
                };
                let row = &self.data_rows[*row_index];
                let cells: Vec<String> = row.values.iter().map(render_cell).collect();
                body.push(Line::from(Span::styled(
                    format!("{marker} {}", cells.join(" | ")),
                    style,
                )));
            }
        }
        if self.viewer_filtering {
            body.push(Line::from(Span::styled(
                format!(
                    "filter: {}\u{258f} ({} match(es))",
                    self.data_filter,
                    rows.len()
                ),
                Style::default().fg(Color::Cyan),
            )));
        }

        let pending_mark = if self.data_pending { "\u{27f3}" } else { "" };
        let rows_label = match self.data_total {
            Some(total) => format!(
                "rows {}-{}/{}",
                if self.data_rows.is_empty() {
                    0
                } else {
                    self.data_offset + 1
                },
                self.data_offset + self.data_rows.len(),
                total
            ),
            None => format!("rows {}", self.data_rows.len()),
        };
        frame.render_widget(
            Paragraph::new(body).block(
                Block::default()
                    .title(format!(
                        " table data \u{b7} {}.{} \u{b7} page {} {}{pending_mark} \u{b7} f {}/{} \u{b7} j/k scroll / filter n next b back Enter detail Esc close ",
                        self.data_schema,
                        self.data_table,
                        self.data_page_index,
                        rows_label,
                        rows.len(),
                        self.data_rows.len(),
                    ))
                    .borders(Borders::ALL),
            ),
            popup,
        );
    }

    fn draw_row_detail(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(78, 84, area);
        frame.render_widget(Clear, popup);
        let visible = popup.height.saturating_sub(2).max(1) as usize;
        let total = self.detail_lines.len();
        let body: Vec<Line> = if total == 0 {
            vec![Line::from("no row detail loaded")]
        } else {
            let start = self.detail_scroll.min(total.saturating_sub(1));
            self.detail_lines
                .range(start, start + visible)
                .into_iter()
                .map(|line| match line.find("): ") {
                    Some(idx) => {
                        let (label, rest) = line.split_at(idx + 1);
                        Line::from(vec![
                            Span::styled(
                                label.to_string(),
                                Style::default().add_modifier(Modifier::BOLD),
                            ),
                            Span::raw(rest.to_string()),
                        ])
                    }
                    None => Line::from(line),
                })
                .collect()
        };
        frame.render_widget(
            Paragraph::new(body)
                .block(
                    Block::default()
                        .title(format!(
                            " row detail \u{b7} {} \u{b7} line {}/{} \u{b7} j/k PgUp/PgDn scroll \u{b7} Esc close ",
                            self.detail_title,
                            (self.detail_scroll + 1).min(total.max(1)),
                            total
                        ))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_schema_prompt(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(56, 32, area);
        frame.render_widget(Clear, popup);
        let candidates = self.filtered_schemas();
        let mut lines = vec![
            Line::from("switch schema (type to filter):"),
            Line::from(Span::styled(
                format!("{}\u{258f}", self.schema_input),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )),
        ];
        if candidates.is_empty() {
            lines.push(Line::from("(no schema matches)"));
        }
        for (idx, name) in candidates.iter().enumerate() {
            let marker = if idx == self.schema_choice { ">" } else { " " };
            let style = if idx == self.schema_choice {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Green)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            lines.push(Line::from(Span::styled(format!("{marker} {name}"), style)));
        }
        lines.push(Line::from("Enter switches \u{b7} Esc cancels"));
        frame.render_widget(
            Paragraph::new(lines)
                .block(Block::default().title(" Schema ").borders(Borders::ALL))
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_confirm_quit(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(56, 22, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Quit with active work?",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(self.active_work_summary()),
                Line::from("y/Enter: quit \u{b7} any other key: stay"),
            ])
            .block(
                Block::default()
                    .title(" Confirm Quit ")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_error_banner(&self, frame: &mut Frame, area: Rect, error: &str) {
        let banner = Rect {
            x: area.x + 1,
            y: area.height.saturating_sub(6),
            width: area.width.saturating_sub(2),
            height: 3,
        };
        frame.render_widget(Clear, banner);
        frame.render_widget(
            Paragraph::new(vec![Line::from(Span::styled(
                safe_error_summary(error),
                Style::default().fg(Color::Red),
            ))])
            .block(
                Block::default()
                    .title(" Error (any key acknowledges) ")
                    .borders(Borders::ALL)
                    .style(Style::default().fg(Color::Red)),
            )
            .wrap(Wrap { trim: false }),
            banner,
        );
    }
}

impl BoundedLines {
    fn new(max_lines: usize, max_bytes: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            bytes: 0,
            dropped_lines: 0,
            max_lines,
            max_bytes,
        }
    }

    fn len(&self) -> usize {
        self.lines.len()
    }

    fn clear(&mut self) {
        self.lines.clear();
        self.bytes = 0;
        self.dropped_lines = 0;
    }

    fn push_line(&mut self, mut line: String) {
        if line.len() > self.max_bytes {
            line.truncate(self.max_bytes);
        }
        self.bytes = self.bytes.saturating_add(line.len());
        self.lines.push_back(line);
        while self.lines.len() > self.max_lines || self.bytes > self.max_bytes {
            if let Some(removed) = self.lines.pop_front() {
                self.bytes = self.bytes.saturating_sub(removed.len());
                self.dropped_lines = self.dropped_lines.saturating_add(1);
            } else {
                break;
            }
        }
    }

    fn range(&self, start: usize, end: usize) -> Vec<String> {
        self.lines
            .iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .cloned()
            .collect()
    }
}

/// Render a cell's full text. NULL/jsonb/arrays render the way the service
/// loader produces them so the operator sees the stored shape.
fn render_cell_text(value: &CellValue) -> String {
    match value {
        CellValue::Null => "(null)".to_string(),
        CellValue::Bool(b) => b.to_string(),
        CellValue::Int(i) => i.to_string(),
        CellValue::Float(f) => f.to_string(),
        CellValue::Text(s) | CellValue::Unknown(s) => s.clone(),
        CellValue::Date(s) | CellValue::Time(s) | CellValue::DateTime(s) => s.clone(),
        CellValue::Json(s) | CellValue::Uuid(s) => s.clone(),
        CellValue::Blob(bytes) => {
            let all: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            format!("\\x{all}")
        }
    }
}

/// Render a cell for the data grid (short cap so rows stay scannable).
fn render_cell(value: &CellValue) -> String {
    truncate_chars(&render_cell_text(value), CELL_CAP)
}

/// Render a cell for the row-detail viewer with a larger cap so long jsonb
/// documents stay readable.
fn render_detail_cell(value: &CellValue) -> String {
    truncate_chars(&render_cell_text(value), DETAIL_CAP)
}

fn truncate_chars(text: &str, cap: usize) -> String {
    if cap == 0 {
        return String::new();
    }
    if text.chars().count() <= cap {
        text.to_string()
    } else {
        let head: String = text.chars().take(cap.saturating_sub(1)).collect();
        format!("{head}\u{2026}")
    }
}

/// Defensive redaction: if any service error ever embeds the connection
/// string, the password material must not reach the banner, status line, or
/// evidence output. The live connection factory never logs the connection
/// string (T-03-01); this keeps the TUI safe even if an upstream message
/// ever does.
fn redact_error_text(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    for word in message.split(' ') {
        if let Some(idx) = word.find("password=") {
            let (prefix, _) = word.split_at(idx + "password=".len());
            out.push_str(prefix);
            out.push_str("redacted");
        } else {
            out.push_str(word);
        }
        out.push(' ');
    }
    out.trim_end().to_string()
}

fn safe_error_summary(message: &str) -> String {
    let mut summary = redact_error_text(message).replace('\n', " ");
    if summary.len() > 180 {
        summary.truncate(177);
        summary.push_str("...");
    }
    summary
}

fn secret_leak_markers(text: &str) -> Vec<String> {
    [
        "postgres_password_value",
        "postgres_secret_data_value",
        "plaintext_credential_value",
        "raw_connection_url",
    ]
    .into_iter()
    .filter(|marker| text.contains(marker))
    .map(str::to_string)
    .collect()
}

fn window_start(selected: usize, visible: usize, len: usize) -> usize {
    if visible == 0 || len <= visible {
        0
    } else if selected >= visible {
        (selected + 1).saturating_sub(visible)
    } else {
        0
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn table_value(table: &FixtureTableData) -> Value {
    json!({
        "schema": table.schema,
        "name": table.name,
        "table_type": table.table_type,
        "row_estimate": table.row_estimate,
        "columns": table.columns.iter().map(column_value).collect::<Vec<_>>(),
        "total_rows": table.rows.len(),
        "has_structure": table.structure.is_some()
    })
}

fn column_value(column: &ColumnInfo) -> Value {
    json!({
        "name": column.name,
        "data_type": column.data_type,
        "nullable": column.nullable,
        "is_primary_key": column.is_primary_key,
        "default_value": column.default_value
    })
}

struct StandalonePostgresTabManager {
    render_tx: tokio::sync::mpsc::UnboundedSender<()>,
    should_quit: Arc<AtomicBool>,
}

impl StandalonePostgresTabManager {
    fn new(should_quit: Arc<AtomicBool>) -> Self {
        let (render_tx, _render_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            render_tx,
            should_quit,
        }
    }

    fn unsupported_tabs_error() -> anyhow::Error {
        anyhow::anyhow!(
            "Postgres TUI does not host plugin tabs; use plugin-owned CLI commands instead"
        )
    }
}

impl TabManager for StandalonePostgresTabManager {
    fn open(&self, _title: String, _plugin_id: String, _context: Value) -> Result<()> {
        Err(Self::unsupported_tabs_error())
    }

    fn close_current(&self) -> Result<()> {
        self.quit()
    }

    fn set_title(&self, _title: String) -> Result<()> {
        Ok(())
    }

    fn request_render(&self) -> Result<()> {
        let _ = self.render_tx.send(());
        Ok(())
    }

    fn list_tabs(&self) -> Result<Vec<TabInfo>> {
        Ok(vec![TabInfo {
            index: 0,
            title: "PostgreSQL".to_string(),
            plugin_id: "postgres".to_string(),
            context: json!({}),
            is_active: true,
        }])
    }

    fn close_tab(&self, index: usize) -> Result<()> {
        if index == 0 {
            self.quit()
        } else {
            Err(anyhow::anyhow!("Postgres TUI has no tab {index}"))
        }
    }

    fn switch_to(&self, index: usize) -> Result<()> {
        if index == 0 {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Postgres TUI has no tab {index}"))
        }
    }

    fn active_tab_index(&self) -> Result<usize> {
        Ok(0)
    }

    fn quit(&self) -> Result<()> {
        self.should_quit.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crossterm::event::KeyModifiers;

    fn fixture_launch(readonly: bool) -> PostgresTuiLaunch {
        PostgresTuiLaunch {
            profile_label: "fixture".to_string(),
            config: None,
            source: PostgresTuiSource::Fixture,
            fixture_path: Some(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("fixtures/postgres_tui_operations.json")
                    .display()
                    .to_string(),
            ),
            purpose: "operations".to_string(),
            readonly,
            restore: true,
            launch_plan: None,
        }
    }

    fn app() -> PostgresTuiApp {
        PostgresTuiApp::new(fixture_launch(false)).unwrap()
    }

    fn press(app: &mut PostgresTuiApp, code: KeyCode) {
        app.handle_key(KeyEvent::new(code, KeyModifiers::NONE));
    }

    fn type_text(app: &mut PostgresTuiApp, text: &str) {
        for ch in text.chars() {
            app.handle_key(KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE));
        }
    }

    #[test]
    fn fixture_evidence_has_coverage_and_no_secret_markers() {
        let evidence = build_postgres_tui_evidence(&fixture_launch(false)).unwrap();
        let rendered = serde_json::to_string(&evidence).unwrap();
        assert!(rendered.contains("postgres_tui_fixture_evidence"));
        assert!(rendered.contains("table_paging_offset_history"));
        assert!(rendered.contains("row_detail_viewer"));
        assert!(rendered.contains("schema_switch_prompt"));
        assert!(rendered.contains("later candidate"));
        assert_eq!(evidence["secret_leak_scan"]["passed"], json!(true));
        assert_eq!(evidence["quality_gate"]["plugin_id"], json!("postgres"));
        assert_eq!(
            evidence["quality_gate"]["layout"]["minimum_viewport"]["width"],
            json!(80)
        );
        assert_eq!(evidence["mutation_policy"]["slice"], json!("read_only"));
    }

    #[test]
    fn preflight_redacts_password_material() {
        let launch = PostgresTuiLaunch {
            profile_label: "live-postgres".to_string(),
            config: Some(PostgresConfig {
                host: "pg.internal".to_string(),
                port: 5432,
                username: "triage".to_string(),
                password: "postgres_password_value".to_string(),
                database: "appdb".to_string(),
                ssl_mode: None,
            }),
            source: PostgresTuiSource::Profile,
            fixture_path: None,
            purpose: "triage".to_string(),
            readonly: false,
            restore: true,
            launch_plan: None,
        };
        let rendered = serde_json::to_string(&preflight_value(&launch)).unwrap();
        assert!(rendered.contains("redacted"));
        assert!(!rendered.contains("postgres_password_value"));
        assert_eq!(preflight_value(&launch)["ok"], json!(true));
        assert_eq!(preflight_value(&launch)["command"], json!("postgres tui"));
        assert_eq!(preflight_value(&launch)["database"], json!("appdb"));
        assert_eq!(
            preflight_value(&launch)["connection"]["auth"],
            json!("password")
        );
    }

    #[test]
    fn error_text_redacts_embedded_password() {
        let leaked = "connection failed: FATAL: password=hunter2 for user triage";
        let summary = safe_error_summary(leaked);
        assert!(!summary.contains("hunter2"));
        assert!(summary.contains("password=redacted"));
        // Long messages stay bounded in the banner.
        let long = "x".repeat(400);
        assert!(safe_error_summary(&long).chars().count() <= 181);
    }

    #[test]
    fn startup_target_error_banner_is_non_modal() {
        let mut app = app();
        assert!(app.error_banner.is_some());
        assert!(
            app.error_banner
                .as_deref()
                .unwrap()
                .contains("target error")
        );
        assert_eq!(app.mode, Mode::Browser, "errors must not hijack the mode");
        assert_eq!(
            app.tables.len(),
            5,
            "fixture tables load despite the banner"
        );
        press(&mut app, KeyCode::Char('j'));
        assert!(app.error_banner.is_none(), "any key acknowledges");
        assert_eq!(app.selection, 1, "the key keeps its normal meaning");
    }

    #[test]
    fn hotkeys_switch_views_and_tab_cycles() {
        let mut app = app();
        assert_eq!(app.view, View::Tables);
        press(&mut app, KeyCode::Char('2'));
        assert_eq!(app.view, View::Structure);
        assert!(app.status.contains("structure"));
        assert!(
            app.structure_view.is_some(),
            "fixture structure loads for the selected table"
        );
        press(&mut app, KeyCode::Tab);
        assert_eq!(app.view, View::Tables);
        press(&mut app, KeyCode::Char('1'));
        assert_eq!(app.view, View::Tables);
    }

    #[test]
    fn schema_prompt_switches_schema_and_refreshes_tables() {
        let mut app = app();
        assert_eq!(app.schema, "public");
        press(&mut app, KeyCode::Char('s'));
        assert_eq!(app.overlay, Overlay::SchemaPrompt);
        type_text(&mut app, "ana");
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::None);
        assert_eq!(app.schema, "analytics");
        assert_eq!(app.tables.len(), 2, "analytics schema has two tables");
        assert_eq!(app.tables[0].name, "daily_rollups");
        assert!(app.status.contains("schema set to analytics"));
        // Switch back through the prompt with no typed filter.
        press(&mut app, KeyCode::Char('s'));
        press(&mut app, KeyCode::Up);
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.schema, "public");
        assert_eq!(app.tables.len(), 5);
    }

    #[test]
    fn table_filter_reports_match_count() {
        let mut app = app();
        press(&mut app, KeyCode::Char('/'));
        assert_eq!(app.mode, Mode::Filter);
        type_text(&mut app, "pro");
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.mode, Mode::Browser);
        assert_eq!(app.filtered_tables().len(), 1);
        assert!(app.status.contains("(1 matches)"));
        press(&mut app, KeyCode::Char('/'));
        press(&mut app, KeyCode::Esc);
        assert!(app.filter.is_empty());
        assert!(app.status.contains("filter cleared"));
    }

    #[test]
    fn data_viewer_pages_with_offset_history() {
        let mut app = app();
        app.set_selected(2); // public.orders: 23 rows
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::DataViewer);
        assert_eq!(app.data_offset, 0);
        assert_eq!(app.data_rows.len(), 10);
        assert_eq!(app.data_total, Some(23));
        press(&mut app, KeyCode::Char('n'));
        assert_eq!(app.data_offset, PAGE_SIZE);
        assert_eq!(app.data_rows.len(), 10);
        assert_eq!(app.data_page_index, 2);
        press(&mut app, KeyCode::Char('n'));
        assert_eq!(app.data_offset, 2 * PAGE_SIZE);
        assert_eq!(app.data_rows.len(), 3, "final page is partial");
        assert_eq!(app.data_page_index, 3);
        press(&mut app, KeyCode::Char('n'));
        assert!(app.status.contains("end of table reached"));
        press(&mut app, KeyCode::Char('b'));
        assert_eq!(app.data_offset, PAGE_SIZE);
        press(&mut app, KeyCode::Char('b'));
        assert_eq!(app.data_offset, 0);
        assert_eq!(app.data_page_index, 1);
        press(&mut app, KeyCode::Char('b'));
        assert!(app.status.contains("no previous page"));
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn data_viewer_filter_reports_match_count() {
        let mut app = app();
        app.set_selected(2); // public.orders
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('/'));
        assert!(app.viewer_filtering);
        type_text(&mut app, "pending");
        press(&mut app, KeyCode::Enter);
        assert!(!app.viewer_filtering);
        let matches = app.filtered_data_rows().len();
        assert_eq!(matches, 3, "fixture orders page 1 has 3 pending rows");
        assert!(app.status.contains("3 match(es)"));
        // Clearing the filter restores the full page.
        press(&mut app, KeyCode::Char('/'));
        press(&mut app, KeyCode::Esc);
        assert!(app.data_filter.is_empty());
        assert_eq!(app.filtered_data_rows().len(), 10);
        assert!(app.status.contains("viewer filter cleared"));
    }

    #[test]
    fn row_detail_viewer_renders_typed_cells() {
        let mut app = app();
        app.set_selected(4); // public.products: jsonb/array/bool/uuid/null cells
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::DataViewer);
        let grid = app
            .data_rows
            .iter()
            .map(|row| {
                row.values
                    .iter()
                    .map(render_cell)
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(grid.contains("{hardware,sensor}"), "array literal renders");
        assert!(
            grid.contains("\"color\":\"graphite\""),
            "jsonb document renders as stored"
        );
        assert!(grid.contains("(null)"), "NULL renders visibly");
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::RowDetail);
        let detail = app.detail_lines.range(0, app.detail_lines.len()).join("\n");
        assert!(detail.contains("tags (text[]): {hardware,sensor}"));
        assert!(detail.contains("attributes (jsonb): {"));
        assert!(detail.contains("last_review (uuid): 6f9619ff"));
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::DataViewer);
        // A NULL cell renders as (null) in the detail viewer too.
        app.set_data_selection(1); // WIDG-0043 has a NULL last_review
        press(&mut app, KeyCode::Enter);
        let detail = app.detail_lines.range(0, app.detail_lines.len()).join("\n");
        assert!(detail.contains("last_review (uuid): (null)"));
        press(&mut app, KeyCode::Esc);
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn structure_view_shows_columns_indexes_fks() {
        let mut app = app();
        app.set_selected(2); // public.orders
        press(&mut app, KeyCode::Char('2'));
        assert_eq!(app.view, View::Structure);
        let view = app.structure_view.as_ref().expect("structure loaded");
        assert_eq!(view.columns.len(), 5);
        assert_eq!(view.indexes.len(), 3);
        assert_eq!(view.foreign_keys.len(), 1);
        assert!(view.indexes.iter().any(|idx| idx.name == "orders_pkey"));
        assert!(
            view.indexes
                .iter()
                .any(|idx| idx.name == "orders_created_at_idx" && idx.index_type == "btree")
        );
        let fk = &view.foreign_keys[0];
        assert_eq!(fk.referenced_table, "customers");
        assert_eq!(fk.on_delete, "RESTRICT");
        press(&mut app, KeyCode::Char('1'));
        assert_eq!(app.view, View::Tables);
        // The error table rejects the structure read before the fixture
        // structure lookup, proving target errors surface in this view too.
        app.set_selected(3); // public.payments is the error table
        press(&mut app, KeyCode::Char('2'));
        assert!(app.error_banner.is_some());
        assert!(
            app.error_banner
                .as_deref()
                .unwrap()
                .contains("permission denied for table public.payments")
        );
        press(&mut app, KeyCode::Char('j'));
        assert!(app.error_banner.is_none(), "banner stays non-modal");
    }

    #[test]
    fn empty_table_shows_empty_state() {
        let mut app = app();
        app.set_selected(0); // public.audit_events: 0 rows
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::DataViewer);
        assert!(app.data_rows.is_empty());
        assert_eq!(app.data_total, Some(0));
        press(&mut app, KeyCode::Char('n'));
        assert!(app.status.contains("end of table reached"));
        press(&mut app, KeyCode::Enter);
        assert!(app.status.contains("no row selected"));
        assert_eq!(app.overlay, Overlay::DataViewer);
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn erroring_table_shows_banner_and_stays_responsive() {
        let mut app = app();
        app.set_selected(3); // public.payments is the error table
        press(&mut app, KeyCode::Enter);
        assert!(app.error_banner.is_some());
        assert!(
            app.error_banner
                .as_deref()
                .unwrap()
                .contains("target error: permission denied for table public.payments")
        );
        assert_eq!(app.overlay, Overlay::None, "the viewer stays closed");
        assert_eq!(app.view, View::Tables);
        assert_eq!(app.mode, Mode::Browser);
        press(&mut app, KeyCode::Char('j'));
        assert!(app.error_banner.is_none(), "banner stays non-modal");
        assert_eq!(app.selection, 4, "the key keeps its normal meaning");
        // The TUI is still fully usable: open another table afterwards.
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::DataViewer);
        assert_eq!(app.data_table, "products");
        press(&mut app, KeyCode::Esc);
    }

    #[test]
    fn readonly_launch_reports_and_offers_no_mutations() {
        let mut app = PostgresTuiApp::new(fixture_launch(true)).unwrap();
        assert!(app.readonly);
        app.set_selected(2);
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::DataViewer);
        // The pilot binds no mutation keys: 'd'/'t'/'R' restate the hints and
        // never stage a plan or send a mutating command.
        press(&mut app, KeyCode::Char('d'));
        assert!(
            app.status.contains("viewer:"),
            "the viewer has no mutation bindings"
        );
        press(&mut app, KeyCode::Esc);
        press(&mut app, KeyCode::Char('1'));
        press(&mut app, KeyCode::Char('d'));
        assert!(
            app.status.contains(TABLES_HINTS),
            "unbound keys restate the hints"
        );
        assert!(
            app.tables.iter().any(|meta| meta.name == "orders"),
            "readonly launches leave the catalog intact"
        );
        let rendered = serde_json::to_string(&preflight_value(&fixture_launch(true))).unwrap();
        assert!(rendered.contains("\"readonly\":true"));
        assert!(rendered.contains("read_only"));
    }

    #[test]
    fn quit_with_active_fetch_requires_confirmation() {
        {
            let mut app = app();
            app.data_pending = true;
            press(&mut app, KeyCode::Char('q'));
            assert_eq!(app.overlay, Overlay::ConfirmQuit);
            assert!(!app.should_quit);
            press(&mut app, KeyCode::Esc);
            assert_eq!(app.overlay, Overlay::None);
            assert!(!app.should_quit);
            press(&mut app, KeyCode::Char('q'));
            press(&mut app, KeyCode::Char('y'));
            assert!(app.should_quit);
        }
        // With no active fetch, q quits directly.
        let mut app = app();
        press(&mut app, KeyCode::Char('q'));
        assert!(app.should_quit);
    }

    #[test]
    fn page_keys_move_selection() {
        let mut app = app();
        press(&mut app, KeyCode::PageDown);
        assert_eq!(app.selected(), 5.min(app.filtered_tables().len() - 1));
        press(&mut app, KeyCode::PageUp);
        assert_eq!(app.selected(), 0);
    }

    #[test]
    fn typed_fixture_cells_match_service_shapes() {
        assert_eq!(fixture_cell(&json!(42), "integer"), CellValue::Int(42));
        assert_eq!(
            fixture_cell(&json!(12.5), "numeric"),
            CellValue::Float(12.5)
        );
        assert_eq!(fixture_cell(&json!(true), "boolean"), CellValue::Bool(true));
        assert_eq!(fixture_cell(&json!(null), "text"), CellValue::Null);
        assert_eq!(
            fixture_cell(&json!("2026-11-03 08:15:00"), "timestamp without time zone"),
            CellValue::DateTime("2026-11-03 08:15:00".into())
        );
        assert_eq!(
            fixture_cell(&json!({"a":1}), "jsonb"),
            CellValue::Json("{\"a\":1}".into())
        );
        assert_eq!(
            fixture_cell(&json!(["a", "b"]), "text[]"),
            CellValue::Text("{a,b}".into())
        );
        assert_eq!(
            fixture_cell(&json!("6f9619ff-8b86-d011-b42d-00c04fc964ff"), "uuid"),
            CellValue::Uuid("6f9619ff-8b86-d011-b42d-00c04fc964ff".into())
        );
        assert_eq!(render_cell(&CellValue::Null), "(null)");
    }
}
