use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};
use voidb_core::database::types::QueryResult;
use voidb_core::formatters::{cell_value_to_csv, cell_value_to_json, print_table};
use voidb_core::plugin::cli::{CliContext, CliPlugin};
use voidb_core::VoidbError;

use crate::config::PostgresConfig;
use crate::service::{PostgresService, StatementResult};

pub struct PostgresCliPlugin;

pub fn create_postgres_cli_plugin() -> Box<dyn CliPlugin> {
    Box::new(PostgresCliPlugin)
}

#[async_trait]
impl CliPlugin for PostgresCliPlugin {
    fn plugin_id(&self) -> &str {
        "postgres"
    }

    fn name(&self) -> &str {
        "PostgreSQL"
    }

    fn commands(&self) -> Vec<Command> {
        let conn_arg = Arg::new("connection")
            .short('c')
            .long("connection")
            .required(true)
            .help("Connection name");

        vec![
            Command::new("query")
                .about("Execute a SQL query")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("schema")
                        .short('s')
                        .long("schema")
                        .help("Schema name (default: public)"),
                )
                .arg(
                    Arg::new("format")
                        .short('f')
                        .long("format")
                        .value_parser(["table", "json", "csv"])
                        .default_value("table")
                        .help("Output format"),
                )
                .arg(Arg::new("sql").required(true).help("SQL query to execute")),
            Command::new("databases")
                .about("List databases")
                .arg(conn_arg.clone()),
            Command::new("tables")
                .about("List tables in a schema")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("schema")
                        .default_value("public")
                        .help("Schema name"),
                ),
            Command::new("describe")
                .about("Describe table schema")
                .arg(conn_arg)
                .arg(
                    Arg::new("schema")
                        .short('s')
                        .long("schema")
                        .default_value("public")
                        .help("Schema name"),
                )
                .arg(Arg::new("table").required(true).help("Table name")),
        ]
    }

    async fn execute(
        &self,
        command: &str,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        match command {
            "query" => self.handle_query(matches, ctx).await,
            "databases" => self.handle_databases(matches, ctx).await,
            "tables" => self.handle_tables(matches, ctx).await,
            "describe" => self.handle_describe(matches, ctx).await,
            _ => Err(VoidbError::Plugin(format!("Unknown command: {}", command))),
        }
    }
}

impl PostgresCliPlugin {
    async fn create_service(
        &self,
        conn_name: &str,
        ctx: &CliContext,
    ) -> Result<PostgresService, VoidbError> {
        let config = ctx.find_connection(conn_name).ok_or_else(|| {
            VoidbError::Plugin(format!("Connection '{}' not found", conn_name))
        })?;

        let pid = config.effective_plugin_id();
        if pid != "postgres" && pid != "postgresql" {
            return Err(VoidbError::Plugin(format!(
                "Connection '{}' is not a PostgreSQL connection (plugin: {})",
                conn_name, pid
            )));
        }

        let pg_config: PostgresConfig = serde_json::from_value(
            config
                .plugin_config
                .as_ref()
                .ok_or_else(|| VoidbError::Config("Missing plugin_config".into()))?
                .clone(),
        )
        .map_err(|e| VoidbError::Config(e.to_string()))?;

        PostgresService::new_direct(&pg_config)
            .await
            .map_err(VoidbError::Connection)
    }

    async fn handle_query(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let sql = matches.get_one::<String>("sql").unwrap();
        let _schema = matches.get_one::<String>("schema").map(|s| s.as_str());
        let format = matches
            .get_one::<String>("format")
            .map(|s| s.as_str())
            .unwrap_or("table");

        let service = self.create_service(conn_name, ctx).await?;
        // Postgres execute_multi_query doesn't take schema — use SET search_path if needed
        let full_sql = if let Some(schema) = _schema {
            format!("SET search_path TO {}; {}", schema, sql)
        } else {
            sql.to_string()
        };
        let results = service
            .execute_query(&full_sql)
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        for stmt_result in &results {
            match stmt_result {
                StatementResult::Select { columns, rows } => {
                    let result = QueryResult {
                        columns: columns.clone(),
                        rows: rows.clone(),
                        execution_time: std::time::Duration::ZERO,
                        rows_affected: None,
                    };

                    match format {
                        "json" => {
                            let json_rows: Vec<serde_json::Value> = result
                                .rows
                                .iter()
                                .map(|row| {
                                    let mut obj = serde_json::Map::new();
                                    for (i, col) in result.columns.iter().enumerate() {
                                        let val = row
                                            .values
                                            .get(i)
                                            .map(cell_value_to_json)
                                            .unwrap_or(serde_json::Value::Null);
                                        obj.insert(col.name.clone(), val);
                                    }
                                    serde_json::Value::Object(obj)
                                })
                                .collect();
                            println!(
                                "{}",
                                serde_json::to_string_pretty(&json_rows)
                                    .map_err(|e| VoidbError::Plugin(e.to_string()))?
                            );
                        }
                        "csv" => {
                            let header: Vec<&str> =
                                result.columns.iter().map(|c| c.name.as_str()).collect();
                            println!("{}", header.join(","));
                            for row in &result.rows {
                                let values: Vec<String> =
                                    row.values.iter().map(cell_value_to_csv).collect();
                                println!("{}", values.join(","));
                            }
                        }
                        _ => {
                            print_table(&result);
                        }
                    }
                    eprintln!("({} rows)", rows.len());
                }
                StatementResult::Affected(n) => {
                    eprintln!("{} rows affected", n);
                }
                StatementResult::Empty => {
                    eprintln!("(empty result)");
                }
                StatementResult::Error(e) => {
                    return Err(VoidbError::Query(e.clone()));
                }
            }
        }

        Ok(())
    }

    async fn handle_databases(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let service = self.create_service(conn_name, ctx).await?;
        let databases = service
            .list_databases()
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        for db in databases {
            println!("{}", db);
        }
        Ok(())
    }

    async fn handle_tables(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let schema = matches.get_one::<String>("schema").unwrap();
        let service = self.create_service(conn_name, ctx).await?;
        let tables = service
            .list_tables(schema)
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        println!("{:<40} {:<15} {:>10}", "NAME", "TYPE", "ROWS");
        println!("{}", "-".repeat(65));
        for table in tables {
            println!(
                "{:<40} {:<15} {:>10}",
                table.name,
                table.table_type,
                table
                    .row_estimate
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            );
        }
        Ok(())
    }

    async fn handle_describe(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let schema = matches.get_one::<String>("schema").unwrap();
        let table = matches.get_one::<String>("table").unwrap();
        let service = self.create_service(conn_name, ctx).await?;
        let (columns, indexes, foreign_keys) = service
            .describe_table(schema, table)
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        println!(
            "{:<30} {:<20} {:<8} {:<5} DEFAULT",
            "COLUMN", "TYPE", "NULL", "KEY"
        );
        println!("{}", "-".repeat(80));
        for col in &columns {
            println!(
                "{:<30} {:<20} {:<8} {:<5} {}",
                col.name,
                col.data_type,
                if col.nullable { "YES" } else { "NO" },
                if col.is_primary_key { "PRI" } else { "" },
                col.default_value.as_deref().unwrap_or(""),
            );
        }

        if !indexes.is_empty() {
            println!("\nIndexes:");
            for idx in &indexes {
                println!(
                    "  {} ({}) [{}]",
                    idx.name,
                    idx.columns.join(", "),
                    if idx.is_unique {
                        "UNIQUE"
                    } else {
                        &idx.index_type
                    },
                );
            }
        }

        if !foreign_keys.is_empty() {
            println!("\nForeign Keys:");
            for fk in &foreign_keys {
                println!(
                    "  {} ({}) -> {} ({})",
                    fk.name,
                    fk.columns.join(", "),
                    fk.referenced_table,
                    fk.referenced_columns.join(", "),
                );
            }
        }

        Ok(())
    }
}
