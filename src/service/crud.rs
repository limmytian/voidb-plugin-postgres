//! CRUD inner service.
//!
//! Handles `CrudCommand` variants: single query execution, multi-query
//! execution, saving pending changes (UPDATE/DELETE), and inserting rows.
//!
//! All dynamically constructed SQL uses `quote_ident()` for identifiers
//! and `escape_pg_value()` for values (threat T-03-02).

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use tokio_postgres::Client;
use voidb_core::{CellValue, ColumnInfo, Row};

use super::events::StatementResult;
use super::value_convert::{build_pk_where, convert_row, escape_pg_value, extract_columns, quote_ident};

/// Inner service for CRUD operations.
///
/// Stateless -- receives a `&Client` reference per call from the
/// background task that owns the connection.
pub struct CrudService;

impl CrudService {
    /// Execute a single SQL query and return columns + rows.
    pub async fn execute_query(client: &Client, sql: &str) -> Result<(Vec<ColumnInfo>, Vec<Row>)> {
        let rows = client.query(sql, &[]).await?;

        if rows.is_empty() {
            // Try to get column info from a statement prepare
            let stmt = client.prepare(sql).await?;
            let columns: Vec<ColumnInfo> = stmt
                .columns()
                .iter()
                .map(|col| ColumnInfo {
                    name: col.name().to_string(),
                    data_type: col.type_().name().to_string(),
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    max_length: None,
                    extra: String::new(),
                })
                .collect();
            return Ok((columns, Vec::new()));
        }

        let columns = extract_columns(&rows[0]);
        let col_count = columns.len();
        let data_rows: Vec<Row> = rows.iter().map(|r| convert_row(r, col_count)).collect();

        Ok((columns, data_rows))
    }

    /// Execute multiple SQL statements sequentially on a single connection.
    ///
    /// Returns one `StatementResult` per statement. Stops on first error.
    pub async fn execute_multi_query(
        client: &Client,
        sql: &str,
    ) -> Result<Vec<StatementResult>> {
        let stmts = voidb_core::sql_split::split_statements(sql);
        if stmts.is_empty() {
            return Ok(vec![]);
        }

        let mut results = Vec::new();
        for stmt in stmts {
            let trimmed = stmt.trim_start().to_uppercase();
            let is_select = trimmed.starts_with("SELECT")
                || trimmed.starts_with("SHOW")
                || trimmed.starts_with("EXPLAIN")
                || trimmed.starts_with("WITH")
                || trimmed.starts_with("TABLE")
                || trimmed.starts_with("VALUES");

            if is_select {
                match client.query(stmt, &[]).await {
                    Ok(rows) if rows.is_empty() => {
                        results.push(StatementResult::Empty);
                    }
                    Ok(rows) => {
                        let columns = extract_columns(&rows[0]);
                        let col_count = columns.len();
                        let data_rows: Vec<Row> =
                            rows.iter().map(|r| convert_row(r, col_count)).collect();
                        results.push(StatementResult::Select {
                            columns,
                            rows: data_rows,
                        });
                    }
                    Err(e) => {
                        results.push(StatementResult::Error(e.to_string()));
                        break;
                    }
                }
            } else {
                match client.execute(stmt, &[]).await {
                    Ok(affected) => {
                        results.push(StatementResult::Affected(affected));
                    }
                    Err(e) => {
                        results.push(StatementResult::Error(e.to_string()));
                        break;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Save pending cell edits and row deletions.
    ///
    /// Generates UPDATE statements for modified cells and DELETE statements
    /// for deleted rows, then executes them sequentially.
    ///
    /// Returns total affected rows count.
    #[allow(clippy::too_many_arguments)]
    pub async fn save_changes(
        client: &Client,
        schema: &str,
        table: &str,
        columns: &[ColumnInfo],
        rows: &[Row],
        pk_cols: &[String],
        pending: &HashMap<(usize, usize), CellValue>,
        deleted_rows: &[usize],
    ) -> Result<u64> {
        if pending.is_empty() && deleted_rows.is_empty() {
            return Ok(0);
        }

        let qualified_table = format!("{}.{}", quote_ident(schema), quote_ident(table));
        let mut total_affected: u64 = 0;

        // Build UPDATE statements (skip rows that are also deleted)
        let deleted_set: HashSet<usize> = deleted_rows.iter().copied().collect();
        let mut row_changes: HashMap<usize, Vec<(usize, &CellValue)>> = HashMap::new();
        for (&(row, col), val) in pending {
            if !deleted_set.contains(&row) {
                row_changes.entry(row).or_default().push((col, val));
            }
        }

        for (row_idx, changes) in &row_changes {
            let where_clause = build_pk_where(columns, &rows[*row_idx], pk_cols)?;
            let set_parts: Vec<String> = changes
                .iter()
                .map(|(col, val)| {
                    format!("{} = {}", quote_ident(&columns[*col].name), escape_pg_value(val))
                })
                .collect();
            let sql = format!(
                "UPDATE {} SET {} WHERE {}",
                qualified_table,
                set_parts.join(", "),
                where_clause,
            );
            let affected = client.execute(&sql, &[]).await?;
            total_affected += affected;
        }

        // Build DELETE statements
        for &row_idx in deleted_rows {
            if row_idx < rows.len() {
                let where_clause = build_pk_where(columns, &rows[row_idx], pk_cols)?;
                let sql = format!("DELETE FROM {} WHERE {}", qualified_table, where_clause);
                let affected = client.execute(&sql, &[]).await?;
                total_affected += affected;
            }
        }

        Ok(total_affected)
    }

    /// Insert a new row into a table.
    ///
    /// Uses `INSERT ... RETURNING *` to get the full row back.
    /// Returns the new row if the RETURNING clause succeeds.
    pub async fn insert_row(
        client: &Client,
        schema: &str,
        table: &str,
        columns: &[String],
        values: &[CellValue],
    ) -> Result<Option<Row>> {
        let qualified_table = format!("{}.{}", quote_ident(schema), quote_ident(table));

        let col_names: Vec<String> = columns.iter().map(|c| quote_ident(c.as_str())).collect();
        let val_strs: Vec<String> = values.iter().map(escape_pg_value).collect();

        let sql = if col_names.is_empty() {
            format!("INSERT INTO {} DEFAULT VALUES RETURNING *", qualified_table)
        } else {
            format!(
                "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
                qualified_table,
                col_names.join(", "),
                val_strs.join(", "),
            )
        };

        let rows = client.query(&sql, &[]).await?;
        if let Some(row) = rows.first() {
            let col_count = row.columns().len();
            Ok(Some(convert_row(row, col_count)))
        } else {
            Ok(None)
        }
    }
}
