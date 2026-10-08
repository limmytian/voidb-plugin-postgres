//! Schema introspection inner service.
//!
//! Handles `SchemaCommand` variants: listing schemas, tables, columns,
//! indexes, foreign keys, DDL generation, and combined structure loading.
//!
//! All SQL queries use parameterized `$1`, `$2` notation where possible
//! to prevent SQL injection (threat T-03-02). Identifier quoting uses
//! `quote_ident()` from value_convert for dynamic SQL.

use std::collections::HashMap;

use anyhow::Result;
use tokio_postgres::Client;
use voidb_core::ColumnInfo;
use voidb_core::database::types::ForeignKeyInfo;

use super::events::{IndexInfo, TableMeta};
use super::value_convert::quote_ident;

/// Inner service for schema introspection.
///
/// Stateless -- receives a `&Client` reference per call from the
/// background task that owns the connection.
pub struct SchemaService;

impl SchemaService {
    /// List all user schemas in the database.
    ///
    /// Excludes system schemas: pg_catalog, information_schema, pg_toast.
    pub async fn list_schemas(client: &Client) -> Result<Vec<String>> {
        let rows = client
            .query(
                "SELECT schema_name FROM information_schema.schemata \
                 WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
                 ORDER BY schema_name",
                &[],
            )
            .await?;

        Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
    }

    /// List tables in a schema with metadata.
    ///
    /// Returns table name, type, and approximate row count from pg_class.
    pub async fn list_tables(client: &Client, schema: &str) -> Result<Vec<TableMeta>> {
        let rows = client
            .query(
                "SELECT t.table_name, t.table_type, \
                        (SELECT reltuples::bigint FROM pg_class c \
                         JOIN pg_namespace n ON n.oid = c.relnamespace \
                         WHERE c.relname = t.table_name AND n.nspname = t.table_schema) as row_est \
                 FROM information_schema.tables t \
                 WHERE t.table_schema = $1 \
                 ORDER BY t.table_name",
                &[&schema],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|r| TableMeta {
                name: r.get::<_, String>(0),
                table_type: r.get::<_, String>(1),
                row_estimate: r.try_get::<_, Option<i64>>(2).ok().flatten(),
            })
            .collect())
    }

    /// List columns for a specific table.
    ///
    /// Joins with pg_constraint to detect primary key columns.
    pub async fn list_columns(
        client: &Client,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>> {
        let rows = client
            .query(
                "SELECT c.column_name, c.data_type, c.is_nullable, c.column_default, \
                        c.character_maximum_length, \
                        CASE WHEN pk.column_name IS NOT NULL THEN 'PRI' ELSE '' END as col_key \
                 FROM information_schema.columns c \
                 LEFT JOIN ( \
                     SELECT ku.column_name \
                     FROM information_schema.table_constraints tc \
                     JOIN information_schema.key_column_usage ku \
                         ON tc.constraint_name = ku.constraint_name \
                         AND tc.table_schema = ku.table_schema \
                     WHERE tc.constraint_type = 'PRIMARY KEY' \
                       AND tc.table_schema = $1 \
                       AND tc.table_name = $2 \
                 ) pk ON c.column_name = pk.column_name \
                 WHERE c.table_schema = $1 AND c.table_name = $2 \
                 ORDER BY c.ordinal_position",
                &[&schema, &table],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|r| {
                let nullable_str: String = r.get(2);
                let key: String = r.get(5);
                ColumnInfo {
                    name: r.get::<_, String>(0),
                    data_type: r.get::<_, String>(1),
                    nullable: nullable_str == "YES",
                    is_primary_key: key == "PRI",
                    default_value: r.try_get::<_, Option<String>>(3).ok().flatten(),
                    max_length: r
                        .try_get::<_, Option<i32>>(4)
                        .ok()
                        .flatten()
                        .map(|v| v as u64),
                    extra: String::new(),
                }
            })
            .collect())
    }

    /// List indexes for a specific table.
    ///
    /// Queries pg_index, pg_class, pg_namespace, pg_am, and pg_attribute
    /// to get index names, columns, uniqueness, and type.
    pub async fn list_indexes(
        client: &Client,
        schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>> {
        let rows = client
            .query(
                "SELECT i.relname as index_name, \
                        a.attname as column_name, \
                        ix.indisunique, \
                        am.amname as index_type \
                 FROM pg_index ix \
                 JOIN pg_class t ON t.oid = ix.indrelid \
                 JOIN pg_class i ON i.oid = ix.indexrelid \
                 JOIN pg_namespace n ON n.oid = t.relnamespace \
                 JOIN pg_am am ON am.oid = i.relam \
                 JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) \
                 WHERE n.nspname = $1 AND t.relname = $2 \
                 ORDER BY i.relname, a.attnum",
                &[&schema, &table],
            )
            .await?;

        let mut index_map: HashMap<String, IndexInfo> = HashMap::new();

        for row in &rows {
            let idx_name: String = row.get(0);
            let col_name: String = row.get(1);
            let unique: bool = row.get(2);
            let idx_type: String = row.get(3);

            index_map
                .entry(idx_name.clone())
                .or_insert_with(|| IndexInfo {
                    name: idx_name,
                    columns: Vec::new(),
                    is_unique: unique,
                    index_type: idx_type,
                })
                .columns
                .push(col_name);
        }

        Ok(index_map.into_values().collect())
    }

    /// List foreign keys for a specific table.
    pub async fn list_foreign_keys(
        client: &Client,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let rows = client
            .query(
                "SELECT tc.constraint_name, \
                        kcu.column_name, \
                        ccu.table_name AS referenced_table, \
                        ccu.column_name AS referenced_column, \
                        rc.update_rule, \
                        rc.delete_rule \
                 FROM information_schema.table_constraints tc \
                 JOIN information_schema.key_column_usage kcu \
                     ON tc.constraint_name = kcu.constraint_name \
                     AND tc.table_schema = kcu.table_schema \
                 JOIN information_schema.constraint_column_usage ccu \
                     ON tc.constraint_name = ccu.constraint_name \
                     AND tc.table_schema = ccu.table_schema \
                 JOIN information_schema.referential_constraints rc \
                     ON tc.constraint_name = rc.constraint_name \
                     AND tc.table_schema = rc.constraint_schema \
                 WHERE tc.constraint_type = 'FOREIGN KEY' \
                   AND tc.table_schema = $1 \
                   AND tc.table_name = $2 \
                 ORDER BY tc.constraint_name, kcu.ordinal_position",
                &[&schema, &table],
            )
            .await?;

        let mut fk_map: HashMap<String, ForeignKeyInfo> = HashMap::new();

        for row in &rows {
            let fk_name: String = row.get(0);
            let col_name: String = row.get(1);
            let ref_table: String = row.get(2);
            let ref_col: String = row.get(3);
            let update_rule: String = row.get(4);
            let delete_rule: String = row.get(5);

            let entry = fk_map.entry(fk_name.clone()).or_insert_with(|| ForeignKeyInfo {
                name: fk_name,
                columns: Vec::new(),
                referenced_table: ref_table,
                referenced_columns: Vec::new(),
                on_update: update_rule,
                on_delete: delete_rule,
            });
            entry.columns.push(col_name);
            entry.referenced_columns.push(ref_col);
        }

        Ok(fk_map.into_values().collect())
    }

    /// Generate CREATE TABLE DDL for a table.
    ///
    /// Builds DDL from column metadata, primary keys, and constraints.
    /// PostgreSQL does not have `SHOW CREATE TABLE`, so we generate it
    /// from information_schema data.
    pub async fn show_create_table(
        client: &Client,
        schema: &str,
        table: &str,
    ) -> Result<String> {
        let columns = Self::list_columns(client, schema, table).await?;

        let mut sql = format!(
            "CREATE TABLE {}.{} (\n",
            quote_ident(schema),
            quote_ident(table)
        );

        let mut col_defs = Vec::new();
        let mut pk_cols = Vec::new();

        for col in &columns {
            let mut def = format!("  {} {}", quote_ident(&col.name), col.data_type);
            if !col.nullable {
                def.push_str(" NOT NULL");
            }
            if let Some(default) = &col.default_value {
                // Skip nextval() defaults (serial/identity columns)
                if !default.starts_with("nextval(") {
                    def.push_str(&format!(" DEFAULT {}", default));
                }
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
        Ok(sql)
    }

    /// Load full structure info (columns + indexes + foreign keys).
    ///
    /// Combines three queries in a single service call to avoid the TUI needing
    /// to coordinate multiple async responses.
    pub async fn load_structure(
        client: &Client,
        schema: &str,
        table: &str,
    ) -> Result<(Vec<ColumnInfo>, Vec<IndexInfo>, Vec<ForeignKeyInfo>)> {
        let columns = Self::list_columns(client, schema, table).await?;
        let indexes = Self::list_indexes(client, schema, table).await?;
        let foreign_keys = Self::list_foreign_keys(client, schema, table).await?;
        Ok((columns, indexes, foreign_keys))
    }
}
