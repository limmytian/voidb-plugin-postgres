//! Data loading inner service.
//!
//! Handles `LoadCommand` variants: paginated table data loading,
//! row count queries, and primary key discovery.

use anyhow::Result;
use tokio_postgres::Client;
use voidb_core::{ColumnInfo, Row};

use super::value_convert::{convert_row, extract_columns, quote_ident};

/// Inner service for data loading operations.
///
/// Stateless -- receives a `&Client` reference per call from the
/// background task that owns the connection.
pub struct DataLoaderService;

impl DataLoaderService {
    /// Load a page of table data with total row count.
    ///
    /// Returns column metadata, rows for the given offset/limit, and
    /// the total row count (from COUNT(*)).
    pub async fn load_table_data(
        client: &Client,
        schema: &str,
        table: &str,
        page_size: usize,
        offset: usize,
    ) -> Result<(Vec<ColumnInfo>, Vec<Row>, Option<i64>)> {
        let qualified_table = format!("{}.{}", quote_ident(schema), quote_ident(table));

        // Load total count
        let count_sql = format!("SELECT COUNT(*) FROM {}", qualified_table);
        let count_row = client.query_one(&count_sql, &[]).await?;
        let total_count: i64 = count_row.get(0);

        // Load page data
        let query = format!(
            "SELECT * FROM {} LIMIT {} OFFSET {}",
            qualified_table, page_size, offset
        );

        let rows = client.query(&query, &[]).await?;

        if rows.is_empty() {
            // Still need column info even when page is empty
            let meta_sql = format!("SELECT * FROM {} LIMIT 0", qualified_table);
            let stmt = client.prepare(&meta_sql).await?;
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
            return Ok((columns, Vec::new(), Some(total_count)));
        }

        let columns = extract_columns(&rows[0]);
        let col_count = columns.len();
        let data_rows: Vec<Row> = rows.iter().map(|r| convert_row(r, col_count)).collect();

        Ok((columns, data_rows, Some(total_count)))
    }

    /// Load the total row count for a table.
    pub async fn load_row_count(
        client: &Client,
        schema: &str,
        table: &str,
    ) -> Result<i64> {
        let qualified_table = format!("{}.{}", quote_ident(schema), quote_ident(table));
        let sql = format!("SELECT COUNT(*) FROM {}", qualified_table);
        let row = client.query_one(&sql, &[]).await?;
        let count: i64 = row.get(0);
        Ok(count)
    }

    /// Load primary key column names for a table.
    ///
    /// Queries pg_constraint + pg_attribute for the primary key column names.
    pub async fn load_primary_keys(
        client: &Client,
        schema: &str,
        table: &str,
    ) -> Result<Vec<String>> {
        let rows = client
            .query(
                "SELECT a.attname \
                 FROM pg_constraint c \
                 JOIN pg_class t ON t.oid = c.conrelid \
                 JOIN pg_namespace n ON n.oid = t.relnamespace \
                 JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey) \
                 WHERE c.contype = 'p' \
                   AND n.nspname = $1 \
                   AND t.relname = $2 \
                 ORDER BY array_position(c.conkey, a.attnum)",
                &[&schema, &table],
            )
            .await?;

        Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
    }
}
